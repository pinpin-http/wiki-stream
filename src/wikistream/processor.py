"""Consommateur : topic -> sink Parquet -> fenêtrage -> détecteurs -> journaux.

Ordre des opérations pour chaque lot (c'est lui qui fixe les garanties) :

    poll -> écriture Parquet durable (dédoublonnée) -> commit des offsets
         -> fenêtrage et détection sur les seuls enregistrements nouveaux

Un crash avant le commit fait rejouer le lot ; le sink écarte ce qui est déjà
écrit, et ces doublons ne repassent pas non plus dans les agrégats.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .aggregates import PageActivity, TopPages, WikiStats
from .anomalies import Alert, PageSpikeDetector, RobustEWMADetector, label_wiki
from .broker import Consumer
from .config import Settings
from .events import EDIT_TYPES, decode, page_key
from .metrics import LatencyRecorder, MetricsLog, RateMeter, rss_mb
from .sink import ParquetSink
from .sketch import CountMinSketch, SpaceSaving
from .windowing import LateEvent, Pipeline, SlidingWindows, TumblingWindows, WatermarkTracker, WindowOperator, WindowResult

log = logging.getLogger(__name__)


def _is_edit(e: dict[str, Any]) -> bool:
    return e.get("type") in EDIT_TYPES


def _is_human_article_edit(e: dict[str, Any]) -> bool:
    return e.get("type") in EDIT_TYPES and not e.get("bot") and e.get("ns") == 0


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._f = open(path, "a", encoding="utf-8")

    def write(self, obj: dict[str, Any]) -> None:
        self._f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def flush(self) -> None:
        self._f.flush()

    def close(self) -> None:
        self._f.close()


class Processor:
    def __init__(
        self,
        consumer: Consumer,
        sink: ParquetSink,
        settings: Settings,
        *,
        metrics: MetricsLog | None = None,
        stop: threading.Event | None = None,
        clock: Callable[[], float] = time.time,
        report_every: float = 10.0,
    ) -> None:
        self.consumer = consumer
        self.sink = sink
        self.s = settings
        self.metrics = metrics
        self.stop = stop or threading.Event()
        self.clock = clock
        self.report_every = report_every
        out = settings.out_dir
        out.mkdir(parents=True, exist_ok=True)
        self.out_dir = out

        self.w_wiki_1m = JsonlWriter(out / "windows_wiki_1m.jsonl")
        self.w_wiki_5m = JsonlWriter(out / "windows_wiki_5m.jsonl")
        self.w_top_1h = JsonlWriter(out / "top_pages_1h.jsonl")
        self.w_alerts = JsonlWriter(out / "alerts.jsonl")
        self.w_late = JsonlWriter(out / "late_events.jsonl")

        wm = WatermarkTracker(settings.max_out_of_orderness, settings.idle_timeout)
        self.pipeline = Pipeline(wm)
        lat = settings.allowed_lateness
        self.pipeline.add(
            WindowOperator("wiki_1m", TumblingWindows(60), WikiStats(), lambda e: e["wiki"],
                           allowed_lateness=lat, late_sink=self._on_late)
        )
        self.pipeline.add(
            WindowOperator("wiki_5m", SlidingWindows(300, 60), WikiStats(), lambda e: e["wiki"], allowed_lateness=lat)
        )
        self.pipeline.add(
            WindowOperator("page_10m", SlidingWindows(600, 60), PageActivity(), page_key, allowed_lateness=lat),
            accept=_is_human_article_edit,
        )
        self.pipeline.add(
            WindowOperator("top_pages_1h", TumblingWindows(3600), TopPages(500), lambda e: "*", allowed_lateness=lat),
            accept=_is_edit,
        )
        self.wiki_detector = RobustEWMADetector()
        self.page_detector = PageSpikeDetector()
        self.tracked_wikis: set[str] = set()
        self.servers: dict[str, str] = {}

        # top pages sur tout le run, mémoire bornée
        self.top_all = SpaceSaving(2000)
        self.cms = CountMinSketch.from_error(epsilon=5e-4, delta=0.01, seed=42, conservative=False)

        self.consumed = RateMeter()
        self.committed = RateMeter()
        self.latency = LatencyRecorder()
        self.buffer: list[dict[str, Any]] = []
        self.pending: dict[int, int] = {}
        self.buffer_since = 0.0
        self.duplicates = 0
        self.decode_errors = 0
        self.alerts: list[Alert] = []
        self.started = self.clock()
        self.max_wm_lag = 0.0

    # --- sorties latérales -------------------------------------------------
    def _on_late(self, le: LateEvent) -> None:
        e = le.event
        self.w_late.write(
            {"operator": le.operator, "id": e["id"], "wiki": e["wiki"], "title": e.get("title"),
             "type": e.get("type"), "ts": le.ts, "watermark": le.watermark, "lateness_s": round(le.lateness, 3),
             "delay_vs_recv_s": round(e["recv_ts"] - e["ts"], 3)}
        )

    # --- boucle principale -------------------------------------------------
    def run(self, duration: float | None = None, drain: bool = True, idle_exit: float | None = None) -> dict[str, Any]:
        deadline = self.clock() + duration if duration else None
        last_report = self.clock()
        last_consumed = 0
        last_data = self.clock()
        while True:
            now = self.clock()
            if self.stop.is_set() or (deadline and now >= deadline):
                break
            got = self._poll(timeout=0.2)
            now = self.clock()
            if got:
                last_data = now
            elif idle_exit is not None and now - last_data >= idle_exit:
                break
            if self.buffer and (len(self.buffer) >= self.s.flush_max_records or now - self.buffer_since >= self.s.flush_interval):
                self.flush()
            elif not self.buffer:
                self._emit(self.pipeline.advance(now))
            if now - last_report >= self.report_every:
                n = self.consumed.total - last_consumed
                self._report(n / (now - last_report))
                last_report, last_consumed = now, self.consumed.total
        if drain:
            empty = 0
            while empty < 3:
                empty = 0 if self._poll(timeout=0.5) else empty + 1
                if len(self.buffer) >= self.s.flush_max_records:
                    self.flush()
        self.flush()
        return self.finish()

    def _poll(self, timeout: float) -> int:
        recs = self.consumer.poll_batch(500, timeout)
        if not recs:
            return 0
        now = self.clock()
        if not self.buffer:
            self.buffer_since = now
        for r in recs:
            try:
                e = decode(r.value)
            except (ValueError, UnicodeDecodeError):
                self.decode_errors += 1
                continue
            e["_partition"] = r.partition
            e["_offset"] = r.offset
            self.buffer.append(e)
            if r.offset + 1 > self.pending.get(r.partition, -1):
                self.pending[r.partition] = r.offset + 1
        self.consumed.mark(len(recs), now)
        return len(recs)

    def flush(self) -> None:
        if not self.buffer and not self.pending:
            return
        res = self.sink.write_batch(self.buffer)
        self.consumer.commit(self.pending)  # après l'écriture durable, jamais avant
        t = self.clock()
        self.duplicates += res.duplicates
        self.committed.mark(len(res.written), t)
        pipe = self.pipeline
        for e in res.written:
            if e.get("dt") is not None:
                self.latency.add("source_to_recv", e["recv_ts"] - e["dt"])
                self.latency.add("end_to_end", t - e["dt"])
            self.latency.add("recv_to_commit", t - e["recv_ts"])
            self.servers.setdefault(e["wiki"], e.get("server") or "")
            pipe.process(e, e["ts"], e["_partition"], t)
            if _is_edit(e):
                k = page_key(e)
                self.top_all.add(k)
                self.cms.add(k)
        self.buffer = []
        self.pending = {}
        self._emit(pipe.advance(t))

    # --- résultats de fenêtres -> sorties et détecteurs -----------------------
    def _emit(self, results: list[WindowResult]) -> None:
        if not results:
            return
        now = self.clock()
        wiki_1m_by_end: dict[float, dict[str, WindowResult]] = defaultdict(dict)
        for r in results:
            row = {"start": r.window.start, "end": r.window.end, "key": r.key, "update": r.is_update, **r.value}
            if r.operator == "wiki_1m":
                self.w_wiki_1m.write(row)
                if not r.is_update:
                    wiki_1m_by_end[r.window.end][r.key] = r
            elif r.operator == "wiki_5m":
                self.w_wiki_5m.write(row)
            elif r.operator == "top_pages_1h":
                self.w_top_1h.write(row)
            elif r.operator == "page_10m" and not r.is_update:
                a = self.page_detector.update(r.key, r.value, r.window.start, r.window.end)
                if a:
                    self._alert(a, now)
        for end in sorted(wiki_1m_by_end):
            got = wiki_1m_by_end[end]
            self.tracked_wikis.update(got)
            for wiki in sorted(self.tracked_wikis):
                r = got.get(wiki)
                value = r.value["human_edits"] if r else 0
                # un pic porté par une ou deux personnes (outil semi-automatique) n'est pas signalé
                ok = r is not None and r.value["human_users"] >= 3
                a = self.wiki_detector.update(wiki, value, end - 60, end, context_ok=ok)
                if a:
                    if r:
                        a.details = {k: r.value[k] for k in ("edits", "bot_share", "human_users", "new_pages", "bytes_added",
                                                             "bytes_removed", "top_pages", "top_users", "top3_users_share")}
                    a.label = label_wiki(a.details)
                    self._alert(a, now)
        lag = now - self.pipeline.watermarks.current
        if lag < 1e9:
            self.max_wm_lag = max(self.max_wm_lag, lag)

    def _alert(self, a: Alert, now: float) -> None:
        a.detected_at = now
        wiki = a.key.split(":", 1)[0]
        server = self.servers.get(wiki, "")
        d = a.to_dict()
        if a.kind == "page_spike" and server:
            title = a.key.split(":", 1)[1]
            d["url"] = f"https://{server}/wiki/{title.replace(' ', '_')}"
        self.alerts.append(a)
        self.w_alerts.write(d)
        self.w_alerts.flush()
        log.warning("ALERTE %s %s : %s (référence %.1f, score %.1f) %s", a.kind, a.key, a.value, a.baseline, a.score, a.label)

    # --- rapports --------------------------------------------------------------
    def _op_stats(self) -> dict[str, Any]:
        return {op.name: {**vars(op.stats), "open_windows": op.open_windows()} for op in self.pipeline.operators}

    def _report(self, rate: float) -> None:
        for w in (self.w_wiki_1m, self.w_wiki_5m, self.w_top_1h, self.w_late):
            w.flush()
        now = self.clock()
        wm = self.pipeline.watermarks.current
        if self.metrics:
            self.metrics.write(
                "processor",
                consumed=self.consumed.total,
                written=self.committed.total,
                rate=round(rate, 2),
                duplicates=self.duplicates,
                files=self.sink.files_written,
                lag=self.consumer.lag(),
                watermark=wm if wm > -1e18 else None,
                watermark_lag_s=round(now - wm, 2) if wm > -1e18 else None,
                operators=self._op_stats(),
                alerts=len(self.alerts),
                rss_mb=rss_mb(),
            )
        log.info("processeur : %d lus, %.1f évt/s, %d alertes, retard watermark %.1f s",
                 self.consumed.total, rate, len(self.alerts), now - wm if wm > -1e18 else -1)

    def finish(self) -> dict[str, Any]:
        for r in self.pipeline.operators[0].flush_open() + self.pipeline.operators[1].flush_open():
            row = {"start": r.window.start, "end": r.window.end, "key": r.key, "update": False, "partial": True, **r.value}
            (self.w_wiki_1m if r.operator == "wiki_1m" else self.w_wiki_5m).write(row)
        for w in (self.w_wiki_1m, self.w_wiki_5m, self.w_top_1h, self.w_alerts, self.w_late):
            w.flush()
        n = self.top_all.total
        top = self.top_all.top(100)
        sketch = {
            "n_edits": n,
            "space_saving": {"k": self.top_all.k, "error_bound": n / self.top_all.k},
            "count_min": {"width": self.cms.width, "depth": self.cms.depth, "epsilon": self.cms.epsilon,
                          "delta": self.cms.delta, "error_bound": self.cms.error_bound(),
                          "memory_bytes": self.cms.memory_bytes()},
            "top": [{"page": x, "ss_count": c, "ss_error": err, "cms_estimate": self.cms.estimate(x)} for x, c, err in top],
        }
        (self.out_dir / "sketch_top_pages.json").write_text(json.dumps(sketch, ensure_ascii=False, indent=1))
        summary = {
            "started": self.started,
            "ended": self.clock(),
            "consumed": self.consumed.total,
            "written": self.committed.total,
            "duplicates_skipped": self.duplicates,
            "decode_errors": self.decode_errors,
            "parquet_files": self.sink.files_written,
            "throughput_consumed": self.consumed.summary(),
            "throughput_committed": self.committed.summary(),
            "latency_s": self.latency.summary(),
            "operators": self._op_stats(),
            "alerts": {"total": len(self.alerts),
                       "wiki_spike": sum(a.kind == "wiki_spike" for a in self.alerts),
                       "page_spike": sum(a.kind == "page_spike" for a in self.alerts)},
            "watermark": {"max_out_of_orderness_s": self.s.max_out_of_orderness,
                          "allowed_lateness_s": self.s.allowed_lateness,
                          "max_watermark_lag_s": round(self.max_wm_lag, 2)},
            "rss_mb": rss_mb(),
        }
        (self.out_dir / "run_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1))
        if self.metrics:
            self.metrics.write("processor", final=True, summary=summary)
        return summary

    def close(self) -> None:
        for w in (self.w_wiki_1m, self.w_wiki_5m, self.w_top_1h, self.w_alerts, self.w_late):
            w.close()
        self.consumer.close()
