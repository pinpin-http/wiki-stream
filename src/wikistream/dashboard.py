"""Rapport : requêtes DuckDB sur le lac Parquet, figures PNG et tableau de bord HTML statique.

Tout est recalculé à partir de ce que le pipeline a écrit (lac, journaux JSONL,
résumé de run). Rien n'est saisi à la main.
"""

from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from .anomalies import RobustEWMADetector  # noqa: E402
from .config import Settings  # noqa: E402
from .sink import lake_glob  # noqa: E402

# Palette de référence (slots catégoriels 1 et 2 validés), encres et surface claires
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
BLUE = "#2a78d6"
ORANGE = "#eb6834"


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "axes.edgecolor": AXIS,
            "axes.linewidth": 0.8,
            "axes.labelcolor": INK2,
            "axes.titlecolor": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "xtick.labelcolor": INK2,
            "ytick.labelcolor": INK2,
            "font.size": 10,
            "font.family": "DejaVu Sans",
            "legend.frameon": False,
            "legend.labelcolor": INK2,
            "savefig.dpi": 150,
            "savefig.facecolor": SURFACE,
        }
    )


def _fmt_int(n: float) -> str:
    return f"{int(round(n)):,}".replace(",", " ")


def _hhmm(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # ligne tronquée par un arrêt brutal
    return out


class Report:
    def __init__(self, s: Settings, docs: Path) -> None:
        self.s = s
        self.docs = docs
        docs.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect()
        self.con.execute(
            f"create view rc as select * from read_parquet('{lake_glob(s.lake_dir)}', hive_partitioning=true)"
        )
        out = s.out_dir
        self.summary = json.loads((out / "run_summary.json").read_text()) if (out / "run_summary.json").exists() else {}
        self.alerts = _read_jsonl(out / "alerts.jsonl")
        self.late = _read_jsonl(out / "late_events.jsonl")
        self.wiki_1m = [r for r in _read_jsonl(out / "windows_wiki_1m.jsonl") if not r.get("partial") and not r.get("update")]
        self.sketch = json.loads((out / "sketch_top_pages.json").read_text()) if (out / "sketch_top_pages.json").exists() else {}
        self.r: dict[str, Any] = {}

    def q(self, sql: str) -> list[tuple]:
        return self.con.sql(sql).fetchall()

    # ------------------------------------------------------------------ chiffres
    def compute(self) -> None:
        r = self.r
        (n, n_ids, t0, t1, wikis) = self.q(
            "select count(*), count(distinct id), min(epoch(recv_time)), max(epoch(recv_time)), count(distinct wiki) from rc"
        )[0]
        r["events"] = n
        r["distinct_ids"] = n_ids
        r["duplicates_in_lake"] = n - n_ids
        r["wikis"] = wikis
        r["recv_span_s"] = t1 - t0
        r["start_utc"] = datetime.fromtimestamp(t0, tz=timezone.utc).isoformat(timespec="seconds")
        r["end_utc"] = datetime.fromtimestamp(t1, tz=timezone.utc).isoformat(timespec="seconds")
        r["by_type"] = dict(self.q("select type, count(*) from rc group by type order by 2 desc"))
        edits, bots, added, removed, new = self.q(
            "select count(*), sum(bot::int), sum(greatest(len_new - coalesce(len_old, 0), 0)),"
            " sum(greatest(coalesce(len_old, 0) - len_new, 0)), sum((type = 'new')::int)"
            " from rc where type in ('edit', 'new')"
        )[0]
        r["edits"] = edits
        r["bot_share_edits"] = round(bots / edits, 4) if edits else None
        r["bytes_added"] = int(added or 0)
        r["bytes_removed"] = int(removed or 0)
        r["new_pages"] = int(new or 0)

        # débit à la réception, par seconde (secondes vides comptées à zéro)
        per_s = self.q(
            "with s as (select floor(epoch(recv_time))::bigint sec, count(*) n from rc group by 1),"
            " g as (select range sec from range((select min(sec) from s), (select max(sec) from s) + 1))"
            " select g.sec, coalesce(s.n, 0) from g left join s using (sec) order by 1"
        )
        counts = [c for _, c in per_s]
        tens = [sum(counts[i : i + 10]) / 10 for i in range(0, len(counts) - 9, 10)]
        r["throughput"] = {
            "mean_per_s": round(sum(counts) / len(counts), 2),
            "peak_1s": max(counts),
            "peak_10s_avg": max(tens) if tens else None,
            "p50_per_s": sorted(counts)[len(counts) // 2],
        }
        self._per_s = per_s

        # latences calculées depuis le lac (write_time = début de l'écriture du lot)
        lat = self.q(
            "select quantile_cont(epoch(recv_time) - epoch(meta_dt), [0.5, 0.9, 0.99]),"
            " quantile_cont(epoch(write_time) - epoch(meta_dt), [0.5, 0.9, 0.99]),"
            " quantile_cont(epoch(recv_time) - epoch(event_time), [0.5, 0.9, 0.99, 0.999]),"
            " avg((epoch(recv_time) - epoch(event_time) > 10)::int)"
            " from rc where meta_dt is not null"
        )[0]
        r["lake_latency_s"] = {
            "source_to_recv": [round(x, 3) for x in lat[0]],
            "source_to_write": [round(x, 3) for x in lat[1]],
            "edit_to_recv": [round(x, 3) for x in lat[2]],
            "share_edit_to_recv_over_10s": round(lat[3], 5),
        }
        r["processor_latency_s"] = self.summary.get("latency_s", {})
        r["processor_throughput"] = self.summary.get("throughput_consumed", {})

        # fenêtrage et retards
        ops = self.summary.get("operators", {})
        r["late_events_wiki_1m"] = ops.get("wiki_1m", {}).get("late_events")
        r["late_window_drops"] = {k: v.get("late_window_drops") for k, v in ops.items()}
        r["watermark"] = self.summary.get("watermark", {})
        if self.late:
            ls = sorted(x["lateness_s"] for x in self.late)
            r["lateness_s"] = {"n": len(ls), "p50": ls[len(ls) // 2], "max": ls[-1]}

        # wikis
        dur_min = max(r["recv_span_s"] / 60, 1e-9)
        rows = self.q(
            "select wiki, count(*) n, avg(bot::int), sum((type = 'new')::int),"
            " sum(greatest(len_new - coalesce(len_old, 0), 0)), sum(greatest(coalesce(len_old, 0) - len_new, 0))"
            " from rc where type in ('edit', 'new') group by wiki order by n desc limit 15"
        )
        r["top_wikis"] = [
            {"wiki": w, "edits": n, "edits_per_min": round(n / dur_min, 1), "bot_share": round(b, 3),
             "new_pages": int(nw), "bytes_added": int(a or 0), "bytes_removed": int(rm or 0)}
            for w, n, b, nw, a, rm in rows
        ]

        # sketches vs comptage exact
        if self.sketch:
            exact = dict(
                self.q(
                    "select wiki || ':' || title, count(*) from rc where type in ('edit', 'new') group by 1 order by 2 desc limit 2000"
                )
            )
            n_edits = self.sketch["n_edits"]
            cms_bound = self.sketch["count_min"]["error_bound"]
            cmp = []
            for t in self.sketch["top"][:20]:
                f = exact.get(t["page"])
                if f is None:
                    f = self.con.execute(
                        "select count(*) from rc where type in ('edit','new') and wiki || ':' || title = ?", [t["page"]]
                    ).fetchone()[0]
                cmp.append({**t, "exact": f,
                            "ss_ok": t["ss_count"] - t["ss_error"] <= f <= t["ss_count"],
                            "cms_ok": f <= t["cms_estimate"] <= f + cms_bound})
            exact_top20 = list(exact)[:20]
            r["sketch"] = {
                "n_edits": n_edits,
                "space_saving_k": self.sketch["space_saving"]["k"],
                "space_saving_bound": round(self.sketch["space_saving"]["error_bound"], 2),
                "cms_width": self.sketch["count_min"]["width"],
                "cms_depth": self.sketch["count_min"]["depth"],
                "cms_bound": round(cms_bound, 2),
                "cms_memory_kib": self.sketch["count_min"]["memory_bytes"] // 1024,
                "max_cms_error_top20": max(c["cms_estimate"] - c["exact"] for c in cmp),
                "max_ss_error_top20": max(c["ss_count"] - c["exact"] for c in cmp),
                "all_bounds_hold": all(c["ss_ok"] and c["cms_ok"] for c in cmp),
                "top20_overlap_with_exact": len({c["page"] for c in cmp} & set(exact_top20)),
                "distinct_pages": self.q("select count(distinct wiki || ':' || title) from rc where type in ('edit','new')")[0][0],
                "comparison": cmp,
            }

        # alertes
        r["alerts"] = {
            "total": len(self.alerts),
            "wiki_spike": sum(a["kind"] == "wiki_spike" for a in self.alerts),
            "page_spike": sum(a["kind"] == "page_spike" for a in self.alerts),
            "by_label": _count(a.get("label", "") for a in self.alerts),
        }
        if self.alerts:
            dl = sorted(a["detected_at"] - a["window_end"] for a in self.alerts)
            r["alerts"]["detection_delay_after_window_end_s"] = {"p50": round(dl[len(dl) // 2], 1), "max": round(dl[-1], 1)}
        r["run_summary"] = {k: self.summary.get(k) for k in ("consumed", "written", "duplicates_skipped", "parquet_files", "rss_mb")}

    # ------------------------------------------------------------------ figures
    def fig_throughput(self) -> str:
        secs = [s for s, _ in self._per_s]
        counts = [c for _, c in self._per_s]
        t0 = secs[0]
        step = 10 if len(counts) >= 60 else 1  # petit jeu de données (fixture) : pas d'1 s
        xs, ys = [], []
        for i in range(0, len(counts) - step + 1, step):
            xs.append((secs[i] - t0) / 60)
            ys.append(sum(counts[i : i + step]) / step)
        fig, ax = plt.subplots(figsize=(8, 3.2))
        ax.plot(xs, ys, color=BLUE, lw=2, solid_joinstyle="round", solid_capstyle="round")
        ax.fill_between(xs, ys, color=BLUE, alpha=0.08, lw=0)
        mean = self.r["throughput"]["mean_per_s"]
        ax.axhline(mean, color=MUTED, lw=1)
        ax.text(xs[-1] * 0.01, max(ys) * 1.08, f"moyenne sur le run : {mean:.1f} évt/s (ligne grise)".replace(".", ","),
                va="center", ha="left", color=INK2, fontsize=9)
        imax = max(range(len(ys)), key=ys.__getitem__)
        ax.plot([xs[imax]], [ys[imax]], "o", ms=6, color=BLUE, mec=SURFACE, mew=2)
        ax.annotate(f"pic {ys[imax]:.0f}/s", (xs[imax], ys[imax]), xytext=(6, 4), textcoords="offset points", color=INK2, fontsize=9)
        ax.set_ylim(0, max(ys) * 1.18)
        ax.set_xlim(0, max(xs[-1], 1e-3))
        ax.set_xlabel(f"minutes depuis le début du run ({self.r['start_utc'][11:16]} UTC)")
        ax.set_ylabel("événements / s")
        ax.set_title(f"Débit reçu, moyenne par tranche de {step} s")
        return self._save(fig, "throughput.png")

    def fig_latency(self) -> str:
        rows = self.q(
            "select epoch(recv_time) - epoch(meta_dt), epoch(write_time) - epoch(meta_dt) from rc where meta_dt is not null"
        )
        a = sorted(x for x, _ in rows)
        b = sorted(y for _, y in rows)
        fig, ax = plt.subplots(figsize=(8, 3.2))
        for vals, color, name in ((a, BLUE, "émission → réception (SSE)"), (b, ORANGE, "émission → écriture Parquet")):
            n = len(vals)
            step = max(1, n // 2000)
            xs = vals[::step] + [vals[-1]]
            ys = [i / n for i in range(0, n, step)] + [1.0]
            ax.plot(xs, ys, color=color, lw=2, label=name)
            p99 = vals[int(0.99 * (n - 1))]
            ax.plot([p99], [0.99], "o", ms=6, color=color, mec=SURFACE, mew=2)
            ax.annotate(f"p99 {p99:.2f} s".replace(".", ","), (p99, 0.99), xytext=(6, -14), textcoords="offset points",
                        color=INK2, fontsize=9)
        ax.set_xscale("log")
        ax.set_xlim(left=0.05)
        ax.set_ylim(0, 1.02)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _pct(v, 0)))
        ax.set_xlabel("latence (s, échelle log)")
        ax.set_ylabel("part des événements")
        ax.set_title("Latence de bout en bout (fonction de répartition)")
        ax.legend(loc="lower right")
        return self._save(fig, "latency.png")

    def fig_wikis(self) -> str:
        top = self.r["top_wikis"][:12][::-1]
        names = [t["wiki"] for t in top]
        human = [t["edits_per_min"] * (1 - t["bot_share"]) for t in top]
        bot = [t["edits_per_min"] * t["bot_share"] for t in top]
        fig, ax = plt.subplots(figsize=(8, 4.2))
        y = range(len(top))
        ax.barh(y, human, height=0.55, color=BLUE, label="humains", edgecolor=SURFACE, linewidth=2)
        ax.barh(y, bot, left=human, height=0.55, color=ORANGE, label="bots", edgecolor=SURFACE, linewidth=2)
        for i, t in enumerate(top):
            ax.text(t["edits_per_min"], i, f"  {t['edits_per_min']:.0f}", va="center", color=INK2, fontsize=9)
        ax.set_yticks(list(y), names)
        ax.grid(axis="y", visible=False)
        ax.set_xlabel("modifications / min (edit + new), moyenne sur le run")
        ax.set_title("Wikis les plus actifs, part humains / bots")
        ax.legend(loc="lower right")
        ax.set_xlim(0, max(t["edits_per_min"] for t in top) * 1.12)
        return self._save(fig, "wikis.png")

    def fig_delay(self) -> str:
        rows = [x for (x,) in self.q("select epoch(recv_time) - epoch(event_time) from rc")]
        rows.sort()
        n = len(rows)
        fig, ax = plt.subplots(figsize=(8, 3.2))
        xs = [max(v, 0.01) for v in rows[:: max(1, n // 3000)]] + [max(rows[-1], 0.01)]
        ys = [1 - i / n for i in range(0, n, max(1, n // 3000))] + [1 / n]
        ax.plot(xs, ys, color=BLUE, lw=2)
        bound = self.s.max_out_of_orderness
        ax.axvline(bound, color=MUTED, lw=1)
        share = self.r["lake_latency_s"]["share_edit_to_recv_over_10s"]
        ax.text(bound, 0.5, f"  borne de désordre du watermark ({bound:.0f} s)\n  {_pct(share, 2)} des événements au-delà",
                color=INK2, fontsize=9, va="center")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("délai entre l'horodatage de la modification et la réception (s, log)")
        ax.set_ylabel("part des événements au-delà (log)")
        ax.set_title("Pourquoi un watermark : la queue des événements en retard")
        return self._save(fig, "delay_tail.png")

    def fig_wiki_alert(self) -> str | None:
        wa = [a for a in self.alerts if a["kind"] == "wiki_spike"]
        if not wa:
            return None
        a = max(wa, key=lambda x: x["score"])
        series = defaultdict(int)
        for r in self.wiki_1m:
            if r["key"] == a["key"]:
                series[r["end"]] = r["human_edits"]
        ends_all = sorted({r["end"] for r in self.wiki_1m})
        first = min(series)
        ends = [e for e in ends_all if e >= first]
        values = [series.get(e, 0) for e in ends]
        det = RobustEWMADetector()
        base = []
        for e, v in zip(ends, values):
            st = det.state.get(a["key"])
            base.append(st.ewma if st else v)
            det.update(a["key"], v, e - 60, e)
        xs = [(e - ends[0]) / 60 for e in ends]
        fig, ax = plt.subplots(figsize=(8, 3.2))
        ax.plot(xs, values, color=BLUE, lw=2, label="modifications humaines / min")
        ax.plot(xs, base, color=MUTED, lw=1.5, label="référence EWMA")
        ia = ends.index(a["window_end"]) if a["window_end"] in ends else None
        if ia is not None:
            ax.plot([xs[ia]], [values[ia]], "o", ms=7, color=ORANGE, mec=SURFACE, mew=2)
            ax.annotate(f"alerte : {values[ia]:.0f}/min (z = {a['score']:.1f})", (xs[ia], values[ia]),
                        xytext=(8, 0), textcoords="offset points", color=INK, fontsize=9, va="center")
        ax.set_ylim(0, max(values) * 1.2)
        ax.set_xlabel(f"minutes (fenêtres fixes d'1 min, temps d'événement, début {_hhmm(ends[0])} UTC)")
        ax.set_title(f"Pic détecté sur {a['key']}")
        ax.legend(loc="upper left")
        return self._save(fig, "alert_wiki.png")

    def fig_page_alert(self) -> str | None:
        pa = [a for a in self.alerts if a["kind"] == "page_spike"]
        if not pa:
            return None
        a = max(pa, key=lambda x: (x["details"]["users"], x["value"]))
        wiki, title = a["key"].split(":", 1)
        rows = self.con.execute(
            "select floor(epoch(event_time) / 60)::bigint * 60 m, count(*) from rc"
            " where wiki = ? and title = ? and type in ('edit','new') and not bot group by 1 order by 1",
            [wiki, title],
        ).fetchall()
        t0 = min(self.q("select floor(min(epoch(event_time)) / 60)::bigint * 60 from rc")[0][0], rows[0][0])
        t1 = self.q("select floor(max(epoch(event_time)) / 60)::bigint * 60 from rc")[0][0]
        counts = dict(rows)
        mins = list(range(int(t0), int(t1) + 60, 60))
        xs = [(m - t0) / 60 for m in mins]
        ys = [counts.get(m, 0) for m in mins]
        fig, ax = plt.subplots(figsize=(8, 3.2))
        ax.bar(xs, ys, width=0.8, color=BLUE, edgecolor=SURFACE, linewidth=0.5)
        xa = (a["window_end"] - t0) / 60
        ax.axvspan((a["window_start"] - t0) / 60, xa, color=ORANGE, alpha=0.10, lw=0)
        ax.axvline(xa, color=ORANGE, lw=1.5)
        ax.text(xa, max(ys) * 1.05, f" alerte : {a['value']:.0f} modifs, {a['details']['users']} contributeurs en 10 min",
                color=INK, fontsize=9, va="bottom")
        ax.set_ylim(0, max(ys) * 1.3)
        ax.set_xlim(-0.5, xs[-1] + 0.5)
        ax.set_xlabel(f"minutes depuis {_hhmm(t0)} UTC (temps d'événement)")
        ax.set_ylabel("modifications humaines / min")
        short = title if len(title) <= 60 else title[:57] + "…"
        ax.set_title(f"{wiki} : « {short} »")
        return self._save(fig, "alert_page.png")

    def _save(self, fig, name: str) -> str:
        fig.tight_layout()
        fig.savefig(self.docs / name)
        plt.close(fig)
        return name

    # ------------------------------------------------------------------ sorties
    def write_json(self) -> None:
        (self.docs / "results.json").write_text(json.dumps(self.r, ensure_ascii=False, indent=1, default=str))
        keep = [{k: a.get(k) for k in ("kind", "key", "label", "window_start", "window_end", "value", "baseline", "score",
                                         "detected_at", "url", "details")} for a in self.alerts]
        (self.docs / "alerts.json").write_text(json.dumps(keep, ensure_ascii=False, indent=1))

    def write_html(self, figures: list[tuple[str, str]]) -> None:
        r = self.r
        pl = r.get("processor_latency_s", {}).get("end_to_end", {})
        tiles = [
            ("événements traités", _fmt_int(r["events"]), f"{r['wikis']} wikis, {r['recv_span_s'] / 60:.0f} min"),
            ("débit moyen", f"{r['throughput']['mean_per_s']:.1f} évt/s", f"pic {r['throughput']['peak_1s']} évt/s sur 1 s"),
            ("latence p50 / p99", f"{pl.get('p50', float('nan')):.2f} / {pl.get('p99', float('nan')):.2f} s",
             "émission Wikimedia → commit"),
            ("alertes", str(r["alerts"]["total"]), f"{r['alerts']['wiki_spike']} wiki, {r['alerts']['page_spike']} page"),
            ("en retard", _fmt_int(r.get("late_events_wiki_1m") or 0), "hors fenêtre, comptés à part"),
            ("doublons dans le lac", str(r["duplicates_in_lake"]), f"{r['run_summary'].get('duplicates_skipped') or 0} écartés au sink"),
        ]
        e = html.escape
        tiles_html = "".join(
            f'<div class="tile"><div class="lbl">{e(a)}</div><div class="val">{e(b)}</div><div class="sub">{e(c)}</div></div>'
            for a, b, c in tiles
        )
        figs_html = "".join(
            f'<figure><img src="{e(f)}" alt="{e(cap)}" loading="lazy"><figcaption>{e(cap)}</figcaption></figure>'
            for f, cap in figures
        )
        wiki_rows = "".join(
            f"<tr><td>{e(t['wiki'])}</td><td>{_fmt_int(t['edits'])}</td><td>{t['edits_per_min']:.1f}</td>"
            f"<td>{_pct(t['bot_share'], 0)}</td><td>{_fmt_int(t['new_pages'])}</td>"
            f"<td>{_fmt_int(t['bytes_added'])}</td><td>{_fmt_int(t['bytes_removed'])}</td></tr>"
            for t in r["top_wikis"]
        )
        alert_rows = "".join(
            f"<tr><td>{_hhmm(a['window_end'])}</td><td>{e(a['kind'])}</td>"
            f"<td>{_link(a)}</td><td>{e(a.get('label', ''))}</td>"
            f"<td>{a['value']:.0f}</td><td>{a['baseline']:.1f}</td><td>{a['score']:.1f}</td>"
            f"<td>{e(_alert_detail(a))}</td></tr>"
            for a in sorted(self.alerts, key=lambda x: x["window_end"])
        )
        sk = r.get("sketch")
        sketch_rows = ""
        sketch_intro = ""
        if sk:
            sketch_intro = (
                f"<p>{_fmt_int(sk['n_edits'])} modifications sur {_fmt_int(sk['distinct_pages'])} pages distinctes. "
                f"Space-Saving à k = {sk['space_saving_k']} compteurs (erreur garantie ≤ N/k = {sk['space_saving_bound']:.1f}), "
                f"Count-Min {sk['cms_width']} × {sk['cms_depth']} ({sk['cms_memory_kib']} Kio, erreur ≤ εN = {sk['cms_bound']:.1f} "
                f"avec probabilité ≥ 99 %). Sur le top 20 : erreur max Space-Saving {sk['max_ss_error_top20']}, "
                f"Count-Min {sk['max_cms_error_top20']} ; bornes respectées : {'oui' if sk['all_bounds_hold'] else 'NON'} ; "
                f"{sk['top20_overlap_with_exact']}/20 pages en commun avec le top 20 exact.</p>"
            )
            sketch_rows = "".join(
                f"<tr><td>{e(c['page'])}</td><td>{c['exact']}</td><td>{c['ss_count']} (±{c['ss_error']})</td>"
                f"<td>{c['cms_estimate']}</td></tr>"
                for c in sk["comparison"]
            )
        lat = r["lake_latency_s"]
        page = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>wiki-stream : tableau de bord</title>
<style>
:root {{ --bg:#f9f9f7; --surface:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --muted:#898781; --line:#e1e0d9; --accent:#2a78d6; }}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{ --bg:#0d0d0d; --surface:#1a1a19; --ink:#f0efec; --ink2:#c3c2b7; --muted:#898781; --line:#2c2c2a; --accent:#3987e5; }} }}
:root[data-theme="dark"] {{ --bg:#0d0d0d; --surface:#1a1a19; --ink:#f0efec; --ink2:#c3c2b7; --muted:#898781; --line:#2c2c2a; --accent:#3987e5; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }}
main {{ max-width:1040px; margin:0 auto; padding:32px 16px 64px; }}
h1 {{ font-size:24px; margin:0 0 4px; }} h2 {{ font-size:18px; margin:40px 0 12px; }}
p.lead {{ color:var(--ink2); margin:0 0 24px; }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; }}
.tile {{ background:var(--surface); border:1px solid var(--line); border-radius:10px; padding:14px; }}
.lbl {{ color:var(--ink2); font-size:13px; }} .val {{ font-size:22px; font-weight:650; margin:2px 0; }} .sub {{ color:var(--muted); font-size:12px; }}
figure {{ margin:16px 0; background:#fcfcfb; border:1px solid var(--line); border-radius:10px; padding:8px; }}
figure img {{ width:100%; height:auto; display:block; }} figcaption {{ color:#52514e; font-size:13px; padding:4px 8px; }}
.scroll {{ overflow-x:auto; }}
table {{ border-collapse:collapse; width:100%; font-size:13px; font-variant-numeric:tabular-nums; background:var(--surface); }}
th, td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }}
th {{ color:var(--ink2); font-weight:600; }} a {{ color:var(--accent); }}
code {{ font-size:13px; }}
</style></head>
<body><main>
<h1>wiki-stream</h1>
<p class="lead">Run du {e(r['start_utc'][:10])}, {e(r['start_utc'][11:16])} → {e(r['end_utc'][11:16])} UTC, flux
<code>mediawiki.recentchange</code> de Wikimedia. Page générée par <code>make report</code> à partir du lac Parquet et des journaux du pipeline.</p>
<div class="tiles">{tiles_html}</div>
<h2>Figures</h2>
{figs_html}
<h2>Latences mesurées</h2>
<p>Depuis le lac (p50 / p90 / p99) : émission Wikimedia → réception {_triple(lat['source_to_recv'])} s,
émission → début d'écriture Parquet {_triple(lat['source_to_write'])} s. Horodatage de la modification → réception
{_triple(lat['edit_to_recv'][:3])} s, avec {_pct(lat['share_edit_to_recv_over_10s'], 2)} des événements au-delà de 10 s.</p>
<h2>Alertes ({r['alerts']['total']})</h2>
<div class="scroll"><table><thead><tr><th>fin de fenêtre (UTC)</th><th>type</th><th>clé</th><th>étiquette</th><th>valeur</th><th>référence</th><th>score</th><th>contexte</th></tr></thead>
<tbody>{alert_rows or '<tr><td colspan="8">aucune</td></tr>'}</tbody></table></div>
<h2>Wikis les plus actifs</h2>
<div class="scroll"><table><thead><tr><th>wiki</th><th>modifs</th><th>modifs/min</th><th>bots</th><th>nouvelles pages</th><th>octets ajoutés</th><th>octets retirés</th></tr></thead>
<tbody>{wiki_rows}</tbody></table></div>
<h2>Top pages : sketches contre comptage exact</h2>
{sketch_intro}
<div class="scroll"><table><thead><tr><th>page</th><th>exact (DuckDB)</th><th>Space-Saving</th><th>Count-Min</th></tr></thead>
<tbody>{sketch_rows}</tbody></table></div>
</main></body></html>
"""
        (self.docs / "dashboard.html").write_text(page, encoding="utf-8")

    def terminal(self) -> str:
        r = self.r
        pl = r.get("processor_latency_s", {})
        lines = [
            f"run {r['start_utc']} → {r['end_utc']} ({r['recv_span_s'] / 60:.1f} min)",
            f"événements dans le lac : {r['events']} (ids distincts {r['distinct_ids']}, doublons {r['duplicates_in_lake']})",
            f"débit : moyenne {r['throughput']['mean_per_s']} évt/s, pic 1 s {r['throughput']['peak_1s']}, pic 10 s {r['throughput']['peak_10s_avg']}",
        ]
        for k, v in pl.items():
            lines.append(f"latence {k} : p50 {v['p50']} s, p90 {v['p90']} s, p99 {v['p99']} s, max {v['max']} s")
        lines.append(f"événements en retard (wiki_1m) : {r.get('late_events_wiki_1m')}")
        lines.append(f"alertes : {r['alerts']['total']} ({r['alerts']['wiki_spike']} wiki, {r['alerts']['page_spike']} page)")
        for a in sorted(self.alerts, key=lambda x: -x["score"])[:8]:
            lines.append(f"  - {_hhmm(a['window_end'])} {a['kind']} {a['key']} : {a['value']:.0f} (réf {a['baseline']}) {a.get('label', '')}")
        if r.get("sketch"):
            sk = r["sketch"]
            lines.append(f"sketch : bornes respectées {sk['all_bounds_hold']}, erreur max CMS top20 {sk['max_cms_error_top20']} (borne {sk['cms_bound']})")
        return "\n".join(lines)


def _count(items) -> dict[str, int]:
    out: dict[str, int] = {}
    for x in items:
        out[x] = out.get(x, 0) + 1
    return out


def _pct(x: float, digits: int = 1) -> str:
    return f"{x * 100:.{digits}f} %".replace(".", ",")


def _triple(v) -> str:
    return " / ".join(f"{x:.2f}" for x in v).replace(".", ",")


def _link(a: dict[str, Any]) -> str:
    key = html.escape(a["key"])
    if a.get("url"):
        return f'<a href="{html.escape(a["url"])}">{key}</a>'
    return key


def _alert_detail(a: dict[str, Any]) -> str:
    d = a.get("details") or {}
    if a["kind"] == "page_spike":
        return f"{d.get('users')} contributeurs, {d.get('reverts')} révocations, {d.get('net_bytes')} octets nets"
    tops = ", ".join(t for t, _ in (d.get("top_pages") or [])[:3])
    return f"{d.get('human_users')} contributeurs humains ; pages : {tops}"


def build_report(s: Settings, docs: Path) -> dict[str, Any]:
    _style()
    rep = Report(s, docs)
    rep.compute()
    figures = [
        (rep.fig_throughput(), "Débit reçu depuis EventStreams (moyenne par tranche de 10 s)."),
        (rep.fig_latency(), "Latence mesurée depuis l'horodatage d'émission Wikimedia (meta.dt)."),
        (rep.fig_delay(), "Délai entre la modification et sa réception : la queue justifie la borne du watermark."),
        (rep.fig_wikis(), "Modifications par minute sur les wikis les plus actifs."),
    ]
    for f, cap in ((rep.fig_wiki_alert(), "Série minute d'un wiki et référence EWMA au moment de l'alerte."),
                   (rep.fig_page_alert(), "Modifications humaines par minute sur la page en alerte (fenêtre de 10 min en orange).")):
        if f:
            figures.append((f, cap))
    rep.write_json()
    rep.write_html(figures)
    text = rep.terminal()
    print(text)
    (docs / "summary.txt").write_text(text + "\n", encoding="utf-8")
    return rep.r

