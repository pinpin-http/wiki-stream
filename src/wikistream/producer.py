"""Producteur : flux SSE Wikimedia -> topic (clé = wiki, valeur = JSON compact)."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Iterable

from .broker import Producer
from .events import encode, normalize
from .metrics import MetricsLog, RateMeter, rss_mb
from .source import SSEClient, SSEEvent

log = logging.getLogger(__name__)


class Ingestor:
    def __init__(
        self,
        events: Iterable[SSEEvent],
        producer: Producer,
        *,
        metrics: MetricsLog | None = None,
        client: SSEClient | None = None,
        stop: threading.Event | None = None,
        report_every: float = 10.0,
    ) -> None:
        self.events = events
        self.producer = producer
        self.metrics = metrics
        self.client = client
        self.stop = stop or threading.Event()
        self.report_every = report_every
        self.rate = RateMeter()
        self.skipped = 0
        self.bad_json = 0
        self.bytes_in = 0
        self.bytes_out = 0

    def run(self, duration: float | None = None) -> None:
        deadline = time.time() + duration if duration else None
        last_report = time.time()
        last_count = 0
        try:
            for ev in self.events:
                now = time.time()
                self.bytes_in += len(ev.data)
                try:
                    raw = json.loads(ev.data)
                except json.JSONDecodeError:
                    self.bad_json += 1
                    continue
                rec = normalize(raw, recv_ts=now)
                if rec is None:
                    self.skipped += 1
                    continue
                value = encode(rec)
                self.bytes_out += len(value)
                self.producer.produce(rec["wiki"], value)
                self.producer.poll(0)
                self.rate.mark(1, now)
                if now - last_report >= self.report_every:
                    n = self.rate.total - last_count
                    self._report(n / (now - last_report))
                    if self.client:
                        self.client.save_state()
                    last_report, last_count = now, self.rate.total
                if self.stop.is_set() or (deadline and now >= deadline):
                    break
        finally:
            left = self.producer.flush(15)
            if left:
                log.error("%d messages non livrés à l'arrêt", left)
            if self.client:
                self.client.save_state()
            self._report(None, final=True)

    def _report(self, rate: float | None, final: bool = False) -> None:
        fields = {
            "produced": self.rate.total,
            "rate": round(rate, 2) if rate is not None else None,
            "skipped": self.skipped,
            "bad_json": self.bad_json,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "delivery_errors": self.producer.delivery_errors,
            "reconnects": self.client.reconnects if self.client else 0,
            "rss_mb": rss_mb(),
        }
        if final:
            fields["summary"] = self.rate.summary()
        if self.metrics:
            self.metrics.write("producer", final=final, **fields)
        if rate is not None:
            log.info("producteur : %d évts, %.1f évt/s, reconnexions %s", self.rate.total, rate, fields["reconnects"])
