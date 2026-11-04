"""Métriques de débit et de latence, écrites en JSONL (une ligne toutes les N secondes)."""

from __future__ import annotations

import json
import resource
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any


def percentile(sorted_values: list[float], q: float) -> float | None:
    """Percentile par interpolation linéaire (méthode « linear » de numpy)."""
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


class RateMeter:
    """Compte les événements par seconde de temps de traitement (pour moyenne et pic)."""

    def __init__(self) -> None:
        self.per_second: Counter[int] = Counter()
        self.total = 0
        self.started = time.time()

    def mark(self, n: int = 1, now: float | None = None) -> None:
        self.per_second[int(now if now is not None else time.time())] += n
        self.total += n

    def summary(self) -> dict[str, Any]:
        if not self.per_second:
            return {"total": 0}
        first, last = min(self.per_second), max(self.per_second)
        span = max(1, last - first + 1)
        secs = [self.per_second.get(s, 0) for s in range(first, last + 1)]
        tens = [sum(secs[i : i + 10]) / 10 for i in range(0, len(secs) - 9, 10)]
        return {
            "total": self.total,
            "span_s": span,
            "mean_per_s": round(self.total / span, 2),
            "peak_1s": max(secs),
            "peak_10s_avg": round(max(tens), 2) if tens else None,
            "p50_per_s": percentile(sorted(secs), 0.5),
            "p99_per_s": percentile(sorted(secs), 0.99),
        }


class LatencyRecorder:
    def __init__(self) -> None:
        self.samples: dict[str, list[float]] = {}

    def add(self, name: str, value: float) -> None:
        self.samples.setdefault(name, []).append(value)

    def summary(self) -> dict[str, Any]:
        out = {}
        for name, vals in self.samples.items():
            s = sorted(vals)
            out[name] = {
                "n": len(s),
                "p50": round(percentile(s, 0.5), 3),
                "p90": round(percentile(s, 0.9), 3),
                "p99": round(percentile(s, 0.99), 3),
                "max": round(s[-1], 3),
            }
        return out


def rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


class MetricsLog:
    """Écrit des lignes JSON horodatées ; sûr entre threads."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(path, "a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, role: str, **fields: Any) -> None:
        line = json.dumps({"t": round(time.time(), 3), "role": role, **fields}, ensure_ascii=False)
        with self._lock:
            self._f.write(line + "\n")
            self._f.flush()

    def close(self) -> None:
        self._f.close()
