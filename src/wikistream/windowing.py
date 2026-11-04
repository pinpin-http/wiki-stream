"""Moteur de fenêtrage en temps d'événement.

Concepts (mêmes définitions que le modèle Dataflow / Flink) :

* Temps d'événement : l'horodatage porté par l'événement (`ts`, heure de la
  modification sur le wiki), pas l'heure à laquelle on le reçoit.
* Watermark : affirmation « je ne verrai plus d'événement antérieur à W ».
  Ici, désordre borné : W = max(ts vus) - max_out_of_orderness, calculé par
  partition source puis on prend le minimum sur les partitions actives (une
  partition muette depuis `idle_timeout` secondes de temps de traitement ne
  retient plus le watermark).
* Une fenêtre [début, fin) est émise dès que W >= fin. Elle reste ensuite en
  mémoire `allowed_lateness` secondes (en temps d'événement) : un retardataire
  qui tombe dedans la met à jour et provoque une ré-émission (`is_update`).
* Au-delà, l'événement est en retard : il n'est ajouté à aucune fenêtre, il est
  compté à part et envoyé dans une sortie latérale (`late`).
"""

from __future__ import annotations

import heapq
import math
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

A = TypeVar("A")
R = TypeVar("R")


@dataclass(frozen=True, slots=True, order=True)
class Window:
    start: float
    end: float


class TumblingWindows:
    """Fenêtres fixes, contiguës et sans recouvrement."""

    def __init__(self, size: float, offset: float = 0.0) -> None:
        if size <= 0:
            raise ValueError("size > 0")
        self.size = size
        self.offset = offset

    def assign(self, ts: float) -> list[Window]:
        start = math.floor((ts - self.offset) / self.size) * self.size + self.offset
        return [Window(start, start + self.size)]


class SlidingWindows:
    """Fenêtres glissantes de taille `size`, une nouvelle toutes les `slide` secondes.

    Chaque événement appartient à ceil(size / slide) fenêtres.
    """

    def __init__(self, size: float, slide: float, offset: float = 0.0) -> None:
        if size <= 0 or slide <= 0 or slide > size:
            raise ValueError("il faut 0 < slide <= size")
        self.size = size
        self.slide = slide
        self.offset = offset

    def assign(self, ts: float) -> list[Window]:
        last_start = math.floor((ts - self.offset) / self.slide) * self.slide + self.offset
        out = []
        start = last_start
        while start > ts - self.size:
            out.append(Window(start, start + self.size))
            start -= self.slide
        return out


class Aggregator(Protocol[A, R]):
    def create(self) -> A: ...

    def add(self, acc: A, event: dict[str, Any]) -> A: ...

    def result(self, acc: A) -> R: ...


class WatermarkTracker:
    def __init__(self, max_out_of_orderness: float, idle_timeout: float = 30.0) -> None:
        self.max_out_of_orderness = max_out_of_orderness
        self.idle_timeout = idle_timeout
        self._max_ts: dict[Hashable, float] = {}
        self._last_seen: dict[Hashable, float] = {}
        self._current = -math.inf

    def observe(self, partition: Hashable, ts: float, now: float) -> None:
        if ts > self._max_ts.get(partition, -math.inf):
            self._max_ts[partition] = ts
        self._last_seen[partition] = now

    def update(self, now: float) -> float:
        """Recalcule le watermark (monotone : il ne recule jamais)."""
        active = [
            m for p, m in self._max_ts.items() if now - self._last_seen[p] <= self.idle_timeout
        ]
        if not active:
            return self._current  # tout est muet : on ne devine rien, on attend
        candidate = min(active) - self.max_out_of_orderness
        if candidate > self._current:
            self._current = candidate
        return self._current

    @property
    def current(self) -> float:
        return self._current


@dataclass(slots=True)
class WindowResult(Generic[R]):
    operator: str
    key: Hashable
    window: Window
    value: R
    is_update: bool = False


@dataclass(slots=True)
class LateEvent:
    operator: str
    key: Hashable
    ts: float
    watermark: float
    event: dict[str, Any]

    @property
    def lateness(self) -> float:
        return self.watermark - self.ts


@dataclass(slots=True)
class _Slot:
    acc: Any
    fired: bool = False
    dirty: bool = False


@dataclass
class OperatorStats:
    accepted: int = 0
    late_events: int = 0  # en retard pour toutes leurs fenêtres : comptés à part
    late_window_drops: int = 0  # (événement, fenêtre) écartés, y compris retards partiels
    updates: int = 0
    fired: int = 0


class WindowOperator(Generic[A, R]):
    """Fenêtrage par clé : assigner + agrégateur incrémental.

    `process()` range l'événement dans ses fenêtres (ou le déclare en retard),
    `advance()` émet les fenêtres que le watermark a dépassées.
    """

    def __init__(
        self,
        name: str,
        assigner: TumblingWindows | SlidingWindows,
        aggregator: Aggregator[A, R],
        key_fn: Callable[[dict[str, Any]], Hashable],
        *,
        allowed_lateness: float = 0.0,
        late_sink: Callable[[LateEvent], None] | None = None,
    ) -> None:
        self.name = name
        self.assigner = assigner
        self.aggregator = aggregator
        self.key_fn = key_fn
        self.allowed_lateness = allowed_lateness
        self.late_sink = late_sink
        self.watermark = -math.inf
        self.stats = OperatorStats()
        # fin de fenêtre -> {(début, clé): slot} ; les fins sont rangées dans un tas
        self._by_end: dict[float, dict[tuple[float, Hashable], _Slot]] = {}
        self._ends: list[float] = []

    def open_windows(self) -> int:
        return sum(len(v) for v in self._by_end.values())

    def process(self, event: dict[str, Any], ts: float) -> bool:
        """Renvoie False si l'événement est en retard (ajouté à aucune fenêtre)."""
        key = self.key_fn(event)
        added = False
        for w in self.assigner.assign(ts):
            if w.end + self.allowed_lateness <= self.watermark:
                self.stats.late_window_drops += 1
                continue
            bucket = self._by_end.get(w.end)
            if bucket is None:
                bucket = self._by_end[w.end] = {}
                heapq.heappush(self._ends, w.end)
            slot = bucket.get((w.start, key))
            if slot is None:
                slot = bucket[(w.start, key)] = _Slot(self.aggregator.create())
            slot.acc = self.aggregator.add(slot.acc, event)
            if slot.fired:
                slot.dirty = True
            added = True
        if added:
            self.stats.accepted += 1
        else:
            self.stats.late_events += 1
            if self.late_sink is not None:
                self.late_sink(LateEvent(self.name, key, ts, self.watermark, event))
        return added

    def advance(self, watermark: float) -> list[WindowResult[R]]:
        if watermark < self.watermark:
            watermark = self.watermark
        self.watermark = watermark
        out: list[WindowResult[R]] = []
        # ré-émissions des fenêtres déjà tirées et modifiées par des retardataires
        for end, bucket in self._by_end.items():
            if end > watermark:
                continue
            for (start, key), slot in bucket.items():
                if slot.fired and slot.dirty:
                    slot.dirty = False
                    self.stats.updates += 1
                    out.append(
                        WindowResult(self.name, key, Window(start, end), self.aggregator.result(slot.acc), True)
                    )
        # premières émissions, dans l'ordre des fins de fenêtre
        for end in sorted(e for e in self._by_end if e <= watermark):
            for (start, key), slot in sorted(self._by_end[end].items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
                if not slot.fired:
                    slot.fired = True
                    self.stats.fired += 1
                    out.append(WindowResult(self.name, key, Window(start, end), self.aggregator.result(slot.acc)))
        # purge : fenêtres dont la tolérance de retard est dépassée
        while self._ends and self._ends[0] + self.allowed_lateness <= watermark:
            end = heapq.heappop(self._ends)
            self._by_end.pop(end, None)
        return out

    def flush_open(self) -> list[WindowResult[R]]:
        """Résultats partiels des fenêtres encore ouvertes (fin de run)."""
        out = []
        for end in sorted(self._by_end):
            for (start, key), slot in self._by_end[end].items():
                if not slot.fired:
                    out.append(WindowResult(self.name, key, Window(start, end), self.aggregator.result(slot.acc)))
        return out


@dataclass
class Pipeline:
    """Un watermark partagé et plusieurs opérateurs alimentés par le même flux."""

    watermarks: WatermarkTracker
    operators: list[WindowOperator] = field(default_factory=list)
    routes: dict[str, Callable[[dict[str, Any]], bool]] = field(default_factory=dict)

    def add(self, op: WindowOperator, accept: Callable[[dict[str, Any]], bool] | None = None) -> WindowOperator:
        self.operators.append(op)
        if accept is not None:
            self.routes[op.name] = accept
        return op

    def process(self, event: dict[str, Any], ts: float, partition: Hashable, now: float) -> None:
        for op in self.operators:
            accept = self.routes.get(op.name)
            if accept is None or accept(event):
                op.process(event, ts)
        self.watermarks.observe(partition, ts, now)

    def advance(self, now: float) -> list[WindowResult]:
        wm = self.watermarks.update(now)
        out: list[WindowResult] = []
        for op in self.operators:
            out.extend(op.advance(wm))
        return out
