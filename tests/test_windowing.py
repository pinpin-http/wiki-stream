"""Moteur de fenêtrage : assignation, watermark, retards, tolérance de retard."""

from __future__ import annotations

import math
import random

import pytest

from conftest import make_event
from wikistream.aggregates import WikiStats
from wikistream.windowing import (
    Pipeline,
    SlidingWindows,
    TumblingWindows,
    WatermarkTracker,
    Window,
    WindowOperator,
)


class Count:
    def create(self):
        return 0

    def add(self, acc, e):
        return acc + 1

    def result(self, acc):
        return acc


def op(assigner, **kw):
    return WindowOperator("t", assigner, Count(), lambda e: e["wiki"], **kw)


def test_tumbling_assignment():
    w = TumblingWindows(60)
    assert w.assign(0) == [Window(0, 60)]
    assert w.assign(59.999) == [Window(0, 60)]
    assert w.assign(60) == [Window(60, 120)]
    assert w.assign(-1) == [Window(-60, 0)]


@pytest.mark.parametrize("size,slide", [(300, 60), (600, 60), (10, 3), (60, 60)])
def test_sliding_assignment_covers_exactly(size, slide):
    w = SlidingWindows(size, slide)
    rng = random.Random(1)
    for _ in range(500):
        ts = rng.uniform(-1000, 1000)
        wins = w.assign(ts)
        assert len(wins) == math.ceil(size / slide) or len(wins) == size // slide
        assert all(x.start <= ts < x.end and x.end - x.start == size for x in wins)
        assert len({x.start for x in wins}) == len(wins)


def test_window_fires_only_when_watermark_passes_end():
    o = op(TumblingWindows(60))
    for ts in (1, 5, 59):
        o.process(make_event(ts), ts)
    assert o.advance(59.9) == []
    out = o.advance(60)
    assert [(r.window, r.value) for r in out] == [(Window(0, 60), 3)]
    assert o.advance(120) == []  # pas de double émission


def test_out_of_order_within_bound_is_on_time():
    pipe = Pipeline(WatermarkTracker(max_out_of_orderness=10))
    o = pipe.add(op(TumblingWindows(60)))
    for ts in (50, 65, 55, 68):  # 55 arrive après 65 : désordre de 10 s, toléré
        pipe.process(make_event(ts), ts, partition=0, now=0)
        pipe.advance(now=0)
    assert o.stats.late_events == 0
    pipe.process(make_event(71), 71, partition=0, now=0)
    out = pipe.advance(now=0)  # watermark = 71 - 10 = 61 >= 60
    assert [(r.window.start, r.value) for r in out] == [(0, 2)]


def test_late_events_counted_apart_and_sent_to_side_output():
    late = []
    pipe = Pipeline(WatermarkTracker(max_out_of_orderness=5))
    o = pipe.add(op(TumblingWindows(60), late_sink=late.append))
    for ts in (10, 20, 70):
        pipe.process(make_event(ts), ts, 0, now=0)
    fired = pipe.advance(now=0)  # watermark 65 : [0,60) émise avec 2 événements
    assert [(r.window.start, r.value) for r in fired] == [(0, 2)]
    pipe.process(make_event(30, title="retardataire"), 30, 0, now=0)
    assert o.stats.late_events == 1
    assert len(late) == 1 and late[0].event["title"] == "retardataire"
    assert late[0].lateness == pytest.approx(35)
    # le retardataire n'a modifié aucun résultat
    assert pipe.advance(now=0) == []


def test_sliding_partially_late_event_is_not_counted_late():
    o = op(SlidingWindows(120, 60))
    o.process(make_event(100), 100)
    o.advance(120)  # [0,120) émise, [60,180) encore ouverte
    assert o.process(make_event(90), 90)  # tombe dans [60,180)
    assert o.stats.late_events == 0 and o.stats.late_window_drops == 1


def test_allowed_lateness_reemits_update_then_drops():
    o = op(TumblingWindows(60), allowed_lateness=30)
    o.process(make_event(10), 10)
    first = o.advance(60)
    assert [(r.value, r.is_update) for r in first] == [(1, False)]
    o.process(make_event(20), 20)  # watermark 60 < 60 + 30 : accepté, mise à jour
    upd = o.advance(70)
    assert [(r.value, r.is_update) for r in upd] == [(2, True)]
    o.advance(95)  # la fenêtre est purgée à 90
    assert o.process(make_event(30), 30) is False
    assert o.stats.late_events == 1 and o.stats.updates == 1


def test_watermark_is_min_over_partitions_and_ignores_idle():
    wm = WatermarkTracker(max_out_of_orderness=0, idle_timeout=30)
    wm.observe(0, 100, now=0)
    wm.observe(1, 40, now=0)
    assert wm.update(now=1) == 40  # la partition lente retient le watermark
    wm.observe(0, 200, now=50)
    # la partition 1 est muette depuis 50 s > 30 s : elle ne bloque plus
    assert wm.update(now=50) == 200
    # monotone : un événement ancien ne fait pas reculer le watermark
    wm.observe(1, 10, now=51)
    assert wm.update(now=51) == 200


def test_keys_are_independent_and_results_ordered():
    o = WindowOperator("t", TumblingWindows(60), Count(), lambda e: e["wiki"])
    for ts, wiki in [(1, "b"), (2, "a"), (61, "a"), (3, "a")]:
        o.process(make_event(ts, wiki=wiki), ts)
    out = o.advance(1000)
    assert [(r.window.start, r.key, r.value) for r in out] == [(0, "a", 2), (0, "b", 1), (60, "a", 1)]
    assert o.open_windows() == 0


def test_wiki_stats_aggregate(records):
    o = WindowOperator("w", TumblingWindows(60), WikiStats(), lambda e: e["wiki"])
    for e in records:
        o.process(e, e["ts"])
    out = o.advance(math.inf)
    total = sum(r.value["events"] for r in out)
    assert total == len(records)
    edits = [e for e in records if e["type"] in ("edit", "new")]
    assert sum(r.value["edits"] for r in out) == len(edits)
    assert sum(r.value["new_pages"] for r in out) == sum(e["type"] == "new" for e in records)
    assert sum(r.value["bot_edits"] for r in out) == sum(bool(e["bot"]) for e in edits)
    added = sum(max(0, (e["len_new"] or 0) - (e["len_old"] or 0)) for e in edits if e["len_new"] is not None)
    assert sum(r.value["bytes_added"] for r in out) == added
    for r in out:
        assert 0.0 <= r.value["bot_share"] <= 1.0
