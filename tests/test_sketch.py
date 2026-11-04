"""Sketches : bornes d'erreur vérifiées sur des flux de Zipf et sur la fixture."""

from __future__ import annotations

import math
import random
from collections import Counter

import pytest

from wikistream.events import page_key
from wikistream.sketch import CountMinSketch, HeavyHitters, SpaceSaving


def zipf_stream(n_items: int, n: int, s: float = 1.1, seed: int = 7) -> list[str]:
    rng = random.Random(seed)
    weights = [1 / (i + 1) ** s for i in range(n_items)]
    return [f"page-{i}" for i in rng.choices(range(n_items), weights=weights, k=n)]


@pytest.fixture(scope="module")
def stream():
    return zipf_stream(20_000, 100_000)


def test_cms_dimensions_from_error():
    cms = CountMinSketch.from_error(epsilon=0.001, delta=0.01)
    assert cms.width == math.ceil(math.e / 0.001) and cms.depth == math.ceil(math.log(100))
    assert cms.epsilon <= 0.001 and cms.delta <= 0.01


@pytest.mark.parametrize("conservative", [False, True])
def test_cms_error_bound(stream, conservative):
    eps, delta = 0.002, 0.01
    cms = CountMinSketch.from_error(eps, delta, seed=3, conservative=conservative)
    for x in stream:
        cms.add(x)
    true = Counter(stream)
    n = len(stream)
    bound = eps * n
    over = []
    for item, f in true.items():
        est = cms.estimate(item)
        assert est >= f  # jamais de sous-estimation
        over.append(est - f > bound)
    # la borne eps*N doit tenir pour au moins (1 - delta) des éléments ; marge x2 pour l'aléa
    assert sum(over) / len(over) <= 2 * delta
    # éléments jamais vus : estimation bornée de la même façon
    unseen = [cms.estimate(f"absent-{i}") for i in range(2000)]
    assert sum(u > bound for u in unseen) / len(unseen) <= 2 * delta


def test_cms_conservative_never_worse(stream):
    a = CountMinSketch(500, 4, seed=1)
    b = CountMinSketch(500, 4, seed=1, conservative=True)
    for x in stream[:30_000]:
        a.add(x)
        b.add(x)
    true = Counter(stream[:30_000])
    for item, f in list(true.items())[:3000]:
        assert f <= b.estimate(item) <= a.estimate(item)


def test_cms_merge_equals_single_pass(stream):
    a, b, c = (CountMinSketch(1000, 3, seed=9) for _ in range(3))
    half = len(stream) // 2
    for x in stream[:half]:
        a.add(x)
        c.add(x)
    for x in stream[half:]:
        b.add(x)
        c.add(x)
    a.merge(b)
    assert a.rows == c.rows and a.total == c.total


@pytest.mark.parametrize("k", [50, 200, 1000])
def test_space_saving_deterministic_bounds(stream, k):
    ss = SpaceSaving(k)
    ss.update(stream)
    true = Counter(stream)
    n = len(stream)
    assert len(ss.counts) <= k and ss.total == n
    for item, c in ss.counts.items():
        err = ss.errors[item]
        assert c - err <= true[item] <= c  # encadrement garanti
        assert err <= n / k
    # tout élément de fréquence > N/k est forcément suivi
    for item, f in true.items():
        if f > n / k:
            assert item in ss.counts
    # pour un élément non suivi, la fréquence réelle est <= au plus petit compteur
    floor = min(ss.counts.values())
    for item, f in true.items():
        if item not in ss.counts:
            assert f <= floor


def test_space_saving_top_matches_exact_head(stream):
    ss = SpaceSaving(500)
    ss.update(stream)
    exact = [x for x, _ in Counter(stream).most_common(10)]
    guaranteed = [x for x, _, _ in ss.guaranteed_top(10)]
    assert guaranteed == exact[: len(guaranteed)]
    assert [x for x, _, _ in ss.top(10)] == exact


def test_heavy_hitters_recall(stream):
    sub = stream[:30_000]
    hh = HeavyHitters(k=50, epsilon=1e-3, delta=1e-3, seed=5)
    for x in sub:
        hh.add(x)
    exact = {x for x, _ in Counter(sub).most_common(20)}
    got = {x for x, _ in hh.top(20)}
    assert len(exact & got) >= 19


def test_on_real_fixture(records):
    keys = [page_key(e) for e in records]
    ss = SpaceSaving(16)
    cms = CountMinSketch.from_error(0.01, 0.01)
    for k in keys:
        ss.add(k)
        cms.add(k)
    true = Counter(keys)
    for item, c in ss.counts.items():
        assert c - ss.errors[item] <= true[item] <= c
    bound = cms.error_bound()
    misses = 0
    for item, f in true.items():
        est = cms.estimate(item)
        assert est >= f
        misses += est - f > bound
    assert misses / len(true) <= 0.05
