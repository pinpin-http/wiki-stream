"""Détecteurs : silence sur du bruit stationnaire, alerte sur un pic, épisodes, robustesse."""

from __future__ import annotations

import math
import random

from wikistream.anomalies import PageSpikeDetector, RobustEWMADetector, label_page


def poisson(rng: random.Random, lam: float) -> int:
    # algorithme de Knuth, suffisant pour lam < 100
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def feed(det: RobustEWMADetector, key: str, values, start: int = 0, **kw):
    alerts = []
    for i, v in enumerate(values, start=start):
        a = det.update(key, v, i * 60, (i + 1) * 60, **kw)
        if a:
            alerts.append((i, a))
    return alerts


def test_no_alert_on_stationary_poisson_noise():
    rng = random.Random(0)
    det = RobustEWMADetector()
    total = 0
    for lam in (1, 3, 8, 20, 60):
        total += len(feed(det, f"wiki{lam}", [poisson(rng, lam) for _ in range(3000)]))
    assert total == 0


def test_alert_on_spike_with_one_alert_per_episode():
    rng = random.Random(1)
    det = RobustEWMADetector()
    series = [poisson(rng, 30) for _ in range(40)] + [150, 160, 155] + [poisson(rng, 30) for _ in range(20)]
    alerts = feed(det, "enwiki", series)
    assert [i for i, _ in alerts] == [40]
    a = alerts[0][1]
    assert a.kind == "wiki_spike" and a.value == 150 and a.score >= 6 and 20 < a.baseline < 40


def test_cold_start_from_partial_first_minute_is_not_an_alert():
    """Régression observée en live : première minute incomplète (52) puis un niveau stable vers 220.

    L'ancienne version partait de 52 et écrêtait les résidus : la référence mettait 30 minutes à
    rattraper la série et la première minute « normale » après l'échauffement passait pour un pic.
    """
    rng = random.Random(3)
    det = RobustEWMADetector()
    series = [52] + [poisson(rng, 220) for _ in range(40)]
    assert feed(det, "wikidatawiki", series) == []
    assert 200 < det.state["wikidatawiki"].ewma < 240


def test_wiki_label_flags_batch_tools():
    from wikistream.anomalies import label_wiki

    assert label_wiki({"top3_users_share": 0.62}).startswith("lot semi-automatique probable")
    assert label_wiki({"top3_users_share": 0.2}) == "pic d'activité humaine sur le wiki"


def test_warmup_suppresses_early_alerts():
    det = RobustEWMADetector(warmup=10)
    assert feed(det, "w", [5] * 5 + [200]) == []


def test_context_gate_blocks_alert_but_keeps_learning():
    det = RobustEWMADetector()
    feed(det, "w", [10] * 20)
    assert feed(det, "w", [200], start=20, context_ok=False) == []
    assert det.state["w"].n == 21


def test_baseline_not_contaminated_by_spike():
    det = RobustEWMADetector()
    rng = random.Random(2)
    base = [poisson(rng, 20) for _ in range(30)]
    feed(det, "w", base + [400])
    ewma_after = det.state["w"].ewma
    assert ewma_after < 40  # le pic écrêté ne déplace presque pas la référence
    # un second pic quelques minutes après est toujours détecté
    later = feed(det, "w", [poisson(rng, 20) for _ in range(5)] + [300], start=31)
    assert len(later) == 1


def stats(edits, users, reverts=0, new=False, top_share=0.3):
    return {"edits": edits, "users": users, "reverts": reverts, "new": new, "top_user_share": top_share}


def run_page(det, key, values, start_end=600.0):
    out = []
    for i, st in enumerate(values):
        end = start_end + 60 * i
        a = det.update(key, st, end - 600, end)
        if a:
            out.append((i, a))
    return out


def test_page_spike_requires_several_users():
    det = PageSpikeDetector(warmup=0)
    assert run_page(det, "frwiki:Brouillon", [stats(30, 1, top_share=1.0)] * 5) == []
    got = run_page(det, "enwiki:Breaking", [stats(2, 2), stats(12, 6)])
    assert len(got) == 1 and got[0][0] == 1
    assert got[0][1].label.startswith("afflux")


def test_page_warmup_learns_without_alerting():
    det = PageSpikeDetector(warmup=600)
    hot = [stats(20, 5)] * 15
    alerts = run_page(det, "enwiki:Déjà chaud", hot)
    assert alerts == []  # l'épisode a commencé pendant le démarrage : pas d'alerte tardive


def test_page_episode_rearms_after_calm_and_lazy_decay():
    det = PageSpikeDetector(warmup=0)
    key = "dewiki:Seite"
    first = run_page(det, key, [stats(10, 4)] * 3)
    assert [i for i, _ in first] == [0]
    # 30 minutes sans aucune fenêtre pour cette page : décroissance paresseuse et réarmement
    end = 600 + 60 * 2 + 1800
    a = det.update(key, stats(10, 4), end - 600, end)
    assert a is not None and a.baseline < 1


def test_page_labels():
    assert label_page(stats(10, 3, reverts=5)) == "guerre d'édition probable"
    assert label_page(stats(6, 2, reverts=2)) == "guerre d'édition probable"
    assert label_page(stats(10, 3, new=True)).startswith("afflux")
    assert label_page(stats(10, 3, top_share=0.8)) == "rafale d'un contributeur principal"
    assert label_page(stats(10, 3)) == "rafale de modifications"
