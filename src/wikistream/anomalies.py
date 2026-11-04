"""Détection d'anomalies sur les séries produites par le fenêtrage.

Deux détecteurs :

* `RobustEWMADetector` (niveau wiki, une valeur par minute) : après un
  échauffement qui fixe niveau et échelle par médiane et MAD des premières
  valeurs, prévision par moyenne mobile exponentielle (EWMA), puis z-score
  robuste du résidu :
      z = (x - ewma) / max(1.4826 * MAD(résidus récents), sqrt(max(ewma, 1)))
  La MAD (médiane des écarts absolus) ne se laisse pas tirer par les pics comme
  un écart-type ; le plancher de type Poisson évite les z énormes quand une
  série est très régulière. Les points aberrants sont écrêtés avant de mettre à
  jour l'état, pour qu'un pic ne gonfle pas la référence qui sert à le juger.

* `PageSpikeDetector` (niveau page, fenêtres glissantes de 10 min) : la quasi-
  totalité des pages a un taux de base proche de zéro, un z-score sur l'historique
  n'a donc pas de sens. On compare le nombre de modifications humaines à une
  EWMA paresseuse (décroissance appliquée aux fenêtres sans activité) avec un
  score de Poisson, et on exige plusieurs contributeurs distincts : une seule
  personne qui enregistre vingt fois son brouillon n'est pas un événement.

Chaque détecteur fonctionne par épisode : une alerte à l'entrée en anomalie,
puis silence jusqu'à ce que la série soit redescendue (réarmement).
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class Alert:
    kind: str  # "wiki_spike" ou "page_spike"
    key: str
    window_start: float
    window_end: float
    value: float
    baseline: float
    score: float
    label: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    detected_at: float = 0.0  # temps de traitement, rempli par l'appelant

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class _SeriesState:
    ewma: float
    n: int = 1
    residuals: deque = field(default_factory=lambda: deque(maxlen=30))
    in_alert: bool = False
    warm: list = field(default_factory=list)  # valeurs de la période d'échauffement


class RobustEWMADetector:
    def __init__(
        self,
        *,
        alpha: float = 0.3,
        window: int = 30,
        warmup: int = 10,
        z_threshold: float = 6.0,
        min_value: float = 10.0,
        min_ratio: float = 1.5,
        clip: float = 3.0,
    ) -> None:
        self.alpha = alpha
        self.window = window
        self.warmup = warmup
        self.z_threshold = z_threshold
        self.min_value = min_value
        self.min_ratio = min_ratio
        self.clip = clip
        self.state: dict[str, _SeriesState] = {}

    def scale(self, st: _SeriesState) -> float:
        floor = math.sqrt(max(st.ewma, 1.0))
        if len(st.residuals) < 3:
            return floor
        med = statistics.median(st.residuals)
        mad = statistics.median(abs(r - med) for r in st.residuals)
        return max(1.4826 * mad, floor)

    def update(
        self, key: str, value: float, window_start: float, window_end: float, *, context_ok: bool = True
    ) -> Alert | None:
        """`context_ok=False` interdit l'alerte (ex. un seul contributeur) sans fausser l'état."""
        st = self.state.get(key)
        if st is None:
            self.state[key] = _SeriesState(ewma=float(value), residuals=deque(maxlen=self.window), warm=[value])
            return None
        if st.n < self.warmup:
            # échauffement : niveau = médiane des premières valeurs (la première minute est souvent
            # incomplète), échelle = MAD de ces mêmes valeurs. Pas d'écrêtage ici : sinon une
            # référence partie trop bas mettrait des dizaines de minutes à rejoindre la série.
            st.warm.append(value)
            st.n += 1
            st.ewma = statistics.median(st.warm)
            if st.n == self.warmup:
                st.residuals.extend(v - st.ewma for v in st.warm)
                st.warm = []
            return None
        resid = value - st.ewma
        scale = self.scale(st)
        z = resid / scale
        alert = None
        anomalous = (
            context_ok
            and z >= self.z_threshold
            and value >= self.min_value
            and value >= self.min_ratio * max(st.ewma, 1.0)
        )
        if anomalous and not st.in_alert:
            st.in_alert = True
            alert = Alert("wiki_spike", key, window_start, window_end, value, round(st.ewma, 2), round(z, 2))
        elif st.in_alert and z < self.z_threshold / 2:
            st.in_alert = False
        # mise à jour robuste : le résidu est écrêté à +/- clip * échelle
        clipped = max(-self.clip * scale, min(self.clip * scale, resid))
        st.residuals.append(clipped)
        st.ewma += self.alpha * clipped
        st.n += 1
        return alert


@dataclass(slots=True)
class _PageState:
    ewma: float
    last_end: float
    in_alert: bool = False


class PageSpikeDetector:
    def __init__(
        self,
        *,
        slide: float = 60.0,
        alpha: float = 0.1,
        min_edits: int = 5,
        min_users: int = 2,
        z_threshold: float = 3.5,
        warmup: float = 600.0,
        max_state: int = 200_000,
    ) -> None:
        self.slide = slide
        self.warmup = warmup
        self.first_end: float | None = None
        self.alpha = alpha
        self.min_edits = min_edits
        self.min_users = min_users
        self.z_threshold = z_threshold
        self.max_state = max_state
        self.state: dict[str, _PageState] = {}

    def update(self, key: str, stats: dict[str, Any], window_start: float, window_end: float) -> Alert | None:
        x = stats["edits"]
        if self.first_end is None:
            self.first_end = window_end
        # démarrage à froid : on apprend les références sans alerter pendant `warmup` s
        warming = window_end < self.first_end + self.warmup
        st = self.state.get(key)
        if st is None:
            st = self.state[key] = _PageState(ewma=0.0, last_end=window_end - self.slide)
        # fenêtres intermédiaires sans activité : la référence décroît vers 0
        gap = max(0, round((window_end - st.last_end) / self.slide) - 1)
        if gap:
            st.ewma *= (1 - self.alpha) ** gap
            if st.in_alert and gap * self.slide >= window_end - window_start:
                st.in_alert = False
        lam = st.ewma
        z = (x - lam) / math.sqrt(lam + 1.0)
        alert = None
        hot = x >= self.min_edits and stats["users"] >= self.min_users and z >= self.z_threshold
        if hot and not st.in_alert:
            # entrée dans un épisode ; pendant le démarrage il est appris mais pas signalé
            st.in_alert = True
            if not warming:
                alert = Alert(
                    "page_spike", key, window_start, window_end, x, round(lam, 2), round(z, 2),
                    label=label_page(stats), details=stats,
                )
        elif st.in_alert and x < self.min_edits / 2:
            st.in_alert = False
        st.ewma = (1 - self.alpha) * st.ewma + self.alpha * x
        st.last_end = window_end
        if len(self.state) > self.max_state:
            self.evict(window_end)
        return alert

    def evict(self, now_end: float, idle: float = 3600.0) -> None:
        dead = [k for k, s in self.state.items() if not s.in_alert and now_end - s.last_end > idle]
        for k in dead:
            del self.state[k]


def label_page(stats: dict[str, Any]) -> str:
    """Étiquette heuristique d'un pic de page (à vérifier à la main, ce n'est qu'un indice)."""
    edits = stats["edits"] or 1
    if stats["reverts"] >= 2 and stats["reverts"] / edits >= 0.25 and stats["users"] <= 6:
        return "guerre d'édition probable"
    if stats.get("new") or stats["users"] >= 4:
        return "afflux de contributeurs (actualité probable)"
    if stats.get("top_user_share", 0) >= 0.7:
        return "rafale d'un contributeur principal"
    return "rafale de modifications"


def label_wiki(details: dict[str, Any]) -> str:
    """Quand trois comptes font plus de la moitié d'un pic, ce sont presque toujours des outils de
    modification en lot (QuickStatements, Cat-a-lot...) lancés depuis des comptes humains."""
    share = details.get("top3_users_share", 0.0)
    if share >= 0.5:
        return f"lot semi-automatique probable (3 comptes = {share:.0%} des modifications humaines)".replace("%", "\u202f%")
    return "pic d'activité humaine sur le wiki"
