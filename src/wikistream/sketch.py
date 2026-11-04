"""Comptage approximatif en mémoire bornée : Count-Min Sketch et Space-Saving.

Count-Min Sketch (Cormode et Muthukrishnan, 2005)
    largeur w = ceil(e / epsilon), profondeur d = ceil(ln(1 / delta)).
    Pour tout élément x de fréquence réelle f(x), sur un flux de N occurrences :
        f(x) <= estimation(x) <= f(x) + epsilon * N   avec probabilité >= 1 - delta.
    La borne basse est déterministe (on ne sous-estime jamais).

Space-Saving (Metwally, Agrawal et El Abbadi, 2005)
    k compteurs. Pour tout élément suivi : f(x) <= compte(x) <= f(x) + erreur(x),
    avec erreur(x) <= N / k. Tout élément de fréquence > N / k est forcément suivi.
    Bornes déterministes.
"""

from __future__ import annotations

import hashlib
import heapq
import math
from array import array
from collections.abc import Iterable


def _hashes(item: str, seed: int) -> tuple[int, int]:
    digest = hashlib.blake2b(item.encode("utf-8"), digest_size=16, salt=seed.to_bytes(8, "little")).digest()
    h1 = int.from_bytes(digest[:8], "little")
    h2 = int.from_bytes(digest[8:], "little") | 1
    return h1, h2


class CountMinSketch:
    def __init__(self, width: int, depth: int, *, seed: int = 0, conservative: bool = False) -> None:
        if width < 1 or depth < 1:
            raise ValueError("width et depth doivent être >= 1")
        self.width = width
        self.depth = depth
        self.seed = seed
        self.conservative = conservative
        self.rows = [array("q", [0]) * width for _ in range(depth)]
        self.total = 0

    @classmethod
    def from_error(cls, epsilon: float, delta: float, **kw) -> CountMinSketch:
        width = math.ceil(math.e / epsilon)
        depth = math.ceil(math.log(1.0 / delta))
        return cls(width, depth, **kw)

    @property
    def epsilon(self) -> float:
        return math.e / self.width

    @property
    def delta(self) -> float:
        return math.exp(-self.depth)

    def _indexes(self, item: str) -> list[int]:
        # double hachage de Kirsch-Mitzenmacher : d fonctions à partir de deux hachés
        h1, h2 = _hashes(item, self.seed)
        w = self.width
        return [(h1 + i * h2) % w for i in range(self.depth)]

    def add(self, item: str, count: int = 1) -> None:
        idx = self._indexes(item)
        self.total += count
        rows = self.rows
        if self.conservative:
            # mise à jour conservatrice : on ne remonte que les compteurs minimaux
            target = min(rows[i][j] for i, j in enumerate(idx)) + count
            for i, j in enumerate(idx):
                if rows[i][j] < target:
                    rows[i][j] = target
        else:
            for i, j in enumerate(idx):
                rows[i][j] += count

    def estimate(self, item: str) -> int:
        return min(self.rows[i][j] for i, j in enumerate(self._indexes(item)))

    def error_bound(self) -> float:
        return self.epsilon * self.total

    def merge(self, other: CountMinSketch) -> None:
        if (other.width, other.depth, other.seed) != (self.width, self.depth, self.seed):
            raise ValueError("sketches incompatibles")
        for r, o in zip(self.rows, other.rows, strict=True):
            for j in range(self.width):
                r[j] += o[j]
        self.total += other.total

    def memory_bytes(self) -> int:
        return self.width * self.depth * 8


class SpaceSaving:
    """Space-Saving avec un tas binaire à suppression paresseuse pour trouver le minimum."""

    def __init__(self, k: int) -> None:
        if k < 1:
            raise ValueError("k doit être >= 1")
        self.k = k
        self.counts: dict[str, int] = {}
        self.errors: dict[str, int] = {}
        self._heap: list[tuple[int, str]] = []
        self.total = 0

    def _push(self, item: str) -> None:
        heapq.heappush(self._heap, (self.counts[item], item))
        if len(self._heap) > 4 * self.k + 64:
            self._heap = [(c, x) for x, c in self.counts.items()]
            heapq.heapify(self._heap)

    def _pop_min(self) -> tuple[int, str]:
        while True:
            c, x = heapq.heappop(self._heap)
            if self.counts.get(x) == c:
                return c, x

    def add(self, item: str, count: int = 1) -> None:
        self.total += count
        if item in self.counts:
            self.counts[item] += count
        elif len(self.counts) < self.k:
            self.counts[item] = count
            self.errors[item] = 0
        else:
            min_count, victim = self._pop_min()
            del self.counts[victim]
            del self.errors[victim]
            self.counts[item] = min_count + count
            self.errors[item] = min_count
        self._push(item)

    def update(self, items: Iterable[str]) -> None:
        for x in items:
            self.add(x)

    def estimate(self, item: str) -> tuple[int, int]:
        """(compte estimé, erreur max). Pour un élément non suivi : (0, min actuel)."""
        if item in self.counts:
            return self.counts[item], self.errors[item]
        floor = min(self.counts.values()) if len(self.counts) >= self.k else 0
        return 0, floor

    def top(self, n: int) -> list[tuple[str, int, int]]:
        items = heapq.nlargest(n, self.counts.items(), key=lambda kv: kv[1])
        return [(x, c, self.errors[x]) for x, c in items]

    def guaranteed_top(self, n: int) -> list[tuple[str, int, int]]:
        """Préfixe du top dont l'ordre est garanti (compte - erreur >= compte du suivant)."""
        ranked = self.top(n + 1)
        out = []
        for i, (x, c, e) in enumerate(ranked[:n]):
            nxt = ranked[i + 1][1] if i + 1 < len(ranked) else 0
            if c - e < nxt:
                break
            out.append((x, c, e))
        return out

    def error_bound(self) -> float:
        return self.total / self.k


class HeavyHitters:
    """Top-k approximatif : Count-Min Sketch + tas des k meilleurs candidats."""

    def __init__(self, k: int, epsilon: float = 1e-3, delta: float = 1e-3, seed: int = 0) -> None:
        self.k = k
        self.cms = CountMinSketch.from_error(epsilon, delta, seed=seed, conservative=True)
        self.candidates: dict[str, int] = {}

    def add(self, item: str, count: int = 1) -> None:
        self.cms.add(item, count)
        est = self.cms.estimate(item)
        if item in self.candidates or len(self.candidates) < self.k:
            self.candidates[item] = est
            return
        victim = min(self.candidates, key=self.candidates.__getitem__)
        if est > self.candidates[victim]:
            del self.candidates[victim]
            self.candidates[item] = est

    def top(self, n: int) -> list[tuple[str, int]]:
        return heapq.nlargest(n, self.candidates.items(), key=lambda kv: kv[1])
