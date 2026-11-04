"""Agrégateurs incrémentaux branchés sur le moteur de fenêtrage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .events import EDIT_TYPES, is_revert, page_key, size_delta
from .sketch import SpaceSaving


@dataclass(slots=True)
class WikiAcc:
    events: int = 0
    edits: int = 0  # edit + new
    new_pages: int = 0
    bot_edits: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    log_events: int = 0
    categorize: int = 0
    users: dict[str, int] = field(default_factory=dict)  # modifications humaines par compte
    pages: SpaceSaving = field(default_factory=lambda: SpaceSaving(32))


class WikiStats:
    """Par wiki : volume, part de bots, octets ajoutés/retirés, nouveaux articles."""

    def create(self) -> WikiAcc:
        return WikiAcc()

    def add(self, acc: WikiAcc, e: dict[str, Any]) -> WikiAcc:
        acc.events += 1
        t = e.get("type")
        if t in EDIT_TYPES:
            acc.edits += 1
            if t == "new":
                acc.new_pages += 1
            if e.get("bot"):
                acc.bot_edits += 1
            else:
                u = e.get("user") or ""
                acc.users[u] = acc.users.get(u, 0) + 1
                acc.pages.add(e["title"])
            d = size_delta(e)
            if d > 0:
                acc.bytes_added += d
            else:
                acc.bytes_removed -= d
        elif t == "log":
            acc.log_events += 1
        elif t == "categorize":
            acc.categorize += 1
        return acc

    def result(self, acc: WikiAcc) -> dict[str, Any]:
        human = acc.edits - acc.bot_edits
        top_users = sorted(acc.users.items(), key=lambda kv: -kv[1])[:3]
        return {
            "events": acc.events,
            "edits": acc.edits,
            "human_edits": human,
            "bot_edits": acc.bot_edits,
            "bot_share": round(acc.bot_edits / acc.edits, 4) if acc.edits else 0.0,
            "new_pages": acc.new_pages,
            "bytes_added": acc.bytes_added,
            "bytes_removed": acc.bytes_removed,
            "log_events": acc.log_events,
            "categorize": acc.categorize,
            "human_users": len(acc.users),
            "top_users": [[u, n] for u, n in top_users],
            "top3_users_share": round(sum(n for _, n in top_users) / human, 3) if human else 0.0,
            "top_pages": [[t, c] for t, c, _ in acc.pages.top(5)],
        }


@dataclass(slots=True)
class PageAcc:
    edits: int = 0
    reverts: int = 0
    new: bool = False
    net_bytes: int = 0
    abs_bytes: int = 0
    users: dict[str, int] = field(default_factory=dict)
    first_ts: float = float("inf")
    last_ts: float = 0.0
    comments: list[str] = field(default_factory=list)


class PageActivity:
    """Par page (wiki:titre) : modifications humaines, contributeurs, révocations."""

    def create(self) -> PageAcc:
        return PageAcc()

    def add(self, acc: PageAcc, e: dict[str, Any]) -> PageAcc:
        acc.edits += 1
        if e.get("type") == "new":
            acc.new = True
        c = e.get("comment") or ""
        if is_revert(c):
            acc.reverts += 1
        d = size_delta(e)
        acc.net_bytes += d
        acc.abs_bytes += abs(d)
        u = e.get("user") or ""
        acc.users[u] = acc.users.get(u, 0) + 1
        ts = e["ts"]
        acc.first_ts = min(acc.first_ts, ts)
        acc.last_ts = max(acc.last_ts, ts)
        if c and len(acc.comments) < 3:
            acc.comments.append(c[:120])
        return acc

    def result(self, acc: PageAcc) -> dict[str, Any]:
        top_user_share = max(acc.users.values()) / acc.edits if acc.edits else 0.0
        return {
            "edits": acc.edits,
            "users": len(acc.users),
            "reverts": acc.reverts,
            "new": acc.new,
            "net_bytes": acc.net_bytes,
            "abs_bytes": acc.abs_bytes,
            "top_user_share": round(top_user_share, 3),
            "first_ts": acc.first_ts,
            "last_ts": acc.last_ts,
            "comments": acc.comments,
        }


class TopPages:
    """Top pages d'une fenêtre en mémoire bornée (Space-Saving, k compteurs)."""

    def __init__(self, k: int = 500) -> None:
        self.k = k

    def create(self) -> SpaceSaving:
        return SpaceSaving(self.k)

    def add(self, acc: SpaceSaving, e: dict[str, Any]) -> SpaceSaving:
        acc.add(page_key(e))
        return acc

    def result(self, acc: SpaceSaving) -> dict[str, Any]:
        return {
            "total": acc.total,
            "k": acc.k,
            "error_bound": acc.error_bound(),
            "top": [[x, c, err] for x, c, err in acc.top(25)],
        }
