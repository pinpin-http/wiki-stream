from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from wikistream.events import normalize

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.jsonl"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Les tests tournent sans réseau : toute connexion IP échoue immédiatement."""
    real_connect = socket.socket.connect

    def guarded(self, address):
        if self.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError(f"accès réseau interdit pendant les tests : {address}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)


def load_raw() -> list[dict]:
    with open(FIXTURE, encoding="utf-8") as f:
        return [json.loads(line) for line in f]


@pytest.fixture(scope="session")
def raw_events() -> list[dict]:
    return load_raw()


@pytest.fixture(scope="session")
def records(raw_events) -> list[dict]:
    out = []
    for i, raw in enumerate(raw_events):
        rec = normalize(raw, recv_ts=raw["timestamp"] + 1.0 + i * 0.001)
        if rec is not None:
            out.append(rec)
    return out


def make_event(ts: float, wiki: str = "frwiki", title: str = "Page", user: str = "u", **kw) -> dict:
    e = {
        "id": kw.pop("id", f"{wiki}-{title}-{ts}-{user}"),
        "wiki": wiki,
        "type": kw.pop("type", "edit"),
        "ns": 0,
        "title": title,
        "user": user,
        "bot": kw.pop("bot", False),
        "ts": ts,
        "recv_ts": ts + 1,
        "dt": ts + 0.5,
        "len_old": 100,
        "len_new": 100 + kw.pop("delta", 10),
        "comment": kw.pop("comment", ""),
    }
    e.update(kw)
    return e
