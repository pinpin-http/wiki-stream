"""Normalisation des événements `mediawiki.recentchange`.

Le flux brut fait environ 1,3 Ko par événement. On ne garde que les champs utiles
au traitement, dans un JSON compact versionné : c'est ce qui circule dans le topic.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

SCHEMA_VERSION = 1
EDIT_TYPES = frozenset({"edit", "new"})
COMMENT_MAX = 300

# Résumés de modification typiques d'une révocation (rollback / undo), par langue.
# Heuristique : MediaWiki ne publie pas les tags de révision dans recentchange.
_REVERT_RE = re.compile(
    r"\b(undid|undo|revert(ed)?|rv[tv]?|rollback)\b"
    r"|révocation|annulation de la modification|révoqué"
    r"|rückgängig|zurückgesetzt"
    r"|revertid[oa]s?|deshech[oa]|desfeit[oa]|revertidas"
    r"|annullat[ae]|ripristinat"
    r"|отмен[аеи]|откат"
    r"|取り消し|巻き戻し|撤销|撤銷|回退|还原|還原"
    r"|wycofan|anulowan|teruggedraaid|ongedaan",
    re.IGNORECASE,
)


def parse_dt(value: str) -> float:
    """`2026-11-04T10:09:16.049Z` -> secondes epoch (float)."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def is_revert(comment: str | None) -> bool:
    return bool(comment) and _REVERT_RE.search(comment) is not None


def normalize(raw: dict[str, Any], recv_ts: float) -> dict[str, Any] | None:
    """Événement Wikimedia brut -> enregistrement compact. None si à ignorer.

    Les événements « canary » (meta.domain == "canary") servent à la supervision
    côté Wikimedia et doivent être jetés.
    """
    meta = raw.get("meta") or {}
    if meta.get("domain") == "canary":
        return None
    event_id = meta.get("id")
    wiki = raw.get("wiki")
    if not event_id or not wiki or "timestamp" not in raw:
        return None
    length = raw.get("length") or {}
    revision = raw.get("revision") or {}
    comment = raw.get("comment") or ""
    return {
        "v": SCHEMA_VERSION,
        "id": event_id,
        "rc_id": raw.get("id"),
        "wiki": wiki,
        "server": raw.get("server_name") or meta.get("domain"),
        "type": raw.get("type"),
        "ns": raw.get("namespace"),
        "title": raw.get("title") or "",
        "user": raw.get("user") or "",
        "bot": bool(raw.get("bot")),
        "minor": bool(raw.get("minor")),
        "ts": float(raw["timestamp"]),  # temps d'événement : horodatage de la modification
        "dt": parse_dt(meta["dt"]) if meta.get("dt") else None,  # émission côté Wikimedia
        "recv_ts": recv_ts,  # réception par notre producteur
        "len_old": length.get("old"),
        "len_new": length.get("new"),
        "rev_old": revision.get("old"),
        "rev_new": revision.get("new"),
        "log_type": raw.get("log_type"),
        "comment": comment[:COMMENT_MAX],
    }


def encode(record: dict[str, Any]) -> bytes:
    return json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode(value: bytes | str) -> dict[str, Any]:
    return json.loads(value)


def size_delta(record: dict[str, Any]) -> int:
    old = record.get("len_old") or 0
    new = record.get("len_new")
    if new is None:
        return 0
    return int(new) - int(old)


def page_key(record: dict[str, Any]) -> str:
    return f"{record['wiki']}:{record['title']}"
