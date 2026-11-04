"""Sink Parquet partitionné par heure (temps d'événement), écritures idempotentes.

Garantie visée : au moins une fois côté broker, effectivement une seule fois dans
le lac. Le consommateur ne commite ses offsets qu'APRÈS l'écriture durable des
fichiers, donc un crash rejoue au pire le dernier lot. Le rejeu ne crée pas de
doublon parce que :

1. chaque fichier est écrit dans un fichier temporaire, fsync, puis renommé
   (`os.replace` est atomique) : un fichier visible est complet ;
2. au démarrage, le sink relit la colonne `id` des partitions récentes et
   reconstruit l'ensemble des identifiants déjà écrits ;
3. tout enregistrement dont l'`id` (meta.id, UUID Wikimedia) est déjà connu est
   écarté, qu'il vienne d'un rejeu Kafka ou d'une reprise SSE avec Last-Event-ID.

Le nom de fichier est dérivé du contenu (hachage des ids), donc réécrire le même
lot écrase le même fichier au lieu d'en créer un second.
"""

from __future__ import annotations

import hashlib
import os
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("rc_id", pa.int64()),
        ("wiki", pa.string()),
        ("server", pa.string()),
        ("type", pa.string()),
        ("ns", pa.int32()),
        ("title", pa.string()),
        ("user", pa.string()),
        ("bot", pa.bool_()),
        ("minor", pa.bool_()),
        ("event_time", pa.timestamp("ms", tz="UTC")),
        ("meta_dt", pa.timestamp("ms", tz="UTC")),
        ("recv_time", pa.timestamp("ms", tz="UTC")),
        ("write_time", pa.timestamp("ms", tz="UTC")),
        ("len_old", pa.int64()),
        ("len_new", pa.int64()),
        ("rev_old", pa.int64()),
        ("rev_new", pa.int64()),
        ("log_type", pa.string()),
        ("comment", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
    ]
)


def _ms(ts: float | None) -> int | None:
    return None if ts is None else int(round(ts * 1000))


def hour_partition(ts: float) -> tuple[str, str]:
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    return d.strftime("%Y-%m-%d"), d.strftime("%H")


@dataclass(slots=True)
class WriteResult:
    written: list[dict[str, Any]]
    duplicates: int
    files: list[Path]
    write_time: float


class CrashInjected(RuntimeError):
    pass


class ParquetSink:
    def __init__(self, root: Path, *, dedup_horizon_hours: int = 6, crash_after_files: int | None = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.dedup_horizon_hours = dedup_horizon_hours
        # injection de panne pour les tests : os._exit juste après le N-ième fichier
        env = os.environ.get("WIKISTREAM_CRASH_AFTER_FILES")
        self.crash_after_files = crash_after_files if crash_after_files is not None else (int(env) if env else None)
        self.files_written = 0
        self._seen: dict[str, set[str]] = {}  # "YYYY-MM-DD/HH" -> ids
        # un crash pendant l'écriture laisse un .tmp-* incomplet : jamais lu, supprimé ici
        self.removed_tmp = self.cleanup_tmp()
        self.loaded_ids = self._load_seen()

    def _partition_dirs(self) -> list[tuple[str, Path]]:
        out = []
        for date_dir in self.root.glob("date=*"):
            for hour_dir in date_dir.glob("hour=*"):
                tag = f"{date_dir.name[5:]}/{hour_dir.name[5:]}"
                out.append((tag, hour_dir))
        return sorted(out)

    def _load_seen(self) -> int:
        dirs = self._partition_dirs()[-self.dedup_horizon_hours :] if self.dedup_horizon_hours else []
        n = 0
        for tag, d in dirs:
            ids = self._seen.setdefault(tag, set())
            for f in d.glob("part-*.parquet"):
                col = pq.read_table(f, columns=["id"]).column("id").to_pylist()
                ids.update(col)
                n += len(col)
        return n

    def is_seen(self, event_id: str) -> bool:
        return any(event_id in s for s in self._seen.values())

    def _evict(self) -> None:
        if len(self._seen) > self.dedup_horizon_hours:
            for tag in sorted(self._seen)[: len(self._seen) - self.dedup_horizon_hours]:
                del self._seen[tag]

    def write_batch(self, rows: list[dict[str, Any]]) -> WriteResult:
        """`rows` : enregistrements normalisés + `_partition` / `_offset`."""
        write_time = time.time()
        fresh: list[dict[str, Any]] = []
        batch_ids: set[str] = set()
        dups = 0
        for r in rows:
            rid = r["id"]
            if rid in batch_ids or self.is_seen(rid):
                dups += 1
                continue
            batch_ids.add(rid)
            fresh.append(r)
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for r in fresh:
            groups[hour_partition(r["ts"])].append(r)
        files = []
        for (date, hour), grp in sorted(groups.items()):
            path = self._write_file(date, hour, grp, write_time)
            files.append(path)
            self._seen.setdefault(f"{date}/{hour}", set()).update(r["id"] for r in grp)
            self.files_written += 1
            if self.crash_after_files is not None and self.files_written >= self.crash_after_files:
                os._exit(77)  # simulation d'un kill brutal : ni flush, ni commit
        self._evict()
        return WriteResult(fresh, dups, files, write_time)

    def _write_file(self, date: str, hour: str, rows: list[dict[str, Any]], write_time: float) -> Path:
        d = self.root / f"date={date}" / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha1("\n".join(sorted(r["id"] for r in rows)).encode()).hexdigest()[:16]
        final = d / f"part-{digest}.parquet"
        table = pa.Table.from_pydict(
            {
                "id": [r["id"] for r in rows],
                "rc_id": [r.get("rc_id") for r in rows],
                "wiki": [r["wiki"] for r in rows],
                "server": [r.get("server") for r in rows],
                "type": [r.get("type") for r in rows],
                "ns": [r.get("ns") for r in rows],
                "title": [r.get("title") for r in rows],
                "user": [r.get("user") for r in rows],
                "bot": [r.get("bot") for r in rows],
                "minor": [r.get("minor") for r in rows],
                "event_time": [_ms(r["ts"]) for r in rows],
                "meta_dt": [_ms(r.get("dt")) for r in rows],
                "recv_time": [_ms(r.get("recv_ts")) for r in rows],
                "write_time": [_ms(write_time)] * len(rows),
                "len_old": [r.get("len_old") for r in rows],
                "len_new": [r.get("len_new") for r in rows],
                "rev_old": [r.get("rev_old") for r in rows],
                "rev_new": [r.get("rev_new") for r in rows],
                "log_type": [r.get("log_type") for r in rows],
                "comment": [r.get("comment") for r in rows],
                "kafka_partition": [r.get("_partition") for r in rows],
                "kafka_offset": [r.get("_offset") for r in rows],
            },
            schema=SCHEMA,
        )
        tmp = d / f".tmp-{uuid.uuid4().hex}.parquet"
        pq.write_table(table, tmp, compression="zstd")
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, final)
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        return final

    def cleanup_tmp(self) -> int:
        """Supprime les fichiers temporaires laissés par un crash (jamais lus par les requêtes)."""
        n = 0
        for f in self.root.glob("date=*/hour=*/.tmp-*.parquet"):
            f.unlink(missing_ok=True)
            n += 1
        return n


def lake_glob(root: Path) -> str:
    return str(Path(root) / "date=*" / "hour=*" / "part-*.parquet")
