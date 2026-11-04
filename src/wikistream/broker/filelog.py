"""Broker de repli : un journal append-only par partition, sur disque.

Format : `<root>/<topic>/p<N>.log`, une ligne JSON par message, l'offset est le
numéro de ligne. Les offsets commités d'un groupe sont dans
`<root>/<topic>/_offsets/<group>.json`, écrits par renommage atomique, avec la
position en octets correspondante pour reprendre sans relire le journal.

Ce n'est pas un Kafka : un seul producteur à la fois, pas de réplication, pas de
rétention. Mais les garanties qui comptent pour le pipeline sont là (ordre par
partition, offsets, reprise après un kill -9).
"""

from __future__ import annotations

import json
import os
import time
import zlib
from pathlib import Path

from .base import Record


def partition_for(key: str | None, partitions: int) -> int:
    if key is None:
        return 0
    return zlib.crc32(key.encode("utf-8")) % partitions


def _log_path(root: Path, topic: str, partition: int) -> Path:
    return root / topic / f"p{partition}.log"


class FileLogProducer:
    def __init__(self, root: Path, topic: str, partitions: int = 3, fsync: bool = False) -> None:
        self.root = Path(root)
        self.topic = topic
        self.partitions = partitions
        self.fsync = fsync
        (self.root / topic).mkdir(parents=True, exist_ok=True)
        self._files = [open(_log_path(self.root, topic, p), "ab") for p in range(partitions)]
        self._errors = 0

    def produce(self, key: str, value: bytes) -> None:
        p = partition_for(key, self.partitions)
        line = json.dumps({"k": key, "v": value.decode("utf-8")}, ensure_ascii=False)
        f = self._files[p]
        f.write(line.encode("utf-8") + b"\n")

    def poll(self, timeout: float = 0.0) -> None:
        for f in self._files:
            f.flush()

    def flush(self, timeout: float = 10.0) -> int:
        for f in self._files:
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        return 0

    @property
    def delivery_errors(self) -> int:
        return self._errors

    def close(self) -> None:
        self.flush()
        for f in self._files:
            f.close()


class FileLogConsumer:
    def __init__(self, root: Path, topic: str, group: str, partitions: int = 3) -> None:
        self.root = Path(root)
        self.topic = topic
        self.group = group
        self.partitions = partitions
        self._offsets_path = self.root / topic / "_offsets" / f"{group}.json"
        committed = self._load_committed()
        self._next: dict[int, int] = {}
        self._pos: dict[int, int] = {}
        self._committed_pos: dict[int, tuple[int, int]] = {}
        for p in range(partitions):
            off, pos = committed.get(p, (0, 0))
            self._next[p] = off
            self._pos[p] = pos
            self._committed_pos[p] = (off, pos)
        # offset -> position en octets juste après ce message, pour pouvoir commiter
        self._pos_after: dict[int, dict[int, int]] = {p: {} for p in range(partitions)}
        self._rr = 0

    def _load_committed(self) -> dict[int, tuple[int, int]]:
        if not self._offsets_path.exists():
            return {}
        data = json.loads(self._offsets_path.read_text())
        return {int(p): (v["offset"], v["pos"]) for p, v in data.items()}

    def poll_batch(self, max_records: int, timeout: float = 0.0) -> list[Record]:
        out: list[Record] = []
        # tourniquet entre partitions pour ne pas affamer les petites
        order = [(self._rr + i) % self.partitions for i in range(self.partitions)]
        self._rr = (self._rr + 1) % self.partitions
        per_part = max(1, max_records // self.partitions)
        for p in order:
            path = _log_path(self.root, self.topic, p)
            if not path.exists():
                continue
            with open(path, "rb") as f:
                f.seek(self._pos[p])
                n = 0
                while n < per_part:
                    line = f.readline()
                    if not line or not line.endswith(b"\n"):
                        break  # ligne incomplète : le producteur est en train d'écrire
                    msg = json.loads(line)
                    off = self._next[p]
                    self._pos[p] += len(line)
                    self._pos_after[p][off] = self._pos[p]
                    self._next[p] = off + 1
                    out.append(Record(self.topic, p, off, msg["k"], msg["v"].encode("utf-8")))
                    n += 1
        if not out and timeout > 0:
            time.sleep(min(timeout, 0.2))
        return out

    def commit(self, next_offsets: dict[int, int]) -> None:
        for p, nxt in next_offsets.items():
            pos = self._pos_after[p].get(nxt - 1)
            if pos is None:
                continue
            self._committed_pos[p] = (nxt, pos)
            # on oublie les positions déjà commitées
            self._pos_after[p] = {o: v for o, v in self._pos_after[p].items() if o >= nxt}
        self._offsets_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._offsets_path.with_suffix(".tmp")
        payload = {str(p): {"offset": o, "pos": pos} for p, (o, pos) in self._committed_pos.items()}
        with open(tmp, "w") as f:
            json.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._offsets_path)

    def committed(self) -> dict[int, int]:
        return {p: o for p, (o, _) in self._committed_pos.items()}

    def lag(self) -> int | None:
        total = 0
        for p in range(self.partitions):
            path = _log_path(self.root, self.topic, p)
            if path.exists():
                total += max(0, path.stat().st_size - self._pos[p])
        return total  # en octets pour ce broker

    def close(self) -> None:
        pass
