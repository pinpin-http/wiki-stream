"""Interface commune des brokers.

Deux implémentations : Kafka/Redpanda (`kafka.py`) et un journal de fichiers
append-only (`filelog.py`) qui sert de repli sans Docker et de support aux tests.
Le reste du code ne voit que ces deux protocoles.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(slots=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes


class Producer(Protocol):
    def produce(self, key: str, value: bytes) -> None: ...

    def poll(self, timeout: float = 0.0) -> None: ...

    def flush(self, timeout: float = 10.0) -> int:
        """Attend la livraison, renvoie le nombre de messages encore en vol."""
        ...

    @property
    def delivery_errors(self) -> int: ...


class Consumer(Protocol):
    def poll_batch(self, max_records: int, timeout: float) -> list[Record]: ...

    def commit(self, next_offsets: dict[int, int]) -> None:
        """Commite, par partition, l'offset du PROCHAIN message à lire."""
        ...

    def lag(self) -> int | None: ...

    def close(self) -> None: ...
