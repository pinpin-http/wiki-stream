"""Fabrique de brokers : `kafka` (Redpanda) ou `file` (repli sans Docker)."""

from __future__ import annotations

from ..config import Settings
from .base import Consumer, Producer, Record
from .filelog import FileLogConsumer, FileLogProducer

__all__ = ["Consumer", "Producer", "Record", "make_consumer", "make_producer"]


def make_producer(s: Settings) -> Producer:
    if s.broker == "file":
        return FileLogProducer(s.broker_dir, s.topic, s.partitions)
    if s.broker == "kafka":
        from .kafka import KafkaProducer, ensure_topic

        ensure_topic(s.bootstrap, s.topic, s.partitions)
        return KafkaProducer(s.bootstrap, s.topic)
    raise ValueError(f"broker inconnu : {s.broker}")


def make_consumer(s: Settings) -> Consumer:
    if s.broker == "file":
        return FileLogConsumer(s.broker_dir, s.topic, s.group, s.partitions)
    if s.broker == "kafka":
        from .kafka import KafkaConsumer, ensure_topic

        ensure_topic(s.bootstrap, s.topic, s.partitions)
        return KafkaConsumer(s.bootstrap, s.topic, s.group)
    raise ValueError(f"broker inconnu : {s.broker}")
