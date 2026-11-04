"""Implémentation Kafka (testée avec Redpanda) via confluent-kafka / librdkafka."""

from __future__ import annotations

import logging

from confluent_kafka import Consumer as _Consumer
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka import Producer as _Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .base import Record

log = logging.getLogger(__name__)


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
    md = admin.list_topics(timeout=10)
    if topic in md.topics:
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for _, f in fut.items():
        try:
            f.result(timeout=10)
        except Exception as exc:  # topic créé entre-temps par quelqu'un d'autre
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


class KafkaProducer:
    def __init__(self, bootstrap: str, topic: str) -> None:
        self.topic = topic
        self._errors = 0
        self._p = _Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "zstd",
                "client.id": "wikistream-producer",
                "broker.address.family": "v4",
            }
        )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._errors += 1
            log.error("livraison échouée : %s", err)

    def produce(self, key: str, value: bytes) -> None:
        while True:
            try:
                self._p.produce(self.topic, key=key.encode("utf-8"), value=value, on_delivery=self._on_delivery)
                return
            except BufferError:
                self._p.poll(0.1)  # file locale pleine : on laisse partir des messages

    def poll(self, timeout: float = 0.0) -> None:
        self._p.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        return self._p.flush(timeout)

    @property
    def delivery_errors(self) -> int:
        return self._errors

    def close(self) -> None:
        self._p.flush(10)


class KafkaConsumer:
    def __init__(self, bootstrap: str, topic: str, group: str) -> None:
        self.topic = topic
        self._c = _Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "enable.auto.commit": False,  # on commite nous-mêmes, après écriture
                "auto.offset.reset": "earliest",
                "enable.partition.eof": False,
                "client.id": "wikistream-processor",
                "broker.address.family": "v4",
            }
        )
        self._c.subscribe([topic])
        self._positions: dict[int, int] = {}

    def poll_batch(self, max_records: int, timeout: float) -> list[Record]:
        msgs = self._c.consume(num_messages=max_records, timeout=timeout)
        out: list[Record] = []
        for m in msgs:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("erreur consommateur : %s", m.error())
                continue
            key = m.key().decode("utf-8") if m.key() is not None else None
            out.append(Record(m.topic(), m.partition(), m.offset(), key, m.value()))
            self._positions[m.partition()] = m.offset() + 1
        return out

    def commit(self, next_offsets: dict[int, int]) -> None:
        if not next_offsets:
            return
        tps = [TopicPartition(self.topic, p, o) for p, o in next_offsets.items()]
        self._c.commit(offsets=tps, asynchronous=False)

    def lag(self) -> int | None:
        total = 0
        for p, pos in self._positions.items():
            try:
                _, high = self._c.get_watermark_offsets(TopicPartition(self.topic, p), timeout=1, cached=False)
            except Exception:
                return None
            total += max(0, high - pos)
        return total

    def close(self) -> None:
        self._c.close()
