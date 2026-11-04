"""Intégration avec un vrai broker (Redpanda). Ignoré sauf si KAFKA_BOOTSTRAP est défini.

    make up && KAFKA_BOOTSTRAP=localhost:19092 .venv/bin/python -m pytest -m kafka
"""

from __future__ import annotations

import os
import uuid

import pytest

from wikistream.config import Settings
from wikistream.events import encode
from wikistream.processor import Processor
from wikistream.sink import ParquetSink

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP non défini (pas de broker)"),
]


def test_redpanda_commit_after_write_and_resume(tmp_path, records):
    from confluent_kafka.admin import AdminClient

    from wikistream.broker.kafka import KafkaConsumer, KafkaProducer, ensure_topic

    topic = f"wikistream-test-{uuid.uuid4().hex[:8]}"
    group = f"g-{topic}"
    ensure_topic(BOOTSTRAP, topic, 3)
    try:
        prod = KafkaProducer(BOOTSTRAP, topic)
        for r in records:
            prod.produce(r["wiki"], encode(r))
        assert prod.flush(15) == 0 and prod.delivery_errors == 0

        s = Settings(data_dir=tmp_path, broker="kafka", bootstrap=BOOTSTRAP, topic=topic, group=group)
        proc = Processor(KafkaConsumer(BOOTSTRAP, topic, group), ParquetSink(s.lake_dir), s)
        summary = proc.run(idle_exit=15)
        proc.close()
        assert summary["written"] == len(records) and summary["duplicates_skipped"] == 0

        # même groupe : les offsets sont commités, il ne reste rien à lire
        proc = Processor(KafkaConsumer(BOOTSTRAP, topic, group), ParquetSink(s.lake_dir), s)
        summary = proc.run(idle_exit=8)
        proc.close()
        assert summary["consumed"] == 0
    finally:
        AdminClient({"bootstrap.servers": BOOTSTRAP, "broker.address.family": "v4"}).delete_topics([topic])
