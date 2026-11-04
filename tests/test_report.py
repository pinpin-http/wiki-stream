"""Le rapport se génère hors ligne à partir d'un petit run sur la fixture."""

from __future__ import annotations

import json

from wikistream.broker.filelog import FileLogConsumer, FileLogProducer
from wikistream.config import Settings
from wikistream.dashboard import build_report
from wikistream.events import encode
from wikistream.processor import Processor
from wikistream.sink import ParquetSink


def test_report_on_fixture(tmp_path, records):
    s = Settings(data_dir=tmp_path / "data", broker="file")
    prod = FileLogProducer(s.broker_dir, s.topic, s.partitions)
    for r in records:
        prod.produce(r["wiki"], encode(r))
    prod.close()
    proc = Processor(FileLogConsumer(s.broker_dir, s.topic, s.group), ParquetSink(s.lake_dir), s)
    proc.run(idle_exit=0.3)
    proc.close()

    docs = tmp_path / "docs"
    r = build_report(s, docs)
    assert r["events"] == len(records) and r["duplicates_in_lake"] == 0
    assert r["sketch"]["all_bounds_hold"]
    for name in ("dashboard.html", "throughput.png", "latency.png", "wikis.png", "delay_tail.png", "results.json"):
        assert (docs / name).stat().st_size > 0
    assert json.loads((docs / "results.json").read_text())["events"] == len(records)
    page = (docs / "dashboard.html").read_text()
    assert "<script" not in page  # page statique, rien à exécuter
