"""Bout en bout sans réseau : fixture -> broker fichier -> processeur -> Parquet + fenêtres."""

from __future__ import annotations

import json

import duckdb

from wikistream.broker.filelog import FileLogConsumer, FileLogProducer
from wikistream.config import Settings
from wikistream.events import encode
from wikistream.processor import Processor
from wikistream.sink import ParquetSink, lake_glob


def test_fixture_end_to_end(tmp_path, records):
    s = Settings(data_dir=tmp_path, broker="file")
    prod = FileLogProducer(s.broker_dir, s.topic, s.partitions)
    for r in records:
        prod.produce(r["wiki"], encode(r))
    prod.close()

    consumer = FileLogConsumer(s.broker_dir, s.topic, s.group, s.partitions)
    proc = Processor(consumer, ParquetSink(s.lake_dir), s)
    summary = proc.run(idle_exit=0.5)
    proc.close()

    assert summary["consumed"] == summary["written"] == len(records)
    assert summary["duplicates_skipped"] == 0
    assert sum(consumer.committed().values()) == len(records)

    con = duckdb.connect()
    q = f"select wiki, count(*) n, sum(bot::int) bots from read_parquet('{lake_glob(s.lake_dir)}', hive_partitioning=true) group by wiki order by n desc"
    rows = con.sql(q).fetchall()
    assert sum(n for _, n, _ in rows) == len(records)
    by_wiki = {}
    for r in records:
        by_wiki[r["wiki"]] = by_wiki.get(r["wiki"], 0) + 1
    assert dict((w, n) for w, n, _ in rows) == by_wiki

    # partition horaire lisible par DuckDB (colonnes de partition Hive)
    hours = con.sql(f"select distinct date, hour from read_parquet('{lake_glob(s.lake_dir)}', hive_partitioning=true)").fetchall()
    assert len(hours) >= 1

    # fenêtres 1 min : émises (ou vidées en fin de run) et cohérentes avec le lac
    lines = [json.loads(x) for x in (s.out_dir / "windows_wiki_1m.jsonl").read_text().splitlines()]
    assert sum(x["events"] for x in lines) == len(records)
    sketch = json.loads((s.out_dir / "sketch_top_pages.json").read_text())
    assert sketch["n_edits"] == sum(r["type"] in ("edit", "new") for r in records)


def test_rerun_on_same_log_is_noop(tmp_path, records):
    s = Settings(data_dir=tmp_path, broker="file")
    prod = FileLogProducer(s.broker_dir, s.topic, s.partitions)
    for r in records:
        prod.produce(r["wiki"], encode(r))
    prod.close()
    for _ in range(2):
        proc = Processor(FileLogConsumer(s.broker_dir, s.topic, s.group), ParquetSink(s.lake_dir), s)
        summary = proc.run(idle_exit=0.3)
        proc.close()
    assert summary["consumed"] == 0  # offsets commités : rien à relire
    # nouveau groupe : relit tout, mais le sink ne réécrit rien
    proc = Processor(FileLogConsumer(s.broker_dir, s.topic, "autre-groupe"), ParquetSink(s.lake_dir), s)
    summary = proc.run(idle_exit=0.3)
    proc.close()
    assert summary["consumed"] == len(records) and summary["written"] == 0
    assert summary["duplicates_skipped"] == len(records)


def test_verify_command_detects_loss(tmp_path, records, capsys):
    from wikistream.cli import main

    s = Settings(data_dir=tmp_path, broker="file")
    prod = FileLogProducer(s.broker_dir, s.topic, s.partitions)
    for r in records:
        prod.produce(r["wiki"], encode(r))
    prod.close()
    proc = Processor(FileLogConsumer(s.broker_dir, s.topic, s.group), ParquetSink(s.lake_dir), s)
    proc.run(idle_exit=0.3)
    proc.close()
    assert main(["--broker", "file", "--data", str(tmp_path), "verify"]) == 0
    # un message arrivé dans le topic mais jamais écrit : verify doit échouer
    prod = FileLogProducer(s.broker_dir, s.topic, s.partitions)
    prod.produce(records[0]["wiki"], encode({**records[0], "id": "jamais-ecrit"}))
    prod.close()
    assert main(["--broker", "file", "--data", str(tmp_path), "verify"]) == 1
    assert '"missing_in_lake": 1' in capsys.readouterr().out


def test_backtest_replays_a_lake(tmp_path, records):
    from wikistream.cli import main

    s = Settings(data_dir=tmp_path / "live", broker="file")
    prod = FileLogProducer(s.broker_dir, s.topic, s.partitions)
    for r in records:
        prod.produce(r["wiki"], encode(r))
    prod.close()
    proc = Processor(FileLogConsumer(s.broker_dir, s.topic, s.group), ParquetSink(s.lake_dir), s)
    proc.run(idle_exit=0.3)
    proc.close()
    out = tmp_path / "rejeu"
    assert main(["backtest", str(s.lake_dir), "--out", str(out)]) == 0
    con = duckdb.connect()
    n = con.sql(f"select count(*), count(distinct id) from read_parquet('{lake_glob(out / 'lake' / 'recentchange')}')").fetchall()[0]
    assert n == (len(records), len(records))
    assert main(["backtest", str(s.lake_dir), "--out", str(out)]) == 2  # refuse un répertoire non vide
