"""Sink Parquet : idempotence, et consommateur tué en plein travail sans perte ni doublon."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from wikistream.broker.filelog import FileLogConsumer, FileLogProducer
from wikistream.events import encode, normalize
from wikistream.sink import ParquetSink, lake_glob

ROOT = Path(__file__).resolve().parents[1]


def with_offsets(recs):
    return [{**r, "_partition": 0, "_offset": i} for i, r in enumerate(recs)]


def lake_ids(root: Path) -> list[str]:
    files = list(root.glob("date=*/hour=*/part-*.parquet"))
    if not files:
        return []
    return duckdb.sql(f"select id from read_parquet('{lake_glob(root)}', hive_partitioning=true)").fetchnumpy()["id"].tolist()


def test_write_is_idempotent_within_and_across_instances(tmp_path, records):
    rows = with_offsets(records)
    sink = ParquetSink(tmp_path)
    r1 = sink.write_batch(rows + rows[:10])  # doublons dans le même lot
    assert len(r1.written) == len(records) and r1.duplicates == 10
    r2 = sink.write_batch(rows)  # rejeu complet
    assert r2.written == [] and r2.duplicates == len(rows) and r2.files == []
    # un nouveau sink (redémarrage) recharge les ids depuis les fichiers
    sink2 = ParquetSink(tmp_path)
    assert sink2.loaded_ids == len(records)
    assert sink2.write_batch(rows).written == []
    ids = lake_ids(tmp_path)
    assert len(ids) == len(set(ids)) == len(records)


def test_files_are_content_addressed_and_hour_partitioned(tmp_path, records):
    rows = with_offsets(records)
    shifted = [{**r, "id": r["id"] + "-h", "ts": r["ts"] + 3600} for r in rows]
    sink = ParquetSink(tmp_path)
    res = sink.write_batch(rows + shifted)
    hours = sorted(p.parent.name for p in res.files)
    assert len(hours) == 2 and hours[0] != hours[1]
    # réécrire le même contenu dans un autre répertoire donne le même nom de fichier
    other = ParquetSink(tmp_path / "bis").write_batch(rows)
    assert {p.name for p in other.files} <= {p.name for p in res.files}
    t = pq.read_table(res.files[0])
    assert t.schema.field("event_time").type.tz == "UTC"
    assert not list(tmp_path.glob("**/.tmp-*"))


def build_broker(data: Path, raw_events, copies: int, injected_dups: int) -> set[str]:
    """Fabrique un journal de `copies` x fixture, étalé sur plusieurs heures, + des doublons."""
    prod = FileLogProducer(data / "broker", "wiki.recentchange", 3)
    ids: set[str] = set()
    first = []
    for j in range(copies):
        for raw in raw_events:
            r = json.loads(json.dumps(raw))
            r["meta"]["id"] = f"{raw['meta']['id']}-{j}"
            r["timestamp"] = raw["timestamp"] + j * 600
            rec = normalize(r, recv_ts=time.time())
            prod.produce(rec["wiki"], encode(rec))
            ids.add(rec["id"])
            if len(first) < injected_dups:
                first.append(rec)
    for rec in first:  # même id, nouvel offset : un rejeu SSE après reconnexion
        prod.produce(rec["wiki"], encode(rec))
    prod.close()
    return ids


def run_consumer(data: Path, env_extra: dict | None = None, wait: bool = True):
    env = {**os.environ, "WIKISTREAM_FLUSH_MAX": "100", "WIKISTREAM_FLUSH_INTERVAL": "0.05",
           "PYTHONPATH": str(ROOT / "src"), "MPLBACKEND": "Agg", **(env_extra or {})}
    cmd = [sys.executable, "-m", "wikistream", "--broker", "file", "--data", str(data), "process", "--idle-exit", "1.0"]
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if wait:
        _, p.err = p.communicate(timeout=120)
    return p


def committed(data: Path) -> dict:
    return FileLogConsumer(data / "broker", "wiki.recentchange", "wikistream-processor").committed()


def test_crash_between_write_and_commit(tmp_path, raw_events):
    """Le cas difficile : fichiers renommés, offsets pas encore commités."""
    expected = build_broker(tmp_path, raw_events, copies=8, injected_dups=40)
    p = run_consumer(tmp_path, {"WIKISTREAM_CRASH_AFTER_FILES": "5"})
    assert p.returncode == 77
    lake = tmp_path / "lake" / "recentchange"
    written_before = lake_ids(lake)
    assert 0 < len(written_before) < len(expected)
    assert sum(committed(tmp_path).values()) < len(written_before)  # le dernier lot n'est pas commité

    p = run_consumer(tmp_path)
    assert p.returncode == 0, p.err.decode()[-3000:]
    ids = lake_ids(lake)
    assert len(ids) == len(set(ids)), "doublons dans le lac"
    assert set(ids) == expected, "événements perdus"
    summary = json.loads((tmp_path / "out" / "run_summary.json").read_text())
    assert summary["duplicates_skipped"] >= 40


def test_sigkill_mid_run_no_loss_no_duplicate(tmp_path, raw_events):
    expected = build_broker(tmp_path, raw_events, copies=30, injected_dups=25)
    kills = 0
    for threshold in (4, 12):
        p = run_consumer(tmp_path, wait=False)
        deadline = time.time() + 60
        while time.time() < deadline and p.poll() is None:
            if len(list(tmp_path.glob("lake/recentchange/date=*/hour=*/part-*.parquet"))) >= threshold:
                p.send_signal(signal.SIGKILL)
                break
            time.sleep(0.005)
        p.communicate(timeout=60)
        kills += p.returncode == -signal.SIGKILL
    p = run_consumer(tmp_path)
    assert p.returncode == 0, p.err.decode()[-3000:]
    if kills == 0:
        pytest.skip("le consommateur a fini avant d'être tué (machine trop rapide)")
    ids = lake_ids(tmp_path / "lake" / "recentchange")
    assert len(ids) == len(set(ids)), "doublons dans le lac"
    assert set(ids) == expected, "événements perdus"


def test_restart_ignores_and_removes_partial_tmp_file(tmp_path, records):
    sink = ParquetSink(tmp_path)
    res = sink.write_batch(with_offsets(records))
    hour_dir = res.files[0].parent
    (hour_dir / ".tmp-deadbeef.parquet").write_bytes(b"PAR1 tronque")  # kill pendant l'écriture
    sink2 = ParquetSink(tmp_path)
    assert sink2.removed_tmp == 1 and sink2.loaded_ids == len(records)
    assert not list(hour_dir.glob(".tmp-*"))
