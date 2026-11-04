"""Point d'entrée : `python -m wikistream <commande>`."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path

from . import USER_AGENT
from .config import Settings

log = logging.getLogger("wikistream")


def _setup_logging(s: Settings, verbose: bool = False) -> None:
    s.out_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    for h in (logging.StreamHandler(sys.stderr), logging.FileHandler(s.out_dir / "pipeline.log", encoding="utf-8")):
        h.setFormatter(fmt)
        root.addHandler(h)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _install_signals(stop: threading.Event) -> None:
    def handler(signum, frame):
        log.info("signal %s : arrêt propre demandé", signum)
        stop.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def _timer(seconds: float, event: threading.Event) -> None:
    t = threading.Timer(seconds, event.set)
    t.daemon = True
    t.start()


def _settings(args: argparse.Namespace) -> Settings:
    s = Settings()
    if getattr(args, "broker", None):
        s.broker = args.broker
    if getattr(args, "data", None):
        s.data_dir = Path(args.data)
    return s


def _make_ingestor(s: Settings, stop: threading.Event, metrics):
    from .broker import make_producer
    from .producer import Ingestor
    from .source import SSEClient

    client = SSEClient(s.stream_url, stop=stop, state_file=s.state_dir / "sse_last_event_id")
    return Ingestor(client.events(), make_producer(s), metrics=metrics, client=client, stop=stop), client


def _make_processor(s: Settings, stop: threading.Event, metrics):
    from .broker import make_consumer
    from .processor import Processor
    from .sink import ParquetSink

    sink = ParquetSink(s.lake_dir, dedup_horizon_hours=s.dedup_horizon_hours)
    log.info("sink : %d ids déjà présents rechargés, %d fichiers temporaires nettoyés", sink.loaded_ids, sink.removed_tmp)
    return Processor(make_consumer(s), sink, s, metrics=metrics, stop=stop)


def cmd_produce(args) -> int:
    from .metrics import MetricsLog

    s = _settings(args)
    _setup_logging(s)
    stop = threading.Event()
    _install_signals(stop)
    metrics = MetricsLog(s.out_dir / "metrics.jsonl")
    ing, client = _make_ingestor(s, stop, metrics)
    if args.duration:
        _timer(args.duration, stop)
    log.info("producteur -> %s (%s), User-Agent : %s", s.topic, s.broker, USER_AGENT)
    ing.run()
    client.close()
    return 0


def cmd_process(args) -> int:
    from .metrics import MetricsLog

    s = _settings(args)
    _setup_logging(s)
    stop = threading.Event()
    _install_signals(stop)
    metrics = MetricsLog(s.out_dir / "metrics.jsonl")
    proc = _make_processor(s, stop, metrics)
    summary = proc.run(duration=args.duration, idle_exit=args.idle_exit)
    proc.close()
    print(json.dumps({k: summary[k] for k in ("consumed", "written", "duplicates_skipped", "alerts")}, indent=1))
    return 0


def cmd_run(args) -> int:
    """Producteur (thread) + processeur (thread principal) dans un seul processus."""
    from .metrics import MetricsLog

    s = _settings(args)
    _setup_logging(s)
    stop_src = threading.Event()
    stop_proc = threading.Event()
    _install_signals(stop_src)
    metrics = MetricsLog(s.out_dir / "metrics.jsonl")
    ing, client = _make_ingestor(s, stop_src, metrics)
    proc = _make_processor(s, stop_proc, metrics)

    def produce():
        try:
            ing.run()
        finally:
            client.close()
            stop_proc.set()  # le processeur draine ce qui reste puis s'arrête

    t = threading.Thread(target=produce, name="producer", daemon=True)
    if args.duration:
        _timer(args.duration, stop_src)
    log.info("run : %s, durée %s s, broker %s", s.stream_url, args.duration or "illimitée", s.broker)
    t.start()
    summary = proc.run()
    t.join(timeout=30)
    proc.close()
    print(json.dumps({k: summary[k] for k in ("consumed", "written", "duplicates_skipped", "alerts")}, indent=1))
    return 0


def cmd_replay(args) -> int:
    """Rejoue un enregistrement JSONL (événements bruts) dans le broker, sans réseau."""
    from .broker import make_producer
    from .events import encode, normalize

    s = _settings(args)
    prod = make_producer(s)
    n = 0
    with open(args.fixture, encoding="utf-8") as f:
        for line in f:
            raw = json.loads(line)
            rec = normalize(raw, recv_ts=time.time())
            if rec is None:
                continue
            prod.produce(rec["wiki"], encode(rec))
            n += 1
    prod.flush()
    print(f"{n} événements rejoués dans {s.topic} ({s.broker})")
    return 0


def cmd_record(args) -> int:
    """Enregistre N événements bruts du flux, en retirant le champ redondant `parsedcomment`."""
    from .source import SSEClient

    stop = threading.Event()
    client = SSEClient(Settings().stream_url, stop=stop)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(out, "w", encoding="utf-8") as f:
        for ev in client.events():
            raw = json.loads(ev.data)
            if (raw.get("meta") or {}).get("domain") == "canary":
                continue
            raw.pop("parsedcomment", None)
            f.write(json.dumps(raw, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
            if n >= args.n:
                stop.set()
                break
    client.close()
    print(f"{n} événements -> {out} ({out.stat().st_size} octets)")
    return 0


def cmd_report(args) -> int:
    from .dashboard import build_report

    s = _settings(args)
    build_report(s, Path(args.docs))
    return 0


def cmd_sql(args) -> int:
    """Exécute des requêtes DuckDB sur le lac (vue `rc`)."""
    import duckdb

    from .sink import lake_glob

    s = _settings(args)
    con = duckdb.connect()
    con.execute(f"create view rc as select * from read_parquet('{lake_glob(s.lake_dir)}', hive_partitioning = true)")
    text = Path(args.file).read_text(encoding="utf-8") if args.file else args.query
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    for stmt in (x.strip() for x in "\n".join(lines).split(";")):
        if stmt:
            print(stmt.splitlines()[0][:100])
            con.sql(stmt).show(max_width=160)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="wikistream", description=__doc__)
    p.add_argument("--broker", choices=["kafka", "file"], help="défaut : $WIKISTREAM_BROKER ou kafka")
    p.add_argument("--data", help="répertoire de données (défaut : data)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("produce", help="SSE Wikimedia -> topic")
    sp.add_argument("--duration", type=float)
    sp.set_defaults(func=cmd_produce)

    sp = sub.add_parser("process", help="topic -> Parquet + fenêtres + alertes")
    sp.add_argument("--duration", type=float)
    sp.add_argument("--idle-exit", type=float, help="s'arrête après N s sans message")
    sp.set_defaults(func=cmd_process)

    sp = sub.add_parser("run", help="producteur et processeur dans le même processus")
    sp.add_argument("--duration", type=float)
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("replay", help="rejoue un fichier JSONL dans le broker")
    sp.add_argument("fixture")
    sp.set_defaults(func=cmd_replay)

    sp = sub.add_parser("record-fixture", help="enregistre N événements bruts en JSONL")
    sp.add_argument("--n", type=int, default=200)
    sp.add_argument("--out", default="tests/fixtures/recentchange_sample.jsonl")
    sp.set_defaults(func=cmd_record)

    sp = sub.add_parser("report", help="figures PNG + tableau de bord HTML statique")
    sp.add_argument("--docs", default="docs")
    sp.set_defaults(func=cmd_report)

    sp = sub.add_parser("sql", help="requêtes DuckDB sur le lac Parquet")
    sp.add_argument("query", nargs="?", default="select count(*) from rc")
    sp.add_argument("--file")
    sp.set_defaults(func=cmd_sql)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
