"""Configuration centralisée, surchargeable par variables d'environnement."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(slots=True)
class Settings:
    data_dir: Path = field(default_factory=lambda: Path(_env("WIKISTREAM_DATA", "data")))
    broker: str = field(default_factory=lambda: _env("WIKISTREAM_BROKER", "kafka"))
    bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:19092"))
    topic: str = field(default_factory=lambda: _env("WIKISTREAM_TOPIC", "wiki.recentchange"))
    group: str = field(default_factory=lambda: _env("WIKISTREAM_GROUP", "wikistream-processor"))
    partitions: int = 3
    stream_url: str = STREAM_URL

    # Traitement de flux
    max_out_of_orderness: float = 10.0  # secondes de désordre toléré avant le watermark
    idle_timeout: float = 30.0  # une partition muette depuis 30 s ne bloque plus le watermark
    allowed_lateness: float = 0.0

    # Sink
    flush_interval: float = field(default_factory=lambda: float(_env("WIKISTREAM_FLUSH_INTERVAL", "1.0")))
    flush_max_records: int = field(default_factory=lambda: int(_env("WIKISTREAM_FLUSH_MAX", "2000")))
    dedup_horizon_hours: int = 6

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake" / "recentchange"

    @property
    def broker_dir(self) -> Path:
        return self.data_dir / "broker"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def out_dir(self) -> Path:
        return self.data_dir / "out"
