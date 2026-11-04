"""Client SSE (Server-Sent Events) pour Wikimedia EventStreams.

Le parseur suit la spec WHATWG : champs `data`, `id`, `event`, `retry`, lignes de
commentaire `:` et dispatch sur ligne vide. Le client se reconnecte indéfiniment
avec un backoff exponentiel et renvoie l'en-tête `Last-Event-ID` reçu en dernier :
EventStreams reprend alors juste après (les éventuels rejoués sont dédoublonnés
plus loin, au niveau du sink, par identifiant d'événement).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx

from . import USER_AGENT

log = logging.getLogger(__name__)


@dataclass(slots=True)
class SSEEvent:
    data: str
    id: str | None
    event: str = "message"


class SSEParser:
    """Parseur incrémental ligne par ligne."""

    def __init__(self) -> None:
        self._data: list[str] = []
        self._event = ""
        self.last_id: str | None = None
        self.retry_ms: int | None = None

    def feed(self, line: str) -> SSEEvent | None:
        if line == "":
            if not self._data:
                self._event = ""
                return None
            ev = SSEEvent(data="\n".join(self._data), id=self.last_id, event=self._event or "message")
            self._data = []
            self._event = ""
            return ev
        if line.startswith(":"):
            return None
        name, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if name == "data":
            self._data.append(value)
        elif name == "id":
            if "\x00" not in value:
                self.last_id = value
        elif name == "event":
            self._event = value
        elif name == "retry" and value.isdigit():
            self.retry_ms = int(value)
        return None


class SSEClient:
    def __init__(
        self,
        url: str,
        *,
        last_event_id: str | None = None,
        user_agent: str = USER_AGENT,
        transport: httpx.BaseTransport | None = None,
        read_timeout: float = 30.0,
        max_backoff: float = 30.0,
        stop: threading.Event | None = None,
        state_file: Path | None = None,
    ) -> None:
        self.url = url
        self.last_event_id = last_event_id
        self.user_agent = user_agent
        self.read_timeout = read_timeout
        self.max_backoff = max_backoff
        self.stop = stop or threading.Event()
        self.state_file = state_file
        self.connects = 0
        self.reconnects = 0
        self.errors = 0
        self._client = httpx.Client(
            transport=transport,
            timeout=httpx.Timeout(10.0, read=read_timeout),
            headers={"User-Agent": user_agent, "Accept": "text/event-stream"},
        )
        if last_event_id is None and state_file and state_file.exists():
            self.last_event_id = state_file.read_text().strip() or None

    def save_state(self) -> None:
        if self.state_file and self.last_event_id:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(self.last_event_id)
            tmp.replace(self.state_file)

    def events(self) -> Iterator[SSEEvent]:
        backoff = 1.0
        while not self.stop.is_set():
            headers = {}
            if self.last_event_id:
                headers["Last-Event-ID"] = self.last_event_id
            parser = SSEParser()
            try:
                with self._client.stream("GET", self.url, headers=headers) as resp:
                    if resp.status_code != 200:
                        raise httpx.HTTPStatusError(
                            f"HTTP {resp.status_code}", request=resp.request, response=resp
                        )
                    self.connects += 1
                    if self.connects > 1:
                        self.reconnects += 1
                    log.info("SSE connecté (Last-Event-ID=%s)", bool(self.last_event_id))
                    for line in resp.iter_lines():
                        ev = parser.feed(line)
                        if ev is None:
                            continue
                        backoff = 1.0
                        if ev.id:
                            self.last_event_id = ev.id
                        yield ev
                        if self.stop.is_set():
                            return
                # Fin de flux propre côté serveur : on se reconnecte.
                log.warning("SSE : flux terminé par le serveur, reconnexion")
            except (httpx.HTTPError, httpx.StreamError) as exc:
                self.errors += 1
                log.warning("SSE : %s, nouvelle tentative dans %.0f s", exc, backoff)
            if parser.retry_ms:
                backoff = max(backoff, parser.retry_ms / 1000)
            if self.stop.wait(backoff):
                return
            backoff = min(backoff * 2, self.max_backoff)

    def close(self) -> None:
        self.save_state()
        self._client.close()
