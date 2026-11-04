"""Client SSE : parsing conforme à la spec et reprise avec Last-Event-ID (sans réseau)."""

from __future__ import annotations

import json
import threading

import httpx

from wikistream import USER_AGENT
from wikistream.events import normalize
from wikistream.source import SSEClient, SSEParser


def feed_all(parser: SSEParser, text: str):
    return [ev for line in text.split("\n") if (ev := parser.feed(line)) is not None]


def test_parser_multiline_comments_and_id():
    p = SSEParser()
    evs = feed_all(p, ":ok\n\nevent: message\nid: [1]\ndata: {\"a\":\ndata: 1}\n\nretry: 500\ndata: x\n\n")
    assert [e.data for e in evs] == ['{"a":\n1}', "x"]
    assert evs[0].id == "[1]" and evs[1].id == "[1]"  # l'id persiste tant qu'il n'est pas remplacé
    assert p.retry_ms == 500


def test_parser_ignores_empty_dispatch_and_strips_one_space():
    p = SSEParser()
    assert feed_all(p, "\n\n:commentaire\n\n") == []
    evs = feed_all(p, "data:  deux espaces\n\n")
    assert evs[0].data == " deux espaces"


def test_reconnect_sends_last_event_id_and_user_agent(raw_events):
    seen_headers = []
    first, second = raw_events[:3], raw_events[3:5]

    def body(events, start_id):
        chunks = [":ok\n\n"]
        for i, e in enumerate(events):
            chunks.append(f"event: message\nid: [{{\"offset\":{start_id + i}}}]\ndata: {json.dumps(e)}\n\n")
        return "".join(chunks)

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(dict(request.headers))
        if len(seen_headers) == 1:
            return httpx.Response(200, text=body(first, 0))  # le serveur coupe après 3 événements
        if len(seen_headers) == 2:
            return httpx.Response(503, text="busy")  # une erreur transitoire
        return httpx.Response(200, text=body(second, 3))

    stop = threading.Event()
    client = SSEClient("https://stream.test/v2/stream/recentchange", transport=httpx.MockTransport(handler),
                       stop=stop, max_backoff=0.01)
    client_backoff_fast(client)
    got = []
    for ev in client.events():
        got.append(json.loads(ev.data)["meta"]["id"])
        if len(got) == 5:
            stop.set()
            break
    assert got == [e["meta"]["id"] for e in raw_events[:5]]
    assert "last-event-id" not in seen_headers[0]
    assert seen_headers[1]["last-event-id"] == '[{"offset":2}]'
    assert seen_headers[2]["last-event-id"] == '[{"offset":2}]'
    assert all(h["user-agent"] == USER_AGENT for h in seen_headers)
    assert "@" not in USER_AGENT  # la politique Wikimedia : UA descriptif, pas d'email ici
    assert client.reconnects >= 1


def client_backoff_fast(client: SSEClient) -> None:
    # stop.wait(backoff) avec backoff initial 1 s : on raccourcit pour le test
    orig_wait = client.stop.wait
    client.stop.wait = lambda t=None: orig_wait(0.001)  # type: ignore[method-assign]


def test_state_file_resume(tmp_path):
    state = tmp_path / "last_id"
    state.write_text('[{"offset":42}]')
    c = SSEClient("https://stream.test/x", state_file=state, transport=httpx.MockTransport(lambda r: httpx.Response(204)))
    assert c.last_event_id == '[{"offset":42}]'
    c.last_event_id = '[{"offset":43}]'
    c.close()
    assert state.read_text() == '[{"offset":43}]'


def test_normalize_drops_canary_and_keeps_event_time(raw_events):
    raw = dict(raw_events[0])
    rec = normalize(raw, recv_ts=123.0)
    assert rec["ts"] == float(raw["timestamp"]) and rec["recv_ts"] == 123.0
    assert rec["id"] == raw["meta"]["id"]
    canary = {**raw, "meta": {**raw["meta"], "domain": "canary"}}
    assert normalize(canary, recv_ts=0) is None
