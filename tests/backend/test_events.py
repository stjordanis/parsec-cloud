import pytest
import trio
from uuid import uuid4
from async_generator import asynccontextmanager

from parsec.api.protocole import events_subscribe_serializer, events_listen_serializer


BEACON_ID = uuid4()


async def events_subscribe(sock, **kwargs):
    await sock.send(events_subscribe_serializer.req_dump({"cmd": "events_subscribe", **kwargs}))
    raw_rep = await sock.recv()
    rep = events_subscribe_serializer.rep_load(raw_rep)
    assert rep == {"status": "ok"}


async def events_listen_nowait(sock):
    await sock.send(events_listen_serializer.req_dump({"cmd": "events_listen", "wait": False}))
    with trio.fail_after(1):
        raw_rep = await sock.recv()
    return events_listen_serializer.rep_load(raw_rep)


class Listen:
    def __init__(self):
        self.rep = None


@asynccontextmanager
async def events_listen(sock):
    await sock.send(events_listen_serializer.req_dump({"cmd": "events_listen"}))
    listen = Listen()

    yield listen

    with trio.fail_after(1):
        raw_rep = await sock.recv()
    listen.rep = events_listen_serializer.rep_load(raw_rep)


@pytest.mark.trio
@pytest.mark.parametrize(
    "events",
    [
        {"pinged": [], "beacon_updated": [], "message_received": False},
        {"pinged": ["foo"], "beacon_updated": [BEACON_ID], "message_received": True},
        {"beacon_updated": [BEACON_ID], "message_received": True},
        {"pinged": ["foo"], "message_received": True},
        {"pinged": ["foo"], "beacon_updated": [BEACON_ID]},
        {},
    ],
)
async def test_events_subscribe_ok(alice_backend_sock, events):
    await events_subscribe(alice_backend_sock, **events)


@pytest.mark.trio
@pytest.mark.parametrize(
    "events",
    [
        {"dummy": []},
        {"pinged": [42]},
        {"beacon.updated": ["dummy"]},
        {"pinged": ["a" * 100]},  # Too long
        {"message.received": []},
    ],
)
async def test_events_subscribe_bad_msg(alice_backend_sock, events):
    await alice_backend_sock.send({"cmd": "events_subscribe", **events})
    raw_rep = await alice_backend_sock.recv()
    rep = events_subscribe_serializer.rep_load(raw_rep)
    assert rep["status"] == "bad_message"


async def subscribe_pinged(sock, pings):
    await sock.send({"cmd": "events_subscribe", "pinged": pings})
    rep = await sock.recv()
    assert rep == {"status": "ok"}


async def ping(sock, subject):
    await sock.send({"cmd": "ping", "ping": subject})
    rep = await sock.recv()
    assert rep == {"status": "ok", "pong": subject}


async def get_pinged_events(sock):
    # There is no guarantee an event is ready to be received once
    # the sender got it answer (this is true for the in memory stub
    # but not when testing against PostgreSQL).
    # TODO: find a better way to wait for event to be dispatched by PostgreSQL
    await trio.sleep(0.1)
    events = []
    while True:
        rep = await events_listen_nowait(sock)
        if rep["status"] == "no_events":
            return events
        assert rep["status"] == "ok"
        assert rep["event"] == "pinged"
        events.append(rep["ping"])


@pytest.mark.trio
async def test_events_subscribe_ping(alice_backend_sock, alice2_backend_sock):
    await subscribe_pinged(alice_backend_sock, ["foo", "bar"])

    # Should ignore our own events
    await ping(alice2_backend_sock, "nope")
    await ping(alice_backend_sock, "bar")
    await ping(alice2_backend_sock, "foo")

    events = await get_pinged_events(alice_backend_sock)

    assert events == ["foo"]


@pytest.mark.trio
async def test_event_resubscribe(alice_backend_sock, alice2_backend_sock):
    await subscribe_pinged(alice_backend_sock, ["foo", "bar"])

    await ping(alice2_backend_sock, "foo")

    await subscribe_pinged(alice_backend_sock, ["bar", "spam"])

    await ping(alice2_backend_sock, "foo")
    await ping(alice2_backend_sock, "bar")
    await ping(alice2_backend_sock, "spam")

    events = await get_pinged_events(alice_backend_sock)

    assert events == ["foo", "bar", "spam"]


@pytest.mark.trio
@pytest.mark.postgresql
async def test_cross_backend_event(backend_factory, backend_sock_factory, alice, bob):
    async with backend_factory() as backend_1, backend_factory(devices=()) as backend_2:
        async with backend_sock_factory(backend_1, alice) as alice_sock, backend_sock_factory(
            backend_2, bob
        ) as bob_sock:

            await subscribe_pinged(alice_sock, ["foo"])

            async with events_listen(alice_sock) as listen:
                await ping(bob_sock, "bar")
                await ping(bob_sock, "foo")
            assert listen.rep == {"status": "ok", "event": "pinged", "ping": "foo"}

            await ping(bob_sock, "foo")

            # There is no guarantee an event is ready to be received once
            # the sender got it answer
            with trio.fail_after(1):
                while True:
                    rep = await events_listen_nowait(alice_sock)
                    if rep["status"] != "no_events":
                        break
                    await trio.sleep(0.1)
            assert rep == {"status": "ok", "event": "pinged", "ping": "foo"}

            rep = await events_listen_nowait(alice_sock)
            assert rep == {"status": "no_events"}


# TODO: test message.received and beacon.updated events