"""Pub/Sub and the streamed surface, from Python (B-204).

The claim worth the file is the third test: a payload sixteen times the
subscriber budget reaches two subscribers whole, which `publish` cannot do at
all — the B-064 claim, asserted from the binding rather than taken from the
Rust suite.
"""

import asyncio

import pytest

import weida

DEADLINE = 15.0
CAP = 1 << 20


def run(coroutine):
    async def bounded():
        return await asyncio.wait_for(coroutine, DEADLINE)

    return asyncio.run(bounded())


async def served(path):
    """A runtime, a binding and the url a client dials for `path`."""
    runtime = weida.Runtime()
    binding = await runtime.bind("127.0.0.1:0", weida.Identity.generate())
    return runtime, binding, binding.url(path)


async def subscribed(url, filter_):
    """A connected subscriber holding `filter_`."""
    client = weida.Runtime()
    subscriber = client.subscriber(weida.Trust.by_address())
    await subscriber.connect(url)
    await subscriber.subscribe(filter_)
    return client, subscriber


async def await_filters(publisher, count):
    """Waits until the publisher has seen `count` subscriptions.

    A subscription is recorded in the frame-processing task, so it becomes
    visible a moment after `subscribe` returns. Polling the publisher's own
    counter is exact; publishing a probe would leave copies in the
    subscribers' queues and a sleep would be a guess.
    """
    deadline = asyncio.get_running_loop().time() + DEADLINE
    while publisher.filter_count() != count:
        assert asyncio.get_running_loop().time() < deadline, "no subscription arrived"
        await asyncio.sleep(0.005)


def test_a_published_message_reaches_the_matching_filter_only():
    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        _client, subscriber = await subscribed(url, "px.#")

        await await_filters(publisher, 1)
        assert publisher.publish("px.eur", b"1.0812") == 1
        payload, meta = await subscriber.recv(CAP)
        assert payload == b"1.0812"
        assert meta.topic == "px.eur"

        # The segmented grammar is the library's: `px.#` takes everything
        # under `px` and nothing under `fx`.
        assert publisher.publish("fx.chf", b"0.95") == 0
        assert publisher.subscriber_count() == 1

    run(exchange())


def test_a_slow_subscriber_loses_copies_and_the_publisher_says_which_topic():
    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        _client, subscriber = await subscribed(url, "")

        # Nobody reads: the budget fills and the copies are dropped, counted
        # per topic and per cause.
        await await_filters(publisher, 1)
        for _ in range(2000):
            publisher.publish("px.eur", b"x" * 32768)
        assert publisher.dropped() > 0
        total, budget, queue, no_parked = publisher.dropped_on("px.eur")
        assert total == publisher.dropped()
        assert budget + queue + no_parked == total
        assert publisher.dropped_on("fx.chf") is None

        # The subscriber still works: what it got is well formed.
        payload, meta = await subscriber.recv(CAP)
        assert len(payload) == 32768
        assert meta.topic == "px.eur"

    run(exchange())


def test_a_streamed_publish_carries_what_publish_cannot():
    """B-064 from Python: the budget bounds a chunk, not the payload."""

    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        first_client, first = await subscribed(url, "px.#")
        second_client, second = await subscribed(url, "px.#")
        assert (first_client, second_client) is not None

        # Both subscriptions, with nothing published yet: a probe would leave
        # copies in the queues and the budget below is the point of the test.
        await await_filters(publisher, 2)

        budget = 8 * 1024 * 1024  # Limits::subscriber_buffer_bytes
        chunk = b"a" * (64 * 1024)
        chunks = 160  # 10 MiB: more than the budget
        with pytest.raises(weida.LimitExceeded):
            publisher.publish("px.eur", b"b" * (budget + 1))

        readers = asyncio.gather(first.recv(CAP * 16), second.recv(CAP * 16))
        fan = publisher.open("px.eur")
        assert fan.topic == "px.eur"
        assert await fan.subscribers() == 2
        for _ in range(chunks):
            left = await fan.write_within(chunk, DEADLINE)
            assert left == 2, "a reading subscriber must not lose the transfer"
        assert await fan.finish() == 2

        (one, _), (two, _) = await readers
        assert len(one) == len(chunk) * chunks
        assert one == two

    run(exchange())


def test_a_streamed_transfer_is_written_and_read_in_pieces():
    async def exchange():
        _server, binding, url = await served("/ingest")
        puller = binding.puller("/ingest")

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url)

        async def drain():
            stream, meta = await puller.recv_stream()
            assert meta.endpoint == "/ingest"
            pieces = []
            while True:
                piece = await stream.read(4096)
                if not piece:
                    break
                pieces.append(piece)
            return b"".join(pieces), len(pieces)

        draining = asyncio.create_task(drain())
        out = await pusher.open()
        for _ in range(4):
            await out.write(b"c" * 8192)
        await out.finish()

        payload, pieces = await draining
        assert payload == b"c" * 32768
        # More than one read: the point of the surface is that the payload is
        # never held whole on either side.
        assert pieces > 1

    run(exchange())


def test_a_streamed_exchange_reads_the_reply_while_writing():
    async def exchange():
        _server, binding, url = await served("/echo")
        replier = binding.replier("/echo")

        async def serve():
            request = await replier.accept(CAP)
            await request.reply(request.payload)

        answering = asyncio.create_task(serve())

        client = weida.Runtime()
        requester = client.requester(weida.Trust.by_address())
        await requester.connect(url)
        request, reply = await requester.open()
        await request.write(b"streamed ")
        await request.write(b"request")
        await request.finish()

        stream, meta = await reply.recv()
        assert meta.endpoint is None, "a reply half carries no endpoint"
        assert await stream.collect(CAP) == b"streamed request"
        await answering

    run(exchange())


def test_a_finished_transfer_is_finished():
    async def exchange():
        _server, binding, url = await served("/md")
        publisher = binding.publisher("/md")
        fan = publisher.open("px.eur")
        assert await fan.finish() == 0
        with pytest.raises(weida.NoReply):
            await fan.finish()
        with pytest.raises(weida.NoReply):
            await fan.write_now(b"after the end")

        client = weida.Runtime()
        pusher = client.pusher(weida.Trust.by_address())
        await pusher.connect(url.replace("/md", "/md"))

    run(exchange())
