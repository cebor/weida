"""`weida.sync`: the same patterns with no event loop anywhere (B-205).

No `asyncio.run` in this file, which is the point. The endpoints run on
threads, because that is how a synchronous messaging program is written — and
the GIL is released while a call waits, so the thread that serves and the
thread that requests make progress at the same time.
"""

import threading

import pytest

import weida
from weida import sync

CAP = 1 << 20
DEADLINE = 15.0


def test_a_request_is_answered_with_no_event_loop():
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    replier = binding.replier("/echo")

    def serve():
        request = replier.accept(CAP)
        assert request.payload == b"ping"
        assert request.meta.endpoint == "/echo"
        request.reply(b"pong")

    answering = threading.Thread(target=serve, daemon=True)
    answering.start()

    client = sync.Runtime()
    requester = client.requester(weida.Trust.by_address())
    requester.connect(binding.url("/echo"))
    assert requester.request(b"ping", CAP) == b"pong"

    answering.join(DEADLINE)
    assert not answering.is_alive(), "the serving thread must have finished"
    client.shutdown()
    server.shutdown()


def test_a_push_and_a_publish_reach_their_endpoints():
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    puller = binding.puller("/ingest")
    publisher = binding.publisher("/md")

    client = sync.Runtime()
    pusher = client.pusher(weida.Trust.by_address())
    pusher.connect(binding.url("/ingest"))
    subscriber = client.subscriber(weida.Trust.by_address())
    subscriber.connect(binding.url("/md"))
    subscriber.subscribe("px.#")

    received = []
    drain = threading.Thread(target=lambda: received.append(puller.recv(CAP)), daemon=True)
    drain.start()
    pusher.send(b"a sample")
    drain.join(DEADLINE)
    assert received and received[0][0] == b"a sample"
    assert received[0][1].endpoint == "/ingest"

    # The publisher's counter is how a synchronous caller waits for a
    # subscription without a probe message or a sleep.
    deadline = threading.Event()
    for _ in range(int(DEADLINE * 200)):
        if publisher.subscriber_count() == 1:
            break
        deadline.wait(0.005)
    got = []
    reading = threading.Thread(target=lambda: got.append(subscriber.recv(CAP)), daemon=True)
    reading.start()
    for _ in range(int(DEADLINE * 200)):
        if publisher.publish("px.eur", b"1.0812") == 1:
            break
        deadline.wait(0.005)
    reading.join(DEADLINE)
    assert got and got[0][0] == b"1.0812"
    assert got[0][1].topic == "px.eur"

    client.shutdown()
    server.shutdown()


def test_the_failure_classes_are_the_same_on_both_surfaces():
    server = sync.Runtime()
    binding = server.bind("127.0.0.1:0", weida.Identity.generate())
    _replier = binding.replier("/echo")

    client = sync.Runtime()
    requester = client.requester(weida.Trust.by_address())
    wrong = "weida://sha256:%s@%s/echo" % ("0" * 64, binding.local_addr())
    with pytest.raises(weida.Untrusted) as refused:
        requester.connect(wrong)
    assert binding.fingerprint() in refused.value.cause

    other = client.requester(weida.Trust.by_address())
    other.connect(binding.url("/nowhere"))
    with pytest.raises(weida.UnknownEndpoint):
        other.request(b"ping", CAP)

    client.shutdown()
    server.shutdown()


def test_a_runtime_is_shut_down_once():
    runtime = sync.Runtime()
    delivered, outstanding = runtime.drain(1.0)
    assert (delivered, outstanding) == (0, 0)
    # The facade's runtime is consumed by a drain, so the second call is a
    # refusal rather than a second drain.
    with pytest.raises(weida.RuntimeFailure):
        runtime.drain(1.0)
    with pytest.raises(weida.RuntimeFailure):
        runtime.requester(weida.Trust.by_address())
    # The one renamed class, and the rename is visible rather than implied:
    # `weida.Runtime` is the runtime, so the failure is `RuntimeFailure` and
    # says so in its own errno.
    assert not issubclass(weida.Runtime, BaseException)


def test_the_submodule_is_importable_and_shares_its_values():
    import weida.sync as imported

    assert imported is sync
    for name in imported.__all__:
        assert hasattr(imported, name), name
    # The value classes are the asyncio surface's, not copies: one Trust, one
    # Identity, one IncomingMeta for the whole module.
    assert weida.Trust.by_address() is not None
    runtime = sync.Runtime()
    binding = runtime.bind("127.0.0.1:0", weida.Identity.generate())
    assert binding.url("/x").startswith("weida://sha256:")
    runtime.shutdown()
