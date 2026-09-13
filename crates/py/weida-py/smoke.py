"""What the release wheel must do, run from a virtualenv that holds only it.

Not a test suite: the suite is `tests/`, and it runs against an editable
build. This is the wheel's own proof - installed into a fresh virtualenv with
no `cargo`, `rustc` or `maturin` on `PATH` - and it checks the three claims a
wheel can be wrong about: that it imports at all, that the metadata is the
one `pyproject.toml` and the workspace version declare, and that a round trip
over QUIC works with both halves inside this one process.
"""

import asyncio
import importlib.metadata
import sys

import weida

CAP = 1 << 20


def metadata() -> None:
    """The wheel's own metadata, read from the installed distribution."""
    distribution = importlib.metadata.distribution("weida")
    assert distribution.metadata["Name"] == "weida", distribution.metadata["Name"]
    assert distribution.version == "0.1.0", distribution.version
    print(f"weida {distribution.version} installed as a wheel")


def surface() -> None:
    """Every name the module promises is there."""
    for name in weida.__all__:
        assert hasattr(weida, name), name
    assert issubclass(weida.Untrusted, weida.WeidaError)
    print(f"{len(weida.__all__)} names, {weida.VERSION} the wire version")


async def round_trip() -> None:
    """A Req/Rep exchange and a Push/Pull transfer, over real QUIC."""
    server = weida.Runtime()
    binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
    replier = binding.replier("/echo")
    puller = binding.puller("/ingest")

    async def serve() -> None:
        request = await replier.accept(CAP)
        await request.reply(request.payload)

    answering = asyncio.create_task(serve())

    client = weida.Runtime()
    requester = client.requester(weida.Trust.by_address())
    await requester.connect(binding.url("/echo"))
    reply = await requester.request(b"through a wheel", CAP)
    assert reply == b"through a wheel", reply
    await answering
    print(f"req/rep over {binding.local_addr()}: {reply!r}")

    pusher = client.pusher(weida.Trust.by_address())
    await pusher.connect(binding.url("/ingest"))
    receiving = asyncio.create_task(puller.recv(CAP))
    await pusher.send(b"a sample")
    payload, meta = await receiving
    assert payload == b"a sample", payload
    print(f"push/pull to {meta.endpoint}: {payload!r}")

    # The address names the key, so a wrong key is refused rather than
    # trusted: the one security claim a wheel could silently lose.
    wrong = "weida://sha256:%s@%s/echo" % ("0" * 64, binding.local_addr())
    try:
        await client.requester(weida.Trust.by_address()).connect(wrong)
    except weida.Untrusted as refused:
        assert binding.fingerprint() in refused.cause
        print("a wrong fingerprint is refused, naming the key that answered")
    else:
        raise AssertionError("an untrusted key must not connect")


def main() -> int:
    metadata()
    surface()
    asyncio.run(asyncio.wait_for(round_trip(), 30.0))
    print("the wheel works")
    return 0


if __name__ == "__main__":
    sys.exit(main())
