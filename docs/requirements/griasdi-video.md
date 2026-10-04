# Video over weida (griasdi screen share and camera)

## Context

griasdi is a chat and voice application in which every byte travels over
weida. Inside a voice session a member can share a screen, and later a camera.
The server relays the video to the session's viewers and cannot read it: every
encoded frame is SFrame ciphertext (RFC 9605) under the share's keys before
weida sees it. The codec is AV1, encoded through GStreamer (`nvav1enc` on
NVIDIA hardware, `svtav1enc` on the CPU), with a keyframe every 2 s
(`GOP_SECONDS = 2`); one GOP travels as one weida stream segment.

Voice already runs on 0034's mechanisms: a datagram flow up to the server, a
RADIO datagram segment per frame down to every listener. Video is the stream
half of the same note, and the screen-sharing branch builds on a draft weida
change (tuco86/weida#2) that main does not have. This document states what
griasdi needs for video, what weida provides today, and a proposal. It is a
requirements document, not a decision; the decision belongs in a note under
`docs/decisions/`.

Everything under "What weida provides today" was read from the weida source at
commit `05a6b8e` (`0.1.0-alpha.3`); file and line references are given so a
reader can check rather than trust. The griasdi side was read from its branch
`screen-sharing` at `75e7edd` and from its proposal at `65bb85f`
(`docs/proposals/weida-media-transport.md`), which was written against weida
`6dcb64b`. Everything under "Proposal" is design, not fact.

**Answered by [decisions/0037](../decisions/0037-layered-segments.md)
(accepted; shipped in `9c92b10`, `c1f3ed8`, `f9c90fa`, `11bcfcb`, `cfb75c8`, `845e504`,
`8c718e4` and `8def210`).** Priority (gap 1 below) is 0037 §4.4, the uplink
(gap 2) is §4.2, path statistics (gap 3) were already answered by
[0036](../decisions/0036-connection-statistics.md)'s `connection_stats()`, the
per-dish signal (gap 4) is §4.6, and layers (the fifth ask) are §4.3.

## What griasdi needs

| Need | Shape | Volume |
| --- | --- | --- |
| Video from the sharer to its server | one segment per GOP, one chunk per encoded frame, on the connection that already carries the sharer's voice | 1080p at 30 fps, 60 frames per 2 s segment, at the bitrate the path allows |
| Video from the server to its viewers | fan-out of the same segments, each viewer at the quality its own path allows | one copy per viewer; tens of viewers per share possible |
| Voice and the share's sound ahead of video | on the same path, never queued behind a GOP | 50 datagrams/s per speaker, as today |
| Quality that adapts without re-encoding | AV1 scalable coding (SVC): temporal and spatial layers, W3C webrtc-svc modes such as `L1T3` and `L3T3`, so the server or the transport drops the upper layers for one viewer while the base layer keeps decoding | one encode per share, whatever the number of viewers |
| Signals for adaptation | the sender sets its encoder target from the uplink; a viewer picks the highest layer its downlink carries | read about once a second |

Today the sharer's encoder holds one bitrate; "a new bitrate is a rebuild: the
properties cannot change once negotiated" (`crates/griasdi-screen/src/encoder.rs`,
`software_properties`), and griasdi's open items list both "Schlechte Leitung:
Bitratenanpassung" and an encoder rebuilt about every 10 s on a steady line
(new NVENC session, new keyframe) (`SCREEN-SHARING-OFFEN.md`). One encoding
for the slowest viewer costs every other viewer quality; one rebuild per
viewer change costs everybody a keyframe.

## What weida provides today

Verified at `05a6b8e`.

- **RADIO stream segments exist.** `Radio::segment(&self, topic: &str) ->
  Result<Segment, Error>` (`crates/weida/src/radio.rs:448`) opens one uni DATA
  stream per joined dish and supersedes the previous segment on the topic.
  `Segment` (`radio.rs:639-705`) offers `number`, `topic`, `write(chunk)`, which
  never waits and drops a dish without budget or queue room, and `finish() ->
  usize`. Each copy has a queue of `COPY_QUEUE = 64` chunks (`radio.rs:50`).
- **A dish states a latency budget and nothing else.** `Dish::join(&self,
  filter, max_age: Option<Duration>)` (`radio.rs:1116`). Freshness is one
  number per topic: `DishShared::fresh(topic, segment)` (`radio.rs:961`) and
  `deliver_segment` (`radio.rs:986-1006`) discard any arrival whose segment is
  not newer than the newest delivered. A segment arrives as
  `Received::Segment(IncomingTransfer)` (`radio.rs:933-936`) with
  `IncomingMeta::segment: Option<u64>` (`crates/weida/src/transfer.rs:214`).
- **Segment numbers are DATA key `13`**, then "written by a radio only"
  (`docs/PROTOCOL.md:533`; since `7094fc7` also by `Peer::segment`). The next free DATA key is `14`; SUBSCRIBE uses keys
  `0..=2` (`docs/PROTOCOL.md:709-713`), so the next free one is `3`. Unknown
  keys are skipped (`docs/PROTOCOL.md:470`, §5).
- **No priority reaches a segment copy.** The only public priority is
  `OutgoingTransfer::set_priority` (`transfer.rs:434`); the radio never calls
  it, so every copy runs at `quinn`'s default 0. Priority orders the streams of
  one connection only (0034 §2.4), and datagrams outrank every stream on their
  connection by `quinn`'s packing (0034 §2.3).
- **Drops are counted per topic, not per dish.** `TopicDrops`
  (`crates/weida/src/pubsub.rs:121-138`; not `#[non_exhaustive]`) counts
  budget, queue, superseded, expired and three datagram causes for a topic.
- **Statistics exist on every dialling handle.** `Peer::connection_stats`
  (`crates/weida/src/stream.rs:474`) and `Dish::connection_stats`
  (`radio.rs:1103`) from 0036; `ConnectionStats::remote`, the peer's view of
  the link, is decided there and not yet built (B-301, B-302).
- **A radio sees every join.** `Radio::with_admission` (`radio.rs:603`) is
  consulted before a join is recorded and gets a `Join { peer, peer_chain,
  filter }` (`radio.rs:62`); `Radio::evict` (`radio.rs:617`) withdraws one.
  Both are 0035's.
- **A dialling client cannot send a segment.** A connecting radio is a recorded
  deferral (0034 §6), and `Peer::open` (`stream.rs:501`) carries no segment
  number, supersedes nothing and waits on flow control.

The draft PR tuco86/weida#2 ("Peer::segment and supersession that keeps whole
copies", head `5e6ee45`, based on `6dcb64b`) adds what the screen-sharing
branch uses:

- `Peer::segment(topic, max_age) -> Segment` and `Peer::segment_drops(topic)`,
  with the per-topic numbering moved out of `RadioHub` into `SegmentTopics` and
  the radio's `segment_copy` shared.
- A finish grace for superseded copies past their FIN, `rtt + 50 ms + rtt *
  bytes / cwnd` (`SUPERSEDE_SLACK`, `finish_grace`, `finish_grace_over`), and a
  biased write select so that "a finished copy outlives its successor's open";
  a write held back by flow control is a stalled dish and is reset at once.
- Four tests and an order-keeping UDP delay proxy (`common::delay_proxy`). Its
  probe: "150/150 frames, nothing superseded" in every configuration, against
  "13/15 GOPs at 100 ms and 30 fps" before.
- It does **not compile on main**: `finish_grace_over` calls
  `conn.conn.path_stats()`, which main replaced with `Link::transport_stats()`
  (`crates/weida/src/transport.rs:389`; `flow.rs:928` reads it as
  `.map(|t| t.path)`).
- It also carries an unrelated Windows change (`current_account_sid` in
  `crates/runtime/src/pipe.rs` and `crates/winpipe`).

On the griasdi side the sharer writes one segment per GOP on its `/voice/up`
peer with `peer.segment(&topic, Some(SCREEN_MAX_AGE))`, each chunk `u32 LE
length || SFrame(ScreenFrame)`, and its sound as datagrams on a flow of the same
peer (`crates/griasdi-client/src/share/video.rs`). The server reads each uplink
segment and writes it chunk by chunk into `radio.segment(...)`
(`crates/griasdi-server/src/screen.rs`).

## Proposal

griasdi's proposal at `65bb85f` named four gaps. Against `05a6b8e`:

1. **Priority on RADIO segment copies.** Still missing. The priority has to be
   set before the copies open their streams, because the application never
   sees them.
2. **A superseding uplink unit from a dialling client.** Still missing on main;
   the draft PR has it as `Peer::segment`. "Supersede the previous unit on this
   topic, expire it at its age, never wait" is the RADIO contract minus the
   fan-out, and griasdi should not rebuild it beside weida's copy machinery.
3. **Path statistics without a datagram flow.** Answered by 0036:
   `Peer::connection_stats()` and `Dish::connection_stats()`.
4. **A per-dish delivery signal at a radio.** Still missing. A radio counts
   drops per topic; the copies that drop live inside the radio, and the
   application cannot count what it never sees.

And a fifth:

5. **Layers inside a segment, with a plaintext layer index.** One GOP of an SVC
   encoding is a base layer and enhancement layers, each depending only on the
   layers below it. If the sender writes each layer separately and the index
   travels in the header, a radio or a relay can drop a viewer's upper layers
   from the top, and a viewer can state the highest layer it wants, without
   anybody re-encoding. The index must be readable without the payload: SFrame
   makes the payload opaque, which is why the IETF draft for a codec-agnostic
   RTP payload format pairs SFrame with a separate Dependency Descriptor for the
   SFU to route on. weida needs to know the order of the layers and nothing
   about the codec.

A keyframe request stays griasdi's: a joining or lossy viewer asks the sender
over an exchange. `Radio::with_admission` sees every join and is the natural
trigger point for it on the server.

## Not asked of weida

- **Keyframe and join-point requests.** Application signalling about the codec.
- **The mapping of a codec's layers to weida's.** Which temporal and spatial ids
  form which layer is the encoder's decision.
- **Encryption.** Every chunk is SFrame ciphertext, opaque to weida and to the
  server.
- **Lip sync** between the share's sound and its video. Timestamps are inside
  griasdi's frames.
- **Bitrate control.** weida supplies signals; the encoder's target and a
  viewer's layer choice are griasdi's.

## Open questions

1. **RTT variation.** A receiver sizing its jitter buffer would use it; `quinn`
   exposes none, and 0036 §4.7 item 1 left it until `quinn` does
   ([0034](../decisions/0034-late-is-lost.md) §4.9).
2. **Simulcast or SVC for cameras.** Simulcast, one independent encoding per
   resolution, is expressible today as one topic per resolution; whether a
   camera wants it rather than spatial layers depends on the encoders griasdi
   can rely on.
3. **A delay-based controller for the media path.** B-289 measured BBR on a
   bulk path sharing a bottleneck with voice; whether video beside voice on one
   connection wants a different controller is unmeasured.
