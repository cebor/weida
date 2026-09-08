# Apache Kafka 4.3 protocol and client semantics

## 0. Identity card

- Apache Kafka is an Apache Software Foundation project and an event-streaming system built around partitioned append-only logs. [1]
- This sheet researches the Kafka 4.3 documentation and its binary client protocol as published in 2026. [1][2]
- The wire protocol is binary, length-delimited, and normally carried over TCP; TLS and SASL are listener/security layers rather than alternate Kafka transports. [2][16]
- The normative practical specification is the Apache protocol guide plus the versioned broker/client documentation; KIPs specify accepted and evolving semantics. [2][3][4]
- Apache Kafka’s Java client is the reference client implementation. [1]
- In KRaft deployments, a Raft controller quorum stores cluster metadata in a metadata log; Kafka no longer requires ZooKeeper for that role. [5]

## 1. Connection and session lifecycle

- A client opens TCP connections to bootstrap brokers, obtains metadata, then keeps connections to brokers that lead or host the required partitions. [2]
- A socket has no Kafka handshake at connect or disconnect; persistent connections merely amortize TCP setup. [2]
- A request starts with a four-byte size, followed by a request header and the version-selected request body; responses are likewise sized. [2]
- API key and API version are 16-bit header fields that identify a request schema. [2]
- A client should use the highest API version supported by both endpoints, discovered with ApiVersions where available. [2]
- Flexible versions can add tagged fields without a version bump; unknown tagged fields are ignored. [2]
- A client authenticates after TLS setup, if configured, using the SASL handshake/authentication sequence before ordinary requests. [2]
- TCP connection order is preserved: a broker processes requests and returns responses in send order on one connection, though clients may pipeline into the socket buffer. [2]
- A broker disconnects a client request exceeding its configured maximum request size. [2]
- `connections.max.idle.ms` lets clients close idle connections; reconnection entails re-establishing capability knowledge for the new connection. [2][10][11]
- A consumer-group member’s application session is distinct from its TCP connection and is sustained by coordinator heartbeats. [8][10]
- With the classic group protocol, missed heartbeats for `session.timeout.ms` remove a member and initiate rebalance; the consumer protocol moves the timeout and heartbeat interval to broker configuration. [8][10]
- `max.poll.interval.ms` bounds progress between polls: failure to poll makes the member failed and leads to reassignment. [10]

## 2. Primitives

- A cluster is a set of brokers; a broker stores replicas and answers client requests. [1]
- A topic is named and split into a pre-defined number of numbered partitions. [2]
- A partition is an ordered commit log and the unit of replication, leadership, offset ordering, and consumer-group assignment. [1][2]
- A replica is a broker’s copy of one partition; one replica is leader and the others are followers under normal operation. [1]
- The ISR is the leader-maintained set of replicas that are sufficiently live and caught up. [1]
- A producer owns batching, partition selection, producer identity/sequence state when idempotent, and optional transaction state. [1][3][11]
- A consumer owns a current position per assigned partition and fetches records beginning at an explicit offset. [1]
- A consumer group is identified by `group.id`; the coordinator assigns each partition to at most one active member in that group. [1][10]
- A committed group offset is the durable restart position for a group and partition, not an acknowledgement on each record. [1][3]
- A group coordinator maintains membership and assignment state; KIP-848 makes the coordinator drive declarative assignment reconciliation. [8]
- A transactional producer is identified by `transactional.id`, receives a producer ID and epoch, and fences a concurrent incarnation using that ID. [3]
- A share group is a distinct group type that cooperatively consumes regular topics using per-record acquisition state. [7]

## 3. Message model

- A Produce request and a Fetch response carry sequences of records, rather than a single-record envelope. [1][2]
- On disk, a partition log is a directory of segment files containing appended record batches in the same binary format sent between producer, broker, and consumer. [1]
- A record batch has batch-level attributes including compression, timestamps, producer identity/epoch and base sequence where applicable; records within it carry key, value, timestamp delta, offset delta, and headers. [2][3]
- Record keys and values are opaque bytes to Kafka after serialization; key interpretation belongs to producer and consumer code. [2]
- Headers are named byte values attached to individual records and provide application metadata without changing key/value interpretation. [2]
- Producer timestamp policy is encoded in the batch/record format; brokers may validate timestamp ranges according to configuration. [2]
- The partition offset is the sequential identity and position of a record in that partition; offsets are not topic-wide identities. [1][12]
- Identical records may legitimately occupy distinct offsets, so content is not Kafka’s duplicate identity. [3]
- A producer chooses a partition explicitly or uses a partitioner; the default uses a keyed hash when a key exists and sticky batching when it does not. [11]
- Therefore key-based ordering is obtained only by consistently mapping a key to one partition. [1][2]
- Record batches can be compressed as gzip, snappy, LZ4, or Zstandard; the compressed batch persists and is transmitted until the consumer decompresses it. [1][11]
- `message.max.bytes` on a broker or `max.message.bytes` on a topic bounds accepted record-batch size. [10]
- A producer’s `max.request.size` separately bounds a request and effectively bounds an uncompressed record batch it will send. [11]
- Log cleanup may be time/size retention deletion, compaction, or both according to topic policy. [1]
- Compaction retains the latest record for each key in a compacted topic while preserving an ordered log and allows tombstones to remove keys after their retention. [1]
- A compacted topic requires keys for meaningful table state; a null key is invalid for compaction and can produce `CORRUPT_MESSAGE`. [2]
- Compaction is not an immediate snapshot guarantee: consumers must tolerate older versions and tombstones until cleaner progress and retention make them unavailable. [1]
- Tiered storage can copy completed local log segments and their indexes to remote storage, retaining local and remote tiers for separate periods. [6]
- KIP-405 does not support compacted topics with tiered storage enabled. [6]

## 4. Patterns and topologies

- The primary topology is producers appending to partitioned topics while independent consumer groups each read the same topic independently. [1]
- Within one consumer group, a partition has one active consumer, so partitions bound parallelism and preserve per-partition order. [1]
- A producer connects directly to the current partition leader; consumers fetch from leaders or, when configured, eligible followers. [1][2]
- A key-partitioning topology puts all records for a chosen key on one partition, allowing a single consumer task to process that key in log order. [1][2]
- A compacted-topic topology represents a changelog/table: replaying retained keyed records reconstructs the latest retained value per key. [1]
- Kafka Streams composes source topics, processor nodes, state stores, and sink topics into a stream topology; its exactly-once mode relies on Kafka transactions. [3][12]
- Kafka Connect is an integration ecosystem whose source and sink connectors manage data movement and offsets; it is not a distinct wire topology. [1][12]
- Share groups from KIP-932 offer queue-style cooperative consumption of normal topics, rather than exclusive partition ownership. [7]
- In a share group, more consumers than partitions are permitted and multiple consumers may be assigned a partition. [7]
- A share fetch acquires available records under a time-limited lock; acknowledge completes, release retries, reject makes it unprocessable, and expiry releases it. [7]
- No consumer group is required for direct assigned-partition consumption, but application-managed offsets then determine restart behavior. [1]
- Bootstrap is brokered discovery: clients try configured bootstrap addresses, issue Metadata, and refresh cached metadata after a socket or leadership error. [2]

## 5. Flow control and backpressure

- Kafka has no broker-to-consumer per-message credit grant; ordinary consumers pull using Fetch and choose when and how much to request. [1][2]
- A Fetch names offsets and permits a maximum return size, so slow consumers accumulate lag in retained partition logs rather than receive unsolicited records. [1][10]
- `fetch.min.bytes` lets a broker long-poll until sufficient data arrives or the request times out; its default is one byte. [10]
- `fetch.max.bytes` defaults to 50 MiB and limits the aggregate returned fetch data, except the first non-empty oversized batch may still be returned to make progress. [10]
- `max.partition.fetch.bytes` defaults to 1 MiB and similarly permits the first oversized batch for progress. [10]
- Each consumer can make multiple fetches in parallel, so these limits do not alone bound total client memory. [10]
- Producer sends are asynchronous but the producer accumulates records by partition into batches. [1][11]
- `batch.size` is the producer’s default per-partition batch allocation/upper bound; a larger value trades memory for batching opportunity. [11]
- `linger.ms` delays an undersized batch up to its bound; Apache Kafka 4.0 changed its default from 0 to 5 ms. [11]
- `buffer.memory` defaults to 32 MiB for records awaiting send, but is not a hard process-memory cap because compression and in-flight requests need additional memory. [11]
- If the producer exhausts buffer capacity, calls block until `max.block.ms` and then fail. [11]
- `max.block.ms` also bounds producer metadata acquisition for `send()` and selected transactional operations. [11]
- The broker applies configurable client quotas for produce, fetch, and request processing to prevent a client from monopolizing shared capacity. [1]
- Quotas throttle rather than create application-level record credit. [1]
- Retention bounds a consumer backlog by age and/or size; once its committed position precedes log start, the consumer needs offset-reset policy or explicit seeking. [1][10]

## 6. Delivery guarantees and acknowledgement

- `acks=0` asks the producer not to wait for a broker response; it certifies neither leader append nor replication and failures can be silent. [1][11]
- `acks=1` returns after the leader appends; it certifies leader receipt but not replication, so an acknowledged record can be lost with leader failure before followers catch up. [1]
- `acks=all` (or `-1`) waits for all current ISR replicas and is subject to `min.insync.replicas`; it certifies replication to the ISR required for the committed boundary. [1][11]
- If ISR cardinality is below `min.insync.replicas`, `acks=all` produces are rejected with insufficient-replica errors rather than newly acknowledged. [1][2]
- A committed record is defined as applied by all ISR replicas; consumers are given committed records rather than a leader’s uncommitted tail. [1]
- `acks` does not make a broker fsync each record before acknowledgement; crash/power-loss durability is constrained by replica recovery and failure assumptions. [1][15]
- Retrying after a missing Produce response is ambiguous: the broker may have appended before the response was lost. [1][3]
- Without idempotence, that retry can append a duplicate; with multiple in-flight requests and retries it can also reorder batches in a partition. [3][11]
- The idempotent producer sends a broker-assigned producer ID and monotonically checked sequence numbers, so a retry is deduplicated and out-of-order sequence is detected. [1][2][3]
- Idempotence requires `acks=all`, retries greater than zero, and `max.in.flight.requests.per.connection` no greater than five; incompatible explicit settings disable it or cause configuration failure. [11]
- Idempotence gives exactly-once *log append per producer sequence*, not atomic consume-process-produce across partitions. [3]
- A transaction atomically commits or aborts records across topic partitions and may include consumer-group offsets through `sendOffsetsToTransaction`. [3]
- `transactional.id` gives a stable transactional producer identity; `initTransactions()` resolves a prior incarnation’s in-flight transaction and obtains producer ID/epoch. [3]
- The transaction coordinator fences a producer superseded by another producer using the same transactional ID. [3]
- Transaction marker records determine visibility: `read_uncommitted` returns aborted and open transactional records, while `read_committed` returns only committed transactional records and stops at the last stable offset. [3][10]
- A transactional output plus its consumed offsets provides exactly-once processing between Kafka topics only when consumers use assignment/fencing correctly and downstream reads use `read_committed`. [1][3]
- Writing an external system needs that system’s cooperation or an application-level atomic/idempotent design; Kafka alone cannot atomically commit arbitrary external side effects. [1][12]
- Automatic group offset commit periodically writes positions in the background; it is convenient but can commit a fetched batch before processing finishes. [10][12]
- Manual commit after processing yields at-least-once processing because a crash after side effect and before commit replays records. [1][12]
- Commit before processing yields at-most-once processing because a crash after commit can skip unprocessed records. [1][12]
- Kafka defines at-most-once as possible loss without redelivery, at-least-once as no loss but possible redelivery, and exactly-once as processing once and only once within its stated transactional boundary. [1][12]
- Share groups add per-record acknowledgements, unlike conventional consumer groups’ partition-offset checkpointing. [7]

## 7. Ordering and duplicates

- Kafka promises a total append order only within one partition; it promises no total order across partitions or topics. [1][2]
- All replicas of a partition have the same offsets and order after replication convergence. [1]
- A consumer group preserves single active ownership per partition, but rebalances transfer ownership and may replay from the committed offset. [1][8]
- Key order is an application result of stable key-to-partition mapping; changing partitioning or using different partitioners defeats that assumption. [2][11]
- `acks=0` can lose records, and `acks=1` can lose records after an acknowledgement on leader failover. [1]
- A producer retry without idempotence can duplicate a record after an uncertain response. [1][3]
- Consumer processing after side effect but before offset commit can duplicate the side effect after crash or reassignment. [1][12]
- Idempotent sequence checking suppresses producer retry duplicates within the producer session/epoch and detects out-of-order writes. [2][3]
- Transactions plus read-committed isolation suppress aborted outputs from participating Kafka consumers, but they do not deduplicate external effects. [1][3]
- Consumer applications commonly use an idempotent sink keyed by record data when at-least-once processing reaches an external store. [1]
- Share groups count delivery attempts and reoffer records after lock expiration or release, so they explicitly permit redelivery. [7]

## 8. Failure behaviour

| Event | What is observed | Loss and ambiguity |
| --- | --- | --- |
| Leader fails after `acks=1` append but before replication | Metadata changes; producer may receive timeout/socket error and retry after metadata refresh. [1][2] | The acknowledged leader-only record can be lost; retry outcome is ambiguous and can duplicate without idempotence. [1][3] |
| Leader fails after `acks=all` commit with an ISR replica alive | A controller elects an eligible replica; clients refresh metadata after leader error. [1][2] | A committed record remains available under the stated ISR/failure model; availability pauses during election. [1] |
| Producer retry after lost response without idempotence | The same Produce can be sent again. [1][11] | The first append may have succeeded, so two log records can result. [1][3] |
| Consumer crashes before committing a processed batch | Another group member starts from the last committed offset. [1][12] | Processed records can be reprocessed; external side effects require idempotence or transaction coordination. [1] |
| Consumer crashes after committing but before processing | A replacement starts after the committed position. [1][12] | The committed-but-unprocessed records are skipped: at-most-once processing. [1] |
| Rebalance while a consumer processes | Assignment is revoked/reconciled by the coordinator; failure to poll or heartbeat can remove the member. [8][10] | A clean handoff can preserve position, but work after the prior commit can replay; KIP-848’s worst case remains at-least-once. [8] |
| Unclean leader election | If enabled and no ISR leader is available, an out-of-sync replica may become leader. [1] | It restores availability by discarding records absent from that replica; committed-data loss is possible. [1][15] |
| Record batch exceeds `message.max.bytes` | Broker rejects it with `MESSAGE_TOO_LARGE` or related validation failure. [2][10] | The record is not appended; client must change sizing or split its application payload. [2] |
| Consumer falls behind retention | Fetch/position becomes out of range. [2] | Deleted records cannot be replayed; `auto.offset.reset` chooses earliest/latest/by-duration or errors with `none`. [10] |
| Follower network partition or excessive lag | Follower stops fetching sufficiently and leaves ISR after the lag-time threshold. [1] | `acks=all` availability can stop at `min.insync.replicas`; the follower catches up before returning to ISR. [1] |
| Producer addresses stale leader | Broker returns `NOT_LEADER_OR_FOLLOWER` or the socket fails. [2] | Client refreshes metadata and retries according to producer policy; outcome can be ambiguous. [2][11] |
| Broker/controller partition in KRaft | A broker unable to receive controller metadata updates is fenced and omitted from client metadata. [5] | It ceases serving client RPCs as an online broker; partitions can become unavailable if no eligible leader remains. [5] |

## 9. Reliability recipes

- **Replicated committed log:** set a replication factor greater than one, `acks=all`, and a meaningful `min.insync.replicas`; commit only advances through the ISR boundary. [1]
- **Cost:** a narrower ISR improves availability but lowers tolerated replica failure; a wider required ISR increases write latency/unavailability under lag. [1]
- **Clean leader election:** keep `unclean.leader.election.enable` disabled so leadership is chosen from safe in-sync candidates; this can leave a partition unavailable rather than lose data. [1]
- **Idempotent producer:** retain idempotence-compatible acknowledgement, retry, and in-flight settings so uncertain Produce retries do not create duplicate log entries. [3][11]
- **Transactional consume-transform-produce:** use one transactional producer per consumer instance, send offsets in its transaction, abort on abortable errors, reset/recreate consumer position when necessary, and use `read_committed` downstream. [1][3]
- **Cost:** transactions add coordinator/marker work, latency, failure handling, and read-committed withholding behind open transactions. [3][10]
- **At-least-once external sink:** process first, then manually commit; make the sink operation idempotent by a stable application key. [1][12]
- **At-most-once consumer:** commit the next offset before processing; accept loss when processing fails after the commit. [1]
- **Static membership:** set unique `group.instance.id` values to avoid unnecessary rebalances during brief restarts; duplicate IDs are fenced. [1][10]
- **Lag recovery:** choose retention long enough for expected outage/replay time; tiered storage separates short local retention from longer remote retention. [6]
- **KRaft metadata quorum:** use an odd-sized controller quorum so a majority remains; its Raft metadata log supplies ordered controller state and hot standbys. [5]
- **Share-group work queue:** use acknowledge/release/reject and bounded acquisition locks when unit-of-work concurrency must exceed partition count. [7]

## 10. Security and identity

- Kafka listeners may use PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL security protocols. [2][16]
- TLS protects the client-broker transport; mutual TLS can authenticate a client certificate. [2][10][11]
- SASL supports an authentication negotiation followed by SASL tokens or `SaslAuthenticate` request/response depending on version. [2]
- Broker authorization uses principals and ACLs at cluster, topic, group, transactional-ID, and delegation-token resource scopes. [2]
- A record does not carry an authenticated Kafka principal as a native per-message field; authorization is evaluated for the client request that reads or writes it. [2]
- `client.id` is an application-provided request identifier for server-side request logging and quota attribution, not authentication. [11]
- Transactional IDs have a separately authorized scope and can fail with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. [2]

## 11. Limits and resource bounds

- Topic partition count is fixed at creation until changed administratively; it caps simultaneous conventional group consumers that can own work. [1][2]
- Replication factor cannot exceed available brokers and determines replica storage/network cost. [2]
- `message.max.bytes`/`max.message.bytes` cap accepted record batches, while producer `max.request.size` caps a producer request. [10][11]
- Broker request-size limits disconnect an oversized request before unbounded parsing/allocation. [2]
- `fetch.max.bytes` and `max.partition.fetch.bytes` are soft fetch bounds because Kafka returns a first oversized batch to ensure progress. [10]
- `buffer.memory` bounds the producer record accumulator approximately, and `max.block.ms` bounds callers waiting for metadata or capacity. [11]
- A consumer can retain arbitrary logical lag only while the topic retains the records; retention age/size is the broker-side backlog bound. [1]
- Open transactions delay the last stable offset for `read_committed` consumers and can withhold later offsets in that partition. [10]
- Classic heartbeat/session values are constrained by broker minimum/maximum session configuration; consumer-protocol values are broker controlled. [10]
- `group.share.partition.max.record.locks` bounds acquired share records per share group and partition; exhaustion yields no additional records until acknowledgements or lock expiry reduce it. [7]
- Quotas bound a client’s share of broker request and byte capacity rather than its retained topic data. [1]
- Tiered storage still needs bounded local index-cache capacity and relies on remote storage/metadata consistency for remote segments. [6]

### Guarantee-sensitive configuration detail

- `acks` is a producer setting; a topic’s `min.insync.replicas` is checked when an `acks=all` producer requests the full ISR acknowledgement. [1][11]
- A successful `acks=all` response requires enough ISR replicas at the commit boundary, not merely the configured replication factor. [1]
- `min.insync.replicas` does not strengthen `acks=0` or `acks=1` producer acknowledgement semantics. [1]
- A replication factor of one has no follower from which to recover a lost leader log. [1]
- ISR membership is dynamic and can shrink when a follower misses `replica.lag.time.max.ms`. [1]
- A follower being assigned as a replica does not mean it is currently an ISR member. [1]
- A replica returns to ISR only after catching up to the leader’s log. [1]
- The leader is itself a member of the ISR while it is eligible and live. [1]
- `unclean.leader.election.enable` is an availability-over-durability control for partitions without an ISR leader. [1]
- Leaving unclean election disabled may intentionally leave a partition unavailable until a suitable replica returns. [1]
- Producer retries are bounded by delivery-time semantics as well as by the `retries` setting. [11]
- `delivery.timeout.ms` includes batching delay, broker acknowledgement wait, and time consumed by retriable send failures. [11]
- A delivery timeout is a producer result deadline, not evidence that no broker append occurred. [inference][1][11]
- `enable.idempotence` protects only a producer’s writes to Kafka, not arbitrary effects performed after consuming a record. [3]
- Idempotent sequence state is carried by producer ID, epoch, partition, and sequence context rather than record key. [2][3]
- `OUT_OF_ORDER_SEQUENCE_NUMBER` tells a producer that the broker observed a sequence gap. [2][3]
- `DUPLICATE_SEQUENCE_NUMBER` reports a duplicate producer sequence at protocol level. [2]
- An idempotent producer uses a bounded number of in-flight requests to preserve ordered retry behavior. [11]
- Without idempotence, setting more than one in-flight request allows a later batch to overtake a retried earlier batch. [11]
- A transactional producer is necessarily idempotent because transactional delivery uses producer identity and sequencing. [3][11]
- A transaction may cover multiple topic partitions, unlike idempotence alone. [3]
- Transactional commit does not atomically include records read from a topic unless the producer explicitly sends those group offsets into the transaction. [3]
- A group offset committed by `sendOffsetsToTransaction` becomes visible only if that transaction commits. [3]
- A transaction abort hides its transactional output from `read_committed` consumers. [3][10]
- A `read_uncommitted` consumer can observe output that a transaction later aborts. [3][10]
- `read_committed` is a visibility mode, not a guarantee that an application’s external side effects occurred exactly once. [1][3]
- The last stable offset can lag the high watermark because of an open transaction. [10]
- A long-running open transaction can thus delay a read-committed reader behind unrelated later offsets in that partition. [10]
- `transactional.id` is a durable logical producer identity, whereas a producer ID/epoch is its fenced incarnation state. [3]
- Reusing a transactional ID concurrently fences the older producer. [3]
- Applications must classify transactional exceptions because some require retry, some abort, and some recreate the producer. [1][3]
- A consumer’s current in-memory position can advance beyond its committed group offset. [1]
- Offset commit is a checkpoint of the next record position, not proof that every prior record’s business effect succeeded. [1][12]
- `enable.auto.commit=true` causes periodic background commit, whose timing is independent of application processing completion. [10][12]
- A manual asynchronous commit can itself fail or race with a rebalance and must have error handling. [1][2]
- A group generation/member epoch fences stale offset commits after assignment change. [2][8]
- A consumer must stop processing revoked partitions before ownership transfers to preserve the normal single-owner premise. [8]
- KIP-848 replaces a global rebalance barrier with coordinator-driven, incremental member reconciliation. [8]
- KIP-848 still requires revocation before a partition is newly assigned, preventing simultaneous normal owners. [8]
- KIP-848 moves ordinary consumer assignment logic to a server-side assignor by default. [8]
- The classic group protocol retains client-side heartbeat interval and session timeout configuration. [10]
- The consumer group protocol makes the broker control heartbeat interval and session timeout. [10]
- `group.instance.id` makes a consumer a static member; it does not make processing state or output automatically transactional. [1][10]
- Duplicate static member identities are fenced by the coordinator. [1][10]
- `auto.offset.reset=earliest` chooses the current log start when no valid committed offset exists. [10]
- `auto.offset.reset=latest` chooses the log end in that situation. [10]
- `auto.offset.reset=none` surfaces the missing/out-of-range position as an error rather than selecting data implicitly. [10]
- `auto.offset.reset=by_duration` chooses an offset based on configured duration from current time. [10]
- An offset reset cannot recover records that retention has already deleted. [1][10]
- Increasing partitions does not redistribute existing records and changes the future key-to-partition mapping space. [2]
- A producer partitioner is therefore part of a key-ordering contract. [2][11]
- Null-key records are permitted on ordinary topics but are unsuitable for compacted-topic key state. [1][2]
- A tombstone is a keyed record with a null value used by compaction to remove a key eventually. [1]
- Compaction retains offsets and order even when obsolete keyed records are removed. [1]
- Retention deletion can remove whole old segments regardless of whether a group has committed beyond them. [1]
- Consumers that need replay must arrange retention independently of current group consumption. [1]
- Remote tier retention is distinct from local-tier retention when tiered storage is enabled. [6]
- Tiered storage transfers rolled segments and indexes rather than changing the record format. [6]
- A remote-tier fetch can have different latency characteristics from a local page-cache fetch. [6]
- The documented tiered-storage fetch limitation serves only one remote partition per fetch request. [10]
- The broker uses page cache and sequential segment access; a producer acknowledgement is not synonymous with a physical disk flush. [1][15]
- TLS disables Kafka’s `sendfile` zero-copy path because TLS processing occurs in user space. [1]
- Producer batch compression works on full batches, so larger useful batches improve compression opportunity. [1][11]
- Compression does not change Kafka’s offset ordering of records. [1]
- A Produce request can include data for multiple topic partitions. [2]
- A Fetch request can request data from multiple topic partitions. [2]
- Fetching is pull-based even though production is pushed to the partition leader. [1]
- Long-poll fetch avoids tight polling when no data is currently available. [1][10]
- Fetch byte settings constrain a response, not the amount of retained broker data. [10]
- A first oversized record batch is deliberately returned despite configured fetch byte limits so a consumer does not stall forever. [10]
- The consumer must provision enough memory to deserialize an individually oversized-but-valid returned batch. [inference][10]
- `fetch.min.bytes` trades latency for larger transfers by letting a broker wait for accumulation. [10]
- The producer’s sticky partition choice for unkeyed records improves batch accumulation but supplies no semantic-key order. [11]
- Explicit partition selection overrides the default key/sticky partitioning behavior. [2][11]
- The metadata response gives broker endpoints, partition leaders, and partition topology needed for direct requests. [2]
- A bootstrap list need not contain every broker, but multiple addresses tolerate one bootstrap broker being down. [2][10][11]
- Cached metadata is refreshed after network or leadership errors rather than continually polled. [2]
- `NOT_LEADER_OR_FOLLOWER` is retriable because current metadata may be stale. [2]
- `LEADER_NOT_AVAILABLE` indicates an election interval with no current leader. [2]
- `NOT_ENOUGH_REPLICAS` means a produce cannot meet its required ISR condition before append. [2]
- `NOT_ENOUGH_REPLICAS_AFTER_APPEND` means append occurred but the required replication condition was not met. [2]
- These two insufficient-replica errors make a retry outcome potentially ambiguous without idempotence. [inference][2][3]
- Protocol error codes label retriability, but client recovery still depends on request semantics and configured timeout. [2][11]
- The protocol does not expose a universal schema registry or payload type system; serializer/deserializer choice is client code. [2][10][11]
- Headers can transport tracing context, but Kafka does not interpret application trace semantics. [2]
- `client.id` can distinguish logical request sources in broker logging without becoming a security principal. [11]
- ACL denial is reported at topic, group, cluster, or transactional-ID scope as appropriate. [2]
- A SASL failure closes the connection after the broker reports authentication failure. [2]
- ApiVersions availability precedes normal authentication on some listeners and is documented as potential version-information disclosure. [2]
- A client must repeat API capability discovery after reconnection because the broker may have changed version. [2]
- The normal client compatibility policy is bidirectional across a supported version range, not a promise that every API exists on every broker. [2]
- Older brokers may ignore or close an ApiVersions request that they do not implement. [2]
- KRaft’s controller quorum itself needs a majority to continue metadata operations. [5]
- A three-controller KRaft quorum tolerates one controller failure while retaining majority. [5]
- KRaft broker metadata fetches serve as both metadata update mechanism and broker liveness heartbeats. [5]
- A KRaft broker that cannot contact the active controller is fenced and should not serve client requests. [5]
- Follower replicas pull from their leader, which permits replication batching analogous to consumer fetch. [1]
- Leader epoch is used to identify leadership eras and supports replica log-divergence handling. [6]
- A replica that diverged is truncated to the elected leader’s log lineage during recovery. [6]
- Unclean election can turn divergence recovery into durable-record loss because followers follow the newly elected deficient leader. [1][15]
- Kafka’s documented fail/recover model does not attempt Byzantine fault tolerance. [1]

## 12. Answers to the problem catalogue

- **P1 — Loss and retry:** Produce timeout is ambiguous; retry gives at-least-once log delivery unless idempotent producer IDs and sequences deduplicate it. [1][3]
- **P2 — Liveness:** consumer-group heartbeats and `session.timeout.ms` remove an unresponsive member; KRaft broker heartbeats fence an unresponsive broker. [1][5][10]
- **P3 — Capacity work spreading:** a conventional group assigns whole partitions, not arbitrary records, to members; share groups distribute acquired records and may exceed partition count. [1][7]
- **P4 — Slow consumer:** unread records remain in partition retention as lag; the bound is retention, after which the offset is out of range and reset policy applies. [1][10]
- **P5 — Late join state:** consumers can replay retained offsets; compacted topics replay retained latest keyed state, not an atomic point-in-time snapshot. [1]
- **P6 — Failover:** clients refresh metadata and reconnect to a new leader; group membership/offsets are coordinator-managed, but the application must tolerate replay after unclean work handoff. [1][2][8]
- **P7 — Restart durability:** partition logs and committed offsets persist; `acks=all` plus ISR/min-ISR is the producer certification for replicated log commit. [1]
- **P8 — Ordering:** one partition is totally ordered by offset; key order exists only while the same key mapping sends records to that partition. [1][2]
- **P9 — Duplicates:** retries and post-processing/pre-commit crashes create duplicates; idempotent producers and Kafka transactions/read-committed suppress defined Kafka-side cases. [1][3]
- **P10 — Request/reply:** the wire protocol correlates request/response on a TCP connection and request header correlation ID, but Kafka supplies no general application request/reply routing pattern. [2]
- **P11 — Topology and discovery:** it is brokered; clients bootstrap from broker addresses, retrieve metadata, then connect directly to partition leaders/replicas. [2]
- **P12 — Flow credit:** no per-message consumer credit exists; consumers pull byte-bounded Fetch responses, and producer accumulation blocks at `buffer.memory`/`max.block.ms`. [1][10][11]
- **P13 — Large bodies:** records are complete record batches, not streamed bodies; client and broker batch/request limits reject overlarge payloads. [2][10][11]
- **P14 — Identity:** TLS and SASL authenticate connections, and ACLs authorize cluster/topic/group/transactional-ID operations; identity is not embedded per record. [2][16]
- **P15 — Resource bounds:** request, message, fetch, producer-buffer, retention, session, acquisition-lock, and quota settings constrain allocations or shared capacity. [1][2][7][10][11]
- **P16 — Observability:** Produce responses, offsets, group commits, error codes, client IDs, and broker/client metrics expose progress; headers can carry application tracing metadata. [2][11]
- **P17 — Shutdown:** a controlled broker shutdown migrates leaders and flushes data; producer `close`/delivery timeouts and consumer commits determine application-side draining, while uncommitted processing may replay. [5][11][12]
- **P18 — Transports:** Kafka defines its binary protocol over TCP, optionally protected by SSL/TLS and optionally authenticated with SASL; it does not define QUIC, WebSocket, IPC, or in-process transports. [2]

## 13. Ecosystem

- Apache Kafka brokers and the Java client are the reference implementation and client. [1]
- `librdkafka` is a maintained C/C++ Kafka protocol client with producer, consumer, admin, idempotence, transactions, TLS, SASL, and preview KIP-932 share-consumer support. [17]
- `rust-rdkafka` is a Rust binding over librdkafka rather than a separately implemented wire client. [18]
- `kafka-protocol` is a pure-Rust generated codec covering Kafka API versions; it provides protocol types, not by itself a full producer/consumer runtime. [19]
- `rskafka` is a minimal pure-Rust asynchronous client intended for simple write-ahead-log workloads and explicitly lacks offset tracking, consumer groups, and transactions. [20]
- `samsa` is a Rust-native Kafka/Redpanda protocol and client implementation with producer/consumer and low-level protocol bindings. [21]
- Redpanda implements Kafka protocol compatibility and validates the Java client and selected non-Java clients, while documenting exceptions such as one SCRAM mechanism per user and no request-percentage quota. [22]
- WarpStream advertises Kafka-protocol compatibility, but feature/API coverage and operational semantics must be checked against its own release documentation before assuming Apache Kafka equivalence. [inference][23]
- Protocol compatibility does not imply identical quota, storage, controller, transaction, or edge-failure behavior across alternative broker implementations. [inference][2][22]

## 14. Sources

1. Apache Kafka, “Design,” Kafka 4.3 documentation, modified 2026-05-22, https://kafka.apache.org/43/design/design/ — log, producer, consumer, replication, delivery, compaction, quotas.
2. Apache Kafka, “Kafka protocol guide,” Kafka 4.3 documentation, modified 2026-05-22, https://kafka.apache.org/43/design/protocol/ — TCP framing, versioning, bootstrap/metadata, API errors, authentication, records.
3. Apache Kafka, “KIP-98: Exactly Once Delivery and Transactional Messaging,” adopted, updated 2026-03-04, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging — idempotence and transactions.
4. Apache Kafka, “KIP-129: Kafka Streams Exactly Once Semantics,” accepted, 2017, https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Kafka+Streams+Exactly+Once+Semantics — Streams EOS design.
5. Apache Kafka, “KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum,” accepted, updated 2020-07-09, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/123898922/KIP-500+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum — KRaft metadata quorum and broker states.
6. Apache Kafka, “KIP-405: Kafka Tiered Storage,” accepted/production-ready in Kafka 3.9, updated 2025-06-11, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/97554472/KIP-405+Kafka+Tiered+Storage — local/remote log semantics.
7. Apache Kafka, “KIP-932: Queues for Kafka,” accepted, updated 2026-01-26, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255070434/KIP-932+Queues+for+Kafka — share groups, locks, acknowledgements.
8. Apache Kafka, “KIP-848: The Next Generation of the Consumer Rebalance Protocol,” accepted; server-side assignors GA in Kafka 4.0, updated 2026-06-10, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848+The+Next+Generation+of+the+Consumer+Rebalance+Protocol — consumer protocol and rebalances.
9. Apache Kafka, “Configuration,” Kafka 4.3 documentation, accessed 2026-09-08, https://kafka.apache.org/documentation/ — configuration index and version navigation.
10. Apache Kafka, “Consumer Configs,” Kafka 4.1 documentation, modified 2025-12-19, https://kafka.apache.org/41/configuration/consumer-configs/ — fetch, offsets, isolation, sessions, group protocol.
11. Apache Kafka, “Producer Configs,” Kafka 4.1 documentation, modified 2025-12-19, https://kafka.apache.org/41/configuration/producer-configs/ — acks, retries, buffers, batching, idempotence prerequisites.
12. Confluent, “Kafka Message Delivery Guarantees,” documentation accessed 2026-09-08, https://docs.confluent.io/kafka/design/delivery-semantics.html — semi-official explanation of producer/consumer guarantees and external systems.
13. Jay Kreps, “The Log: What every software engineer should know about real-time data’s unifying abstraction,” LinkedIn Engineering, 2013-12, https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-data — maintainer-written conceptual background.
14. Jack Vanlightly, “Kafka KIP-966 — Fixing the Last Replica Standing issue,” 2023-08-17, https://jack-vanlightly.com/blog/2023/8/17/kafka-kip-966-fixing-the-last-replica-standing-issue — third-party reliability analysis and fsync caveat.
15. Apache Kafka, “KIP-966: Eligible Leader Replicas,” accepted, accessed 2026-09-08, https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas — eligible-leader-replica design.
16. Apache Kafka, “Security,” Kafka documentation accessed 2026-09-08, https://kafka.apache.org/documentation/#security — TLS, SASL, ACL configuration.
17. Confluent, “librdkafka,” GitHub repository README, accessed 2026-09-08, https://github.com/confluentinc/librdkafka — supported C/C++ client features and status.
18. fede1024, “rust-rdkafka,” GitHub repository, accessed 2026-09-08, https://github.com/fede1024/rust-rdkafka — Rust/librdkafka binding.
19. InfluxData, “kafka-protocol,” crates.io, accessed 2026-09-08, https://crates.io/crates/kafka-protocol — generated Rust protocol codec scope.
20. InfluxData, “rskafka,” GitHub repository, accessed 2026-09-08, https://github.com/influxdata/rskafka — minimal Rust-client scope and omissions.
21. CallistoLabsNYC, “samsa,” GitHub repository, accessed 2026-09-08, https://github.com/CallistoLabsNYC/samsa — Rust-native client/protocol implementation.
22. Redpanda Data, “Kafka Compatibility,” Redpanda 26.2 documentation, modified 2026-09-04, https://docs.redpanda.com/streaming/current/develop/kafka-clients/ — compatibility claims and exceptions.
23. WarpStream, “Kafka APIs,” documentation, accessed 2026-09-08, https://docs.warpstream.com/warpstream/reference/kafka-apis/ — alternative implementation API-compatibility reference; URL was unavailable to this research fetch.
