# Yet Another Kafka (YAK)

A from-scratch implementation of Apache Kafka's core architecture in Go.

The goal isn't to reimplement Kafka feature-for-feature. It's to build the pieces that make Kafka *Kafka* — a coordination service, leader-elected brokers, partitioned append-only logs, replication with catch-up, and pub/sub over a simple HTTP protocol — and get the concurrency and failure-handling actually right along the way.

## Architecture

```
                ┌────────────┐
                │  ZooKeeper │   broker registry, leader election
                └─────┬──────┘
                       │  register / health check / elect
        ┌──────────────┼──────────────┐
        │              │              │
   ┌────▼───┐     ┌────▼───┐     ┌────▼───┐
   │Broker 1│◄────►Broker 2│◄────►Broker 3│   leader replicates to followers
   │(follow)│     │(leader)│     │(follow)│
   └───▲────┘     └───▲────┘     └───▲────┘
       │              │              │
  ┌────┴────┐   ┌─────┴────┐   ┌─────┴────┐
  │Consumers│   │Producers/│   │Consumers │ 
  └─────────┘   │Consumers │   └──────────┘   
                └──────────┘
```

Everything talks over plain REST/JSON — no gRPC yet (see Roadmap).

### ZooKeeper (`internals/zookeeper`, `cmd/zookeeper`)

A minimal single-instance coordination service. It does not store any message data — only broker membership and leadership.

- Brokers register themselves with a stable, self-declared broker ID.
- The first broker to register becomes the leader.
- Runs a periodic health check (every 7s) against the current leader (`GET /health` with retry + exponential backoff).
- If the leader is unreachable, it removes it and elects a new leader from the remaining registered brokers, notifying the new leader over `POST /leader`.
- Exposes `GET /leader` (current leader address) and `GET /brokers` (a random *alive* broker, used by producers/consumers to find any entry point into the cluster).

### Brokers (`internals/broker`, `cmd/broker`)

Each broker stores its data as CSV files on local disk, one file per `(topic, partition)`, with the schema `offset,key,value`. Topics are hash-partitioned by key (FNV-1a) across a fixed partition count chosen at topic-creation time.

**Topic creation** — the leader creates the topic locally and pushes the creation out to every known follower.

**Producing** — a produce request assigns the topic's next offset and appends the record to the right partition file as a single atomic operation (per-topic locking, not just per-write), then asynchronously notifies registered consumers and replicates to followers.

**Replication** — replication is asynchronous: the leader acknowledges the producer as soon as the local write lands, then pushes the message to followers in the background. Each follower reports back the offset it actually has after applying a message. If that doesn't match what the leader expects, the leader queues a backfill request (via a buffered channel + a dedicated worker goroutine) that streams everything the follower is missing for that topic, straight from the leader's own log. This means a follower that fell behind — because it just joined, or missed a message — self-heals off the very next message it sees for that topic, without needing a heavyweight consensus protocol.

**Follower sync on startup** — before a newly-started (or restarted) broker finishes registering with ZooKeeper, it syncs with the current leader: it reports what topics/offsets it already has, the leader replies with everything newer, plus full history for any topic the follower doesn't know about at all. Only after this sync does the broker complete registration and become eligible to serve consumers.

**Consuming** — any broker (leader or follower) can accept a consumer registration for a topic. With `from_beginning`, it replays the full topic history first — a k-way merge across all partition files by offset, using a min-heap over `container/heap` — before switching the consumer over to live push notifications as new messages are produced/replicated.

### Producers & Consumers (`internals/producer`, `internals/consumer`, `cmd/producer`, `cmd/consumer`)

Thin CLI clients. A producer looks up the current leader from ZooKeeper once at startup and sends every message there. The producer CLI reads `key:value` lines from stdin (or treats the whole line as the value if there's no `:`).

A consumer looks up *any* broker from ZooKeeper, registers itself against a topic (optionally from the beginning), and runs its own tiny HTTP server to receive pushed messages, printing each one as it arrives.

## Running it

Start ZooKeeper first (fixed port 9998):

```
go run ./cmd/zookeeper
```

Start one or more brokers, each with a unique ID:

```
go run ./cmd/broker -id 1 -port 9988 -zookeeper <zk-host>:9998
go run ./cmd/broker -id 2 -port 9989 -zookeeper <zk-host>:9998
```

Create a topic and produce (the producer looks up the leader itself):

```
go run ./cmd/producer -topic orders -zookeeper <zk-host>:9998 -create-topic -partitions 3
```

Consume from the beginning:

```
go run ./cmd/consumer -topic orders -zookeeper <zk-host>:9998 -from-beginning
```

Brokers auto-detect their own LAN IP (via a UDP dial trick) so multiple brokers can run across machines on the same network, not just on localhost.

## What's implemented

- ZooKeeper-style broker registry with leader election and periodic health-check-driven failover
- Hash-partitioned topics, CSV append-only log per partition
- Atomic, per-topic-locked offset assignment + log append
- Async leader → follower replication with per-message ack and gap detection
- Automatic backfill of a lagging follower straight from the leader's log
- Full startup/restart sync for a (re)joining broker, including topics it has never seen before
- Consumer replay from offset 0 via k-way merge across partitions, followed by live push delivery
- Graceful shutdown for ZooKeeper and consumer (SIGINT/SIGTERM)

## Known limitations

I'm keeping this list honest rather than pretending it's production-ready — it's a learning project first:

- **Offsets are per-topic, not per-partition.** Real Kafka assigns offsets independently per partition; here there's a single counter per topic. It's simpler but means the log ordering guarantee is stronger (and more restrictive) than real Kafka's.
- **No fencing for a partitioned-but-still-alive leader.** The failure model assumed right now is crash-stop (a broker either works or is fully down), not network partitions — there's no leader lease/epoch mechanism yet to stop a partitioned old leader from still accepting writes.
- **Replication is fire-and-forget, ack-before-replicated.** A message is acknowledged to the producer as soon as it's written locally on the leader. If the leader crashes before that message reaches any follower, it can be lost.
- **No consumer groups.** Every registered consumer gets the full stream; there's no concept of partition assignment across a consumer group.
- **Single ZooKeeper instance, no HA.** If it goes down, the cluster can't register new brokers or run elections until it's back.
- No log compaction, retention, or cleanup — files just grow.

## Roadmap

- Gate produce/topic-creation to the leader only
- Move producer → broker and broker → consumer from plain HTTP to gRPC (client streaming for producers, server streaming for consumer push)
- Per-partition offsets, matching real Kafka's ordering model
- Some form of leader lease/fencing if I want to go beyond crash-stop assumptions

## Course context

Originally built for PES University's UE20CS322 (Big Data) course as a group project. This repo is a personal rebuild/refactor afterward — same core idea, but redesigned with cleaner package boundaries, proper concurrency handling, and (eventually) real tests, done on my own as a way to get better at Go and distributed systems design.
