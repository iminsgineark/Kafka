<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/0/05/Apache_kafka.svg" alt="Apache Kafka Logo" width="300"/>
</p>

<h1 align="center">Apache Kafka — Advanced Event Streaming Lab</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Apache-Kafka-black?style=for-the-badge&logo=apachekafka">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python">
  <img src="https://img.shields.io/badge/KRaft-Mode-orange?style=for-the-badge">
  <img src="https://img.shields.io/badge/Event--Driven-Architecture-success?style=for-the-badge">
  <img src="https://img.shields.io/badge/SSL-Secured-red?style=for-the-badge&logo=letsencrypt">
  <img src="https://img.shields.io/badge/Docker-Ready-blue?style=for-the-badge&logo=docker">
</p>

<p align="center">
  <b>A complete, production-grade hands-on implementation of Apache Kafka event streaming — producers, consumers, stateful processors, fault-tolerant pipelines, throughput benchmarking, SSL security, and multi-broker configuration — all in Python.</b>
</p>

---

## 📋 Table of Contents

- [What is Apache Kafka?](#-what-is-apache-kafka)
- [Core Kafka Concepts](#-core-kafka-concepts)
- [Project Architecture](#-project-architecture)
- [Features](#-features)
- [Repository Structure](#-repository-structure)
- [Technologies Used](#-technologies-used)
- [Prerequisites](#-prerequisites)
- [Setup & Installation](#-setup--installation)
- [Running the Lab](#-running-the-lab)
- [Producers Deep Dive](#-producers-deep-dive)
- [Consumers Deep Dive](#-consumers-deep-dive)
- [Stream Processing Modules](#-stream-processing-modules)
- [Kafka Configuration](#-kafka-configuration)
- [SSL Security Setup](#-ssl-security-setup)
- [Multi-Broker Configuration](#-multi-broker-configuration)
- [Performance Testing](#-performance-testing)
- [Kafka Internals](#-kafka-internals)
- [Troubleshooting](#-troubleshooting)
- [Learning Outcomes](#-learning-outcomes)
- [Future Improvements](#-future-improvements)
- [Author](#-author)

---

## 📖 What is Apache Kafka?

**Apache Kafka** is a distributed, fault-tolerant, high-throughput event streaming platform originally developed at LinkedIn and open-sourced in 2011. It is designed to handle trillions of events per day.

```
Traditional Messaging                    Apache Kafka
─────────────────────                    ────────────
Producer → Queue → Consumer              Producer → Topic (Log) → Consumer Group
                                                       │
                                         ┌─────────────┼─────────────┐
                                         │             │             │
                                      Partition 0  Partition 1  Partition 2
                                      (Broker 1)   (Broker 2)   (Broker 3)
```

### Why Kafka?

| Challenge | Traditional Systems | Apache Kafka |
|---|---|---|
| High throughput | Limited by single broker | Millions of messages/sec across partitions |
| Durability | Messages lost on consumer ack | Log-based persistence (configurable retention) |
| Scalability | Vertical only | Horizontal — add brokers & partitions |
| Replayability | One-time delivery | Consumers can re-read from any offset |
| Real-time | Polling-based delays | Sub-millisecond end-to-end latency |
| Fault tolerance | Single point of failure | Replication across brokers |

### Where is Kafka Used?

- **LinkedIn** — Activity tracking (500B+ events/day)
- **Netflix** — Real-time monitoring, 700B+ messages/day
- **Uber** — Surge pricing, trip events, driver matching
- **Airbnb** — Logging, A/B testing, analytics
- **Goldman Sachs** — Market data distribution, trade events
- **HDFC / Zerodha** — Order book updates, transaction events

---

## 🧠 Core Kafka Concepts

### Topic

A **Topic** is a logical channel to which producers write and from which consumers read. Think of it as a database table, but append-only.

```
Topic: bank-events
─────────────────────────────────────────────────────
  Partition 0: [msg0] → [msg1] → [msg2] → [msg3] →
  Partition 1: [msg0] → [msg1] → [msg2] → [msg3] →
  Partition 2: [msg0] → [msg1] → [msg2] → [msg3] →
```

### Partition

Each topic is split into **Partitions** — ordered, immutable sequences of records. Partitions enable parallelism and horizontal scaling.

- Each message within a partition has a unique sequential **offset**
- Messages are ordered within a partition, not across partitions
- Number of partitions = maximum consumer parallelism

### Offset

An **Offset** is the unique ID of a message within a partition.

```
Partition 0:
┌──────┬──────┬──────┬──────┬──────┐
│  0   │  1   │  2   │  3   │  4   │  ← Offsets
├──────┼──────┼──────┼──────┼──────┤
│msg_A │msg_B │msg_C │msg_D │msg_E │
└──────┴──────┴──────┴──────┴──────┘
                         ↑
               Consumer committed here
```

### Producer

A **Producer** publishes records to a topic. Producers choose which partition to write to using:
- Round-robin (no key)
- Hash of the message key (keyed messages)
- Custom partitioner

### Consumer & Consumer Group

A **Consumer** subscribes to topics and reads records. **Consumer Groups** allow parallel processing:

```
Topic: bank-events (3 partitions)

Consumer Group: payment-processors
  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
  │ Consumer 1  │   │ Consumer 2  │   │ Consumer 3  │
  │ Partition 0 │   │ Partition 1 │   │ Partition 2 │
  └─────────────┘   └─────────────┘   └─────────────┘
```

### Broker

A **Broker** is a Kafka server. A Kafka cluster consists of multiple brokers for fault tolerance and load distribution.

### KRaft Mode

**KRaft (Kafka Raft Metadata)** is Kafka's built-in consensus mechanism that replaces the older ZooKeeper dependency. This project uses KRaft mode exclusively.

```
Old Architecture:          New KRaft Architecture:
────────────────           ─────────────────────────
ZooKeeper Ensemble    →    Raft-based metadata quorum
  + Kafka Brokers            built into Kafka itself
```

---

## 🏗 Project Architecture

### High-Level System Design

```
                    ┌─────────────────────────────────────┐
                    │          PRODUCER LAYER              │
                    │                                      │
                    │  fast-producer.py  throughput-       │
                    │  producer10..15    producer.py       │
                    └─────────────────┬───────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    APACHE KAFKA CLUSTER                          │
│                                                                  │
│   ┌───────────────┐    ┌───────────────┐    ┌────────────────┐  │
│   │   Broker 1    │    │   Broker 2    │    │   Broker N     │  │
│   │ (KRaft Mode)  │    │               │    │                │  │
│   │               │◄──►│               │◄──►│                │  │
│   │  Partition 0  │    │  Partition 1  │    │  Partition 2   │  │
│   │  (Leader)     │    │  (Leader)     │    │  (Leader)      │  │
│   │  SSL:9093     │    │  SSL:9094     │    │                │  │
│   └───────────────┘    └───────────────┘    └────────────────┘  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
  ┌─────────────────┐ ┌──────────────────┐ ┌─────────────────────┐
  │  STATEFUL       │ │  AGGREGATION     │ │  FAULT TOLERANT     │
  │  PROCESSOR      │ │  PROCESSOR       │ │  PROCESSOR          │
  │                 │ │                  │ │                     │
  │ Maintains state │ │ Groups & counts  │ │ Retry + recovery    │
  │ across events   │ │ event windows    │ │ logic               │
  └────────┬────────┘ └────────┬─────────┘ └──────────┬──────────┘
           │                   │                       │
           └───────────────────┼───────────────────────┘
                               │
                               ▼
           ┌───────────────────────────────────────────┐
           │              CONSUMER LAYER                │
           │                                            │
           │  consumer4.py  consumer5.py  consumer6.py  │
           │  slow-consumer.py  throughput-consumer.py  │
           └───────────────────────────────────────────┘
```

### Data Flow Diagram

```
Bank Event Generated
       │
       ▼
Producer serializes → JSON / Avro
       │
       ▼
Producer selects Partition (hash of account_id key)
       │
       ├─► Partition 0: account_id % 3 == 0
       ├─► Partition 1: account_id % 3 == 1
       └─► Partition 2: account_id % 3 == 2
                │
                ▼
       Kafka persists to disk (WAL)
                │
                ▼
       Replication to follower replicas
                │
                ▼
       Consumer Group reads from offset
                │
                ▼
       Processing: Stateful / Aggregation / Fault Tolerant
                │
                ▼
       Offset committed → __consumer_offsets topic
```

---

## ✨ Features

### Producer Features

| Feature | Description |
|---|---|
| High throughput | Batching + linger config for max msgs/sec |
| Keyed messages | Consistent partition routing by key |
| Multi-version testing | producer10–15 for iteration experiments |
| Throughput benchmarking | Measures actual msgs/sec |
| Async production | Non-blocking sends with callbacks |
| Event publishing pipelines | Sequential event chains |

### Consumer Features

| Feature | Description |
|---|---|
| Real-time consumers | Low-latency polling loop |
| Slow consumer simulation | Controlled lag for backpressure testing |
| Throughput consumers | Measures consumption rate |
| Parallel processing | Consumer group with multiple instances |
| Offset management | Manual vs auto commit control |

### Stream Processing Features

| Feature | Description |
|---|---|
| Stateful processing | In-memory state (account balances, counters) |
| Aggregation | Windowed event aggregation |
| Fault tolerance | Retry logic, dead-letter handling |
| Recovery | Resume from last committed offset |

---

## 📁 Repository Structure

```
kafka_2.13-3.9.2/
│
├── config/                          # Kafka configuration files
│   ├── kraft/
│   │   └── server.properties        # KRaft mode server config
│   └── broker-2.properties          # Secondary broker config
│
├── ssl/                             # SSL certificates & keystores
│   ├── kafka.server.keystore.jks    # Server keystore
│   ├── kafka.server.truststore.jks  # Server truststore
│   └── ssl-client.properties        # Client SSL config
│
├── logs/                            # Kafka runtime logs
│
├── # ── STREAM PROCESSORS ──────────────────────────────────
│
├── aggregation-processor.py         # Windowed aggregation processor
├── fault-tolerant-processor.py      # Resilient event handler
├── stateful-processor.py            # Stateful stream processor
│
├── # ── PRODUCERS ──────────────────────────────────────────
│
├── fast-producer.py                 # High-speed event producer
├── fast-prod1.py                    # Fast producer variant
├── producer10.py                    # Iteration 10 — batch tuning
├── producer11.py                    # Iteration 11 — linger tuning
├── producer12.py                    # Iteration 12 — compression
├── producer13.py                    # Iteration 13 — acks config
├── producer14.py                    # Iteration 14 — idempotent
├── producer15.py                    # Iteration 15 — transactional
├── throughput-producer.py           # Benchmark producer
│
├── # ── CONSUMERS ──────────────────────────────────────────
│
├── consumer4.py                     # Base consumer v4
├── consumer5.py                     # Consumer with manual commit
├── consumer6.py                     # Consumer group demo
├── slow-consumer.py                 # Lag simulation consumer
├── slow-consum1.py                  # Slow consumer variant
├── throughput-consumer.py           # Benchmark consumer
│
└── README.md
```

---

## 🛠 Technologies Used

| Technology | Version | Purpose |
|---|---|---|
| Apache Kafka | 3.9.2 | Event streaming platform |
| Python | 3.10+ | Producer & consumer clients |
| `confluent-kafka` | Latest | Python Kafka client library |
| KRaft | Built-in | Raft-based metadata (replaces ZooKeeper) |
| SSL / TLS | TLSv1.2+ | Encrypted broker communication |
| Docker | 24+ | Containerization (future) |
| Linux / WSL2 | Ubuntu 22+ | Runtime environment |
| Java | JDK 17+ | Kafka broker runtime |

---

## ✅ Prerequisites

### System Requirements

```bash
# Check Java version (must be 11+, recommend 17)
java -version

# Check Python version (must be 3.8+)
python3 --version

# Check available memory (Kafka needs at least 2GB heap)
free -h
```

### Install Python Dependencies

```bash
pip install confluent-kafka
pip install kafka-python        # Alternative client
pip install python-dotenv       # Environment variables
```

### Install Java (if not present)

```bash
# Ubuntu / Debian
sudo apt update
sudo apt install openjdk-17-jdk -y

# Verify
java -version
# Expected: openjdk version "17.x.x"
```

---

## ⚙️ Setup & Installation

### Step 1 — Clone the Repository

```bash
git clone <your-repository-url>
cd kafka_2.13-3.9.2
```

### Step 2 — Download Apache Kafka (if not already present)

```bash
# Download Kafka 3.9.2 (Scala 2.13 binary)
wget https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz

# Extract
tar -xzf kafka_2.13-3.9.2.tgz
cd kafka_2.13-3.9.2
```

### Step 3 — Configure KRaft Mode

KRaft mode does not require ZooKeeper. Generate a unique cluster ID and format the storage directory:

```bash
# Generate a new Cluster UUID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Cluster ID: $KAFKA_CLUSTER_ID"

# Format the storage directory
bin/kafka-storage.sh format \
  -t $KAFKA_CLUSTER_ID \
  -c config/kraft/server.properties

# Expected output:
# Formatting /tmp/kraft-combined-logs with metadata.version=3.9.x
```

### Step 4 — Verify server.properties

Key settings to confirm in `config/kraft/server.properties`:

```properties
# KRaft mode process roles
process.roles=broker,controller

# Node ID (unique per broker)
node.id=1

# Controller quorum voters
controller.quorum.voters=1@localhost:9093

# Listeners
listeners=PLAINTEXT://:9092,CONTROLLER://:9093

# Log directories
log.dirs=/tmp/kraft-combined-logs

# Replication defaults
default.replication.factor=1
num.partitions=1
```

### Step 5 — Start Kafka Server

```bash
# Start in foreground (for development)
bin/kafka-server-start.sh config/kraft/server.properties

# Start in background (for persistent sessions)
bin/kafka-server-start.sh config/kraft/server.properties &

# Verify it's running
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### Step 6 — Create the Topic

```bash
bin/kafka-topics.sh --create \
  --topic bank-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topic creation
bin/kafka-topics.sh --describe \
  --topic bank-events \
  --bootstrap-server localhost:9092
```

Expected output:
```
Topic: bank-events   TopicId: xxxx   PartitionCount: 3   ReplicationFactor: 1
  Topic: bank-events   Partition: 0   Leader: 1   Replicas: 1   Isr: 1
  Topic: bank-events   Partition: 1   Leader: 1   Replicas: 1   Isr: 1
  Topic: bank-events   Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

---

## 🚀 Running the Lab

### Quick Start

```bash
# Terminal 1 — Start Kafka
bin/kafka-server-start.sh config/kraft/server.properties

# Terminal 2 — Start Consumer
python3 consumer5.py

# Terminal 3 — Start Producer
python3 fast-producer.py
```

### Throughput Benchmark

```bash
# Terminal 1 — Consumer (watch throughput)
python3 throughput-consumer.py

# Terminal 2 — Producer (max throughput)
python3 throughput-producer.py
```

### Stateful Processing Pipeline

```bash
# Terminal 1 — Start stateful processor
python3 stateful-processor.py

# Terminal 2 — Feed events
python3 producer13.py
```

### Fault Tolerance Test

```bash
# Terminal 1 — Start fault-tolerant processor
python3 fault-tolerant-processor.py

# Terminal 2 — Feed events (including malformed ones)
python3 producer14.py

# Simulate failure: Ctrl+C the processor, then restart it
# Observe: it resumes from last committed offset
```

---

## 📤 Producers Deep Dive

### How Kafka Producers Work

```
Application Code
      │
      ▼
Producer.send(ProducerRecord)
      │
      ▼
┌─────────────────────────────┐
│     Serializer              │  key → bytes, value → bytes
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│     Partitioner             │  Assigns record to a partition
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│     RecordAccumulator       │  Batches records by partition
│     (in-memory buffer)      │
└─────────────┬───────────────┘
              │  (batch.size or linger.ms reached)
              ▼
┌─────────────────────────────┐
│     Sender Thread           │  Network I/O thread
└─────────────┬───────────────┘
              │
              ▼
         Kafka Broker
```

### Producer Configuration Reference

```python
producer_config = {
    'bootstrap.servers': 'localhost:9092',

    # Throughput tuning
    'batch.size': 65536,         # 64KB batch (default 16KB)
    'linger.ms': 5,              # Wait 5ms to fill batches
    'compression.type': 'snappy', # snappy/gzip/lz4/zstd

    # Reliability tuning
    'acks': 'all',               # Wait for all ISR replicas
    'retries': 5,                # Retry on transient errors
    'retry.backoff.ms': 300,

    # Exactly-once semantics
    'enable.idempotence': True,  # Prevents duplicate messages

    # Buffer settings
    'buffer.memory': 33554432,   # 32MB producer buffer
    'max.block.ms': 60000,       # Block before throwing exception
}
```

### fast-producer.py — Core Pattern

```python
from confluent_kafka import Producer
import json, time, random

conf = {
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 5,
    'batch.size': 65536
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

while True:
    event = {
        'event_type': 'TRANSACTION',
        'account_id': f'ACC{random.randint(1000, 9999)}',
        'amount': round(random.uniform(100, 50000), 2),
        'timestamp': time.time()
    }
    producer.produce(
        topic='bank-events',
        key=event['account_id'],
        value=json.dumps(event),
        callback=delivery_report
    )
    producer.poll(0)   # Trigger delivery callbacks
    time.sleep(0.01)

producer.flush()       # Wait for all messages to be delivered
```

### Producer Evolution (producer10 → producer15)

| File | Key Experiment |
|---|---|
| `producer10.py` | Baseline — default configs |
| `producer11.py` | `linger.ms` tuning — batching vs latency |
| `producer12.py` | Compression comparison — snappy vs gzip |
| `producer13.py` | `acks=all` vs `acks=1` reliability tradeoff |
| `producer14.py` | `enable.idempotence=True` — exactly-once |
| `producer15.py` | Transactional producer — atomic multi-topic writes |

---

## 📥 Consumers Deep Dive

### How Kafka Consumers Work

```
Kafka Broker (Partition Leader)
          │
          │ Poll(max_records=500, timeout_ms=1000)
          │
          ▼
┌──────────────────────────────┐
│    Consumer.poll()           │
│    Returns ConsumerRecords   │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│    Deserialize               │  bytes → Python dict
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│    Process Record            │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│    Commit Offset             │  Manual or Auto
│    → __consumer_offsets      │
└──────────────────────────────┘
```

### Consumer Configuration Reference

```python
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bank-payment-processors',
    'auto.offset.reset': 'earliest',   # earliest / latest / none

    # Offset commit strategy
    'enable.auto.commit': False,        # Manual commit for reliability

    # Polling
    'max.poll.records': 500,
    'fetch.min.bytes': 1,
    'fetch.max.wait.ms': 500,

    # Session & heartbeat
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 3000,
    'max.poll.interval.ms': 300000,
}
```

### consumer5.py — Core Pattern (Manual Commit)

```python
from confluent_kafka import Consumer, KafkaError
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bank-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe(['bank-events'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition {msg.partition()} at offset {msg.offset()}')
            else:
                raise Exception(msg.error())
            continue

        # Process
        event = json.loads(msg.value().decode('utf-8'))
        print(f'[Partition {msg.partition()}] Offset {msg.offset()} → {event}')

        # Manual commit AFTER successful processing
        consumer.commit(asynchronous=False)

finally:
    consumer.close()
```

### slow-consumer.py — Consumer Lag Simulation

```python
import time

# Simulates a slow downstream system (DB write, API call, etc.)
# Creates consumer lag visible in Kafka monitoring

while True:
    msg = consumer.poll(timeout=1.0)
    if msg:
        process(msg)
        time.sleep(2)   # Artificial 2-second delay per message
        consumer.commit()
```

Consumer lag = `(Latest Offset) − (Current Consumer Offset)`. Monitor with:
```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bank-consumer-group
```

---

## 🔄 Stream Processing Modules

### 1. Stateful Processor (`stateful-processor.py`)

Maintains in-memory state across events — e.g., running account balances:

```
Events:
  TRANSACTION(ACC1001, +5000)
  TRANSACTION(ACC1001, -2000)
  TRANSACTION(ACC1001, +1500)

State (in-memory dict):
  { 'ACC1001': 4500 }

Output event:
  { account: ACC1001, balance: 4500, updated_at: ... }
```

```python
state = {}   # account_id → running_balance

while True:
    msg = consumer.poll(1.0)
    if msg:
        event = json.loads(msg.value())
        account = event['account_id']
        amount  = event['amount']

        # Update state
        state[account] = state.get(account, 0) + amount

        # Emit downstream
        producer.produce(
            'account-balances',
            key=account,
            value=json.dumps({'account': account, 'balance': state[account]})
        )
        consumer.commit()
```

### 2. Aggregation Processor (`aggregation-processor.py`)

Groups and counts events in time windows:

```
Input events (30-second window):
  TRANSACTION × 150
  LOGIN       × 42
  FRAUD_ALERT × 3

Aggregated output:
  { window: "14:30–14:31", TRANSACTION: 150, LOGIN: 42, FRAUD_ALERT: 3 }
```

```python
import time
from collections import defaultdict

WINDOW_SECONDS = 30
window_start = time.time()
counts = defaultdict(int)

while True:
    msg = consumer.poll(0.1)
    if msg:
        event = json.loads(msg.value())
        counts[event['event_type']] += 1

    # Flush window
    if time.time() - window_start >= WINDOW_SECONDS:
        producer.produce('aggregated-events', value=json.dumps({
            'window_start': window_start,
            'window_end': time.time(),
            'counts': dict(counts)
        }))
        counts.clear()
        window_start = time.time()
```

### 3. Fault Tolerant Processor (`fault-tolerant-processor.py`)

Handles errors gracefully — retry, dead-letter queue, resume from offset:

```
Normal flow:       Event → Process → Commit Offset → Next

Error flow:        Event → Process fails
                       → Retry (up to 3x)
                       → Still fails → Dead-Letter Topic
                       → Commit Offset (skip bad message)
                       → Continue processing

Restart flow:      Process crashes at offset 1042
                   On restart: reads __consumer_offsets
                   Resumes from offset 1042
```

```python
MAX_RETRIES = 3

def process_with_retry(event):
    for attempt in range(MAX_RETRIES):
        try:
            process(event)
            return True
        except Exception as e:
            print(f'Attempt {attempt+1} failed: {e}')
            time.sleep(2 ** attempt)   # Exponential backoff
    return False   # All retries exhausted

while True:
    msg = consumer.poll(1.0)
    if msg:
        event = json.loads(msg.value())
        success = process_with_retry(event)

        if not success:
            # Send to dead-letter topic
            producer.produce('bank-events-dlq', value=msg.value())

        consumer.commit()   # Always commit to avoid infinite loop
```

---

## ⚙️ Kafka Configuration

### KRaft `server.properties` — Annotated

```properties
# ─── KRaft Mode ──────────────────────────────────────────────────────────────

# This node acts as both broker and controller
process.roles=broker,controller

# Unique node ID in the cluster
node.id=1

# Controller quorum — format: node_id@host:port
controller.quorum.voters=1@localhost:9093

# ─── Network ─────────────────────────────────────────────────────────────────

# External listeners (clients connect here)
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092

# Listener security mappings
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

# ─── Log Storage ─────────────────────────────────────────────────────────────

# Where Kafka stores its data
log.dirs=/tmp/kraft-combined-logs

# Number of log cleaner threads
num.recovery.threads.per.data.dir=1

# ─── Topic Defaults ───────────────────────────────────────────────────────────

num.partitions=3
default.replication.factor=1
min.insync.replicas=1

# ─── Retention ────────────────────────────────────────────────────────────────

# Keep messages for 7 days
log.retention.hours=168

# Max size per segment file (1GB)
log.segment.bytes=1073741824

# Check retention every 5 minutes
log.retention.check.interval.ms=300000

# ─── Performance ──────────────────────────────────────────────────────────────

# Number of threads handling network requests
num.network.threads=3

# Number of threads doing I/O
num.io.threads=8

# Socket buffer sizes
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
```

---

## 🔐 SSL Security Setup

### Why SSL for Kafka?

```
Without SSL:                    With SSL:
─────────────────               ─────────────────────────────
Producer → [PLAINTEXT] →        Producer → [TLS Encrypted] →
         Kafka Broker                        Kafka Broker
  (anyone on network can                (only authorized clients
   intercept messages)                   with valid certificates)
```

### Generate SSL Certificates

```bash
# Create CA key and certificate
openssl req -new -x509 -keyout ssl/ca-key.pem \
  -out ssl/ca-cert.pem -days 365 \
  -subj "/CN=KafkaCA/OU=Engineering/O=Meritshot/C=IN"

# Create server keystore
keytool -keystore ssl/kafka.server.keystore.jks \
  -alias localhost \
  -validity 365 \
  -genkey \
  -keyalg RSA \
  -dname "CN=localhost, OU=Engineering, O=Meritshot, C=IN"

# Create certificate signing request
keytool -keystore ssl/kafka.server.keystore.jks \
  -alias localhost \
  -certreq \
  -file ssl/cert-file.csr

# Sign the certificate with CA
openssl x509 -req \
  -CA ssl/ca-cert.pem \
  -CAkey ssl/ca-key.pem \
  -in ssl/cert-file.csr \
  -out ssl/cert-signed.pem \
  -days 365 \
  -CAcreateserial

# Import CA + signed cert into keystore
keytool -keystore ssl/kafka.server.keystore.jks -alias CARoot -import -file ssl/ca-cert.pem
keytool -keystore ssl/kafka.server.keystore.jks -alias localhost -import -file ssl/cert-signed.pem

# Create truststore
keytool -keystore ssl/kafka.server.truststore.jks \
  -alias CARoot \
  -import \
  -file ssl/ca-cert.pem
```

### SSL `server.properties` additions

```properties
listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093

ssl.keystore.location=/path/to/ssl/kafka.server.keystore.jks
ssl.keystore.password=your_keystore_password
ssl.key.password=your_key_password
ssl.truststore.location=/path/to/ssl/kafka.server.truststore.jks
ssl.truststore.password=your_truststore_password

ssl.client.auth=required
ssl.enabled.protocols=TLSv1.2,TLSv1.3
```

### SSL Python Client Config

```python
ssl_config = {
    'bootstrap.servers': 'localhost:9093',
    'security.protocol': 'SSL',
    'ssl.ca.location': 'ssl/ca-cert.pem',
    'ssl.certificate.location': 'ssl/client-cert.pem',
    'ssl.key.location': 'ssl/client-key.pem',
    'ssl.key.password': 'your_key_password'
}
```

---

## 🗃 Multi-Broker Configuration

### Why Multiple Brokers?

```
Single Broker                    3-Broker Cluster
─────────────────                ────────────────────────────────
Topic: bank-events               Topic: bank-events
  Partition 0 → Broker 1           Partition 0 → Broker 1 (Leader)
                                                → Broker 2 (Replica)
                                   Partition 1 → Broker 2 (Leader)
                                                → Broker 3 (Replica)
                                   Partition 2 → Broker 3 (Leader)
                                                → Broker 1 (Replica)

If Broker 1 fails:               If Broker 1 fails:
  ALL data LOST ✗                  Broker 2 elected new leader ✓
                                   No data loss ✓
```

### `broker-2.properties` — Key Differences

```properties
# Unique node ID (must differ from broker 1)
node.id=2

# Different port
listeners=PLAINTEXT://localhost:9094

# Different log directory
log.dirs=/tmp/kraft-broker-2-logs

# Same controller quorum as broker 1
controller.quorum.voters=1@localhost:9093
```

### Start Second Broker

```bash
# Format storage for broker 2 (same cluster ID as broker 1!)
bin/kafka-storage.sh format \
  -t $KAFKA_CLUSTER_ID \
  -c config/broker-2.properties

# Start broker 2
bin/kafka-server-start.sh config/broker-2.properties &
```

### Create Replicated Topic

```bash
bin/kafka-topics.sh --create \
  --topic bank-events-replicated \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 2    # Each partition has 2 replicas
```

---

## 📊 Performance Testing

### Kafka Built-in Benchmarks

```bash
# ─── Producer Benchmark ──────────────────────────────────────────────────────

bin/kafka-producer-perf-test.sh \
  --topic bank-events \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput 100000 \
  --producer-props bootstrap.servers=localhost:9092

# Sample output:
# 100000 records sent, 99234.1 records/sec (97.1 MB/sec),
# 1.2 ms avg latency, 182 ms max latency.

# ─── Consumer Benchmark ──────────────────────────────────────────────────────

bin/kafka-consumer-perf-test.sh \
  --topic bank-events \
  --bootstrap-server localhost:9092 \
  --messages 1000000 \
  --group perf-test-group

# Sample output:
# start.time: 2025-01-01 10:00:00:000
# end.time:   2025-01-01 10:00:08:243
# data.consumed.in.MB: 976.6
# MB.sec: 118.5
# nMsg.sec: 121344.3
```

### Python Throughput Benchmarks

The `throughput-producer.py` measures:
- Messages per second
- Bytes per second
- P50 / P95 / P99 latency

The `throughput-consumer.py` measures:
- Consumption rate (msgs/sec)
- Consumer lag over time
- Poll duration distribution

### Performance Tuning Guide

```
Goal: Maximum Throughput
─────────────────────────
Producer:
  batch.size=1048576    (1MB)
  linger.ms=50
  compression.type=lz4
  acks=1

Consumer:
  fetch.min.bytes=65536
  fetch.max.wait.ms=500
  max.poll.records=5000

─────────────────────────────
Goal: Minimum Latency
─────────────────────────
Producer:
  batch.size=1
  linger.ms=0
  acks=1

Consumer:
  fetch.min.bytes=1
  fetch.max.wait.ms=0
  max.poll.records=1
```

---

## 🔬 Kafka Internals

### Log Storage Format

```
/tmp/kraft-combined-logs/bank-events-0/   ← Partition 0 directory
│
├── 00000000000000000000.log              ← Segment data file
├── 00000000000000000000.index            ← Sparse offset index
├── 00000000000000000000.timeindex        ← Timestamp index
└── 00000000001048576000.log              ← Next segment (after 1GB)
```

### Message Format (v2 — Kafka 0.11+)

```
Record Batch Header:
  baseOffset        (int64)   ← First offset in batch
  batchLength       (int32)   ← Length of batch
  partitionLeaderEpoch (int32)
  magic             (int8)    ← Version = 2
  crc               (int32)   ← CRC32C of everything after
  attributes        (int16)   ← Compression, timestamp type
  lastOffsetDelta   (int32)
  firstTimestamp    (int64)
  maxTimestamp      (int64)
  producerId        (int64)   ← For idempotence
  producerEpoch     (int16)
  baseSequence      (int32)
  records           (ARRAY)

Each Record:
  length            (varint)
  attributes        (int8)
  timestampDelta    (varint)
  offsetDelta       (varint)
  keyLength         (varint)
  key               (bytes)
  valueLen          (varint)
  value             (bytes)
  headers           (ARRAY)
```

### Consumer Group Coordination

```
Consumer Joins Group
      │
      ▼
GroupCoordinator (elected broker)
      │
      ├─► JoinGroup request
      │       ↓ GroupCoordinator picks Leader Consumer
      │
      ├─► SyncGroup request (leader sends partition assignment)
      │
      └─► Assignment:
          Consumer 1 → [Partition 0]
          Consumer 2 → [Partition 1]
          Consumer 3 → [Partition 2]

Rebalance triggers when:
  ✦ New consumer joins
  ✦ Consumer leaves / crashes (session.timeout.ms exceeded)
  ✦ New partitions added to topic
```

---

## 🔧 Troubleshooting

### Common Issues

#### Kafka fails to start

```bash
# Check if port 9092 is already in use
lsof -i :9092
netstat -tulnp | grep 9092

# Kill stale Kafka processes
pkill -f kafka

# Remove stale lock files
rm -f /tmp/kraft-combined-logs/.lock

# Re-format storage (⚠️ deletes all data)
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) \
  -c config/kraft/server.properties
```

#### Consumer not receiving messages

```bash
# Check consumer group status
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list

bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bank-consumer-group

# Reset offset to beginning
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group bank-consumer-group \
  --topic bank-events \
  --reset-offsets \
  --to-earliest \
  --execute
```

#### High consumer lag

```bash
# Check lag
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group bank-consumer-group

# Look for LAG column > 0
# Solutions:
#   1. Add more consumers (up to partition count)
#   2. Increase max.poll.records
#   3. Optimize downstream processing
```

#### SSL handshake failure

```bash
# Test SSL connectivity
openssl s_client -connect localhost:9093 \
  -CAfile ssl/ca-cert.pem

# Verify keystore
keytool -list -keystore ssl/kafka.server.keystore.jks

# Check SSL debug logs
export KAFKA_OPTS="-Djavax.net.debug=ssl,handshake"
bin/kafka-server-start.sh config/kraft/server.properties
```

---

## 🎓 Learning Outcomes

After completing this lab, you will understand:

### Architecture
- ✅ Event-driven architecture patterns
- ✅ Distributed log storage design
- ✅ Partition-based horizontal scaling
- ✅ Leader-follower replication model
- ✅ KRaft consensus protocol (vs ZooKeeper)

### Development
- ✅ Writing high-throughput Python producers
- ✅ Building reliable consumers with manual offset management
- ✅ Implementing stateful stream processors
- ✅ Windowed aggregation pipelines
- ✅ Dead-letter queue patterns

### Operations
- ✅ Kafka performance benchmarking
- ✅ Consumer lag monitoring and diagnosis
- ✅ SSL/TLS encryption setup
- ✅ Multi-broker cluster configuration
- ✅ Kafka storage internals

### Real-World Patterns
- ✅ Exactly-once semantics (idempotent producers)
- ✅ At-least-once delivery guarantees
- ✅ Consumer group rebalancing
- ✅ Fault-tolerant pipeline design
- ✅ Backpressure handling

---

## 🔮 Future Improvements

| Feature | Description | Priority |
|---|---|---|
| Docker Compose | One-command 3-broker cluster setup | 🔴 High |
| Kafka UI | Visual topic/consumer monitoring (Redpanda Console) | 🔴 High |
| Schema Registry | Avro schema enforcement | 🟡 Medium |
| Avro Serialization | Binary format (smaller, schema-enforced) | 🟡 Medium |
| Kafka Connect | JDBC source/sink connectors | 🟡 Medium |
| Multi-Node Cluster | True distributed deployment | 🟡 Medium |
| Apache Spark | Structured Streaming integration | 🟢 Low |
| Kubernetes | Production-grade k8s deployment | 🟢 Low |
| Prometheus / Grafana | Kafka metrics dashboards | 🟡 Medium |
| ksqlDB | SQL over Kafka streams | 🟢 Low |

---

## 👨‍💻 Author

### Utkrist Ark

**Data Science Instructor | Kafka & Distributed Systems Enthusiast | Event Streaming Developer**

Specialising in real-time data pipelines, distributed systems, and production-grade event streaming architectures.

---

## 📜 License

This project is intended for **educational and learning purposes**.

Feel free to use, fork, and adapt for your own learning or teaching materials.

---

## ⭐ Support This Project

If you found this lab helpful, please consider:

```
⭐ Star the repository
🍴 Fork and extend it
📢 Share with your network
🐛 Open issues for improvements
```

```
Star • Fork • Learn • Build • Contribute
```