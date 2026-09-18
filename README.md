# Kafka Order Processing 

A Kafka-based system that produces and consumes simulated order events using
Avro serialization, with real-time price aggregation, retry logic for
temporary failures, and a Dead Letter Queue (DLQ) for permanent failures.

## Architecture

- **Broker**: single-node Apache Kafka 4.3.1, KRaft mode (no ZooKeeper), run via Docker Compose.
- **Topics**: `orders` (main topic), `orders-dlq` (Dead Letter Queue).
- **Schema**: `schemas/order.avsc` — Avro record with `orderId` (string), `product` (string), `price` (float).
- **Producer** (`producer/producer.py`): generates randomized order events and
  publishes them Avro-encoded to `orders`. ~10% of messages carry an
  intentionally invalid price (≤ 0) to simulate bad upstream data.
- **Consumer** (`consumer/consumer.py`):
  - Decodes each Avro message.
  - Validates it; an invalid price is a **permanent** failure — routed straight to the DLQ, no retries.
  - Otherwise "processes" the order (simulated business logic with a
    configurable random failure rate representing a flaky downstream
    dependency). On failure, retries up to `MAX_RETRIES` times with
    exponential backoff (1s, 2s, 4s).
  - If retries are exhausted, the message is routed to the DLQ with reason
    `"temporary failures exceeded max retries"`.
  - On success, updates an in-memory running average of order prices and
    prints it.
  - Kafka offsets are committed manually, only after a message is fully
    handled (processed or DLQ'd), for at-least-once delivery semantics.
- **DLQ inspector** (`dlq_inspector.py`): standalone script that reads and
  prints all messages currently in `orders-dlq`, along with their failure
  reason (stored as a Kafka message header).

## Prerequisites
- Docker Desktop
- Python 3.13 (conda environment recommended)
- Git

## Setup

1. Start the broker:
docker compose up -d

2. Create topics (one-time):

docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

3. Create the Python environment and install dependencies:

conda create -n kafka-assignment python=3.13 -y
conda activate kafka-assignment
pip install confluent-kafka fastavro


## Running

Run the consumer and producer in separate terminals (consumer first, so you can watch it process messages live):

python consumer/consumer.py

python producer/producer.py


Inspect the DLQ at any time:

python dlq_inspector.py


## Design notes / tunable parameters

- `consumer/consumer.py`: `MAX_RETRIES` (default 3), `BASE_BACKOFF_SECONDS`
  (default 1, doubles each attempt), `TRANSIENT_FAILURE_RATE` (default 0.25 —
  can be temporarily raised, e.g. to 0.85, to reliably demonstrate the
  retry-exhaustion → DLQ path in a live demo).
- `producer/producer.py`: `BAD_PRICE_RATE` (default 0.10), `NUM_MESSAGES`
  (default 30).

## No Schema Registry

This project uses a local Avro schema file (`schemas/order.avsc`) shared
directly between producer and consumer, rather than a Confluent Schema
Registry, since the assignment does not require centralized schema
management.
