"""
Kafka Order Consumer
Reads Avro-encoded order messages from the 'orders' topic, computes a
running average price in real time, retries messages that fail for
simulated "temporary" reasons, and routes permanently failed messages
to the 'orders-dlq' Dead Letter Queue topic.

"""

import io
import random
import time

from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import schemaless_reader, schemaless_writer
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-consumer-group"
SCHEMA_PATH = "schemas/order.avsc"

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1
TRANSIENT_FAILURE_RATE = 0.25  


class TransientProcessingError(Exception):
    """Simulates a temporary downstream failure (e.g. a flaky pricing service)."""


class PermanentValidationError(Exception):
    """Raised when the message itself is invalid and cannot be fixed by retrying."""


running_total = 0.0
running_count = 0


def deserialize(payload: bytes, schema) -> dict:
    buf = io.BytesIO(payload)
    return schemaless_reader(buf, schema)


def serialize(order: dict, schema) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, order)
    return buf.getvalue()


def validate(order: dict):
    """Permanent validation: bad data that no retry will fix."""
    if order["price"] <= 0:
        raise PermanentValidationError(f"Invalid price: {order['price']}")


def process_order(order: dict):
    """Simulated business logic (e.g. calling a pricing/inventory service
    that occasionally fails temporarily) plus the real-time aggregation."""
    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientProcessingError("Simulated temporary downstream failure")

    global running_total, running_count
    running_total += order["price"]
    running_count += 1
    avg = running_total / running_count
    print(f"[PROCESSED] {order}  ->  running average price: {avg:.2f}  (n={running_count})")


def send_to_dlq(dlq_producer: Producer, schema, order: dict, reason: str):
    payload = serialize(order, schema)
    headers = [("failure_reason", reason.encode("utf-8"))]
    dlq_producer.produce(topic=DLQ_TOPIC, key=order["orderId"], value=payload, headers=headers)
    dlq_producer.poll(0)
    print(f"[DLQ] {order}  ->  reason: {reason}")


def handle_message(order: dict, dlq_producer: Producer, schema):
    try:
        validate(order)
    except PermanentValidationError as e:
        send_to_dlq(dlq_producer, schema, order, f"permanent: {e}")
        return

    attempt = 0
    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            process_order(order)
            return  # success
        except TransientProcessingError as e:
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"[RETRY {attempt}/{MAX_RETRIES}] {order['orderId']} failed ({e}). Retrying in {backoff}s...")
            time.sleep(backoff)

    send_to_dlq(dlq_producer, schema, order, "temporary failures exceeded max retries")


def main():
    schema = load_schema(SCHEMA_PATH)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"Listening on '{SOURCE_TOPIC}'... (Ctrl+C to stop)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[CONSUMER ERROR] {msg.error()}")
                continue

            order = deserialize(msg.value(), schema)
            handle_message(order, dlq_producer, schema)

            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()