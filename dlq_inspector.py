"""
DLQ Inspector
Reads and prints every message currently sitting in the 'orders-dlq' topic,
along with the reason it failed. Meant to be run on demand to show DLQ
contents during a live demo.
"""

import io
from confluent_kafka import Consumer
from fastavro import schemaless_reader
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
DLQ_TOPIC = "orders-dlq"
SCHEMA_PATH = "schemas/order.avsc"
IDLE_TIMEOUT_SECONDS = 3  # stop once no new message arrives within this window


def deserialize(payload: bytes, schema) -> dict:
    buf = io.BytesIO(payload)
    return schemaless_reader(buf, schema)


def main():
    schema = load_schema(SCHEMA_PATH)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-inspector",  # a FRESH, dedicated group id each run -> always re-reads the whole DLQ from the start
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([DLQ_TOPIC])

    print(f"Reading all messages currently in '{DLQ_TOPIC}'...\n")
    count = 0

    try:
        while True:
            msg = consumer.poll(IDLE_TIMEOUT_SECONDS)
            if msg is None:
                break  # no message within timeout -> we've read everything currently there
            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue

            order = deserialize(msg.value(), schema)
            reason = None
            for key, value in (msg.headers() or []):
                if key == "failure_reason":
                    reason = value.decode("utf-8")

            count += 1
            print(f"{count}. {order}  ->  {reason}")
    finally:
        consumer.close()

    print(f"\nTotal messages currently in DLQ: {count}")


if __name__ == "__main__":
    main()