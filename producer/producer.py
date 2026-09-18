"""
Kafka Order Producer
Generates simulated order events and publishes them to the 'orders' topic,
serialized using Avro (schema: schemas/order.avsc).

"""

import io
import random
import time

from confluent_kafka import Producer
from fastavro import schemaless_writer
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SCHEMA_PATH = "schemas/order.avsc"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
BAD_PRICE_RATE = 0.10   # fraction of messages with an intentionally invalid price
NUM_MESSAGES = 30
DELAY_SECONDS = 0.5     # pause between sends so you can watch the consumer live


def generate_order(order_counter: int) -> dict:
    price = round(random.uniform(5.0, 500.0), 2)
    if random.random() < BAD_PRICE_RATE:
        price = round(random.uniform(-50.0, 0.0), 2)  # intentionally invalid -> permanent failure later

    return {
        "orderId": str(1000 + order_counter),
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def serialize(order: dict, schema) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, order)
    return buf.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY FAILED] {msg.key()}: {err}")
    else:
        print(f"[DELIVERED] partition={msg.partition()} offset={msg.offset()}")


def main():
    schema = load_schema(SCHEMA_PATH)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Producing {NUM_MESSAGES} order messages to topic '{TOPIC}'...\n")

    run_offset = int(time.time()) % 100000  # unique starting point each run

    for i in range(NUM_MESSAGES):
        order = generate_order(run_offset + i)
        payload = serialize(order, schema)

        producer.produce(
            topic=TOPIC,
            key=order["orderId"],
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)  # let delivery report callbacks fire

        print(f"[SENT] {order}")
        time.sleep(DELAY_SECONDS)

    print("\nFlushing remaining messages...")
    producer.flush()
    print("Done.")


if __name__ == "__main__":
    main()