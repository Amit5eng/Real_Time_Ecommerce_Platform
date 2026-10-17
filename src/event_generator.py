import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer


EVENT_TYPES = [
    "order_created",
    "payment_completed",
    "payment_failed",
    "order_cancelled",
    "inventory_updated"
]

CITIES = [
    "Mumbai",
    "Delhi",
    "Bangalore",
    "Hyderabad",
    "Pune",
    "Chennai"
]


# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def generate_event():

    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "order_id": f"ORD_{random.randint(10000, 99999)}",
        "customer_id": f"CUS_{random.randint(1000, 9999)}",
        "product_id": f"PROD_{random.randint(100, 999)}",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "city": random.choice(CITIES),
        "event_time": datetime.now().isoformat()
    }

    return event


if __name__ == "__main__":

    print("E-Commerce Kafka Producer Started...")

    try:

        while True:

            event = generate_event()

            # Send event to Kafka
            producer.send("ecommerce_events", value=event)

            # Make sure event is delivered
            producer.flush()

            print("Sent to Kafka:", json.dumps(event))

            time.sleep(2)

    except KeyboardInterrupt:

        print("\nProducer stopped.")

    finally:

        producer.close()