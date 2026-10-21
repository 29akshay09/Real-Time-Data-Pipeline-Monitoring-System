import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration (overridable via environment variables)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pipeline-events")
MESSAGE_INTERVAL_SECONDS = float(os.getenv("MESSAGE_INTERVAL_SECONDS", "2"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

running = True


def handle_shutdown(signum, frame):
    global running
    logger.info("Received signal %s, shutting down", signum)
    running = False


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,  # small batching window
    )
    logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
    return producer


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "amount": round(random.uniform(100, 10000), 2),
        "status": random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def on_success(metadata):
    logger.info(
        "Sent | topic=%s | partition=%s | offset=%s",
        metadata.topic, metadata.partition, metadata.offset,
    )


def on_error(exc):
    logger.error("Failed to publish: %s", exc)


def main():
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    producer = None
    try:
        producer = create_producer()
        logger.info("Producing to topic: %s", KAFKA_TOPIC)

        while running:
            event = generate_event()
            producer.send(
                KAFKA_TOPIC,
                key=event["customer_id"],   # same customer -> same partition
                value=event,
            ).add_callback(on_success).add_errback(on_error)

            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except KafkaError:
        logger.exception("Kafka error")
    except Exception:
        logger.exception("Unexpected error")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        logger.info("Kafka producer closed")


if __name__ == "__main__":
    main()