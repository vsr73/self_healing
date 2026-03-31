import json

from pipeline.config import KAFKA_SETTINGS

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError as exc:
    KafkaConsumer = None
    KafkaProducer = None
    KAFKA_IMPORT_ERROR = exc
else:
    KAFKA_IMPORT_ERROR = None


def ensure_kafka_dependency():
    if KAFKA_IMPORT_ERROR is not None:
        raise ImportError(
            "Missing Kafka client library. Install it with `pip install kafka-python`."
        ) from KAFKA_IMPORT_ERROR


def build_producer():
    ensure_kafka_dependency()
    return KafkaProducer(
        bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"],
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )


def build_consumer(topics):
    ensure_kafka_dependency()
    if isinstance(topics, str):
        topics = [topics]
    return KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_SETTINGS["bootstrap_servers"],
        group_id=KAFKA_SETTINGS["consumer_group"],
        auto_offset_reset=KAFKA_SETTINGS["auto_offset_reset"],
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )
