"""
Wrapper for Kafka to integrate into the pipeline when
it is used as the orchestrator of choice in the MLOPs
pipeline
"""


from kafka import KafkaConsumer, KafkaProducer

from ..pipeline_logger import PipelineLogger
from .messaging import json_deserializer, json_serializer

# -----------------------------------------------
# Helper Functions
# -----------------------------------------------

# -----------------------------------------------
# Connection
# -----------------------------------------------


def kafka_producer_connect(endpoint: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=endpoint,
        value_serializer=lambda msg: json_serializer(msg),
    )


def kafka_consumer_connect(endpoint: str, subscription: str) -> KafkaConsumer:
    return KafkaConsumer(
        subscription,
        bootstrap_servers=endpoint,
        value_deserializer=lambda msg: json_deserializer(msg),
    )


# -----------------------------------------------
# Producer
# -----------------------------------------------


def kafka_complete_message(prod: KafkaProducer, prolog: PipelineLogger):
    pass


def kafka_failure_message(prolog: PipelineLogger):
    pass


def kafka_progress_message(prolog: PipelineLogger):
    pass


def kafka_start_message(prolog: PipelineLogger):
    pass


# -----------------------------------------------
# Consumer
# -----------------------------------------------


def kafka_subscribe(prolog: PipelineLogger):
    pass


# -----------------------------------------------
# Admin
# -----------------------------------------------
