"""
Wrapper for Kafka to integrate into the pipeline when
it is used as the orchestrator of choice in the MLOPs
pipeline
"""

import socket

from kafka import KafkaConsumer, KafkaProducer

from ..pipeline_logger import PipelineLogger
from .messaging import (
    Stage_Status,
    build_kafka_json,
    json_deserializer,
    json_serializer,
    set_producer_consumer_type,
)

# Application exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# -----------------------------------------------
# Helper Functions
# -----------------------------------------------

# -----------------------------------------------
# Connection
# -----------------------------------------------

# -----------------------------------------------
# Producer
# -----------------------------------------------

# -----------------------------------------------
# Consumer
# -----------------------------------------------

# -----------------------------------------------
# Admin
# -----------------------------------------------
