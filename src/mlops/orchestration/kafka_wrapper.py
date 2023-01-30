"""
Wrapper for Kafka to integrate into the pipeline when
it is used as the orchestrator of choice in the MLOPs
pipeline
"""

import socket

from kafka import KafkaConsumer, KafkaProducer
from .messaging import Stage_Status, json_serializer, json_deserializer, set_producer_consumer_type, build_kafka_json
from ..pipeline_logger import PipelineLogger

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
