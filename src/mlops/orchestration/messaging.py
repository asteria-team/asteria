"""
Messaging protocols and serialization/deserialization
for the orchestration wrappers. Allows the orchestration
library producers and consumers to determine which
messages apply to them and process those messages accordingly
"""

import json
import logging
from enum import Enum
from typing import Any, Dict

ENCODER = "utf-8"

# -----------------------------------------------
# Messaging Data
# -----------------------------------------------

MONITOR_DICT = {
    "ingestion": ["dump"],
    "annotation": ["ingestion"],
    "backend": ["annotation", "mlops"],
    "mlops": ["backend"],
    "monitor": ["dump", "ingestion", "annotation", "backend", "mlops"],
}

# allowed user input strings
dump_opts = ["dump", "storage", "offload", "drop"]
ingest_opts = ["ingestion", "ingest", "ingestor", "processor"]
annotation_opts = [
    "annotation",
    "annotator",
    "labels",
    "labeler",
    "labeling",
    "annotations",
]
backend_opts = [
    "backend",
    "curation",
    "data curation",
    "dataset",
    "dataset builder",
]
mlops_opts = [
    "mlops",
    "train",
    "training",
    "eval",
    "evaluation",
    "testing",
    "test",
    "retrain",
    "retraining",
    "fine-tune",
    "fine-tuning",
    "fine tune",
    "fine tuning",
    "transfer learning",
    "etl",
    "preprocessing",
    "augmentation",
    "augments",
]
monitor_opts = ["monitor", "monitoring", "db monitor", "pipeline monitor"]

ALLOWED_PRODUCERS_CONSUMERS = {
    "dump": dump_opts,
    "ingestion": ingest_opts,
    "annotation": annotation_opts,
    "backend": backend_opts,
    "mlops": mlops_opts,
    "monitor": monitor_opts,
}

# -----------------------------------------------
# Stage Status Enums
# -----------------------------------------------


class Stage_Status(Enum):
    START = 1
    COMPLETE = 2
    FAIL = 3
    INPROGRESS = 4
    # RETRY = 5


class Annotation_Substage(Enum):
    STAGED = 1
    IN_ANNOTATIONS = 2
    ANNOTATIONS_COMPLETE = 3
    UNSTAGED = 4


class Mlops_Substage(Enum):
    TRAIN = 1
    FINE_TUNE = 2
    EVALUATION = 3
    ETL = 4


# -----------------------------------------------
# JSON serial/deserialization
# -----------------------------------------------


def json_serializer(msg_to_serial: Dict[str, Any]) -> str:
    """Serialize a dictionary into a json string"""
    if msg_to_serial is None:
        return None
    else:
        return json.dumps(msg_to_serial).encode(ENCODER)


def json_deserializer(serial_msg: bytearray) -> Dict[str, Any]:
    """Deserialize a json bytearray into a dictionary"""
    try:
        if serial_msg is None:
            return None
        else:
            return json.loads(serial_msg.decode(ENCODER))
    except json.decoder.JSONDecodeError:
        logging.error(f"Error decoding JSON message: {serial_msg}")
        return None


# -----------------------------------------------
# Conversion to Allowed Types
# -----------------------------------------------


def set_producer_consumer_type(user_input: str) -> str:
    """Convert passed producer and consumer initializer strings"""
    for pc_type, options in ALLOWED_PRODUCERS_CONSUMERS.items():
        if user_input.lower() in [opt for opt in options]:
            return pc_type
    logging.error(f"Type {user_input} is not recognized. Failed to set type")
    return "unknown"


def set_stage_types(user_input: str) -> str:
    """TODO Convert passed pipeline stage strings"""
    return user_input


# -----------------------------------------------
# Kafka Messages
# -----------------------------------------------


def build_kafka_json(msg_type, producer: str, **kwargs) -> Dict[str, str]:
    """
    Builds a dictionary message for kafka producers
    - :param msg_type: message status (status of the tool) (required)
    - :param producer: tool that sent the message (required)
    - :param stage: additional information about the status (optional)
    - :param output: output information from the event if any (optional)
    - :param user_msg: The message from the producer (optional)
    - :param retries: number of attempts at the stage (optional)
    """
    kafka_dict = {"msg_type": str(msg_type), "producer": producer}

    # get **kwargs if passed
    stage = kwargs.get("stage", None)
    output = kwargs.get("output", None)
    user_msg = kwargs.get("user_msg", None)
    retries = kwargs.get("retries", 0)

    # add **kwargs and return
    kafka_dict["stage"] = str(stage)
    kafka_dict["output"] = str(output)
    kafka_dict["user_msg"] = str(user_msg)
    kafka_dict["retries"] = str(retries)

    return kafka_dict
