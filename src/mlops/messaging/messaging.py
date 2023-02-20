"""
Messaging protocols and serialization/deserialization
for the orchestration wrappers. Allows the orchestration
library producers and consumers to determine which
messages apply to them and process those messages accordingly
"""

import logging
from enum import Enum
from pathlib import Path
from typing import Any, List, Union

from .serialize import _json_deserializer, _json_serializer

# -----------------------------------------------
# Messaging Data Restrictions
# -----------------------------------------------

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
    "unknown": ["unknown"],
}

# Message format limitations
ALLOWED_MESSAGE_KWARGS = [
    "creator",
    "topic",
    "stage",
    "user_message",
    "retries",
    "output",
    "recipient",
    "additional_arguments",
]

# -----------------------------------------------
# Messaging Enums
# -----------------------------------------------


class MessageType(Enum):
    """The message type"""

    START = "START"
    """ Starting a tool or process """

    COMPLETE = "COMPLETE"
    """ Completion of a tool or process """

    FAIL = "FAIL"
    """ failure of a tool or process """

    INPROGRESS = "INPROGRESS"
    """ a tool or process is in-progress """

    RETRY = "RETRY"
    """ Retrying a tool or process"""

    OTHER = "OTHER"
    """ Alternative message type option for unique user input """

    def __str__(self):
        return self.value


class Stages(Enum):
    STAGED = "STAGED"
    """ Dataset to be annotated is staged in shared FS """

    IN_ANNOTATIONS = "IN_ANNOTATIONS"
    """ Dataset is in labeler being annotated (hopefully) """

    ANNOTATIONS_COMPLETE = "ANNOTATIONS_COMPLETE"
    """ Annotations have been completed and annotations are in shared FS """

    UNSTAGED = "UNSTAGED"
    """ Dataset and annotations have been moved from FS to Datalake """

    TRAIN = "TRAIN"
    """ Training model operations """

    FINE_TUNE = "FINE_TUNE"
    """ Fine-tuning or retraining model operations """

    EVALUATION = "EVALUATION"
    """ Evaluating or testing model operations """

    ETL = "ETL"
    """ ETL or pre-processing operations """

    def __str__(self):
        return self.value


# -----------------------------------------------
# Conversion to Allowed Types
# -----------------------------------------------


def _set_caller_type(user_input: str) -> str:
    """Convert passed producer and consumer initializer strings"""
    for pc_type, options in ALLOWED_PRODUCERS_CONSUMERS.items():
        if user_input.lower() in [opt for opt in options]:
            return pc_type
    logging.error(f"Type {user_input} is not recognized. Failed to set type")
    return "unknown"


# -----------------------------------------------
# Messaging Classes
# -----------------------------------------------


class MessageBuilder:
    """
    User-friendly interface to build an MLOpsMessage. Once build is
    called, the resulting MLOps_Message will be immutable

    At minimum, every message will contain a message type and a creator
    type. If the caller does not pass any values, both of these values
    will be set as `MessageType.Other` and `unkown` respectively. It
    is recommended to pass at least a message type and a creator type.

    MessageBuilder supports inputs for all information that can be
    passed in the defined MLOpsMessage structure, and ensures message
    is constructed correctly

    Arguments that can be passed:
    :param msg_type: The type of message being sent. If no user input is
                passed for this parameter, it will default to 'OTHER'
    :type msg_type: MessageType
    :param topic: the topic/stream/log to publish the message to
            This is recommended for consumer filtering purposes
    :type topic: Union[str, List[str]]
    :param creator: the user passed name or id for the creator
    :type creator: str
    :param creator_type: the standardized type of creator. If no
            creator_type is passed, it will be set to `unknown`
    :type creator_type: str
    :param stage: the stage within a tool the message refers to
    :type stage: Stages
    :param next_task: The subsequent task(s) that should start
            with the sending of the message
    :param user_message: The message passed by the producer
    :param user_message: str
    :param retries: The number of retries that have occured
    :type retries: int
    :param output: The output from the event
    :type output: str or Path or List[str] or List[Path]
    :param recipient: The intended recipient of the message
    :type recipient: str or List[str]
    :param additional_data: any other arguments desired
    :type additional_data: Any
    """

    def __init__(self):
        """Initializes a MessageBuilder"""
        self._msg_type = MessageType.OTHER
        self._topic = None
        self._creator = None
        self._creator_type = "unknown"
        self._stage = None
        self._next_task = None
        self._user_message = None
        self._retries = 0
        self._output = None
        self._recipient = None
        self._additional_data = None

    def with_msg_type(self, msg_type: MessageType):
        """Add message type to builder"""
        self._msg_type = msg_type
        return self

    def with_topic(self, topic: Union[str, List[str]]):
        """Add topic(s) to builder"""
        self._topic = topic
        return self

    def with_creator(self, creator: str):
        """Add a creator to builder"""
        self._creator = creator
        return self

    def with_creator_type(self, creator_type: str):
        """Add creator type to builder"""
        self._creator_type = _set_caller_type(creator_type)
        return self

    def with_stage(self, stage: Stages):
        """Add a stage to builder"""
        self._stage = stage
        return self

    def with_next_task(self, task: Union[str, List[str]]):
        """Add tasks to builder"""
        self._next_task = task
        return self

    def with_user_msg(self, usr_msg: str):
        """Add a user message to builder"""
        self._user_message = usr_msg
        return self

    def with_retries(self, retries: int):
        """Add number of retries to builder"""
        self._retries = retries
        return self

    def with_output(self, output: Union[str, List[str], Path, List[Path]]):
        """Add output(s) to builder"""
        self._output = output
        return self

    def with_recipient(self, recipient: Union[str, List[str]]):
        """Add recipient(s) to builder"""
        self._recipient = recipient
        return self

    def with_additional_data(self, add_data: Any):
        """Add additional user data to builder"""
        self._additional_data = add_data
        return self

    def build(self):
        """
        Build the MLOpsMessage class with the previously passed input.
        Ensures integrity of data being passed

        :return: an MLOpsMessage to use with the orchestration
        :rtype: MLOpsMessage
        """
        assert isinstance(self._msg_type, MessageType)
        assert isinstance(self._creator_type, str)

        return MLOpsMessage(
            msg_type=self._msg_type,
            topic=self._topic,
            creator=self._creator,
            creator_type=self._creator_type,
            stage=self._stage,
            next_task=self._next_task,
            usr_msg=self._user_message,
            retries=self._retries,
            output=self._output,
            recipient=self._recipient,
            add_data=self._additional_data,
        )


class MessageDeserializer:
    def __init__(self, serial_msg: bytearray):
        self.json = _json_deserializer(serial_msg)

    def __call__(self):
        return (
            MessageBuilder()
            .with_msg_type(MessageType(self.json["msg_type"]))
            .with_topic(self.json["topic"])
            .with_creator(self.json["creator"])
            .with_creator_type(self.json["creator_type"])
            .with_stage(
                Stages(self.json["stage"])
                if self.json["stage"] is not None
                else None
            )
            .with_next_task(self.json["next_task"])
            .with_user_msg(self.json["usr_msg"])
            .with_retries(int(self.json["retries"]))
            .with_output(self.json["output"])
            .with_recipient(self.json["recipient"])
            .with_additional_data(self.json["add_data"])
            .build()
        )


class MLOpsMessage:
    """
    Structured message that is passed in the MLOPS_Pipeline and most
    commonly utilized by the underlying orchestration of the pipeline

    At minimum, every message will contain a message type and a creator
    type. If the caller does not pass any values, both of these values
    will be set as `MessageType.Other` and `unkown` respectively. It
    is recommended to pass at least a message type and a creator type.

    This class is only intended to be built with the :class:`MessageBuilder`
    """

    def __init__(self, **kwargs):
        """
        Create a standard message format for orchestration in
        the MLOPS Pipeline. See :class:`MessageBuilder` for more information
        """
        self.kwargs = kwargs

    """ Message conversion for sending """

    def to_json(self):
        """Create a JSON of the MLOpsMessage"""
        return {
            "msg_type": str(self.message_type),
            "topic": self.topic,
            "creator": self.creator,
            "creator_type": self.creator_type,
            "stage": str(self.stage) if self.stage is not None else None,
            "next_task": self.next_task,
            "usr_msg": self.user_message,
            "retries": self.retries,
            "output": self.output,
            "recipient": self.recipient,
            "add_data": self.additional_data,
        }

    def json_serialization(self):
        """Serialize a MLOpsMessage to send to orchestrator"""
        return _json_serializer(self.to_json())

    """ Read data from the message """

    @property
    def message_type(self):
        """Get the type of the message"""
        return self.kwargs["msg_type"]

    @property
    def topic(self):
        """Get the topic(s) if passed in initialization"""
        return self.kwargs["topic"]

    @property
    def creator(self):
        """Get the creator name"""
        return self.kwargs["creator"]

    @property
    def creator_type(self):
        """Get the creator type"""
        return self.kwargs["creator_type"]

    @property
    def stage(self):
        """Get the stage in the pipeline"""
        return self.kwargs["stage"]

    @property
    def next_task(self):
        """Get the next task(s) that should happen"""
        return self.kwargs["next_task"]

    @property
    def user_message(self):
        """Get user message"""
        return self.kwargs["usr_msg"]

    @property
    def retries(self):
        """Get number of retries the event has done"""
        return self.kwargs["retries"]

    @property
    def output(self):
        """
        Get the output of the stage/tool in the pipeline. This is useful
        if a dataset or model is created and the user wants to direct the
        next tool to its location in storage
        """
        return self.kwargs["output"]

    @property
    def recipient(self):
        """Get the target recipient"""
        return self.kwargs["recipient"]

    @property
    def additional_data(self):
        """Get additional data that may have been passed by a producer"""
        return self.kwargs["add_data"]
