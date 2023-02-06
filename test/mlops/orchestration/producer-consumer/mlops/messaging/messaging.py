"""
Messaging protocols and serialization/deserialization
for the orchestration wrappers. Allows the orchestration
library producers and consumers to determine which
messages apply to them and process those messages accordingly
"""

import logging
from enum import Enum
from typing import Any

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


class Message_Type(Enum):
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


class Annotation_Stage(Enum):
    STAGED = "STAGED"
    """ Dataset to be annotated is staged in shared FS """

    IN_ANNOTATIONS = "IN_ANNOTATIONS"
    """ Dataset is in labeler being annotated (hopefully) """

    ANNOTATIONS_COMPLETE = "ANNOTATIONS_COMPLETE"
    """ Annotations have been completed and annotations are in shared FS """

    UNSTAGED = "UNSTAGED"
    """ Dataset and annotations have been moved from FS to Datalake """


class Mlops_Stage(Enum):
    TRAIN = "TRAIN"
    """ Training model operations """

    FINE_TUNE = "FINE_TUNE"
    """ Fine-tuning or retraining model operations """

    EVALUATION = "EVALUATION"
    """ Evaluating or testing model operations """

    ETL = "ETL"
    """ ETL or pre-processing operations """


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


class MLOPS_Message:
    """
    Structured message that is passed in the MLOPS_Pipeline and most
    commonly utilized by the underlying orchestration of the pipeline

    At minimum, every message will contain a message type and a creator
    type. If the caller does not pass any values, both of these values
    will be set as `Message_Type.Other` and `unkown` respectively. It
    is recommended to pass at least a message type and a creator type.
    """

    def __init__(
        self, msg_type: Message_Type = Message_Type.OTHER.name, **kwargs
    ):
        """
        Create a standard message format for orchestration in
        the MLOPS Pipeline.

        :param msg_type: The type of message being sent. If no user input is
                passed for this parameter, it will default to 'OTHER'
        :type msg_type: Message_Type.name
        :param `**kwargs`: optional arguments provided as keywords that can be
                used to add additional information to a MLOPS_Message.

                :Keyword Arguments:
                    :param topic: the topic/stream/log to publish the message to
                            This will be set when a message is sent
                    :type topic: str
                    :param creator: the user passed name or id for the creator
                    :type creator: str
                    :param creator_type: the standardized type of creator. If no
                            creator is passed, it will be set to `unknown`
                    :type creator_type: str
                    :param stage: the stage within a tool the message refers to
                    :type stage: Annotation_Stage.name or MLOPS_stage.name
                    :param user_message: The message passed by the producer
                    :param user_message: str
                    :param retries: The number of retries that have occured
                    :type retries: int
                    :param output: The output from the event
                    :type output: str or Path or List[str] or List[Path]
                    :param recipient: The intended recipient of the message
                    :type recipient: str or List[str]
                    :param additional_arguments: any other arguments desired
                    :type additional_arguments: Any
        """
        self.msg_type = msg_type
        kwargs_dict = kwargs
        # ensure 'creator_type' is standardized
        if "creator_type" not in kwargs_dict.keys():
            kwargs_dict["creator_type"] = (
                _set_caller_type(kwargs_dict["creator"])
                if "creator" in kwargs_dict.keys()
                else "unknown"
            )
        else:
            kwargs_dict["creator_type"] = _set_caller_type(
                kwargs_dict["creator_type"]
            )
        self.kwargs = kwargs_dict

    def to_json(self):
        """Create a JSON of the MLOPS_Message"""
        return {
            "topic": self.get_topic(),
            "msg_type": self.msg_type,
            "creator": self.get_creator(),
            "creator_type": self.get_creator_type(),
            "stage": self.get_stage(),
            "user_message": self.get_user_message(),
            "retries": self.get_retries(),
            "output": self.get_output(),
            "recipient": self.get_recipient(),
            "additional_arguments": self.get_additional_arguments(),
        }

    """ Pull data from the message """

    def get_topic(self):
        """Get the topic(s) if passed in initialization"""
        return self.kwargs["topic"] if "topic" in self.kwargs.keys() else None

    def get_msg_type(self):
        """Get the type of the message"""
        return self.msg_type

    def get_creator_type(self):
        """Get the creator type"""
        return self.kwargs["creator_type"]

    def get_creator(self):
        """Get the creator name"""
        return (
            self.kwargs["creator"] if "creator" in self.kwargs.keys() else None
        )

    def get_stage(self):
        """Get the stage in the pipeline"""
        return self.kwargs["stage"] if "stage" in self.kwargs.keys() else None

    def get_user_message(self):
        """Get user message"""
        return (
            self.kwargs["user_message"]
            if "user_message" in self.kwargs.keys()
            else None
        )

    def get_retries(self):
        """Get number of retries the event has done"""
        return (
            self.kwargs["retries"] if "retries" in self.kwargs.keys() else None
        )

    def get_output(self):
        """
        Get the output of the stage/tool in the pipeline. This is useful
        if a dataset or model is created and the user wants to direct the
        next tool to its location in storage
        """
        return self.kwargs["output"] if "output" in self.kwargs.keys() else None

    def get_recipient(self):
        """Get the target recipient"""
        return (
            self.kwargs["recipient"]
            if "recipient" in self.kwargs.keys()
            else None
        )

    def get_additional_arguments(self):
        """Get additional arguments that may have been passed by a producer"""
        return (
            self.kwargs["additional_arguments"]
            if "additional_arguments" in self.kwargs.keys()
            else None
        )

    """ Set/update message contents """

    def set_message_data(
        self, msg_section: str, new_input: Any, override: bool = True
    ):
        """
        Set or reset contents in an MLOPS_Message. If
        override is not passed, new user input will override
        any previous input

        :param msg_section: The MLOPS_Message item to change
        :type msg_section: str
        :param new_input: The new user input for the item
        :type new_input: Any
        :param override: Flag to determine whether to override
                previous input
        :type override: bool
        """
        if msg_section in ALLOWED_MESSAGE_KWARGS:
            if override:
                self.kwargs[msg_section] = new_input
            else:
                if isinstance(self.kwargs[msg_section], list):
                    if isinstance(new_input, list):
                        self.kwargs[msg_section] = self.kwargs[
                            msg_section
                        ].extend(new_input)
                    else:
                        self.kwargs[msg_section] = self.kwargs[
                            msg_section
                        ].append(new_input)
                else:
                    if isinstance(new_input, list):
                        self.kwargs[msg_section] = self.kwargs[
                            msg_section
                        ].append(new_input)
                    else:
                        self.kwargs[msg_section] = [
                            self.kwargs[msg_section],
                            new_input,
                        ]
        else:
            logging.info(f"{msg_section} is not recognized in MLOPS_Message.")
