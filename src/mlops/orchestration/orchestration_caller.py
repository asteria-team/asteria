"""
orchestration_caller handles internal messaging
requirements based on the orchestration tool
present in the MLOPs Pipeline. Helps to minimize
the functions presented external of the library to
reduce the need to refactor code if the orchestrator
is changed or a new orchestrator is added.

orchestration_caller redirects event bus calls to
the appropriate python wrapper for the orchestration
tool in use.
"""

import logging
import subprocess
from typing import List, Union

from ..messaging import MLOPS_Message
from .kafka_wrapper import (
    _kafka_consumer_connect,
    _kafka_producer_connect,
    _kafka_send_message,
)

# --------------------------------------------------
# Global Variables
# --------------------------------------------------

endpoint_dict = {"kafka": "kafka:9092"}

APPROVED_ORCHESTRATORS = ["kafka"]

KAFKA_TOPIC = "mlops_pipeline"

# -------------------------------------------------
# Helper Functions
# -------------------------------------------------


def _get_orchestrator() -> Union[str, str]:
    """
    Determines if an orchestrator is present and
    returns the name and endpoint of the orchestrator to allow
    the producers/consumers to direct traffic correctly

    :return: the orchestrator name and endpoint to use in connecting
    :rtype: tuple(str, str)
    """
    for orch, endpoint in endpoint_dict.items():
        command = ["ping", "-c", "1", endpoint]
        if subprocess.call(command) == 0:
            # ping successful, return the name
            return orch, endpoint
    logging.warning("No orchestrator found. Is one running?")
    return None, None


def _test_passed_orchestrator(orc_endpoint: str, orc: str) -> bool:
    """
    test the user passed orchestrator and endpoint information
    to ensure it is correct and will intgrate with the pipeline
    orchestration wrapper

    :param orc_endpoint: the endpoint for producers/consumers to connect to
    :type orc_endpoint: str
    :param orc: the name of the orchestrator
    :type orc: str

    :return: True if valid information passed false otherwise
    :rtype: bool
    """
    # validate orchestrator string
    if orc is None:
        logging.info("Orchestrator name not passed")
        return False
    if orc not in APPROVED_ORCHESTRATORS:
        logging.info(f"{orc} is not an approved orchestrator for the pipeline")
        return False

    # validate orchestrator endpoint
    if orc_endpoint is None:
        logging.info("Orchestrator endpoint not passed")
        return False
    command = ["ping", "-c", "1", orc_endpoint]
    if subprocess.call(command) != 0:
        logging.info(f"Unable to ping orchestrator at {orc_endpoint}")
        return False
    return True


# ------------------------------------------------
# Message Classes
# ------------------------------------------------


class Producer:
    def __init__(
        self,
        orchestrator_endpoint: str = None,
        orchestrator: str = None,
        topic: str or List[str] = None,
        **kwargs,
    ):
        """
        Initialize the higher level Producer class exposed to the user

        :param orchestrator_endpoint: The endpoint of the orchestrator
        :type orchestrator_endpoint: str
        :param orchestrator: the name of the orchestrator in use
        :type orchestrator: str
        :param topic: the event bucket this producer writes to
        :type topic: str or List[str]
        :param **kwargs: any additional configuration requirements desired
                for the Producer. Keys based on underlying orchestration
                requirements
        :type **kwargs: Any
        """
        # determine underlying orchestrator
        if _test_passed_orchestrator(orchestrator_endpoint, orchestrator):
            self.orchestrator = orchestrator
            self.orchestrator_endpoint = orchestrator_endpoint
        else:
            (
                self.orchestrator,
                self.orchestrator_endpoint,
            ) = _get_orchestrator()

        # connect to orchestrator and create the producer
        if self.orchestrator == "kafka":
            if topic is not None:
                self.topic = topic
            else:
                self.topic = KAFKA_TOPIC
            self.producer = _kafka_producer_connect(
                self.orchestrator_endpoint, **kwargs
            )
        else:
            logging.warning(
                "No orchestrator. Pipeline tools will have to be run manually."
            )
            self.producer = None
            self.topic = None

    def send(self, message: MLOPS_Message, flush: bool = True) -> bool:
        """
        Directs producer class messages

        :param message: the message to send via the orchestrator
        :type message: MLOPS_Message
        :param flush: Determines whether or not to immediately push
                message to kafka broker and block thread until acks
        :type flush: bool

        :return: True if successfully sent message and flushed message
                if that argument is passed as true
        :rtype: bool
        """
        if self.orchestrator == "kafka":
            _kafka_send_message(self.producer, message, flush)
        else:
            # no orchestrator case
            logging.info(
                f"Message from: {message.get_creator()} with message: {message.get_user_message()}"
            )

    def flush(self) -> bool:
        """
        If not pushed upon creation of the message, flush will
        push all available messages in the buffer of pending records
        and wait for all acknowledgements to return before returning

        :return: True if all messages are flushed. False otherwise
        :rtype: bool
        """
        pass


class Consumer:
    def __init__(
        self,
        orchestrator_endpoint: str = None,
        orchestrator: str = None,
        topic_subscribe: str = None,
        **kwargs,
    ):
        """
        Initialize the higher level Consumer class exposed to the user

        :param orchestrator_endpoint: The endpoint of the orchestrator
        :type orchestrator_endpoint: str
        :param orchestrator: the name of the orchestrator in use
        :type orchestrator: str
        :param topic_subscribe: where to read/ingests the messages from.
                For kafka this is the topic.
        :type topic_subscription: str
        :param **kwargs: any additional configuration requirements desired
                for the Consumer. Keys based on underlying orchestration
                requirements
        :type **kwargs: Any
        """
        # determine underlying orchestrator
        if _test_passed_orchestrator(orchestrator_endpoint, orchestrator):
            self.orchestrator = orchestrator
            self.orchestrator_endpoint = orchestrator_endpoint
        else:
            (
                self.orchestrator,
                self.orchestrator_endpoint,
            ) = _get_orchestrator()

        # connect to orchestrator and create the producer
        if self.orchestrator == "kafka":
            if topic_subscribe is not None:
                self.topic = topic_subscribe
            else:
                self.topic = KAFKA_TOPIC
            self.consumer = _kafka_consumer_connect(
                self.orchestrator_endpoint, self.topic, **kwargs
            )
        else:
            logging.warning(
                "No orchestrator. Pipeline tools will have to be run manually."
            )
            self.consumer = None
            self.topic = None


class Admin:
    def __init__(self):
        # determine underlying orchestrator
        self.orchestrator = _get_orchestrator()
        # establish connection to orchestrator based on the service
        # create the correct version of the admin based on the service
        pass

    # all subsequent functionality simply act as a redirect to the
    # appropriate underlying orchestration wrapper
