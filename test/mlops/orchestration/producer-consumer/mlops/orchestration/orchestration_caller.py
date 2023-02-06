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
    _kafka_close_consumer,
    _kafka_close_producer,
    _kafka_consumer_connect,
    _kafka_flush_messages,
    _kafka_format_topic,
    _kafka_get_subscriptions,
    _kafka_poll_messages,
    _kafka_producer_connect,
    _kafka_send_message,
    _kafka_subscribe,
    _kafka_unsubscribe,
)

# --------------------------------------------------
# Global Variables
# --------------------------------------------------

endpoint_dict = {"kafka": "kafka:9092"}

APPROVED_ORCHESTRATORS = ["kafka"]

KAFKA_TOPIC = "mlops-pipeline"

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
        command = ["ping", "-c", "1", orch]
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
    command = ["ping", "-c", "1", orc]
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
        :param topic: the event bucket this producer writes to. Should not
                contain any white spaces. These will be removed.
        :type topic: str or List[str]
        :param `**kwargs`: any additional configuration requirements desired
                for the Producer. Keys based on underlying orchestration
                requirements. Look at the docs for the orchestrator wrapper
                to determine desired requirements
        :type `**kwargs`: Any
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
                self.topic = _kafka_format_topic(topic)
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

    def send(
        self, message: MLOPS_Message = MLOPS_Message(), flush: bool = False
    ):
        """
        Directs producer class messages

        :param message: the message to send via the orchestrator
        :type message: MLOPS_Message
        :param flush: Determines whether or not to immediately push
                message to kafka broker and block thread until acks
        :type flush: bool
        """
        if self.orchestrator == "kafka":
            if message.get_topic() is not None:
                _kafka_send_message(
                    self.producer,
                    _kafka_format_topic(message.get_topic()),
                    message,
                    flush,
                )
            else:
                _kafka_send_message(self.producer, self.topic, message, flush)
        else:
            # no orchestrator case
            logging.info(
                f"Message from: {message.get_creator()} with message: {message.get_user_message()} not sent. No Orchestrator"
            )

    def flush(self) -> bool:
        """
        If not pushed upon creation of the message, flush will
        push all available messages in the buffer of pending records
        and wait for all acknowledgements to return before returning

        :return: True if all messages are flushed. False otherwise
        :rtype: bool
        """
        if self.orchestrator == "kafka":
            return _kafka_flush_messages(self.producer)
        else:
            # no orchestrator case
            logging.info("No orchestrator set up. Flushing not required.")
            return False

    def close(self):
        """Close the producer"""
        if self.orchestrator == "kafka":
            _kafka_close_producer(self.producer)
        else:
            logging.info("No orchestrator set up. Closing not required.")


class Consumer:
    def __init__(
        self,
        orchestrator_endpoint: str = None,
        orchestrator: str = None,
        topic_subscribe: str or List[str] = None,
        **kwargs,
    ):
        """
        Initialize the higher level Consumer class exposed to the user

        :param orchestrator_endpoint: The endpoint of the orchestrator
        :type orchestrator_endpoint: str
        :param orchestrator: the name of the orchestrator in use
        :type orchestrator: str
        :param topic_subscribe: where to read/ingests the messages from.
                For kafka this is the topic. Should not
                contain any white spaces. These will be removed.
        :type topic_subscription: str
        :param **kwargs: any additional configuration requirements desired
                for the Consumer. Keys based on underlying orchestration
                requirements. Look at the docs for the orchestrator wrapper
                to determine desired requirements
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
                self.topic = _kafka_format_topic(topic_subscribe)
                self.consumer = _kafka_consumer_connect(
                    self.orchestrator_endpoint, self.topic, **kwargs
                )
            else:
                self.topic = None
                self.consumer = _kafka_consumer_connect(
                    self.orchestrator_endpoint, **kwargs
                )
        else:
            logging.warning(
                "No orchestrator. Pipeline tools will have to be run manually."
            )
            self.consumer = None
            self.topic = topic_subscribe

    def subscribe(self, topic: str or List[str], override: bool = False):
        """
        Where to have the consumer subscribe if not subscribed
        in initialization.

        :param topic: The topic/event bus to subscribe to. Should not
                contain any white spaces. These will be removed.
        :type topic: str
        :param override: Whether or not to remove previous
                subscriptions for the consumer if they exist
        """
        # update subscriptions
        if self.topic is None or override:
            self.topic = topic
        else:
            self.topic = [self.topic] + [topic]

        if self.orchestrator == "kafka":
            _kafka_subscribe(self.consumer, _kafka_format_topic(self.topic))
        else:
            logging.info("No orchestrator. Subscription not required")

    def unsubscribe(self):
        """Unsubscribe from all current subscriptions"""
        if self.orchestrator == "kafka":
            _kafka_unsubscribe(self.consumer)
        else:
            self.topic = None
            logging.info("No orchestrator set up. No subscriptions")

    def get_subscriptions(self) -> str or List[str]:
        """Get the current subscriptions of the consumer"""
        if self.orchestrator == "kafka":
            return _kafka_get_subscriptions(self.consumer)
        else:
            logging.warning("No orchestrator set up. Topics not subscribed to")
            return self.topic

    def get_messages(
        self, filter: callable = lambda _: True, **kwargs
    ) -> MLOPS_Message or List[MLOPS_Message]:
        """
        Get messages from wherever the consumer is subscribed. If
        the consumer is not subscribed anywhere, it will be set to
        the default subscription for the particular orchestrator

        :param filter: a function to filter incoming messages. Must
                result in a bool and called on an MLOPS_message.
                Examples: `lambda msg: msg.get_topic()=="Pipeline"`
                          `lambda msg: msg.get_creator_type()=="ingestor"
                                    and msg.get_output() is not None
        :type filter: callable
        :param **kwargs: additional data to pass to the various underlying
                functions to drive how messages are retrieved. Currently
                the only kwarg to pass is `timeout` (int) which will adjust
                the timeout in ms for the kafka poll
        :param **kwargs: Any

        :return: the latest message(s) in MLOPS_Message format that
                satisfy the filter passed
        :rtype: MLOPS_Message or List[MLOPS_Message]
        """
        if self.orchestrator == "kafka":
            # ensure consumer is subscribed to at least one topic
            if len(self.get_subscriptions()) == 0:
                if self.topic is None:
                    self.topic = KAFKA_TOPIC
                self.subscribe(_kafka_format_topic(self.topic))
            return _kafka_poll_messages(self.consumer, filter, **kwargs)
        else:
            logging.info("No orchestrator set up. Cannot get messages")

    def close(self):
        """Close the consumer"""
        if self.orchestrator == "kafka":
            _kafka_close_consumer(self.consumer)
        else:
            logging.info("No orchestrator set up. Closing not required.")


class Admin:
    def __init__(self):
        # determine underlying orchestrator
        self.orchestrator = _get_orchestrator()
        # establish connection to orchestrator based on the service
        # create the correct version of the admin based on the service
        pass

    # all subsequent functionality simply act as a redirect to the
    # appropriate underlying orchestration wrapper
