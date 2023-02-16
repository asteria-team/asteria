"""
Orchestration handles internal messaging
requirements based on the orchestration tool
present in the MLOPs Pipeline. Helps to minimize
the functions presented external of the library to
reduce the need to refactor code if the orchestrator
is changed or a new orchestrator is added.

orchestration redirects event bus calls to
the appropriate python wrapper for the orchestration
tool in use while only exposing three classes and
a simple set of methods to the user
"""

import logging
from typing import List, Union

from ..messaging import MLOpsMessage
from .kafka import KA, KC, KP
from .orchestration_discovery import Orchestration

# ------------------------------------------------
# Message Classes
# ------------------------------------------------


class Producer:
    """Producer class for Orchestration"""

    def __init__(
        self,
        orchestration: Orchestration = Orchestration(),
        topic: Union[str, List[str]] = None,
        **kwargs,
    ):
        """
        Initialize the generalized Producer class exposed to the user

        :param orchestration: the orchestrator class that contains the
                required information to connect to the underlying
                orchestration. If not passed, the Producer will attempt
                to determine the orchestrator
        :type orchestrator: Orchestrator
        :param topic: the event bucket/location(s) this producer writes to.
                Should only contain alphanumeric characters or dashes or
                underscores. Not required to pass but a topic is needed to
                send messages
        :type topic: str or List[str]
        :param `**kwargs`: any additional configuration requirements desired
                for the Producer. Keys based on underlying orchestration
                requirements. Look at the docs for the orchestrator wrapper
                to determine desired requirements
        :type `**kwargs`: Any
        """
        self.topic = topic

        if orchestration.orchestrator == "kafka":
            self.producer = KP(orchestration, **kwargs)
        elif orchestration.orchestrator == "rabbitmq":
            # Currently rabbitmq is not supported
            self.producer = None
        else:
            logging.error(
                "No orchestrator. Pipeline tools will have to be run manually."
            )
            self.producer = None

    def send(self, message: MLOpsMessage = MLOpsMessage(), flush: bool = False):
        """
        Send a Message with the Producer class

        :param message: the message to send via the orchestrator
        :type message: MLOpsMessage
        :param flush: Determines whether or not to immediately push
                message to kafka broker and block thread until acks
        :type flush: bool
        """
        try:
            # prioritize topic(s) in passed message
            if message.topic is not None:
                self.producer.send(message.topic, message, flush)
            elif self.topic is not None:
                self.producer.send(self.topic, message, flush)
            else:
                raise ValueError(
                    "No topic was passed to Producer or with message. Cannot send"
                )
        except Exception as e:
            raise RuntimeError(f"failed to send producer message: {e}")

    def close(self):
        """Close the producer"""
        try:
            self.producer.close()
        except Exception as e:
            raise RuntimeError(f"Failed to close the Producer: {e}")


class Consumer:
    """Consumer class for Orchestration"""

    def __init__(
        self,
        subscription: Union[str, List[str]],
        orchestration: Orchestration = Orchestration(),
        **kwargs,
    ):
        """
        Initialize the generalized Consumer class exposed to the user

        :param subscription: where to read/ingests the messages from.
                For kafka this is the topic. Should only contain
                alphanumeric characters or dashes or underscores
        :type subscription: str
        :param orchestration: the orchestrator class that contains the
                required information to connect to the underlying
                orchestration. If not passed, the Producer will attempt
                to determine the orchestrator
        :type orchestrator: Orchestrator
        :param **kwargs: any additional configuration requirements desired
                for the Consumer. Keys based on underlying orchestration
                requirements. Look at the docs for the orchestrator wrapper
                to determine desired requirements
        :type **kwargs: Any
        """
        self.subscription = subscription

        if orchestration.orchestrator == "kafka":
            self.consumer = KC(orchestration, subscription, **kwargs)
        elif orchestration.orchestrator == "rabbitmq":
            # Currently rabbitmq is not supported
            self.consumer = None
        else:
            logging.error(
                "No orchestrator. Pipeline tools will have to be run manually."
            )
            self.consumer = None

    def subscribe(self, subscription: str or List[str], override: bool = True):
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
        if override:
            self.subscription = subscription
        else:
            if isinstance(self.subscription, list):
                if isinstance(subscription, list):
                    self.subscription.extend(subscription)
                else:
                    self.subscription.append(subscription)
            else:
                if isinstance(subscription, list):
                    self.subscription = subscription.append(self.subscription)
                else:
                    self.subscription = list(self.subscription, subscription)

        self.consumer.subscribe(self.subscription)

    def recieve(
        self, filter_func: callable = lambda _: True, **kwargs
    ) -> MLOpsMessage or List[MLOpsMessage]:
        """
        Get messages from wherever the consumer is subscribed.

        :param filter: a function to filter incoming messages. Must
                result in a bool and called on an MLOpsMessage.
                Examples: `lambda msg: msg.topic=="Pipeline"`
                          `lambda msg: msg.creator_type=="ingestor"
                                    and msg.output is not None
        :type filter: callable
        :param **kwargs: additional data to pass to the various underlying
                functions to drive how messages are retrieved. Currently
                the only kwarg to pass is `timeout` (int) which will adjust
                the timeout in ms for polls
        :param **kwargs: Any

        :return: the latest message(s) in MLOpsMessage format that
                satisfy the filter passed
        :rtype: MLOpsMessage or List[MLOpsMessage]
        """
        try:
            return self.consumer.recieve(filter_func, **kwargs)
        except Exception as e:
            raise RuntimeError(f"failed to recieve message: {e}")

    def close(self):
        """Close the consumer"""
        try:
            return self.consumer.close()
        except Exception as e:
            raise RuntimeError(f"Failed to close the Consumer: {e}")


class Admin:
    """Admin class for Orchestration"""

    def __init__(
        self,
        orchestration: Orchestration = Orchestration(),
        **kwargs,
    ):
        """
        Initialize the generalized Admin class exposed to the user

        :param orchestration: the orchestrator class that contains the
                required information to connect to the underlying
                orchestration. If not passed, the Producer will attempt
                to determine the orchestrator
        :type orchestrator: Orchestrator
        :param `**kwargs`: any additional configuration requirements desired
                for the Admin. Keys based on underlying orchestration
                requirements. Look at the docs for the orchestrator wrapper
                to determine desired requirements
        :type `**kwargs`: Any
        """
        if orchestration.orchestrator == "kafka":
            self.admin = KA(orchestration, **kwargs)
        elif orchestration.orchestrator == "rabbitmq":
            # Currently rabbitmq is not supported
            self.admin = None
        else:
            logging.error(
                "No orchestrator. Pipeline tools will have to be run manually."
            )
            self.admin = None

    def validate_connection(self) -> bool:
        """Validate that the Admin client is connected to the orchestrator"""
        try:
            return self.admin.validate_connection()
        except Exception as e:
            raise RuntimeError(f"failed to validate admin connection: {e}")

    def close(self):
        """Disconnect the Admin client from the orchestrator"""
        try:
            self.admin.close()
        except Exception as e:
            raise RuntimeError(f"failed to validate admin connection: {e}")
