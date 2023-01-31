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

import subprocess
from typing import Tuple

from ..pipeline_logger import PipelineLogger
from .kafka_wrapper import (
    kafka_complete_message,
    kafka_consumer_connect,
    kafka_failure_message,
    kafka_producer_connect,
    kafka_progress_message,
    kafka_start_message,
)
from .messaging import set_producer_consumer_type

# --------------------------------------------------
# Global Variables
# --------------------------------------------------

endpoint_dict = {"kafka": "kafka:9092"}

APPROVED_ORCHESTRATORS = ["kafka"]

KAFKA_TOPIC = "mlops_pipeline"

# -------------------------------------------------
# Helper Functions
# -------------------------------------------------


def get_orchestrator(prolog: PipelineLogger) -> Tuple(str, str):
    """
    Determines if an orchestrator is present and
    returns the name and endpoint of the orchestrator to allow
    the producers/consumers to direct traffic correctly
    :param prolog: Pipeline logger for this producer
    :return: the orchestrator name and endpoint to use in connecting
    """
    for orch, endpoint in endpoint_dict.items():
        command = ["ping", "-c", "1", endpoint]
        if subprocess.call(command) == 0:
            # ping successful, return the name
            return orch, endpoint
    prolog.warn("No orchestrator found. Is one running?")
    return None, None


def test_passed_orchestrator(
    orc_endpoint: str, orc: str, prolog: PipelineLogger
) -> bool:
    """
    test the user passed orchestrator and endpoint information
    to ensure it is correct and will intgrate with the pipeline
    orchestration wrapper
    :param orc_endpoint: the endpoint for producers/consumers to connect to
    :param orc: the name of the orchestrator
    :param prolog: Pipeline logger for this producer
    :return: True if valid information passed false otherwise
    """
    # validate orchestrator string
    if orc is None:
        prolog.info("Orchestrator name not passed")
        return False
    if orc not in APPROVED_ORCHESTRATORS:
        prolog.info(f"{orc} is not an approved orchestrator for the pipeline")
        return False

    # validate orchestrator endpoint
    if orc_endpoint is None:
        prolog.info("Orchestrator endpoint not passed")
        return False
    command = ["ping", "-c", "1", orc_endpoint]
    if subprocess.call(command) != 0:
        prolog.info(f"Unable to ping orchestrator at {orc_endpoint}")
        return False
    return True


# ------------------------------------------------
# Message Classes
# ------------------------------------------------


class Producer:
    def __init__(
        self,
        initializer: str,
        stage: str = None,
        orchestrator_endpoint: str = None,
        orchestrator: str = None,
        location: str = None,
    ):
        """
        Initialize the higher level Producer class exposed to the user
        :param initializer: the tool/app that is calling the producer
                (required for message building)
        :param stage: identifies the stage if applicable
        :param orchestrator_endpoint: The endpoint of the orchestrator if
                not predefined
        :param orchestrator: the name of the orchestrator in use
        :param location: where to write the messages to.
                For kafka this is the topic
        """
        self.name = set_producer_consumer_type(initializer)
        self.stage = stage
        self.retries = 0
        # initialize the Producer logger
        self.prolog = PipelineLogger((self.name + "-producer"))
        # determine underlying orchestrator
        if orchestrator_endpoint is None and orchestrator is None:
            self.orchestrator, self.orchestrator_endpoint = get_orchestrator(
                self.prolog
            )
        else:
            if test_passed_orchestrator(
                orchestrator_endpoint, orchestrator, self.prolog
            ):
                self.orchestrator = orchestrator
                self.orchestrator_endpoint = orchestrator_endpoint
            else:
                (
                    self.orchestrator,
                    self.orchestrator_endpoint,
                ) = get_orchestrator(self.prolog)

        # connect to orchestrator and create the producer
        if self.orchestrator == "kafka":
            self.producer = kafka_producer_connect(self.orchestrator_endpoint)
            if location is not None:
                self.loc = location
            else:
                self.loc = KAFKA_TOPIC
        else:
            self.prolog.warn("Pipeline tools will have to be run manually.")
            self.producer = None
            self.loc = None

    def complete(self, stage: str = None, output: str = None, usr_msg: str = None):
        """directs producer tool completion messages"""
        if self.orchestrator == "kafka":
            if stage is not None:
                self.stage = stage
            kafka_complete_message(
                self.producer,
                self.stage,
                output,
                usr_msg,
                self.retries,
                self.prolog,
            )
        else:
            # no orchestrator case
            self.prolog(f"{self.producer} complete. Message: {usr_msg}")

    def fail(self, stage: str = None, usr_msg: str = None):
        """directs producer tool failure messages"""
        if self.orchestrator == "kafka":
            if stage is not None:
                self.stage = stage
            kafka_failure_message(
                self.producer, self.stage, usr_msg, self.retries, self.prolog
            )
        else:
            # no orchestrator case
            self.prolog(f"{self.producer} failed. Message: {usr_msg}")

    def start(self, stage: str = None, usr_msg: str = None):
        """directs producer tool start messages"""
        if self.orchestrator == "kafka":
            if stage is not None:
                self.stage = stage
            kafka_start_message(
                self.producer, self.stage, usr_msg, self.retries, self.prolog
            )
        else:
            # no orchestrator case
            self.prolog(f"{self.producer} started. Message: {usr_msg}")

    def update_progress(self, stage: str, output: str = None, usr_msg: str = None):
        """directs producer tool in-progress messages"""
        if self.orchestrator == "kafka":
            if stage is not None:
                self.stage = stage
            kafka_progress_message(
                self.producer,
                self.stage,
                usr_msg,
                output,
                self.retries,
                self.prolog,
            )
        else:
            # no orchestrator case
            self.prolog(f"{self.producer} in progress. Stage: {stage}")

    def retry(self, stage: str = None):
        if stage is not None:
            self.stage = stage
        self.retries += 1


class Consumer:
    def __init__(
        self,
        initializer: str,
        stage: str = None,
        orchestrator_endpoint: str = None,
        orchestrator: str = None,
        loc_subscribe: str = None,
    ):
        """
        Initialize the higher level Consumer class exposed to the user
        :param initializer: the tool/app that is calling the producer
                (required for message building)
        :param stage: identifies the stage if applicable
        :param orchestrator_endpoint: The endpoint of the orchestrator if
                not predefined
        :param orchestrator: the name of the orchestrator in use
        :param loc_subscribe: where to read the messages from.
                For kafka this is the topic
        """
        self.name = initializer
        self.stage = stage
        # initialize the Consumer logger
        self.conlog = PipelineLogger((self.name + "-consumer"))
        # determine underlying orchestrator
        if orchestrator_endpoint is None and orchestrator is None:
            self.orchestrator, self.orchestrator_endpoint = get_orchestrator(
                self.conlog
            )
        else:
            if test_passed_orchestrator(
                orchestrator_endpoint, orchestrator, self.conlog
            ):
                self.orchestrator = orchestrator
                self.orchestrator_endpoint = orchestrator_endpoint
            else:
                (
                    self.orchestrator,
                    self.orchestrator_endpoint,
                ) = get_orchestrator(self.conlog)

        # connect to orchestrator and create consumer and subscribe
        if self.orchestrator == "kafka":
            if loc_subscribe is not None:
                self.loc_sub = loc_subscribe
            else:
                self.loc_sub = KAFKA_TOPIC
            self.consumer = kafka_consumer_connect(
                self.orchestrator_endpoint, self.loc_sub
            )
        else:
            self.conlog.warn("Pipeline tools will have to be run manually.")
            self.consumer = None
            self.loc_sub = None


class Admin:
    def __init__(self):
        # determine underlying orchestrator
        self.orchestrator = get_orchestrator()
        # establish connection to orchestrator based on the service
        # create the correct version of the admin based on the service
        pass

    # all subsequent functionality simply act as a redirect to the
    # appropriate underlying orchestration wrapper
