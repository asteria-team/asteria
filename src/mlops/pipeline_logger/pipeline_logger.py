"""
MLOPs internal logging library that allows log calls to
get pushed to Loki for display and persistent storage
"""

import logging
import subprocess
import sys
from typing import List

import logging_loki

"""
Endpoint within Docker Compose for Loki service. Assumes
that the Dockerfile sets Loki's exposed port in the Docker
network as 3100.
"""
DC_LOKI_ENDPOINT = "http://loki:3100/loki/api/v1/push"

# ----------------------------------------------------
# Helper functions
# ----------------------------------------------------


def ping_service() -> bool:
    """
    Returns True if Loki Endpoint responds to a ping request.
    Allows the logger to determine if Loki handler should be
    added
    """

    # Send one packet to endpoint (-c for Unix based system)
    command = ["ping", "-c", "1", DC_LOKI_ENDPOINT]

    return subprocess.call(command) == 0


def build_base_logger(service: str, verbose: bool):
    """
    Build the base level logger called outside of the
    library. Logger will be named based on the service,
    and provide print outs to stdout.
    :param service: the service or app initializing the logger
    :param verbose: determines the level of logs recorded
    :return: a new logger with a stdout handler
    """
    # create logger
    new_logger = logging.getLogger(f"{service}-logger")
    new_logger.setLevel(logging.DEBUG if verbose else logging.WARNING)

    # build stdout handler
    stdout_hand = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stdout_hand.setFormatter(formatter)

    # add handler to logger and return
    new_logger.addHandler(stdout_hand)
    return new_logger


# ----------------------------------------------------
# Logging Class
# ----------------------------------------------------


class PipelineLogger:
    def __init__(self, service: str = "", verbose: bool = False):
        """
        Initializes the pipeline logger
        :param service: the service or app initializing the logger
        :param verbose: Determines the level of logs that
            get recorded. True => debug and info levels included
            in push to Loki and stdout
        """
        self.service = service
        self.verbose = verbose
        # create base logger
        self.logger = build_base_logger(self.service, self.verbose)

        # add Loki handler
        self.loki = ping_service()
        if self.loki:
            # Forwards messages to Loki with the level of the log
            # and the service tag
            logging_loki.emitter.LokiEmitter.level_tag = "level"
            self.logger.addHandler(
                logging_loki.LokiHandler(
                    url=DC_LOKI_ENDPOINT,
                    tags={"service": self.service},
                    version="1",
                )
            )
        else:
            self.logger.warning("Loki unable to be accessed. Is it running?")

    def debug(self, msg: str, add_tags: List[str] = None):
        """Log a debugging message"""
        if add_tags is not None:
            self.logger.debug(
                msg, extra={"tags": {"additional_tags": add_tags}}
            )
        else:
            self.logger.debug(msg)

    def info(self, message: str, add_tags: List[str] = None):
        """Log an informational message"""
        if add_tags is not None:
            self.logger.info(
                message, extra={"tags": {"additional_tags": add_tags}}
            )
        else:
            self.logger.info(message)

    def warn(self, msg: str, add_tags: List[str] = None):
        """Log a warning message"""
        if add_tags is not None:
            self.logger.warning(
                msg, extra={"tags": {"additional_tags": add_tags}}
            )
        else:
            self.logger.warning(msg)

    def error(self, msg: str, add_tags: List[str] = None):
        """Log an error message"""
        if add_tags is not None:
            self.logger.error(
                msg, extra={"tags": {"additional_tags": add_tags}}
            )
        else:
            self.logger.error(msg)

    def crit(self, msg: str, add_tags: List[str] = None):
        """Log a critical message"""
        if add_tags is not None:
            self.logger.critical(
                msg, extra={"tags": {"additional_tags": add_tags}}
            )
        else:
            self.logger.critical(msg)
