""" Orchestration Discovery Class """

from typing import Dict, List, Tuple, Union

from .kafka import KA

# ----------------------------------------------------------
# Orchestration Related Variables
# ----------------------------------------------------------

""" Supported orchestrators and their common endpoints """
CONNECTIONS = {"kafka": ["kafka:9092", "kafka:29092"]}

ORCHESTRATORS = ["kafka", "rabbitmq"]


# ----------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------


def _get_orchestrator(
    connect_dict: Dict[str, Union[str, List[str]]] = CONNECTIONS
) -> Tuple(str, str):
    """
    Determines if an orchestrator is present and up and
    returns the name and endpoint of the orchestrator to allow
    the producers/consumers to direct traffic correctly

    :param connect_dict: a dictionary containing the name and connection
            endpoint for an orchestrator. This can either be passed by
            the user, or if nothing is passed from the user, the
            CONNECTIONS dictionary is used which contains supported
            orchestrators and their common endpoints in various
            environments.

    :return: the orchestrator name and endpoint to use in connecting
    :rtype: tuple(str, str)
    """
    for orch, endpoint in connect_dict.items():
        assert orch in ORCHESTRATORS
        if orch == "kafka":
            if not isinstance(endpoint, list):
                endpoint = [endpoint]
            for end in endpoint:
                # call a KafkaAdmin instance and validate connection
                conn_client = KA(end)
                if conn_client.validate_connection():
                    conn_client.close()
                    return orch, end
        # eventually add additional supported orchestrators.
        if orch == "rabbitmq":
            pass
    raise RuntimeError("Failed to connect to an orchestrator. Is one running?")


def _format_orchestration(
    orchestrator: str, endpoint: Union[str, List[str]]
) -> Dict[str, Union[str, List[str]]]:
    # eventually add additional orchestrators as valid options
    assert (
        orchestrator.lower() in ORCHESTRATORS
    ), f"Orchestrator {orchestrator} is not supported"
    return {orchestrator.lower(): endpoint}


# --------------------------------------------------------------
# Orchestration Class
# --------------------------------------------------------------


class Orchestration:
    """
    The Orchestration object that can be passed to any of the
    Orchestration classes to inform underlying tools on how to
    connect to the underlying orchestrator.
    """

    def __init__(
        self, orchestrator: str = None, endpoint: Union[str, List[str]] = None
    ):
        """
        Build the Orchestration object. This can be built
        without any input, but this requires the orchestrator
        exists at one of its default or common endpoints

        :param orchestrator: The name of the orchestrator. Must be
                a supported orchestrator
        :orchestrator type: str
        :param endpoint: an endpoint or list of endpoints that a
                client can use to connect to the underlying orchestrator
        :endpoint type: Union[str, List[str]]
        """
        try:
            if orchestrator is None or endpoint is None:
                orch, end = _get_orchestrator()
            else:
                orch, end = _get_orchestrator(
                    _format_orchestration(orchestrator, endpoint)
                )
            self.orchestrator = orch
            self.connection = end
        except Exception as e:
            raise ValueError(f"Invalid orchestrator information passed: {e}")

    @property
    def orchestrator(self):
        return self.orchestrator

    @property
    def connection(self):
        return self.connection
