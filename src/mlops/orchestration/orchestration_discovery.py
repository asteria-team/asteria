""" Orchestration Discovery Class """

from typing import Dict, List, Tuple, Union

from kafka import KafkaClient

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
) -> Tuple[str, str]:
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
                conn_client = KafkaClient(bootstrap_servers=end)
                if conn_client.bootstrap_connected():
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


class OrchestrationBuilder:
    """
    The builder class for the orchestration tool
    """

    def __init__(self):
        self._orchestrator = None
        self._connection = None

    def with_orchestrator(self, orchestrator: str):
        """Add orchestrator name"""
        self._orchestrator = orchestrator
        return self

    def with_connection(self, endpoint: Union[str, List[str]]):
        """Add endpoint(s) that the orchestrator can connect to"""
        self._connection = endpoint
        return self

    def build(self):
        if self._orchestrator is None or self._connection is None:
            orch, end = _get_orchestrator()
        else:
            orch, end = _get_orchestrator(
                _format_orchestration(self._orchestrator, self._connection)
            )
        return Orchestration(orchestrator=orch, connection=end)


class Orchestration:
    """
    The Orchestration object that can be passed to any of the
    Orchestration classes to inform underlying tools on how to
    connect to the underlying orchestrator.
    """

    def __init__(self, **kwargs):
        """
        The Orchestration object. This can be built by the
        OrchestrationBuilder without any input, but this requires
        the orchestrator exists at one of its default or common
        endpoints. The build will manage finding and validating
        endpoints

        :param orchestrator: The name of the orchestrator. Must be
                a supported orchestrator
        :orchestrator type: str
        :param endpoint: an endpoint or list of endpoints that a
                client can use to connect to the underlying orchestrator
        :endpoint type: Union[str, List[str]]
        """
        self.kwargs = kwargs

    @property
    def orchestrator(self):
        """Get the orchestrator name"""
        return self.kwargs["orchestrator"]

    @property
    def connection(self):
        """Get the endpoint"""
        return self.kwargs["connection"]
