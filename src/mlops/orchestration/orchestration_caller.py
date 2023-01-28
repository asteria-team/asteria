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

# -------------------------------------------------
# Helper Functions
# -------------------------------------------------


def get_orchestrator() -> str:
    return ""


# -------------------------------------------------
# Connect to Orchestrator
# -------------------------------------------------


def establish_connection():
    pass


# ------------------------------------------------
# Message Classes
# ------------------------------------------------


class Producer:
    def __init__(self):
        self.orchestrator = get_orchestrator()
        pass


class Consumer:
    def __init__(self):
        self.orchestrator = get_orchestrator()
        pass
