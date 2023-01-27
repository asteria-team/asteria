"""
Global datalake functionality.
"""

import time
from pathlib import Path

# -----------------------------------------------------------------------------
# Global Variables
# -----------------------------------------------------------------------------

# The name of the directory in which physical datasets are stored
PHYSICAL_DATASET_DIRNAME = "pdatasets"

# The name of the directory in which logical datasets are stored
LOGICAL_DATASET_DIRNAME = "ldatasets"

# The name of the directory in which model information is stored
MODEL_REGISTRY_DIRNAME = "models"

# -----------------------------------------------------------------------------
# Global State Management
# -----------------------------------------------------------------------------


class GlobalState:
    """Encapsulates package-wide global state."""

    _instance = None

    def __new__(self):
        if self._instance is None:
            self._instance = super(GlobalState, self).__new__(self)
        return self._instance

    def __init__(self):
        # The path for the data lake root location
        self._path: Path = None

    def has_path(self) -> bool:
        return self.path is not None

    def set_path(self, path: Path):
        self.path = path

    def get_path(self) -> Path:
        assert self.has_path(), "Broken precondition."
        return self.path

    # -------------------------------------------------------------------------
    # Variable Access
    # -------------------------------------------------------------------------

    def pdataset_path(self) -> Path:
        """Compute the physical dataset path."""
        assert self.has_path(), "Broken precondition."
        return self.get_path() / PHYSICAL_DATASET_DIRNAME

    def ldataset_path(self) -> Path:
        """Compute the logical dataset path."""
        assert self.has_path(), "Broken precondition."
        return self.get_path() / LOGICAL_DATASET_DIRNAME

    def model_registry_path(self) -> Path:
        """Compute the model registry path."""
        assert self.has_path(), "Broken precondition."
        return self.get_path() / MODEL_REGISTRY_DIRNAME

    # -------------------------------------------------------------------------
    # Initialization
    # -------------------------------------------------------------------------

    def datalake_init(self):
        """Initialize the data lake."""
        self._check_initialized()
        self._allocate_if_required()

    def _check_initialized(self):
        """Determine if the global context is initialized."""
        if not self.has_path():
            raise RuntimeError(
                "Must initialize global context prior to performing this action."
            )

    def _allocate(self):
        """
        Allocate the data lake root directory.
        """
        assert self.has_path(), "Broken precondition."

        if self._is_allocated():
            raise RuntimeError("Data lake is already allocated.")

        # Create the root directory
        self.get_path().mkdir()

        # Create the sub-directories
        self.pdataset_path().mkdir()
        self.ldataset_path().mkdir()
        self.model_registry_path().mkdir()

        assert self._is_allocated(), "Broken postcondition."

    def _deallocate(self):
        """
        Deallocate the data lake root directory.
        """
        assert self.has_path(), "Broken precondition."

        if not self._is_allocated():
            raise RuntimeError("Data lake is not allocated.")

        self.get_path().rmdir()
        assert not self._is_allocated(), "Broken postcondition."

    def _is_allocated(self) -> bool:
        """
        Determine if the data lake is allocated.

        :return: `True` if allocated, `False` otherwise
        :rtype: bool
        """
        return (
            self.has_path()
            and self.get_path().exists()
            and self.get_path().is_dir()
        )

    def _allocate_if_required(self):
        """Allocate the data lake, if required."""
        if not self._is_allocated():
            self._allocate()
        assert self._is_allocated(), "Broken postcondition."

    # -------------------------------------------------------------------------
    # Timing
    # -------------------------------------------------------------------------

    @staticmethod
    def timestamp() -> int:
        """Get the current UNIX timestamp."""
        return int(time.time())

    # -------------------------------------------------------------------------
    # Integrity Verification
    # -------------------------------------------------------------------------

    def verify_integrity():
        """Verify the integrity of the data lake."""
        pass


# Singleton global state
g_state = GlobalState()


def global_state() -> GlobalState:
    """Return the package global state."""
    return g_state


# -----------------------------------------------------------------------------
# Integrity Verification
# -----------------------------------------------------------------------------
