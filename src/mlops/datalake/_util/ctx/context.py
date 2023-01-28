"""
Global context management utilities.
"""

from pathlib import Path
from typing import Union

# -----------------------------------------------------------------------------
# Global Variables
# -----------------------------------------------------------------------------

# The name of the directory in which physical datasets are stored
_PHYSICAL_DATASET_DIRNAME = "pdatasets"

# The name of the directory in which logical datasets are stored
_LOGICAL_DATASET_DIRNAME = "ldatasets"

# The name of the directory in which model information is stored
_MODEL_REGISTRY_DIRNAME = "models"

# -----------------------------------------------------------------------------
# Global State Management (Internal)
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


# Singleton global state
_g_state = GlobalState()


def _global_state() -> GlobalState:
    """Return the package global state."""
    return _g_state


# -----------------------------------------------------------------------------
# Context Management (Exported)
# -----------------------------------------------------------------------------


def set_path(path: Union[Path, str]):
    """Set the global data lake path."""
    _global_state().set_path(path if isinstance(path, Path) else Path(path))


# -----------------------------------------------------------------------------
# Variable Access (Exported)
# -----------------------------------------------------------------------------


def pdataset_path() -> Path:
    """Compute the physical dataset path."""
    state = _global_state()
    assert state.has_path(), "Broken precondition."
    return state.get_path() / _PHYSICAL_DATASET_DIRNAME


def ldataset_path() -> Path:
    """Compute the logical dataset path."""
    state = _global_state()
    assert state.has_path(), "Broken precondition."
    return state.get_path() / _LOGICAL_DATASET_DIRNAME


def model_registry_path() -> Path:
    """Compute the model registry path."""
    state = _global_state()
    assert state.has_path(), "Broken precondition."
    return state.get_path() / _MODEL_REGISTRY_DIRNAME


# -----------------------------------------------------------------------------
# Initialization (Exported)
# -----------------------------------------------------------------------------


def datalake_init():
    """Initialize the data lake."""
    check_initialized()
    _allocate_if_required()


def check_initialized():
    """Determine if the global context is initialized."""
    state = _global_state()
    if not state.has_path():
        raise RuntimeError(
            "Must initialize global context prior to performing this action."
        )


# -----------------------------------------------------------------------------
# Initialization (Internal)
# -----------------------------------------------------------------------------


def _allocate():
    """
    Allocate the data lake root directory.
    """
    state = _global_state()
    assert state.has_path(), "Broken precondition."

    if _is_allocated():
        raise RuntimeError("Data lake is already allocated.")

    # Create the root directory
    state.get_path().mkdir()

    # Create the sub-directories
    pdataset_path().mkdir()
    ldataset_path().mkdir()
    model_registry_path().mkdir()

    assert _is_allocated(), "Broken postcondition."


def _deallocate():
    """
    Deallocate the data lake root directory.
    """
    state = _global_state()
    assert state.has_path(), "Broken precondition."

    if not _is_allocated():
        raise RuntimeError("Data lake is not allocated.")

    state.get_path().rmdir()
    assert not _is_allocated(), "Broken postcondition."


def _is_allocated(self) -> bool:
    """
    Determine if the data lake is allocated.

    :return: `True` if allocated, `False` otherwise
    :rtype: bool
    """
    state = _global_state()
    return (
        state.has_path()
        and state.get_path().exists()
        and state.get_path().is_dir()
    )


def _allocate_if_required(self):
    """Allocate the data lake, if required."""
    if not _is_allocated():
        _allocate()
    assert _is_allocated(), "Broken postcondition."
