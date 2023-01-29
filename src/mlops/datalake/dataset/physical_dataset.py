"""
PhysicalDataset class implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict

import mlops.datalake._util as util
from mlops.datalake.exception import IncompleteError

# -----------------------------------------------------------------------------
# DatasetDomain
# -----------------------------------------------------------------------------


class DatasetDomain(Enum):
    # A compute vision dataset
    COMPUTER_VISION = "CV"
    # A natural language processing dataset
    NATURAL_LANGUAGE_PROCESSING = "NLP"

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON format."""
        return {"domain": self.value.lower()}

    @staticmethod
    def from_json(data: Dict[str, Any]) -> DatasetDomain:
        assert "domain" in data, "Broken precondition."
        if data["domain"] == "cv":
            return DatasetDomain.COMPUTER_VISION
        elif data["domain"] == "nlp":
            return DatasetDomain.NATURAL_LANGUAGE_PROCESSING
        raise RuntimeError(f"Invalid dataset domain: {data['domain']}.")


# -----------------------------------------------------------------------------
# DatasetType
# -----------------------------------------------------------------------------


class DatasetType(Enum):
    """A generic dataset type."""

    def to_json(self) -> Dict[str, Any]:
        raise NotImplementedError("Not implemented.")

    @staticmethod
    def from_json(data: Dict[str, Any]) -> DatasetType:
        raise NotImplementedError("Not implemented.")


# -----------------------------------------------------------------------------
# DatasetIdentifier
# -----------------------------------------------------------------------------


class DatasetIdentifier:
    """A unique identifier for a dataset."""

    def __init__(self, identifier: str):
        self.id = identifier

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON format."""
        return {"identifier": self.id}

    @staticmethod
    def from_json(data: Dict[str, Any]) -> DatasetIdentifier:
        """Deserialize from JSON format."""
        assert "identifier" in data, "Broken precondition."
        return DatasetIdentifier(data["identifier"])

    def __str__(self) -> str:
        """Convert to string."""
        return self.id


# -----------------------------------------------------------------------------
# PhysicalDataset
# -----------------------------------------------------------------------------


def install_hook(func, hook):
    def wrapper(*args, **kwargs):
        hook()
        return func(*args, **kwargs)

    return wrapper


class PhysicalDataset(ABC):
    """PhysicalDataset is the base class for all datasets that exist on disk."""

    def __init__(
        self,
        domain: DatasetDomain,
        type: DatasetType,
        identifier: DatasetIdentifier,
    ):
        self.domain = domain
        """A domain identifier for the dataset."""

        self.type = type
        """A type identifier for the dataset."""

        self.identifier = identifier
        """The unique identifier for the PhysicalDataset instance."""

        # Install hooks for core interface
        self.save = install_hook(self.save, self._global_hook)
        self._load = install_hook(self._load, self._global_hook)
        self.verify_integrity = install_hook(
            self.verify_integrity, self._global_hook
        )

    @abstractmethod
    def save(self):
        """Persist the dataset to the data lake."""
        raise NotImplementedError("Not implemented.")

    @abstractmethod
    def _load(self):
        """Load the dataset from the data lake."""
        raise NotImplementedError("Not implemented.")

    @abstractmethod
    def verify_integrity(self):
        """Verify the integrity of the dataset."""
        raise NotImplementedError("Not implemented.")

    def _global_hook(self):
        """Hook invocation of all core interface methods."""
        util.ctx.datalake_init()


# -----------------------------------------------------------------------------
# PhysicalDatasetMetadata
# -----------------------------------------------------------------------------


@dataclass
class PhysicalDatasetMetadata:
    domain: DatasetDomain = None
    """The dataset domain."""

    type: DatasetType = None
    """The dataset type."""

    identifier: DatasetIdentifier = None
    """The dataset identifier."""

    def _check(self):
        """
        Check that the metadata object is populated.

        :raises: IncompleteError
        """
        if self.domain is None:
            raise IncompleteError("Missing dataset 'domain'.")
        if self.type is None:
            raise IncompleteError("Missing dataset 'type'.")
        if self.identifier is None:
            raise IncompleteError("Missing dataset 'identifier'.")
