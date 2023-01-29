"""
PhysicalDataset class implementation.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
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

    def __str__(self) -> str:
        """Convert to string."""
        return self.value.lower()


# -----------------------------------------------------------------------------
# DatasetType
# -----------------------------------------------------------------------------


class DatasetType(Enum):
    """A generic dataset type."""

    OBJECT_DETECTION = "OBJECT_DETECTION"

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON."""
        return {"type": self.value.lower()}

    @staticmethod
    def from_json(data: Dict[str, Any]) -> DatasetType:
        """Deserialize from JSON."""
        assert "type" in data, "Broken precondition."
        if data["type"] == "object_detection":
            return DatasetType.OBJECT_DETECTION
        raise RuntimeError(f"Invalid dataset type: {data['type']}.")

    def __str__(self) -> str:
        """Convert to string."""
        return self.value.lower()


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

    created_at: int = None
    """The timestamp at which the dataset was created."""

    updated_at: int = None
    """The timestamp at which the dataset was last updated."""

    @staticmethod
    def from_file(path: Path) -> PhysicalDatasetMetadata:
        """
        Load a PhysicalDatasetMetadata instance from file.

        :param path: The path the metadata file
        :type path: Path

        :return: The loaded instance
        :rtype: PhysicalDatasetMetadata
        """
        assert path.is_file(), "Broken precondition."
        assert path.name == "metadata.json", "Broken precondition."

        with path.open("r") as f:
            return PhysicalDatasetMetadata.from_json(json.load(f))

    def to_json(self) -> Dict[str, Any]:
        """
        Serialize to JSON object.

        :return: The serialized object
        :rtype: Dict[str, Any]
        """
        return {
            "domain": self.domain.to_json(),
            "type": self.type.to_json(),
            "identifier": self.identifier.to_json(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> PhysicalDatasetMetadata:
        """
        Deserialize from JSON object.

        :param data: The JSON data
        :type data: Dict[str, Any]

        :return: The loaded instance
        :rtype: PhysicalDatasetMetadata
        """
        return PhysicalDatasetMetadata(
            domain=DatasetDomain.from_json(data["domain"]),
            type=DatasetType.from_json(data["type"]),
            identifier=DatasetIdentifier.from_json(data["identifier"]),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
        )

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
        if self.created_at is None:
            raise IncompleteError("Missing dataset 'created_at'.")
        if self.updated_at is None:
            raise IncompleteError("Missing dataset 'updated_at'.")


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
# PhysicalDatasetView
# -----------------------------------------------------------------------------


class PhysicalDatasetView(ABC):
    """PhysicalDatasetView is the base class for all dataset views."""

    def __init__(
        self,
        domain: DatasetDomain,
        type: DatasetType,
        identifier: DatasetIdentifier,
    ):
        self.domain = domain
        """The dataset domain."""

        self.type = type
        """The dataset type."""

        self.identifier = identifier
        """The dataset identifier."""

        self.metadata = PhysicalDatasetMetadata(
            domain=self.domain, type=self.type, identifier=self.identifier
        )
        """The dataset metadata."""

        # Install hooks
        self._load = install_hook(self._load, self._global_hook)

    @abstractmethod
    def _load(self):
        """Load the dataset from the data lake."""
        raise NotImplementedError("Not implemented.")

    def _global_hook(self):
        """Hook invocation of all core interface methods."""
        util.ctx.datalake_init()
