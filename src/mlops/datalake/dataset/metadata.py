"""
Unified dataset metadata definition.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict

from ..exception import IncompleteError

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

    def __hash__(self) -> int:
        return hash(self.value)

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

    def __hash__(self) -> int:
        return hash(self.value)

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

    def __eq__(self, other: object) -> bool:
        """Compare equality."""
        if not isinstance(other, DatasetIdentifier):
            return False
        return self.id == other.id

    def __neq__(self, other: object) -> bool:
        """Compare inequality."""
        return not self.__eq__(other)

    def __hash__(self) -> int:
        """Implement hash."""
        return hash(self.id)

    def __str__(self) -> str:
        """Convert to string."""
        return self.id


# -----------------------------------------------------------------------------
# SplitIdentifier
# -----------------------------------------------------------------------------


class SplitIdentifier:
    """A unique identifier for a split."""

    def __init__(self, identifier: str):
        self.id = identifier

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON format."""
        return {"identifier": self.id}

    @staticmethod
    def from_json(data: Dict[str, Any]) -> SplitIdentifier:
        """Deserialize from JSON format."""
        assert "identifier" in data, "Broken precondition."
        return SplitIdentifier(data["identifier"])

    def __eq__(self, other: object) -> bool:
        """Compare equality."""
        if not isinstance(other, SplitIdentifier):
            return False
        return self.id == other.id

    def __neq__(self, other: object) -> bool:
        """Compare inequality."""
        return not self.__eq__(other)

    def __hash__(self) -> int:
        """Implement hash."""
        return hash(self.id)

    def __str__(self) -> str:
        """Convert to string."""
        return self.id


# -----------------------------------------------------------------------------
# DatasetMetadata
# -----------------------------------------------------------------------------


@dataclass
class DatasetMetadata:
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
    def from_file(path: Path) -> DatasetMetadata:
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
            return DatasetMetadata.from_json(json.load(f))

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
    def from_json(data: Dict[str, Any]) -> DatasetMetadata:
        """
        Deserialize from JSON object.

        :param data: The JSON data
        :type data: Dict[str, Any]

        :return: The loaded instance
        :rtype: PhysicalDatasetMetadata
        """
        return DatasetMetadata(
            domain=DatasetDomain.from_json(data["domain"]),
            type=DatasetType.from_json(data["type"]),
            identifier=DatasetIdentifier.from_json(data["identifier"]),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
        )

    def merge(self, other: DatasetMetadata) -> DatasetMetadata:
        """Merge metadata from two datasets."""
        raise NotImplementedError("Not implemented.")

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
