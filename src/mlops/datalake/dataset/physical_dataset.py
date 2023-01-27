"""
PhysicalDataset class implementation.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict

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
# PhysicalDataset
# -----------------------------------------------------------------------------


class PhysicalDataset:
    """PhysicalDataset is the base class for all datasets that exist on disk."""

    def __init__(
        self, domain: DatasetDomain, type: DatasetType, identifier: str
    ):
        self.domain = domain
        """A domain identifier for the dataset."""

        self.type = type
        """A type identifier for the dataset."""

        self.identifier = identifier
        """The unique identifier for the PhysicalDataset instance."""

    def _verify_integrity(self):
        raise NotImplementedError("Not implemented.")
