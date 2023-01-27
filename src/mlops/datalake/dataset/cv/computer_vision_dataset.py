"""
ComputerVisionDataset definition.
"""

from __future__ import annotations

from typing import Any, Dict

from ..physical_dataset import DatasetDomain, DatasetType, PhysicalDataset


class ComputerVisionDatasetType(DatasetType):
    OBJECT_DETECTION = "OBJECT_DETECTION"

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON."""
        return {"type": self.value.lower()}

    @staticmethod
    def from_json(data: Dict[str, Any]) -> ComputerVisionDatasetType:
        """Deserialize from JSON."""
        assert "type" in data, "Broken precondition."
        if data["type"] == "object_detection":
            return ComputerVisionDatasetType.OBJECT_DETECTION
        raise RuntimeError(f"Invalid dataset type: {data['type']}.")


class ComputerVisionDataset(PhysicalDataset):
    def __init__(self, type: ComputerVisionDatasetType, identifier: str):
        super().__init__(DatasetDomain.COMPUTER_VISION, type, identifier)

    def _verify_integrity(self):
        raise NotImplementedError("Not implemented.")
