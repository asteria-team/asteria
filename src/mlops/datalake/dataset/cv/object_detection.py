"""
ObjectDetectionDataset definition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from mlops.datalake._global import global_state

from .computer_vision_dataset import (
    ComputerVisionDataset,
    ComputerVisionDatasetType,
)
from .image import Image

# -----------------------------------------------------------------------------
# ObjectDetectionAnnotation
# -----------------------------------------------------------------------------


class BoundingBox:
    """Represents a bounding box in an object detection annotation."""

    def __init__(self, xmin: float, ymin: float, width: float, height: float):
        self.xmin = xmin
        """The relative x coordinate of the upper-left corner."""

        self.ymin = ymin
        """The relative y coordinate of the upper-left corner."""

        self.w = width
        """The relative width of the box."""

        self.h = height
        """The relative height of the box."""


class Object:
    """Represents an object in an object detection annotation."""

    def __init__(self, classname: str, bbox: BoundingBox):
        self.classname = classname
        """The class label."""

        self.bbox = bbox
        """The bounding box."""


class ObjectDetectionAnnotation:
    """Represents a complete object detection annotation for an image."""

    def __init__(self, identifier: str, objects: List[Object]):
        self.id = identifier
        """The identifier for the annotation (associates it with an image)."""

        self.objects = objects
        """The object annotations."""

    @staticmethod
    def from_json(data: Dict[str, Any]) -> ObjectDetectionAnnotation:
        """Parse an ObjectDetectionAnnotation from Label Studio JSON."""
        return None


# -----------------------------------------------------------------------------
# ObjectDetectionDatasetMetadata
# -----------------------------------------------------------------------------


@dataclass
class ObjectDetectionDatasetMetadata:
    tags: List[str] = field(default_factory=lambda: [])
    """A list of descriptive tags for the dataset."""

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON."""
        return {"tags": [tag for tag in self.tags]}

    @staticmethod
    def from_json(data: Dict[str, Any]) -> ObjectDetectionDatasetMetadata:
        """Deserialize from JSON."""
        assert "tags" in data, "Broken precondition."
        return ObjectDetectionDatasetMetadata(
            tags=[tag for tag in data["tags"]]
        )


# -----------------------------------------------------------------------------
# ObjectDetectionDataset
# -----------------------------------------------------------------------------


class ObjectDetectionDataset(ComputerVisionDataset):
    def __init__(self, identifier: str):
        super().__init__(ComputerVisionDatasetType.OBJECT_DETECTION, identifier)

        self._images: List[Image] = []
        """The in-memory collection of images associated with the dataset."""

        self._annotations: List[ObjectDetectionAnnotation] = []
        """The in-memory collection of annotations associated with the dataset."""

        self.metadata = ObjectDetectionDatasetMetadata()
        """Metadata for the dataset."""

    def add_image(self, image: Image):
        """
        Add an image to the dataset.

        :param image: The image to add
        :type image: Image
        """
        identifiers = set(img.id for img in self._images)
        if image.id in identifiers:
            raise RuntimeError(
                f"Image with identifier {image.id} already exists."
            )
        self._images.append(image)

    def add_annotation(self, annotation: ObjectDetectionAnnotation):
        """
        Add an annotation to the dataset.

        :param annotation: The annotation to add
        :type annotation: ObjectDetectionAnnotation
        """
        identifiers = set(annotation.id for annotation in self._annotations)
        if annotation.id in identifiers:
            raise RuntimeError(
                f"Annotation with identifier {annotation.id} already exists."
            )
        self._annotations.append(annotation)

    def save(self):
        """Persist the dataset to the underlying data lake."""
        global_state().datalake_init()

        pdatasets = global_state().pdataset_path()
        assert pdatasets.is_dir(), "Broken invariant."

        # Create the dataset directory, if necessary
        dataset_dir = pdatasets / self.identifier
        if not dataset_dir.is_dir():
            dataset_dir.mkdir()

        # Establish the current state of the metadata file

    def _verify_integrity(self):
        raise NotImplementedError("Not implemented.")
