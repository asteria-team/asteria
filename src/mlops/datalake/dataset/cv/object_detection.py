"""
ObjectDetectionDataset definition.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Union

import mlops.datalake._util as util

from ..physical_dataset import DatasetDomain, DatasetType
from .computer_vision_dataset import (
    ComputerVisionDataset,
    ComputerVisionDatasetMetadata,
    ComputerVisionDatasetType,
)
from .image import Image

# -----------------------------------------------------------------------------
# Global Variables
# -----------------------------------------------------------------------------

# The name of the metadata file within each dataset
METADATA_FILENAME = "metadata.json"

# The name of the images directory within each dataset
IMAGES_DIRNAME = "images"

# The name of the annotations directory within each dataset
ANNOTATIONS_DIRNAME = "annotations"

# -----------------------------------------------------------------------------
# ObjectDetectionAnnotation
# -----------------------------------------------------------------------------


class BoundingBox:
    """Represents a bounding box in an object detection annotation."""

    def __init__(
        self, *, xmin: float, ymin: float, width: float, height: float
    ):
        self.xmin = xmin
        """The relative x coordinate of the upper-left corner."""

        self.ymin = ymin
        """The relative y coordinate of the upper-left corner."""

        self.w = width
        """The relative width of the box."""

        self.h = height
        """The relative height of the box."""

    def to_line(self) -> str:
        """Convert to a whitespace-separated string for serialization."""
        return f"{self.xmin} {self.ymin} {self.w} {self.h}"

    @staticmethod
    def from_line(line: str) -> BoundingBox:
        """Parse from whitespace-separated string for deserialization."""
        array = line.strip().split()
        assert len(array) == 4, "Broken invariant."
        return BoundingBox(
            xmin=float(array[0]),
            ymin=float(array[1]),
            width=float(array[2]),
            height=float(array[3]),
        )


class Object:
    """Represents an object in an object detection annotation."""

    def __init__(self, *, classname: str, bbox: BoundingBox):
        self.classname = classname
        """The class label."""

        self.bbox = bbox
        """The bounding box."""

    def to_line(self) -> str:
        """Convert to whitespace-separated string for serialization."""
        return f"{self.classname} {self.bbox.to_line()}"

    @staticmethod
    def from_line(line: str) -> Object:
        """Parse from whitespace-separated line for deserialization."""
        array = line.strip().split()
        assert len(array) == 5, "Broken invariant."
        return Object(
            classname=array[0], bbox=BoundingBox.from_line(" ".join(array[1:]))
        )


class ObjectDetectionAnnotation:
    """Represents a complete object detection annotation for an image."""

    def __init__(
        self, *, identifier: str, objects: List[Object], persisted: bool = False
    ):
        self.id = identifier
        """The identifier for the annotation (associates it with an image)."""

        self.objects = objects
        """The object annotations."""

        self._persisted = persisted
        """An indicator for whether this annotation has been persisted to data lake."""

    def to_file(self, path: Path):
        """
        Write annotation to file at `path`.

        :param path: The path to which the annotation is written.
        :type path: Path
        """
        assert _basename(path) == self.id, "Broken precondition."
        with path.open("w") as f:
            for object in self.objects:
                f.write(f"{object.to_line()}\n")
        self._persisted = True

    @staticmethod
    def from_file(path: Path) -> ObjectDetectionAnnotation:
        """
        Read annotation from file at `path`.

        :param path The path from which the annotation is read
        :type path: Path

        :return: The loaded annotation
        :rtype: ObjectDetectionAnnotation
        """
        assert path.is_file(), "Broken precondition."
        identifier = _basename(path)

        with path.open("r") as f:
            objects = [
                Object.from_line(nonempty)
                for nonempty in [line for line in f if line.strip() != ""]
            ]
        return ObjectDetectionAnnotation(
            identifier=identifier, objects=objects, persisted=True
        )

    def __key(self) -> str:
        return self.id

    def __hash__(self) -> int:
        return hash(self.__key())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Image):
            return False
        return self.__key() == other.__key()

    def __neq__(self, other: object) -> bool:
        return not self.__eq__(other)


# -----------------------------------------------------------------------------
# ObjectDetectionDatasetMetadata
# -----------------------------------------------------------------------------


@dataclass
class ObjectDetectionDatasetMetadata(ComputerVisionDatasetMetadata):
    tags: List[str] = field(default_factory=lambda: [])
    """A list of descriptive tags for the dataset."""

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON."""
        return {
            "domain": self.domain.to_json(),
            "type": self.type.to_json(),
            "identifier": self.identifier,
            "tags": [tag for tag in self.tags],
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> ObjectDetectionDatasetMetadata:
        """Deserialize from JSON."""
        assert "tags" in data, "Broken precondition."
        return ObjectDetectionDatasetMetadata(
            domain=DatasetDomain.from_json(data["domain"]),
            type=DatasetType.from_json(data["type"]),
            identifier=data["identifier"],
            tags=[tag for tag in data["tags"]],
        )


# -----------------------------------------------------------------------------
# ObjectDetectionDataset
# -----------------------------------------------------------------------------


class ObjectDetectionDataset(ComputerVisionDataset):
    def __init__(self, identifier: str):
        super().__init__(ComputerVisionDatasetType.OBJECT_DETECTION, identifier)

        self.metadata = ObjectDetectionDatasetMetadata()
        """Metadata for the dataset."""

        self._images: List[Image] = []
        """The in-memory collection of images associated with the dataset."""

        self._annotations: List[ObjectDetectionAnnotation] = []
        """The in-memory collection of annotations associated with the dataset."""

        # Load existing data from disk
        self._load()

    def add_image(self, image: Image):
        """
        Add an image to the dataset.

        :param image: The image to add
        :type image: Image
        """
        if image in set(self._images):
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
        if annotation in set(self._annotations):
            raise RuntimeError(
                f"Annotation with identifier {annotation.id} already exists."
            )
        self._annotations.append(annotation)

    def save(self):
        """Persist the dataset to the underlying data lake."""
        util.ctx.datalake_init()

        pdatasets = util.ctx.pdataset_path()

        # Create the dataset directory, if necessary
        dataset_dir = pdatasets / self.identifier
        if not dataset_dir.is_dir():
            dataset_dir.mkdir()

        _save_metadata(dataset_dir, self.metadata)
        _save_images(dataset_dir, self._images)
        _save_annotations(dataset_dir, self._annotations)

    def _load(self):
        """Load data from data lake."""
        util.ctx.check_initialized()

        dataset_dir = util.ctx.pdataset_path() / self.identifier

        # If the dataset does not yet exist, nothing to do
        if not dataset_dir.is_dir():
            return

        # Load metadata
        self.metadata = _load_metadata(dataset_dir)

        # Load images
        self._images = _load_images(dataset_dir)

        # Load annotations
        self._annotations = _load_annotations(dataset_dir)

    def _verify_integrity(self):
        raise NotImplementedError("Not implemented.")


# -----------------------------------------------------------------------------
# Save / Load
# -----------------------------------------------------------------------------


def _save_metadata(dataset_dir: Path, metadata: ObjectDetectionDatasetMetadata):
    """
    Save dataset metadata.

    :param dataset_dir: The path to the dataset directory
    :type dataset_dir: Path
    :param metadata: The dataset metadata
    :type metadata: ObjectDetectionDatasetMetadata
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    metadata_path = dataset_dir / METADATA_FILENAME
    with metadata_path.open("w") as f:
        json.dump(metadata.to_json(), f)


def _save_images(dataset_dir: Path, images: List[Image]):
    """
    Save dataset images.

    :param dataset_dir: The path to the dataset directory
    :type dataset_dir: Path
    :param images: The collection of images to save
    :type images: List[Image]
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    images_dir = dataset_dir / IMAGES_DIRNAME
    if not images_dir.is_dir():
        images_dir.mkdir()

    # Save all unpersisted images to data lake
    images = filter(lambda img: not img._persisted, images)
    for image in images:
        destination = (images_dir / image.id).with_suffix(image.extension)
        assert not destination.exists(), "Broken invariant."
        image.to_file(destination)


def _save_annotations(
    dataset_dir: Path, annotations: List[ObjectDetectionAnnotation]
):
    """
    Save dataset images.

    :param dataset_dir: The path to the dataset directory
    :type dataset_dir: Path
    :param annotations: The collection of annotations to save
    :type annotations: List[ObjectDetectionAnnotation]
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    annotations_dir = dataset_dir / ANNOTATIONS_DIRNAME
    if not annotations_dir.is_dir():
        annotations_dir.mkdir()

    annotations = filter(lambda a: not a._persisted, annotations)
    for annotation in annotations:
        destination = (annotations_dir / annotation.id).with_suffix(".txt")
        assert not destination.exists(), "Broken invariant."
        annotation.to_file(destination)


def _load_metadata(dataset_dir: Path) -> ObjectDetectionDatasetMetadata:
    """
    Load metadata from dataset directory.

    :param dataset_dir: The path to the dataset directory
    :type: Path

    :return: The loaded metadata
    :rtype: ObjectDetectionDatasetMetadata
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    metadata_path = dataset_dir / METADATA_FILENAME
    if not metadata_path.is_file():
        return ObjectDetectionDatasetMetadata()
    with metadata_path.open("r") as f:
        return ObjectDetectionDatasetMetadata.from_json(json.load(f))


def _load_images(dataset_dir: Path) -> List[Image]:
    """
    Load images from dataset directory.

    :param dataset_dir: The path to the dataset directory
    :type: Path

    :return: The loaded collection of images
    :rtype: List[Image]
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    images_dir = dataset_dir / IMAGES_DIRNAME
    if not images_dir.is_dir():
        return []
    return [Image.from_file(p) for p in images_dir.glob("*")]


def _load_annotations(dataset_dir: Path) -> List[ObjectDetectionAnnotation]:
    """
    Load annotations from dataset directory.

    :param dataset_dir: The path to the dataset directory
    :type: Path

    :return: The loaded collection of annotations
    :rtype: List[ObjectDetectionAnnotation]
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    annotations_dir = dataset_dir / ANNOTATIONS_DIRNAME
    if not annotations_dir.is_dir():
        return []
    return [
        ObjectDetectionAnnotation.from_file(p)
        for p in annotations_dir.glob("*.txt")
    ]


# -----------------------------------------------------------------------------
# Filesystem Helpers
# -----------------------------------------------------------------------------


def _basename(path: Union[Path, str]) -> str:
    """Parse path basename."""
    path = path if isinstance(path, Path) else Path(path)
    return os.path.splitext(path.name)[0]


def _extension(path: Union[Path, str]) -> str:
    """Parse path extension."""
    path = path if isinstance(path, Path) else Path(path)
    return os.path.splitext(path.name)[1]
