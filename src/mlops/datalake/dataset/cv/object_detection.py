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
from mlops.datalake._util.time import timestamp
from mlops.datalake.exception import IntegrityError

from ..physical_dataset import (
    DatasetDomain,
    DatasetIdentifier,
    DatasetType,
    PhysicalDataset,
    PhysicalDatasetMetadata,
)
from .computer_vision import ComputerVisionDatasetType
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

    @staticmethod
    def from_ls_json(data: Dict[str, Any]) -> ObjectDetectionAnnotation:
        """
        Parse ObjectDetectionAnnotation from Label Studio JSON.

        :param data: The JSON data
        :type data: Dict[str, Any]

        :return: The parsed annotation
        :rtype: ObjectDetectionAnnotation
        """
        # Parse the identifier
        if "data" not in data:
            raise RuntimeError("Failed to parse.")
        if "image" not in data["data"]:
            raise RuntimeError("Failed to parse.")
        identifier = _basename(data["data"]["image"])

        # Parse objects
        if "annotations" not in data:
            raise RuntimeError("Failed to parse.")
        objects = [_object_from_ls_json(e) for e in data["annotations"]]

        return ObjectDetectionAnnotation(identifier=identifier, objects=objects)

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


def _object_from_ls_json(data: Dict[str, Any]) -> Object:
    """
    Parse an Object from Label Studio JSON.

    :param data: The JSON data
    :type data: Dict[str, Any]

    :return: The parsed object
    :rtype: Object
    """
    if "result" not in data:
        raise RuntimeError("Failed to parse.")

    result = data["result"][0]
    if "value" not in result:
        raise RuntimeError("Failed to parse.")

    value = result["value"]
    if "rectanglelabels" not in value:
        raise RuntimeError("Failed to parse.")

    classname = value["rectanglelabels"][0]
    return Object(
        classname=classname,
        bbox=BoundingBox(
            xmin=value["x"],
            ymin=value["y"],
            width=value["width"],
            height=value["height"],
        ),
    )


# -----------------------------------------------------------------------------
# ObjectDetectionDatasetMetadata
# -----------------------------------------------------------------------------


@dataclass
class ObjectDetectionDatasetMetadata(PhysicalDatasetMetadata):
    tags: List[str] = field(default_factory=lambda: [])
    """A list of descriptive tags for the dataset."""

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON."""
        self._check()
        return {
            "domain": self.domain.to_json(),
            "type": self.type.to_json(),
            "identifier": self.identifier.to_json(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "tags": [tag for tag in self.tags],
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> ObjectDetectionDatasetMetadata:
        """Deserialize from JSON."""
        assert "tags" in data, "Broken precondition."
        return ObjectDetectionDatasetMetadata(
            domain=DatasetDomain.from_json(data["domain"]),
            type=DatasetType.from_json(data["type"]),
            identifier=DatasetIdentifier.from_json(data["identifier"]),
            created_at=data["created__at"],
            updated_at=data["updated_at"],
            tags=[tag for tag in data["tags"]],
        )

    def _check(self):
        """
        Check that the metadata object is populated.

        :raises IncompleteError
        """
        super()._check()


# -----------------------------------------------------------------------------
# ObjectDetectionDataset
# -----------------------------------------------------------------------------


class ObjectDetectionDataset(PhysicalDataset):
    def __init__(self, identifier: str):
        identifier = DatasetIdentifier(identifier)
        super().__init__(
            DatasetDomain.COMPUTER_VISION,
            ComputerVisionDatasetType.OBJECT_DETECTION,
            identifier,
        )

        self.metadata = ObjectDetectionDatasetMetadata(
            domain=self.domain, type=self.type, identifier=self.identifier
        )
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
        pdatasets = util.ctx.pdataset_path()

        # Create the dataset directory, if necessary
        dataset_dir = pdatasets / str(self.identifier)
        if not dataset_dir.is_dir():
            dataset_dir.mkdir()

        _save_metadata(dataset_dir, self.metadata)
        _save_images(dataset_dir, self._images)
        _save_annotations(dataset_dir, self._annotations)

    def _load(self):
        """Load data from data lake."""
        dataset_dir = util.ctx.pdataset_path() / str(self.identifier)

        # If the dataset does not yet exist, nothing to do
        if not dataset_dir.is_dir():
            return

        # Load metadata
        self.metadata = _load_metadata(dataset_dir)

        # Load images
        self._images = _load_images(dataset_dir)

        # Load annotations
        self._annotations = _load_annotations(dataset_dir)

    def verify_integrity(self):
        """
        Verify the integrity of the ObjectDetectionDataset.

        :raises: IntegrityError
        """
        datasets_dir = util.ctx.pdataset_path()
        assert datasets_dir.is_dir(), "Broken invariant."

        _verify_integrity(
            datasets_dir / str(self.identifier), self._images, self._annotations
        )


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

    # Update dataset times
    if metadata.created_at is None:
        metadata.created_at = timestamp()
    metadata.updated_at = timestamp()

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
# Integrity Verification
# -----------------------------------------------------------------------------


def _verify_integrity(
    dataset_dir: Path,
    images: List[Image],
    annotations: List[ObjectDetectionAnnotation],
):
    """
    Verify the integrity of the dataset.

    :param dataset_dir: The path to the dataset
    :type: dataset_dir: Path
    :param images: The collection of images
    :type images: List[Image]
    :param annotations: The collection of annotations
    :type annotations: List[ObjectDetectionAnnotation]

    :raises: IntegrityError
    """
    if not dataset_dir.is_dir():
        _verify_empty_dataset(dataset_dir, images, annotations)
        return

    # If the directory is present, metadata file should be present
    metadata_path = dataset_dir / METADATA_FILENAME
    if not metadata_path.is_file():
        raise IntegrityError("Missing metadata.json file.")

    _verify_persisted_images(dataset_dir, images)
    _verify_persisted_annotations(dataset_dir, annotations)


def _verify_empty_dataset(
    dataset_dir: Path,
    images: List[Image],
    annotations: List[ObjectDetectionAnnotation],
):
    """
    Verify the integrity of a completely empty dataset.

    :param dataset_dir: The path to the dataset
    :type: dataset_dir: Path
    :param images: The collection of images
    :type images: List[Image]
    :param annotations: The collection of annotations
    :type annotations: List[ObjectDetectionAnnotation]

    :raises: IntegrityError
    """
    assert not dataset_dir.exists(), "Broken precondition."

    # None of the images should be persisted if the dataset is empty
    if any(image._persisted for image in images):
        raise IntegrityError("Empty dataset contains a persisted image.")

    # Likewise for annotations
    if any(annotation._persisted for annotation in annotations):
        raise IntegrityError("Empty dataset contains a persisted annotation.")


def _verify_persisted_images(dataset_dir: Path, images: List[Image]):
    """
    Verify the integrity of persisted images.

    :param dataset_dir: The path to the dataset
    :type: dataset_dir: Path
    :param images: The collection of images
    :type images: List[Image]

    :raises: IntegrityError
    """
    images_dir = dataset_dir / IMAGES_DIRNAME
    assert images_dir.is_dir(), "Broken invariant."

    # The number of persisted images should match
    # the total count found on disk in data lake
    in_memory = sum(1 for img in images if img._persisted)
    on_disk = sum(1 for _ in images_dir.glob("*"))
    if in_memory != on_disk:
        raise IntegrityError(
            f"Found {in_memory} images marked persisted in memory, {on_disk} on disk."
        )

    # Each image marked persisted should have corresponding file on disk
    for img in filter(lambda i: i._persisted, images):
        image_path = (images_dir / img.id).with_suffix(img.extension)
        if not image_path.is_file():
            raise IntegrityError(
                f"Image {img.id} marked persisted in memory, not found on disk."
            )


def _verify_persisted_annotations(
    dataset_dir: Path, annotations: List[ObjectDetectionAnnotation]
):
    """
    Verify the integrity of persisted annotations.

    :param dataset_dir: The path to the dataset
    :type: dataset_dir: Path
    :param annotations: The collection of annotations
    :type annotations: List[ObjectDetectionAnnotation]

    :raises: IntegrityError
    """
    annotations_dir = dataset_dir / ANNOTATIONS_DIRNAME
    assert annotations_dir.is_dir(), "Broken invariant."

    # The number of persisted annotations should match
    # the total count found on disk in data lake
    in_memory = sum(1 for a in annotations if a._persisted)
    on_disk = sum(1 for _ in annotations_dir.glob("*.txt"))
    if in_memory != on_disk:
        raise IntegrityError(
            f"Found {in_memory} annotations marked persisted in memory, {on_disk} on disk."
        )

    # Each annotation marked persisted should have a corresponding file on disk
    for annotation in filter(lambda a: a._persisted, annotations):
        annotation_path = (annotations_dir / annotation.id).with_suffix(".txt")
        if not annotation_path.is_file():
            raise IntegrityError(
                f"Annotation {annotation.id} marked persisted in memory, not found on disk."
            )


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
