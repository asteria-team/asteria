"""
LogicalDataset class implementation.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import mlops.datalake._util as util
from mlops.datalake._util.time import timestamp

# Import all dataset-specific metadata types
from .cv import ObjectDetectionDatasetMetadata
from .metadata import (
    DatasetDomain,
    DatasetIdentifier,
    DatasetMetadata,
    DatasetType,
    SplitIdentifier,
)

# -----------------------------------------------------------------------------
# Global Variables
# -----------------------------------------------------------------------------

# The name of the file in which the dataset description is stored
_DESCRIPTION_FILENAME = "description.json"

# -----------------------------------------------------------------------------
# LogicalDataset
# -----------------------------------------------------------------------------


def _install_hook(func, hook):
    def wrapper(*args, **kwargs):
        hook()
        return func(*args, **kwargs)

    return wrapper


class SplitDefinition:
    """SplitDefinition defines a split for a LogicalDataset."""

    def __init__(
        self, identifier: SplitIdentifier, datasets: List[DatasetIdentifier]
    ):
        self.id = identifier
        """The unique identifier for the split."""

        self.datasets = datasets
        """The identifiers for the datasets of which the split is composed."""

    @staticmethod
    def create(identifier: str, datasets: List[str]) -> SplitDefinition:
        """
        Create a SplitDefinition from a collection of dataset identifiers.

        :param identifier: The unique identifier for the split
        :type identifier: str
        :param datasets: The collection of dataset identifiers
        :type datasets: List[str]

        :return: The split definition
        :rtype: SplitDefinition
        """
        return SplitDefinition(
            SplitIdentifier(identifier),
            [DatasetIdentifier(id) for id in datasets],
        )

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON format."""
        return {
            "identifier": self.id.to_json(),
            "datasets": [d.to_json() for d in self.datasets],
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> SplitDefinition:
        """Deserialize from JSON format."""
        assert all(
            e in data for e in ["identifier", "datasets"]
        ), "Broken precondition."

        return SplitDefinition.create(
            data["identifier"],
            [DatasetIdentifier.from_json(e) for e in data["datasets"]],
        )

    def __hash__(self) -> int:
        """Make hashable."""
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        """Compare equality."""
        if not isinstance(other, SplitDefinition):
            return False
        return self.id == other.id

    def __neq__(self, other: object) -> bool:
        """Compare inequality."""
        return not self.__neq__(other)


class LogicalDataset:
    """
    A LogicalDataset represents a collection of PhysicalDataset
    instances that are combined and partitioned to prepare for ML.
    """

    def __init__(self, identifier: str):
        self.splits: List[SplitDefinition] = []
        """The collection of splits of which the dataset is composed."""

        self.metadata = DatasetMetadata(
            identifier=DatasetIdentifier(identifier)
        )
        """Metadata for the dataset."""

        self.save = _install_hook(self.save, self._global_hook)
        self.verify_integrity = _install_hook(
            self.verify_integrity, self._global_hook
        )

        # Load self
        self._load()

    @property
    def id(self) -> DatasetIdentifier:
        """Return dataset identifier."""
        assert self.metadata.identifier is not None, "Broken precondition."
        return self.metadata.identifier

    def add_split(self, split: Dict[str, List[str]]):
        """
        Add a split to the LogicalDataset.

        :param split: The input split
        :type split: Dict[str, List[str]]
        """
        assert self.id is not None, "Broken invariant."

        # Convert to a split
        splitid = next(iter(split))
        split = SplitDefinition(
            identifier=SplitIdentifier(splitid),
            datasets=[DatasetIdentifier(id) for id in split[splitid]],
        )

        if split in self.splits:
            raise RuntimeError(f"Split {split} already exists.")
        # Add the split
        self.splits.append(split)
        # Update the metadata
        self.metadata = _compute_metadata(self.metadata.identifier, self.splits)

    def save(self, overwrite: bool = False):
        """
        Persist the LogicalDataset instance to the data lake.

        :param overwrite: A flag indicating that an existing dataset is overwritten
        :type overwrite: bool
        """
        dataset_dir = util.ctx.ldataset_path() / str(self.id)
        if dataset_dir.is_dir() and not overwrite:
            raise RuntimeError(
                f"Logical dataset {self.id} exists and 'overwrite' not specified."
            )

        if not dataset_dir.is_dir():
            dataset_dir.mkdir()

        # Update times
        if self.metadata.created_at is None:
            self.metadata.created_at = timestamp()
        self.metadata.updated_at = timestamp()

        # Save components
        _save_metadata(dataset_dir, self.metadata)
        _save_description(dataset_dir, self.splits)

    def _load(self):
        """Load a LogicalDataset from the data lake."""
        assert self.metadata.identifier is not None, "Broken precondition."

        dataset_dir = util.ctx.ldataset_path() / str(self.id)
        if not dataset_dir.is_dir():
            return

        # Load dataset metadata
        self.metadata = _load_metadata(dataset_dir)
        # Load dataset description
        self.splits = _load_description(dataset_dir)

    def verify_integrity(self):
        """Verify integrity of the LogicalDataset instance."""
        _verify_integrity(self)

    def _global_hook(self):
        """Hook invocation of all core interface methods."""
        util.ctx.datalake_init()


# -----------------------------------------------------------------------------
# Metadata Computation
# -----------------------------------------------------------------------------


def _compute_metadata(
    dataset_id: DatasetIdentifier, splits: List[SplitDefinition]
) -> DatasetMetadata:
    """
    Compute metadata for the LogicalDataset instance.

    :param dataset_id: The dataset identifier
    :type dataset_id: DatasetIdentifier
    :param splits: The collection of splits
    :type splits: List[SplitDefinition]

    :return: DatasetMetadata
    :rtype: DatasetMetadata
    """
    # Construct the path for each physical dataset in the collection
    dataset_path = [
        util.ctx.pdataset_path() / str(d)
        for split in splits
        for d in split.datasets
    ]
    # Load metadata for each dataset
    dataset_meta = [_load_metadata_for_dataset(d) for d in dataset_path]

    # Merge handles type-specific merge operations
    metadata = _determine_metadata_type(dataset_meta)
    for other in dataset_meta:
        metadata = metadata.merge(other)

    # Apply the identifier
    metadata.identifier = dataset_id
    return metadata


def _load_metadata_for_dataset(
    dataset_id: DatasetIdentifier,
) -> DatasetMetadata:
    """
    Load the metadata object for the specified physical dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: DatasetIdentifier

    :return: The loaded metadata
    :rtype: DatasetMetadata
    """
    dataset_dir = util.ctx.pdataset_path() / str(dataset_id)
    if not dataset_dir.is_dir():
        raise RuntimeError(
            "Attempt to create logical dataset with nonexistent dataset."
        )

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    if not metadata_path.is_file():
        raise RuntimeError("Dataset missing metadata file.")

    # Determine the appropriate loader
    # NOTE(Kyle): This is horrible, load the file twice
    generic = DatasetMetadata.from_file(metadata_path)
    if generic.type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDatasetMetadata.from_file(metadata_path)

    raise RuntimeError(f"Unknown dataset type: {generic.type}.")


def _determine_metadata_type(
    metadata_objects: List[DatasetMetadata],
) -> DatasetMetadata:
    """
    Determine the appropriate metadata object type from a collection of abstract objects.

    :param metadata_objects: The collection of metadata objects
    :type metadata_objects: List[DatasetMetadata]

    :return: The appropriate dataset metadata type
    :rtype: DatasetMetadata
    """
    # Determine the appropriate metadata type
    if not len(set(m.domain for m in metadata_objects)) == 1:
        raise RuntimeError(
            "All datasets within LogicalDataset must have equivalent domain."
        )
    if not len(set(m.type for m in metadata_objects)):
        raise RuntimeError(
            "All datasets within LogicalDataset must have equivalent type."
        )

    sample = metadata_objects[0]
    if sample.type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDatasetMetadata(
            domain=DatasetDomain.COMPUTER_VISION,
            type=DatasetType.OBJECT_DETECTION,
        )

    raise RuntimeError(f"Unknown metadata type: {sample.type}")


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

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    with metadata_path.open("w") as f:
        json.dump(metadata.to_json(), f)


def _save_description(dataset_dir: Path, splits: List[SplitDefinition]):
    """
    Save dataset description.

    :param dataset_dir: The path to the dataset directory
    :type dataset_dir: Path
    :param splits: The dataset splits:
    :type splits: List[SplitDefinition]
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    description_path = dataset_dir / _DESCRIPTION_FILENAME
    with description_path.open("w") as f:
        json.dump({"splits": [split.to_json() for split in splits]}, f)


def _load_metadata(dataset_dir: Path) -> ObjectDetectionDatasetMetadata:
    """
    Load metadata from dataset directory.

    :param dataset_dir: The path to the dataset directory
    :type: Path

    :return: The loaded metadata
    :rtype: ObjectDetectionDatasetMetadata
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    if not metadata_path.is_file():
        return ObjectDetectionDatasetMetadata()
    with metadata_path.open("r") as f:
        return ObjectDetectionDatasetMetadata.from_json(json.load(f))


def _load_description(dataset_dir: Path) -> List[SplitDefinition]:
    """
    Load splits from dataset directory.

    :param dataset_dir: The path to the dataset directory
    :type dataset_dir: Path

    :return: The collection of loaded splits
    :rtype: List[SplitDefinition]
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    description_path = dataset_dir / _DESCRIPTION_FILENAME
    if not description_path.is_file():
        return []

    with description_path.open("r") as f:
        return [SplitDefinition.from_json(s) for s in json.load(f)["splits"]]


# -----------------------------------------------------------------------------
# Integrity Verification
# -----------------------------------------------------------------------------


def _verify_integrity(dataset: LogicalDataset):
    """
    Verify the integrity of a logical dataset.

    :param dataset: The input dataset
    :type dataset: LogicalDataset
    """
    # Ensure that each split contains only unique identifiers
    _verify_unique_within_split(dataset.splits)
    # Ensure that dataset identifiers are not repeated across splits
    _verify_unique_across_splits(dataset.splits)


def _verify_unique_within_split(splits: List[SplitDefinition]):
    """
    Verify that physical dataset identifiers within a split are unique.

    :param splits: The collection of splits
    :type splits: List[SplitDefinition]
    """
    for split_def in splits:
        if len(split_def.datasets) != len(set(split_def.datasets)):
            raise RuntimeError(
                f"Split definition {split_def.id} contains non-unique dataset identifier."
            )


def _verify_unique_across_splits(splits: List[SplitDefinition]):
    """
    Verify that physical dataset identifiers are not reused across splits.

    :param splits: The collection of splits
    :type splits: List[SplitDefinition]
    """
    for split_def in splits:
        for dataset_id in split_def.datasets:
            if any(
                dataset_id in s.datasets
                for s in [split for split in splits if split.id != split_def.id]
            ):
                raise RuntimeError(
                    f"Dataset {dataset_id} reused across splits."
                )
