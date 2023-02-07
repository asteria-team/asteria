"""
LogicalDataset class implementation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import mlops.datalake._util as util
import mlops.datalake.dataset.ops as ops
from mlops.datalake._util.time import timestamp

# Import all dataset-specific metadata types
from .cv import ObjectDetectionDatasetMetadata
from .metadata import (
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
# SplitDefinition
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
# LogicalDatasetMetadata
# -----------------------------------------------------------------------------


@dataclass
class LogicalDatasetMetadata(DatasetMetadata):
    """
    Represents the metadata for a logical dataset.

    For a logical datasets, the top-level contains generic
    dataset metadata, and for each split we maintain a
    dataset-type specific metadata object that is computed
    from the physical datasets of which the split is composed.
    """

    splits: Dict[str, DatasetMetadata] = field(default_factory=lambda: [])
    """The collection of per-split metadata."""

    def to_json(self) -> Dict[str, Any]:
        """Serialize to JSON object."""
        return {
            **super().to_json(),
            "splits": {k: v.to_stripped_json() for k, v in self.splits.items()},
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> LogicalDatasetMetadata:
        """Deserialize from JSON object."""
        assert "splits" in data, "Broken precondition."

        # Populate the base object
        metadata = LogicalDatasetMetadata._populated_from(
            DatasetMetadata.from_json(data)
        )

        meta_type = _metadata_class_for_type(metadata.type)
        for k, v in data["splits"].items():
            metadata.splits[k] = meta_type.from_stripped_json(v)

        return metadata

    @staticmethod
    def _populated_from(
        dataset_metadata: DatasetMetadata,
    ) -> LogicalDatasetMetadata:
        """
        Populate base entries from existing DatasetMetadata instance.

        :param dataset_metadata: The target object
        :type dataset_metadata: DatasetMetadata

        :return: The populated metadata
        :rtype: LogicalDatasetMetadata
        """
        metadata = LogicalDatasetMetadata()
        metadata.domain = dataset_metadata.domain
        metadata.type = dataset_metadata.type
        metadata.identifier = dataset_metadata.identifier
        metadata.created_at = dataset_metadata.created_at
        metadata.updated_at = dataset_metadata.updated_at
        return metadata


# -----------------------------------------------------------------------------
# LogicalDataset
# -----------------------------------------------------------------------------


def _install_hook(func, hook):
    def wrapper(*args, **kwargs):
        hook()
        return func(*args, **kwargs)

    return wrapper


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

        if not len(split) == 1:
            raise RuntimeError("Invalid split.")

        # Ensure that each dataset in the split exists
        if not all(ops.pdataset_exists(did) for did in list(split.values())[0]):
            raise RuntimeError("Missing physical dataset.")

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
        self.metadata = _compute_metadata(
            self.metadata.identifier, self.metadata, self.splits
        )

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

        # Update metadata
        self.metadata = _compute_metadata(self.id, self.metadata, self.splits)
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
# LogicalDatasetView
# -----------------------------------------------------------------------------


class LogicalDatasetView:
    """LogicalDatasetView is the base class for all dataset views."""

    def __init__(self, identifier: str):
        self.metadata = LogicalDatasetMetadata(
            identifier=DatasetIdentifier(identifier)
        )
        """The dataset metadata."""

        # Install hooks
        self._load = _install_hook(self._load, self._global_hook)

    @property
    def id(self) -> DatasetIdentifier:
        """Return the dataset identifier."""
        return self.metadata.identifier

    def _load(self):
        """Load view from disk."""
        dataset_dir = util.ctx.ldataset_path() / str(self.identifier)

        # If the dataset does not yet exist, nothing to do
        if not dataset_dir.is_dir():
            return

        # Load metadata
        self.metadata = _load_metadata(dataset_dir)

    def _global_hook(self):
        """Hook invocation of all core interface methods."""
        util.ctx.datalake_init()


# -----------------------------------------------------------------------------
# Metadata Computation
# -----------------------------------------------------------------------------


def _compute_metadata(
    dataset_id: DatasetIdentifier,
    existing_metadata: LogicalDatasetMetadata,
    splits: List[SplitDefinition],
) -> LogicalDatasetMetadata:
    """
    Compute metadata for the LogicalDataset instance.

    :param dataset_id: The dataset identifier
    :type dataset_id: DatasetIdentifier
    :param existing_metadata: The existing metadata for the dataset
    :type existing_metadata: LogicalDatasetMetadata
    :param splits: The collection of splits
    :type splits: List[SplitDefinition]

    :return: LogicalDatasetMetadata
    :rtype: LogicalDatasetMetadata
    """
    # Compute metadata for each split
    metadata_by_split = {
        str(split.id): _compute_metadata_for_split(dataset_id, split)
        for split in splits
    }
    if (
        not len(
            set(split_meta.domain for split_meta in metadata_by_split.values())
        )
        == 1
    ):
        raise RuntimeError(
            "All datasets within logical dataset must have same domain."
        )
    if (
        not len(
            set(split_meta.type for split_meta in metadata_by_split.values())
        )
        == 1
    ):
        raise RuntimeError(
            "All datasets within logical dataset must have same type."
        )

    # Compute dataset domain
    dataset_domain = list(metadata_by_split.values())[0].domain
    # Compute dataset type
    dataset_type = list(metadata_by_split.values())[0].type

    # Construct unified metadata
    metadata = LogicalDatasetMetadata(
        domain=dataset_domain,
        type=dataset_type,
        identifier=dataset_id,
        created_at=existing_metadata.created_at,
        updated_at=existing_metadata.updated_at,
        splits=metadata_by_split,
    )
    return metadata


def _compute_metadata_for_split(
    dataset_id: DatasetIdentifier, split: SplitDefinition
) -> DatasetMetadata:
    """
    Compute metadata for an individual split within logical dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: DatasetIdentifier
    :param split: The target split
    :type split: SplitDefinition

    :return: LogicalDatasetMetadata
    :rtype: LogicalDatasetMetadata
    """
    if not len(set(ops.pdataset_domain(did) for did in split.datasets)) == 1:
        raise RuntimeError(
            f"All datasets within split {split.id} must have same domain."
        )
    if not len(set(ops.pdataset_type(did) for did in split.datasets)) == 1:
        raise RuntimeError(
            f"All datasets within split {split.id} must have same type."
        )

    # Grab the dataset type (equivalent for all)
    dataset_type = ops.pdataset_type(split.datasets[0])

    # Load the metadata for each dataset
    dataset_metadata = [
        _load_metadata_for_dataset(did, dataset_type) for did in split.datasets
    ]

    # Merge metadata from all datasets
    builder: DatasetMetadata = _metadata_class_for_type(dataset_type)()
    for metadata in dataset_metadata:
        builder = builder.merge(metadata)

    return builder


def _load_metadata_for_dataset(
    dataset_id: DatasetIdentifier, dataset_type: DatasetType
) -> DatasetMetadata:
    """
    Load the metadata object for the specified physical dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: DatasetIdentifier
    :param dataset_type: The type identifier for the dataset
    :type dataset_type: DatasetType

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
    if dataset_type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDatasetMetadata.from_file(metadata_path)
    assert False, "Unreachable."


def _metadata_class_for_type(type: DatasetType) -> type:
    """
    Get the type object for the dataset metadata class of the specified type.

    :param type: The type identifier for the dataset
    :type type: DatasetType

    :return: The type object
    :rtype: type
    """
    if type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDatasetMetadata
    raise RuntimeError(f"Unknown dataset type: {type}.")


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


def _load_metadata(dataset_dir: Path) -> LogicalDatasetMetadata:
    """
    Load metadata from dataset directory.

    :param dataset_dir: The path to the dataset directory
    :type: Path

    :return: The loaded metadata
    :rtype: LogicalDatasetMetadata
    """
    assert dataset_dir.is_dir(), "Broken precondition."

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    if not metadata_path.is_file():
        return LogicalDatasetMetadata()
    with metadata_path.open("r") as f:
        return LogicalDatasetMetadata.from_json(json.load(f))


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
