"""
Queries for physical datasets.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Union

import mlops.datalake._util as util

# Import all dataset types from cv
from ..cv import ObjectDetectionDatasetView
from ..physical_dataset import (
    DatasetType,
    PhysicalDatasetMetadata,
    PhysicalDatasetView,
)

# -----------------------------------------------------------------------------
# QueryFilter
# -----------------------------------------------------------------------------


class QueryFilter:
    """Represents a filter supplied to a query."""

    def __init__(self, filter: Dict[str, Any]):
        self.predicates = [QueryPredicate(k, v) for k, v in filter.items()]
        """Decompose the filter into predicates."""


class QueryPredicate:
    """Represents an individual predicate within a query."""

    def __init__(self, key: str, value: Any):
        self.k = key
        self.v = value


# -----------------------------------------------------------------------------
# Physical Dataset Queries
# -----------------------------------------------------------------------------


def pdatasets():
    """Return a handle from which to issues queries against physical datasets."""
    util.ctx.datalake_init()
    return PhysicalDatasetCollection()


class PhysicalDatasetCollection:
    """A query handle for physical datasets."""

    def __init__(self):
        self.datasets_dir = util.ctx.pdataset_path()
        """The directory in which physical datasets are stored."""

    def count(self, filter: Dict[str, Any]) -> int:
        """Perform a query to count the number of datasets matching the given filter."""
        filter = QueryFilter(filter)
        return sum(1 for _ in _find(self.datasets_dir, filter))

    def find(self, filter: Dict[str, Any]) -> List[PhysicalDatasetView]:
        """Perform a query to collect a view for each dataset matching the given filter."""
        filter = QueryFilter(filter)
        return _find(self.datasets_dir, filter)


def _find(datasets_dir: Path, filter: QueryFilter) -> List[PhysicalDatasetView]:
    """
    Load a view for each dataset matching the provided filter.

    :param datasets_dir: The path at which physical datasets are stored
    :type datasets_dir: Path
    :param filter: The filter to apply
    :type filter: QueryFilter
    """
    assert datasets_dir.is_dir(), "Broken precondition."
    views = [
        _load_view_for_dataset(dataset_path)
        for dataset_path in datasets_dir.glob("*")
        if dataset_path.is_dir()
    ]
    return [view for view in views if _satisfies_filter(view, filter)]


def _load_view_for_dataset(dataset_path: Path) -> PhysicalDatasetView:
    """
    Load a view for the dataset at the specified path.

    :param dataset_path: The path to the dataset
    :type dataset_path: Path

    :return: The loaded view
    :rtype: PhysicalDatasetView
    """
    assert dataset_path.is_dir(), "Broken precondition."

    metadata_path = dataset_path / util.ctx.metadata_filename()
    if not metadata_path.is_file():
        raise RuntimeError("Missing metadata file.")

    meta = PhysicalDatasetMetadata.from_file(metadata_path)
    if meta.type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDatasetView(_basename(dataset_path))
    else:
        raise RuntimeError(f"Unknown dataset type: {meta.type}")


def _satisfies_filter(view: PhysicalDatasetView, filter: QueryFilter) -> bool:
    """
    Determine if a view satisfies the given filter.

    :param view: The dataset view
    :type view: PhysicalDatasetView
    :param filter: The filter to apply
    :type filter: QueryFilter

    :return: `True` if filter is satisfied, `False` otherwise
    :rtype: bool
    """
    return all(
        _satisfies_predicate(view, predicate) for predicate in filter.predicates
    )


def _satisfies_predicate(
    view: PhysicalDatasetView, predicate: QueryPredicate
) -> bool:
    """
    Determine if a view satisfies the given predicate.

    :param view: The dataset view
    :type view: PhysicalDatasetView
    :param predicate: The predicate to apply
    :type predicate: QueryPredicate

    :return `True` if the predicate is satisfied, `False` otherwise
    :rtype: bool
    """
    # Extract the raw JSON from metadata
    metadata = view.metadata.to_json()
    if predicate.k not in metadata:
        # Vacuously satisfied
        return True

    # Work-around for the weird way we serialize these
    if predicate.k in ["domain", "type", "identifier"]:
        return metadata[predicate.k][predicate.k] == predicate.v
    return metadata[predicate.k] == predicate.v


# -----------------------------------------------------------------------------
# Filesystem Helpers
# -----------------------------------------------------------------------------


def _basename(path: Union[Path, str]) -> str:
    """Parse path basename."""
    path = path if isinstance(path, Path) else Path(path)
    return os.path.splitext(path.name)[0]
