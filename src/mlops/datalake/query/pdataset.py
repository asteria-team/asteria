"""
Queries for physical datasets.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Union

import mlops.datalake._util as util

# Import all dataset types from cv
from ..dataset.cv import ObjectDetectionDatasetView
from ..dataset.metadata import DatasetMetadata, DatasetType
from ..dataset.physical_dataset import PhysicalDatasetView
from .filter_document import FilterDocument

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
        filter = FilterDocument.parse(filter)
        return sum(1 for _ in _find(self.datasets_dir, filter))

    def find(self, filter: Dict[str, Any]) -> List[PhysicalDatasetView]:
        """Perform a query to collect a view for each dataset matching the given filter."""
        filter = FilterDocument.parse(filter)
        return _find(self.datasets_dir, filter)


def _find(
    datasets_dir: Path, filter: FilterDocument
) -> List[PhysicalDatasetView]:
    """
    Load a view for each dataset matching the provided filter.

    :param datasets_dir: The path at which physical datasets are stored
    :type datasets_dir: Path
    :param filter: The filter to apply
    :type filter: FilterDocument
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

    meta = DatasetMetadata.from_file(metadata_path)
    if meta.type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDatasetView(_basename(dataset_path))
    else:
        raise RuntimeError(f"Unknown dataset type: {meta.type}")


def _satisfies_filter(
    view: PhysicalDatasetView, filter: FilterDocument
) -> bool:
    """
    Determine if a view satisfies the given filter.

    :param view: The dataset view
    :type view: PhysicalDatasetView
    :param filter: The filter to apply
    :type filter: QueryFilter

    :return: `True` if filter is satisfied, `False` otherwise
    :rtype: bool
    """
    return filter.evaluate(view.metadata.to_json())


# -----------------------------------------------------------------------------
# Filesystem Helpers
# -----------------------------------------------------------------------------


def _basename(path: Union[Path, str]) -> str:
    """Parse path basename."""
    path = path if isinstance(path, Path) else Path(path)
    return os.path.splitext(path.name)[0]
