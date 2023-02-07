"""
Queries for logical datasets.
"""

from pathlib import Path
from typing import Any, Dict, List

import mlops.datalake._util as util

from ..dataset.logical_dataset import LogicalDatasetView
from .filter_document import FilterDocument

# -----------------------------------------------------------------------------
# Logical Dataset Queries
# -----------------------------------------------------------------------------


def ldatasets():
    """Return a handle from which to issues queries against physical datasets."""
    util.ctx.datalake_init()
    return LogicalDatasetCollection()


class LogicalDatasetCollection:
    """A query handle for logical datasets."""

    def __init__(self):
        self.datasets_dir = util.ctx.ldataset_path()
        """The directory in which logical datasets are stored."""

    def count(self, filter: Dict[str, Any]) -> int:
        """Perform a query to count the number of datasets matching the given filter."""
        filter = FilterDocument.parse(filter)
        return sum(1 for _ in _find(self.datasets_dir, filter))

    def find(self, filter: Dict[str, Any]) -> List[LogicalDatasetView]:
        """Perform a query to collect a view for each dataset matching the given filter."""
        filter = FilterDocument.parse(filter)
        return _find(self.datasets_dir, filter)


def _find(
    datasets_dir: Path, filter: FilterDocument
) -> List[LogicalDatasetView]:
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


def _load_view_for_dataset(dataset_path: Path) -> LogicalDatasetView:
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

    raise NotImplementedError("me")


def _satisfies_filter(view: LogicalDatasetView, filter: FilterDocument) -> bool:
    """
    Determine if a view satisfies the given filter.

    :param view: The dataset view
    :type view: LogicalDatasetView
    :param filter: The filter to apply
    :type filter: QueryFilter

    :return: `True` if filter is satisfied, `False` otherwise
    :rtype: bool
    """
    return filter.evaluate(view.metadata.to_json())
