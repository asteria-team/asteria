"""
Dataset integrity verification.
"""

import os
from pathlib import Path
from typing import List

import mlops.datalake._util as util
import mlops.datalake.dataset.ops as ops

# Import all concrete dataset types
from ..cv import ObjectDetectionDataset
from ..logical_dataset import LogicalDataset
from ..metadata import DatasetType
from ..physical_dataset import PhysicalDataset

# -----------------------------------------------------------------------------
# Integrity Verification
# -----------------------------------------------------------------------------


def verify():
    """Verify dataset integrity for all datasets."""
    pdatasets = _pdataset_identifiers()
    pdatasets = [
        (identifier, ops.pdataset_type(identifier)) for identifier in pdatasets
    ]
    for identifier, type in pdatasets:
        instance: PhysicalDataset = _dataset_class_from_type(type)(identifier)
        instance.verify_integrity()

    ldatasets = _ldataset_identifiers()
    for identifier in ldatasets:
        LogicalDataset(identifier).verify_integrity()


def _dataset_class_from_type(type: DatasetType) -> type:
    """
    Get the dataset class from type.

    :param type: The type identifier
    :type type: DatasetType

    :return: The type object
    :rtype: type
    """
    if type == DatasetType.OBJECT_DETECTION:
        return ObjectDetectionDataset


def _pdataset_identifiers() -> List[str]:
    """Discover all pdataset_identifiers."""
    return [_basename(path) for path in util.ctx.pdataset_path().glob("*")]


def _ldataset_identifiers() -> List[str]:
    """Discover all ldataset identifiers."""
    return [_basename(path) for path in util.ctx.ldataset_path().glob("*")]


def _basename(path: Path) -> str:
    """Parse path basename."""
    return os.path.splitext(path.name)[0]
