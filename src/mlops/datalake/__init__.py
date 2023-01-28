"""
Top-level package exports.
"""

# Export top-level context management
from mlops.datalake._util import set_path

# Export all physical dataset concrete implementations
from mlops.datalake.dataset.cv import (
    Image,
    ObjectDetectionAnnotation,
    ObjectDetectionDataset,
)

# Export logical dataset
from mlops.datalake.dataset.logical_dataset import LogicalDataset

__all__ = [
    "set_path",
    "Image",
    "ObjectDetectionDataset",
    "ObjectDetectionAnnotation",
    "LogicalDataset",
]
