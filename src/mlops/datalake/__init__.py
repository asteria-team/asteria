"""
Datalake sub-package exports.
"""

# Expose integrity as submodule
import mlops.datalake.integrity as integrity

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

# Export all exception types
from mlops.datalake.exception import (
    EvaluationError,
    IncompleteError,
    IntegrityError,
    ParseError,
)

# Export query types and collection handles
from mlops.datalake.query import FilterDocument, pdatasets

__all__ = [
    "set_path",
    "Image",
    "ObjectDetectionDataset",
    "ObjectDetectionAnnotation",
    "LogicalDataset",
    "pdatasets",
    "FilterDocument",
    "IntegrityError",
    "IncompleteError",
    "ParseError",
    "EvaluationError",
    "integrity",
]
