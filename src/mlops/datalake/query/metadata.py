"""
Metadata queries for datalake.
"""

from __future__ import annotations

from typing import List

from ..dataset.metadata import DatasetDomain, DatasetType
from .filter_document import FilterDocument

# -----------------------------------------------------------------------------
# DatasetDomain
# -----------------------------------------------------------------------------


def dataset_domains() -> DatasetDomainCollection:
    """
    Get a handle to the dataset domains collection.

    :return: The handle
    :rtype: DatasetDomainCollection
    """
    return DatasetDomainCollection()


class DatasetDomainCollection:
    """A handle for querying dataset domains."""

    def __init__(self):
        pass

    def find(self, _: FilterDocument) -> List[DatasetDomain]:
        """
        Find all available dataset domains.

        :return: The available dataset domains
        :rtype: List[DatasetDomain]
        """
        return [value for value in DatasetDomain]


# -----------------------------------------------------------------------------
# DatasetDomain
# -----------------------------------------------------------------------------


def dataset_types() -> DatasetTypeCollection:
    """
    Get a handle to the dataset types collection.

    :return: The handle
    :rtype: DatasetTypeCollection
    """
    return DatasetTypeCollection()


class DatasetTypeCollection:
    """A handle for querying dataset types."""

    def __init__(self):
        pass

    def find(self, _: FilterDocument) -> List[DatasetType]:
        """
        Find all available dataset types.

        :return: The available dataset types
        :rtype: List[DatasetType]
        """
        return [value for value in DatasetType]
