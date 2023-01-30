"""
PhysicalDataset class implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import mlops.datalake._util as util

from .metadata import (
    DatasetDomain,
    DatasetIdentifier,
    DatasetMetadata,
    DatasetType,
)

# -----------------------------------------------------------------------------
# PhysicalDataset
# -----------------------------------------------------------------------------


def install_hook(func, hook):
    def wrapper(*args, **kwargs):
        hook()
        return func(*args, **kwargs)

    return wrapper


class PhysicalDataset(ABC):
    """PhysicalDataset is the base class for all datasets that exist on disk."""

    def __init__(
        self,
        domain: DatasetDomain,
        type: DatasetType,
        identifier: DatasetIdentifier,
    ):
        self.domain = domain
        """A domain identifier for the dataset."""

        self.type = type
        """A type identifier for the dataset."""

        self.identifier = identifier
        """The unique identifier for the PhysicalDataset instance."""

        # Install hooks for core interface
        self.save = install_hook(self.save, self._global_hook)
        self._load = install_hook(self._load, self._global_hook)
        self.verify_integrity = install_hook(
            self.verify_integrity, self._global_hook
        )

    @abstractmethod
    def save(self):
        """Persist the dataset to the data lake."""
        raise NotImplementedError("Not implemented.")

    @abstractmethod
    def _load(self):
        """Load the dataset from the data lake."""
        raise NotImplementedError("Not implemented.")

    @abstractmethod
    def verify_integrity(self):
        """Verify the integrity of the dataset."""
        raise NotImplementedError("Not implemented.")

    def _global_hook(self):
        """Hook invocation of all core interface methods."""
        util.ctx.datalake_init()


# -----------------------------------------------------------------------------
# PhysicalDatasetView
# -----------------------------------------------------------------------------


class PhysicalDatasetView(ABC):
    """PhysicalDatasetView is the base class for all dataset views."""

    def __init__(
        self,
        domain: DatasetDomain,
        type: DatasetType,
        identifier: DatasetIdentifier,
    ):
        self.domain = domain
        """The dataset domain."""

        self.type = type
        """The dataset type."""

        self.identifier = identifier
        """The dataset identifier."""

        self.metadata = DatasetMetadata(
            domain=self.domain, type=self.type, identifier=self.identifier
        )
        """The dataset metadata."""

        # Install hooks
        self._load = install_hook(self._load, self._global_hook)

    @abstractmethod
    def _load(self):
        """Load the dataset from the data lake."""
        raise NotImplementedError("Not implemented.")

    def _global_hook(self):
        """Hook invocation of all core interface methods."""
        util.ctx.datalake_init()
