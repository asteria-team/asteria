"""
Basic, repetitious operations that are common across datalake.
"""

from typing import Union

import mlops.datalake._util as util

from ..metadata import (
    DatasetDomain,
    DatasetIdentifier,
    DatasetMetadata,
    DatasetType,
)


def pdataset_domain(dataset_id: Union[str, DatasetIdentifier]) -> DatasetDomain:
    """
    Get the domain identifier for the specified dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: Union[str, DatasetIdentifier]

    :return: The dataset domain
    :rtype: DatasetDomain
    """
    dataset_id: str = (
        dataset_id.id
        if isinstance(dataset_id, DatasetIdentifier)
        else dataset_id
    )

    dataset_dir = util.ctx.pdataset_path() / dataset_id
    assert dataset_dir.is_dir(), "Broken invariant."

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    assert metadata_path.is_file(), "Broken invariant."

    metadata = DatasetMetadata.from_file(metadata_path)
    return metadata.domain


def pdataset_type(dataset_id: Union[str, DatasetIdentifier]) -> DatasetType:
    """
    Get the type identifier for the specified dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: Union[str, DatasetIdentifier]

    :return: The dataset type
    :rtype: DatasetType
    """
    dataset_id: str = (
        dataset_id.id
        if isinstance(dataset_id, DatasetIdentifier)
        else dataset_id
    )

    dataset_dir = util.ctx.pdataset_path() / dataset_id
    assert dataset_dir.is_dir(), "Broken invariant."

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    assert metadata_path.is_file(), "Broken invariant."

    metadata = DatasetMetadata.from_file(metadata_path)
    return metadata.type


def pdataset_exists(dataset_id: Union[str, DatasetIdentifier]) -> bool:
    """
    Determine if the physical dataset with identifier `dataset_id` exists.

    :param dataset_id: The dataset identifier
    :type dataset_id: Union[str, DatasetIdentifier]

    :return: `True` if the dataset exists, `False` otherwise
    :rtype: bool
    """
    dataset_id: str = (
        dataset_id.id
        if isinstance(dataset_id, DatasetIdentifier)
        else dataset_id
    )

    dataset_dir = util.ctx.pdataset_path() / dataset_id
    return dataset_dir.is_dir()


def ldataset_domain(dataset_id: Union[str, DatasetIdentifier]) -> DatasetDomain:
    """
    Get the domain identifier for the specified dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: Union[str, DatasetIdentifier]

    :return: The dataset domain
    :rtype: DatasetDomain
    """
    dataset_id: str = (
        dataset_id.id
        if isinstance(dataset_id, DatasetIdentifier)
        else dataset_id
    )

    dataset_dir = util.ctx.pdataset_path() / dataset_id
    assert dataset_dir.is_dir(), "Broken invariant."

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    assert metadata_path.is_file(), "Broken invariant."

    metadata = DatasetMetadata.from_file(metadata_path)
    return metadata.domain


def ldataset_type(dataset_id: Union[str, DatasetIdentifier]) -> DatasetType:
    """
    Get the type identifier for the specified dataset.

    :param dataset_id: The dataset identifier
    :type dataset_id: Union[str, DatasetIdentifier]

    :return: The dataset type
    :rtype: DatasetType
    """
    dataset_id: str = (
        dataset_id.id
        if isinstance(dataset_id, DatasetIdentifier)
        else dataset_id
    )

    dataset_dir = util.ctx.pdataset_path() / dataset_id
    assert dataset_dir.is_dir(), "Broken invariant."

    metadata_path = dataset_dir / util.ctx.metadata_filename()
    assert metadata_path.is_file(), "Broken invariant."

    metadata = DatasetMetadata.from_file(metadata_path)
    return metadata.type
