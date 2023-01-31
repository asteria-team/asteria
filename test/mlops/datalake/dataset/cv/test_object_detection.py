"""
Unit tests for object detection datasets.
"""

import json
import os
import shutil
from pathlib import Path
from typing import List

import pytest

import mlops.datalake as dl

from ...testutil import find_image, find_json

# -----------------------------------------------------------------------------
# Test Data Generation
# -----------------------------------------------------------------------------


def _write_image(src_path: Path, dst_path: Path, id: int) -> Path:
    """
    Write an image for testing purposes.

    :param src_path: The path to a true source image
    :type src_path: Path
    :param dst_path: The destination path at which images are written
    :type dst_path: Path
    :param id: The unique identifier for the image
    :type id: int

    :return: The path at which the image was written
    :rtype: Path
    """
    assert src_path.is_file(), "Broken precondition."
    assert dst_path.is_dir(), "Broken precondition."

    dst_path = (dst_path / f"{id:05}").with_suffix(_ext(src_path))
    shutil.copyfile(src_path, dst_path)

    return dst_path


def _write_images(src_path: Path, dst_path: Path, count: int) -> List[Path]:
    """
    Write images for testing purposes.

    :param src_path: The path to a true source image
    :type src_path: Path
    :param dst_path: The destination path at which images are written
    :type dst_path: Path
    :param count: The number of images to write
    :type count: int

    :return: The collection of paths at which images were written
    :rtype: List[Path]
    """
    assert src_path.is_file(), "Broken precondition."
    assert dst_path.is_dir(), "Broken precondition."
    return [_write_image(src_path, dst_path, id) for id in range(count)]


def _make_fake_images(tmp_dir: Path, count: int) -> List[Path]:
    """
    Make fake images within `tmp_dir`.

    :param tmp_dir: The temporary directory root
    :type tmp_dir: Path
    :param count: The number of images to write
    :type count: int

    :return: The collection of paths
    :rtype: List[Path]
    """
    images_dir = tmp_dir / "images"
    images_dir.mkdir()

    return _write_images(find_image("cat"), images_dir, count)


def _ext(path: Path) -> str:
    """Parse extension."""
    return os.path.splitext(path.name)[1]


# -----------------------------------------------------------------------------
# Annotation Parsing
# -----------------------------------------------------------------------------


def test_annotation_parse_json0():
    """Parsing should succeed."""

    json_path = find_json("label_studio_task0.json")
    with json_path.open("r") as f:
        data = json.load(f)

    _ = dl.ObjectDetectionAnnotation.from_ls_json(data)


def test_annotation_parse_json1():
    """Parsing should succeed with empty annotations."""

    json_path = find_json("label_studio_task1.json")
    with json_path.open("r") as f:
        data = json.load(f)

    _ = dl.ObjectDetectionAnnotation.from_ls_json(data)


def test_annotation_parse_corrupt0():
    """Parsing should fail with corrupt input (missing 'result')."""

    json_path = find_json("label_studio_task_corrupt0.json")
    with json_path.open("r") as f:
        data = json.load(f)

    with pytest.raises(RuntimeError):
        _ = dl.ObjectDetectionAnnotation.from_ls_json(data)


# -----------------------------------------------------------------------------
# Physical Dataset Construction
# -----------------------------------------------------------------------------


def test_init_fail():
    """
    Attempting to create a dataset without context should raise.
    """
    with pytest.raises(RuntimeError):
        _ = dl.ObjectDetectionDataset("id")


def test_init_succeed(tmp_path):
    """
    Creating a dataset after setting context should succeed.
    """
    dl.set_path(f"{tmp_path / 'dl'}")
    _ = dl.ObjectDetectionDataset("id")


def test_empty_integrity(tmp_path):
    """
    Empty dataset should pass integrity check.
    """
    dl.set_path(f"{tmp_path / 'dl'}")
    d = dl.ObjectDetectionDataset("id")
    d.verify_integrity()


def test_save_no_data(tmp_path):
    """
    Attempt to save without adding any data should succeed.
    """
    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id")
    d.save()


def test_write_single_image(tmp_path):
    """Save with a single image."""
    paths = _make_fake_images(tmp_path, 1)

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("dataset0")
    d.add_image(dl.Image(path=paths[0]))
    d.save()

    d.verify_integrity()


def test_write_multi_image(tmp_path):
    """Save with some image."""
    paths = _make_fake_images(tmp_path, 5)

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("dataset0")
    for path in paths:
        d.add_image(dl.Image(path=path))
    d.save()

    d.verify_integrity()


# -----------------------------------------------------------------------------
# Logical Dataset Construction
# -----------------------------------------------------------------------------


def test_logical0(tmp_path):
    """Construction should succeed."""
    dl.set_path(f"{tmp_path / 'dl'}")

    _ = dl.LogicalDataset("id")


def test_logical1(tmp_path):
    """Adding split that does not exist should raise."""
    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.LogicalDataset("id")
    with pytest.raises(RuntimeError):
        d.add_split({"train": ["id0"]})


def test_logical2(tmp_path):
    """Adding split that exists should succeed."""
    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id0")
    d.save()

    d = dl.LogicalDataset("id")
    d.add_split({"train": ["id0"]})


def test_logical3(tmp_path):
    """Adding split that exists should succeed."""
    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id0")
    d.save()

    d = dl.ObjectDetectionDataset("id1")
    d.save()

    d = dl.LogicalDataset("id")
    d.add_split({"train": ["id0", "id1"]})
    d.save()


def test_logical4(tmp_path):
    """Integration of physical and logical."""
    paths = _make_fake_images(tmp_path, 2)

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id0")
    d.add_image(dl.Image(path=paths[0]))
    d.save()

    d.verify_integrity()

    d = dl.ObjectDetectionDataset("id1")
    d.add_image(dl.Image(path=paths[1]))
    d.save()

    d.verify_integrity()

    d = dl.LogicalDataset("id")
    d.add_split({"train": ["id0"]})
    d.add_split({"test": ["id1"]})
    d.save()

    d.verify_integrity()
