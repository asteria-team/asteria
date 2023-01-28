"""
Unit tests for object detection datasets.
"""

import json

import pytest

import mlops.datalake as dl

from ...testutil import find_json

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
# Dataset Construction
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
