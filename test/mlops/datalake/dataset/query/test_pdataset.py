"""
Unit tests for queries for physical datasets.
"""

import mlops.datalake as dl


def test_empty_count(tmp_path):
    """Empty query on empty datalake should return 0 count."""
    dl.set_path(f"{tmp_path / 'dl'}")
    count = dl.pdatasets().count({})
    assert count == 0


def test_empty_find(tmp_path):
    """Empty query on empty datalake should return 0 views."""
    dl.set_path(f"{tmp_path / 'dl'}")
    views = dl.pdatasets().find({})
    assert len(views) == 0


def test_single_count(tmp_path):
    """Count with single item should return 1."""

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id")
    d.save()

    count = dl.pdatasets().count({})
    assert count == 1


def test_single_find(tmp_path):
    """Count with single item should return 1."""

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id")
    d.save()

    views = dl.pdatasets().find({})
    assert len(views) == 1


def test_single_count_with_filter(tmp_path):
    """Count with single item should return 1."""

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id")
    d.save()

    count = dl.pdatasets().count(
        {"$eq": [{"$field": "identifier"}, {"$lit": "id"}]}
    )
    assert count == 1


def test_single_find_with_filter(tmp_path):
    """Find with single item should return it."""

    dl.set_path(f"{tmp_path / 'dl'}")

    d = dl.ObjectDetectionDataset("id")
    d.save()

    views = dl.pdatasets().find(
        {"$eq": [{"$field": "identifier"}, {"$lit": "id"}]}
    )
    assert len(views) == 1
