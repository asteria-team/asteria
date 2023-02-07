"""
Unit tests for metadata queries.
"""

from pathlib import Path

import mlops.datalake as dl


def _dl_path(tmp_path: Path) -> Path:
    """Create a root location for the data lake."""
    path = tmp_path / "dl"
    path.mkdir()
    return path


def test_dataset_domain(tmp_path):
    dl.set_path(_dl_path(tmp_path))

    domains = dl.dataset_domains().find({})
    assert len(domains) > 0


def test_dataset_type(tmp_path):
    dl.set_path(_dl_path(tmp_path))

    types = dl.dataset_types().find({})
    assert len(types) > 0
