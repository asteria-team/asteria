"""
Tests for global datalake integrity.
"""

from pathlib import Path

import mlops.datalake as dl

# -----------------------------------------------------------------------------
# Test Helpers
# -----------------------------------------------------------------------------


def _dl_path(tmp_path: Path) -> Path:
    """Create a root location for the data lake."""
    path = tmp_path / "dl"
    path.mkdir()
    return path


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


def test_empty(tmp_path):
    dl.set_path(_dl_path(tmp_path))
    dl.integrity.verify()
