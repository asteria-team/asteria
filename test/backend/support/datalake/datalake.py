"""
Datalake-related support functionality for backend testing
"""

import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict


def create_tmpdir(artifacts: Dict[str, Any]) -> None:
    """Create a temporary directory."""
    artifacts["tmp_path"] = tempfile.mkdtemp()


def create_datalake(artifacts: Dict[str, Any]) -> None:
    """Create a root storage location for the data lake."""
    tmp_path = Path(artifacts["tmp_path"])
    datalake = Path(f"{tmp_path}/dl")
    datalake.mkdir()
    artifacts["datalake"] = str(datalake)


def delete_tmpdir(artifacts: Dict[str, Any]) -> None:
    """Delete a temporary directory."""
    shutil.rmtree(artifacts["tmp_path"])
