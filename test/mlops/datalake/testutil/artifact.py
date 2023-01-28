"""
Artifact resolution.
"""

import os
from pathlib import Path


def _testdata_root() -> Path:
    """
    Resolve the path to the root of the testdata directory.

    :return: The resolved path
    :rtype: Path
    """
    return (
        Path(os.getcwd()) / "test" / "mlops" / "datalake" / "testdata"
    ).resolve()


def find_json(name: str) -> Path:
    """
    Find a JSON artifact.

    :param name: The name of the artifact
    :type name: str

    :return: The path to the artifact
    :rtype: Path
    """
    json_dir = _testdata_root() / "json"
    assert json_dir.is_dir(), "Broken invariant."

    for path in json_dir.glob("*"):
        if path.name == name:
            return path
    raise RuntimeError(f"Failed to find JSON artifact: {name}.")
