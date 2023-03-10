"""
Resolve the path to the `mlops` package.
"""

import os


def package_root() -> str:
    """Resolve the path to the project root."""
    path = os.path.dirname(os.path.abspath(__file__))
    while os.path.basename(os.path.abspath(path)) != "testbed":
        path = os.path.join(path, "..")
    return os.path.abspath(os.path.join(path, "..", "src/"))
