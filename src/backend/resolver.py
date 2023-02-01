"""
Path resolution.
"""

import os
from pathlib import Path


def mlops_root() -> Path:
    """Resolve the path to the mlops library."""
    current = Path(os.path.dirname(os.path.abspath(__file__)))
    return current.parent / "mlops"
