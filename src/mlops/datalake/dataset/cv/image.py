"""
Image class definition.
"""

import os
from pathlib import Path
from typing import Tuple, Union

import PIL


class Image:
    """Image represents an image on disk."""

    def __init__(self, path: Union[Path, str]):
        self.path = path if isinstance(path, Path) else Path(path)
        """The path to the image on disk."""

    @property
    def id(self) -> str:
        """Return the unique image identifier."""
        return _parse_id_from_path(self.path)

    @property
    def size(self) -> Tuple[int, int]:
        """Return the size of the image (width, height), in pixels."""
        image = PIL.Image(self.path)
        return image.size


def _parse_id_from_path(path: Path) -> str:
    """
    Parse an image identifier from filesystem path.

    :param path: The path to the image
    :type path: Path

    :return: The image identifier
    :rtype: str
    """
    return os.path.splitext(path.name)[0]
