"""
Image class definition.
"""

import os
import shutil
from pathlib import Path
from typing import Tuple, Union

import PIL


class Image:
    """Image represents an image on disk."""

    def __init__(self, *, path: Union[Path, str], persisted: bool = False):
        self.path = path if isinstance(path, Path) else Path(path)
        """The path to the image on disk."""

        self._persisted = persisted
        """An indicator for whether the image is already persisted in data lake."""

    @property
    def id(self) -> str:
        """Return the unique image identifier."""
        return _parse_id_from_path(self.path)

    @property
    def extension(self) -> str:
        """Return the image file extension."""
        return _parse_extension_from_path(self.path)

    @property
    def size(self) -> Tuple[int, int]:
        """Return the size of the image (width, height), in pixels."""
        image = PIL.Image(self.path)
        return image.size

    def to_file(self, path: Path):
        """Save image to a new location (`path`)."""
        shutil.copyfile(self.path, path)
        self._persisted = True

    @staticmethod
    def from_file(path: Path):
        """Load image from a location (`path`)."""
        return Image(path=path, persisted=True)

    def __key(self) -> str:
        return self.id

    def __hash__(self) -> int:
        return hash(self.__key())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Image):
            return False
        return self.__key() == other.__key()

    def __neq__(self, other: object) -> bool:
        return not self.__eq__(other)


def _parse_id_from_path(path: Path) -> str:
    """
    Parse an image identifier from filesystem path.

    :param path: The path to the image
    :type path: Path

    :return: The image identifier
    :rtype: str
    """
    return os.path.splitext(path.name)[0]


def _parse_extension_from_path(path: Path) -> str:
    """
    Parse an image extension from filesystem path.

    :param path: The path to the image
    :type path: Path

    :return: The image extension
    :rtype: str
    """
    return os.path.splitext(path.name)[1]
