"""
Simple test script.
"""

import os
import sys
from pathlib import Path

from resolver import package_root

sys.path.append(package_root())

import mlops.datalake as dl

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

IMAGE_FILE = "cat.jpeg"


def main() -> int:
    datalake_path = Path(os.getcwd()) / "testbed" / "dl"

    image_path = Path(os.getcwd()) / "testbed" / IMAGE_FILE
    assert image_path.exists()

    dl.set_path(datalake_path)

    d = dl.ObjectDetectionDataset("dataset0")
    d.add_image(dl.Image(path=image_path))
    d.add_annotation(dl.ObjectDetectionAnnotation(identifier="cat", objects=[]))
    d.save()

    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
