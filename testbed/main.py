"""
Simple test script.
"""

import sys

from resolver import package_root

sys.path.append(package_root())

import mlops.datalake as dl

EXIT_SUCCESS = 0
EXIT_FAILURE = 1


def main() -> int:
    d = dl.ObjectDetectionDataset("id")
    d.add_image(dl.Image("hello"))
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
