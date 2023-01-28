"""
Test everything.
"""

import mlops.datalake as dl


def test_all(tmp_path):
    dl.set_path(f"{tmp_path}/dl")
    _ = dl.ObjectDetectionDataset("id")
    assert True
