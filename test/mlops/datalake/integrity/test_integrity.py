"""
Tests for global datalake integrity.
"""

import mlops.datalake as dl


def test_empty(tmp_path):
    dl.set_path(f"{tmp_path}/dl")
    dl.integrity.verify()
