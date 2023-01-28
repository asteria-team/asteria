"""
Test everything in orchestration.
"""

import mlops.orchestration as orc


def test_all():
    _ = orc.Producer()
    _ = orc.Consumer()
    assert True
