"""
Test everything in orchestration without an
orchestrator to ensure failures for user
do not occur.
"""

import mlops.orchestration as orc


def test_all():
    _ = orc.Producer()
    _ = orc.Consumer()
    assert True
