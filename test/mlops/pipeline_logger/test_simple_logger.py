"""
Test simple logger without Loki handler in logging.
"""

import mlops.pipeline_logger as pl


def test_simple_logger():
    # test logging capabilities without verbose
    test_logger1 = pl.PipelineLogger("test_suite")
    test_logger1.info("This info line should not show for logger1")
    test_logger1.error("This error line should show for logger1", ["test", "logger1"])
    # test logging capabilites with verbose
    test_logger2 = pl.PipelineLogger("test_suite", True)
    test_logger2.info("This info line should show for logger2")
    assert True
