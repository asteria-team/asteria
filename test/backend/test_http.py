"""
Unit tests for backend HTTP interface.
"""

from copy import deepcopy

import pytest

from .support.datalake import create_datalake, create_tmpdir, delete_tmpdir
from .support.http import TestDefinition, get

DEFINITIONS = [
    TestDefinition(
        "http",
        ["--datalake", "artifact:datalake"],
        {},
        [create_tmpdir, create_datalake],
        [delete_tmpdir],
    )
]

# -----------------------------------------------------------------------------
# Test Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture()
def server(request):
    """A fixture to perform setup and teardown."""
    d: TestDefinition = request.param
    try:
        d.setup()
        yield d
    finally:
        d.teardown()


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_init(server):
    """Ensure that server can initialize."""
    d: TestDefinition = server
    d.start()

    res = get("/api/healthcheck")
    assert res.status_code == 200


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_get_dataset_domains(server):
    """Perform a request for dataset domains metadata."""
    d: TestDefinition = server
    d.start()

    res = get("/api/datalake/metadata/dataset/domain")
    assert res.status_code == 200

    body = res.json()
    assert "domains" in body
    assert len(body["domains"]) > 0


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_get_dataset_types(server):
    """Perform a request for dataset types metadata."""
    d: TestDefinition = server
    d.start()

    res = get("/api/datalake/metadata/dataset/type")
    assert res.status_code == 200

    body = res.json()
    assert "types" in body
    assert len(body["types"]) > 0
