"""
Unit tests for backend HTTP interface.
"""

from copy import deepcopy
from typing import Any, Dict, List, Tuple

import pytest


@pytest.fixture()
def server(request):
    """A fixture to perform setup and teardown."""
    d: TestDefinition = request.param
    try:
        d.setup()
        yield d
    finally:
        d.teardown()


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_init(server):
    """Ensure that server can initialize."""
    d: TestDefinition = server
    d.start()

    res = get("/healthcheck")
    assert res.status_code == 200


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_write(server):
    """Ensure that write can be performed successfully."""
    d: TestDefinition = server
    d.start()

    res = post(
        "/result/m0/v0",
        json=result_from("r0", "", [(0, {"hello": "world"})]).to_json(),
    )
    assert res.status_code == 200


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_read(server):
    """Ensure that a read on empty store gives 404."""
    d: TestDefinition = server
    d.start()

    # Read many results
    res = get("/result/m0/v0")
    assert res.status_code == 404

    # Read individual result
    res = get("/result/m0/v0/r0")
    assert res.status_code == 404

    # Read single version
    res = get("/result/m0/v0/r0/0")
    assert res.status_code == 404


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_delete(server):
    """Ensure that delete on empty store gives 404."""
    d: TestDefinition = server
    d.start()

    res = delete("/result/m0/v0")
    assert res.status_code == 404

    res = delete("/result/m0/v0/r0")
    assert res.status_code == 404

    res = delete("/result/m0/v0/r0/0")
    assert res.status_code == 404


@pytest.mark.parametrize("server", deepcopy(DEFINITIONS), indirect=["server"])
def test_write_read_delete(server):
    """Ensure that a written result can be read and deleted."""
    d: TestDefinition = server
    d.start()

    r = result_from("r0", "", [(0, {"hello": "world"})])

    res = post("/result/m0/v0", json=r.to_json())
    assert res.status_code == 200

    res = get("/result/m0/v0/r0")
    assert res.status_code == 200
    assert "results" in res.json()

    results = res.json()["results"]
    assert len(results) == 1

    result = Result.from_json(results[0])
    assert equal(r, result)

    res = delete("/result/m0/v0/r0")
    assert res.status_code == 200

    res = get("/result/m0/v0/r0")
    assert res.status_code == 404

