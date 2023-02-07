"""
Unit tests for filter document functionality (parse, evaluate.)
"""

import mlops.datalake as dl

# -----------------------------------------------------------------------------
# Special Operator Evaluation
# -----------------------------------------------------------------------------


def test_empty():
    filter = dl.FilterDocument.parse({})
    assert filter.evaluate({"name": "foo"})


# -----------------------------------------------------------------------------
# Binary Operator Evaluation
# -----------------------------------------------------------------------------


def test_eq0():
    filter = dl.FilterDocument.parse(
        {"$eq": [{"$field": "name"}, {"$lit": "foo"}]}
    )
    assert filter.evaluate({"name": "foo"})


def test_eq1():
    filter = dl.FilterDocument.parse(
        {"$eq": [{"$field": "name"}, {"$lit": "foo"}]}
    )
    assert not filter.evaluate({"name": "bar"})


def test_lt0():
    filter = dl.FilterDocument.parse({"$lt": [{"$field": "name"}, {"$lit": 3}]})
    assert filter.evaluate({"name": 1})


def test_lt1():
    filter = dl.FilterDocument.parse({"$lt": [{"$field": "name"}, {"$lit": 3}]})
    assert not filter.evaluate({"name": 5})


def test_lte0():
    filter = dl.FilterDocument.parse(
        {"$lte": [{"$field": "name"}, {"$lit": 3}]}
    )
    assert filter.evaluate({"name": 3})


def test_lte1():
    filter = dl.FilterDocument.parse(
        {"$lte": [{"$field": "name"}, {"$lit": 3}]}
    )
    assert not filter.evaluate({"name": 4})


def test_gt0():
    filter = dl.FilterDocument.parse({"$gt": [{"$field": "name"}, {"$lit": 3}]})
    assert filter.evaluate({"name": 4})


def test_gt1():
    filter = dl.FilterDocument.parse({"$gt": [{"$field": "name"}, {"$lit": 3}]})
    assert not filter.evaluate({"name": 3})


def test_gte0():
    filter = dl.FilterDocument.parse(
        {"$gte": [{"$field": "name"}, {"$lit": 3}]}
    )
    assert filter.evaluate({"name": 3})


def test_gte1():
    filter = dl.FilterDocument.parse(
        {"$gte": [{"$field": "name"}, {"$lit": 3}]}
    )
    assert not filter.evaluate({"name": 2})


def test_contains0():
    filter = dl.FilterDocument.parse(
        {"$contains": [{"$field": "name"}, {"$lit": 3}]}
    )
    assert filter.evaluate({"name": [1, 2, 3]})


def test_contains1():
    filter = dl.FilterDocument.parse(
        {"$contains": [{"$field": "name"}, {"$lit": 3}]}
    )
    assert not filter.evaluate({"name": [1, 2]})


# -----------------------------------------------------------------------------
# Logical Operator Evaluation
# -----------------------------------------------------------------------------


def test_and0():
    filter = dl.FilterDocument.parse(
        {
            "$and": [
                {"$lt": [{"$field": "id"}, {"$lit": 3}]},
                {"$lte": [{"$field": "id"}, {"$lit": 3}]},
            ]
        }
    )
    assert filter.evaluate({"id": 1})


def test_and1():
    filter = dl.FilterDocument.parse(
        {
            "$and": [
                {"$lt": [{"$field": "id"}, {"$lit": 3}]},
                {"$gt": [{"$field": "id"}, {"$lit": 3}]},
            ]
        }
    )
    assert not filter.evaluate({"id": 2})


def test_or0():
    filter = dl.FilterDocument.parse(
        {
            "$or": [
                {"$lt": [{"$field": "id"}, {"$lit": 3}]},
                {"$gt": [{"$field": "id"}, {"$lit": 3}]},
            ]
        }
    )
    assert filter.evaluate({"id": 1})


def test_or1():
    filter = dl.FilterDocument.parse(
        {
            "$or": [
                {"$eq": [{"$field": "id"}, {"$lit": 3}]},
                {"$gt": [{"$field": "id"}, {"$lit": 3}]},
            ]
        }
    )
    assert not filter.evaluate({"id": 2})
