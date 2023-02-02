"""
Generic query filter document definition.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Set

from ..exception import EvaluationError, ParseError

# -----------------------------------------------------------------------------
# FilterExpression
# -----------------------------------------------------------------------------


class ExpressionType(Enum):
    """Enumerates types of FilterExpression."""

    EMPTY_EXPRESSION = "EMPTY"
    """The empty expression."""

    LITERAL = "LITERAL"
    """A literal value."""

    FIELD = "FIELD"
    """Extract a literal field value."""

    BINARY_EQ = "BINARY_EQ"
    """The binary == operator."""

    BINARY_LT = "BINARY_LT"
    """The binary < operator."""

    BINARY_LTE = "BINARY_LTE"
    """The binary <= operator."""

    BINARY_GT = "BINARY_GT"
    """The binary > operator."""

    BINARY_GTE = "BINARY_GTE"
    """The binary >= operator."""

    BINARY_CONTAINS = "BINARY_CONTAINS"
    """The binary 'contains' operator."""

    LOGICAL_AND = "LOGICAL_AND"
    """The logical 'and' operator."""

    LOGICAL_OR = "LOGICAL_OR"
    """The logical 'or' operator."""


class FilterExpression(ABC):
    """FilterExpression is the workhorse of query filtering."""

    def __init__(self, *, type: ExpressionType):
        self.type = type
        """The type of the expression."""

    @abstractmethod
    def evaluate(self, _: Dict[str, Any]) -> bool:
        raise RuntimeError("Not implemented.")


class EmptyExpression(FilterExpression):
    """Represents an empty expression."""

    def __init__(self):
        super().__init__(type=ExpressionType.EMPTY_EXPRESSION)

    def evaluate(self, _: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        return True


class LiteralExpression(FilterExpression):
    """Represents a literal value."""

    def __init__(self, *, value: Any):
        super().__init__(type=ExpressionType.LITERAL)

        self.value = value
        """The literal value."""

    def evaluate(self, _: Dict[str, Any]) -> Any:
        """Evaluate the expression."""
        return self.value


class FieldExpression(FilterExpression):
    """Extracts a particular field's value from input document."""

    def __init__(self, *, field_name: str):
        super().__init__(type=ExpressionType.FIELD)

        self.field_name = field_name
        """The target field name."""

    def evaluate(self, document: Dict[str, Any]) -> Any:
        """Evaluate the expression."""
        if self.field_name not in document:
            raise EvaluationError(
                f"Requested field '{self.field_name}' not found."
            )

        value = document[self.field_name]
        if not isinstance(value, dict):
            return value

        # Account for special serialization scenarios
        if self.field_name in value:
            return value[self.field_name]
        else:
            # TODO(Kyle): How to handle compound values?
            return value


class BinaryEqExpression(FilterExpression):
    """Compares two values for equality."""

    def __init__(self, *, lhs: FilterExpression, rhs: FilterExpression):
        super().__init__(type=ExpressionType.BINARY_EQ)

        self.lhs = lhs
        """The left-hand operand."""

        self.rhs = rhs
        """The right-hand operand."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        lhs = self.lhs.evaluate(document)
        rhs = self.rhs.evaluate(document)
        if not any(isinstance(lhs, t) for t in [int, float, str]):
            raise EvaluationError(
                "$eq only supported for integer, real, and string types."
            )
        if not any(isinstance(rhs, t) for t in [int, float, str]):
            raise EvaluationError(
                "$eq only supported for integer, real, and string types."
            )
        return lhs == rhs


class BinaryLtExpression(FilterExpression):
    """Compares two values for <."""

    def __init__(self, *, lhs: FilterExpression, rhs: FilterExpression):
        super().__init__(type=ExpressionType.BINARY_LT)

        self.lhs = lhs
        """The left-hand operand."""

        self.rhs = rhs
        """The right hand operand."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        lhs = self.lhs.evaluate(document)
        rhs = self.rhs.evaluate(document)
        if not any(isinstance(lhs, t) for t in [int, float]):
            raise EvaluationError(
                "$lt only supported for integer and real types."
            )
        if not any(isinstance(rhs, t) for t in [int, float]):
            raise EvaluationError(
                "$lt only supported for integer and real types."
            )
        return lhs < rhs


class BinaryLteExpression(FilterExpression):
    """Compares two values for <=."""

    def __init__(self, *, lhs: FilterExpression, rhs: FilterExpression):
        super().__init__(type=ExpressionType.BINARY_LTE)

        self.lhs = lhs
        """The left-hand operand."""

        self.rhs = rhs
        """The right hand operand."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        lhs = self.lhs.evaluate(document)
        rhs = self.rhs.evaluate(document)
        if not any(isinstance(lhs, t) for t in [int, float]):
            raise EvaluationError(
                "$lte only supported for integer and real types."
            )
        if not any(isinstance(rhs, t) for t in [int, float]):
            raise EvaluationError(
                "$lte only supported for integer and real types."
            )
        return lhs <= rhs


class BinaryGtExpression(FilterExpression):
    """Compares two values for >."""

    def __init__(self, *, lhs: FilterExpression, rhs: FilterExpression):
        super().__init__(type=ExpressionType.BINARY_GT)

        self.lhs = lhs
        """The left-hand operand."""

        self.rhs = rhs
        """The right hand operand."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        lhs = self.lhs.evaluate(document)
        rhs = self.rhs.evaluate(document)
        if not any(isinstance(lhs, t) for t in [int, float]):
            raise EvaluationError(
                "$gt only supported for integer and real types."
            )
        if not any(isinstance(rhs, t) for t in [int, float]):
            raise EvaluationError(
                "$gt only supported for integer and real types."
            )
        return lhs > rhs


class BinaryGteExpression(FilterExpression):
    """Compares two values for >=."""

    def __init__(self, *, lhs: FilterExpression, rhs: FilterExpression):
        super().__init__(type=ExpressionType.BINARY_GTE)

        self.lhs = lhs
        """The left-hand operand."""

        self.rhs = rhs
        """The right hand operand."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        lhs = self.lhs.evaluate(document)
        rhs = self.rhs.evaluate(document)
        if not any(isinstance(lhs, t) for t in [int, float]):
            raise EvaluationError(
                "$gte only supported for integer and real types."
            )
        if not any(isinstance(rhs, t) for t in [int, float]):
            raise EvaluationError(
                "$gte only supported for integer and real types."
            )
        return lhs >= rhs


class BinaryContainsExpression(FilterExpression):
    """Determines if an array field contains a specified value."""

    def __init__(self, *, lhs: FilterExpression, rhs: FilterExpression):
        super().__init__(type=ExpressionType.BINARY_CONTAINS)

        self.lhs = lhs
        """The left-hand operand."""

        self.rhs = rhs
        """The right-hand operand."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        lhs = self.lhs.evaluate(document)
        rhs = self.rhs.evaluate(document)
        if not any(isinstance(lhs, t) for t in [list, set]):
            raise EvaluationError(
                "$contains only supported with list or set as left-hand operand."
            )
        if not any(isinstance(rhs, t) for t in [int, float, str]):
            raise EvaluationError(
                "$contains only supported with integer, real, or string as right-hand operand."
            )
        return rhs in lhs


class LogicalAndExpression(FilterExpression):
    """Computes logical 'and'."""

    def __init__(self, *, expressions: List[FilterExpression]):
        super().__init__(type=ExpressionType.LOGICAL_AND)

        self.expressions = expressions
        """The collection of expressions."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        evaluated = [expr.evaluate(document) for expr in self.expressions]
        if not all(isinstance(e, bool) for e in evaluated):
            raise EvaluationError(
                "$and only supported between instances of 'bool'."
            )
        return all(evaluated)


class LogicalOrExpression(FilterExpression):
    """Computes logical 'or'."""

    def __init__(self, *, expressions: List[FilterExpression]):
        super().__init__(type=ExpressionType.LOGICAL_OR)

        self.expressions = expressions
        """The collection of expressions."""

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """Evaluate the expression."""
        evaluated = [expr.evaluate(document) for expr in self.expressions]
        if not all(isinstance(e, bool) for e in evaluated):
            raise EvaluationError(
                "$or only supported between instances of 'bool'."
            )
        return any(evaluated)


# -----------------------------------------------------------------------------
# FilterDocument
# -----------------------------------------------------------------------------


class FilterDocument:
    """FilterDocument is the top-level query filtering construct."""

    def __init__(self, *, expression: FilterExpression):
        self.expression = expression
        """The root expression for the document."""

    @staticmethod
    def parse(document: Dict[str, Any]) -> FilterDocument:
        """
        Parse a FilterDocument from a raw document.

        :param document: The input document
        :type document: Dict[str, Any]

        :return: The parsed document
        :rtype: FilterDocument
        """
        return _parse_document(document)

    def evaluate(self, document: Dict[str, Any]) -> bool:
        """
        Determine if the input document satisfies the filter.

        :param document: The input document
        :type document: Dict[str, Any]

        :return: `True` if satisfied, `False` otherwise
        :rtype: bool
        """
        return self.expression.evaluate(document)


def _parse_document(document: Dict[str, Any]) -> FilterDocument:
    """
    Parse a FilterDocument from the provided document.

    :param document: The input document
    :type document: Dict[str, Any]

    :return: The parsed document
    :rtype: FilterDocument

    :raises: ParseError
    """
    root_expression = _parse_expression(document)
    if root_expression.type == ExpressionType.LITERAL:
        raise ParseError("Literal cannot be top-level expression.")
    if root_expression.type == ExpressionType.FIELD:
        raise ParseError("Field cannot be top-level expression.")
    return FilterDocument(expression=root_expression)


def _parse_expression(document: Dict[str, Any]) -> FilterExpression:
    """
    Parse a FilterExpression from the provided document.

    :param document: The input document
    :type document: Dict[str, Any]

    :return: The parsed expression
    :rtype: FilterExpression

    :raises: ParseError
    """
    if len(document) == 0:
        return EmptyExpression()

    if len(document) > 1:
        raise ParseError("Malformed document.")

    key = next(iter(document))
    if key == "$lit":
        return _parse_lit_expr(key, document)
    elif key == "$field":
        return _parse_field_expr(key, document)
    elif key == "$eq":
        return _parse_binary_eq_expr(key, document)
    elif key == "$lt":
        return _parse_binary_lt_expr(key, document)
    elif key == "$lte":
        return _parse_binary_lte_expr(key, document)
    elif key == "$gt":
        return _parse_binary_gt_expr(key, document)
    elif key == "$gte":
        return _parse_binary_gte_expr(key, document)
    elif key == "$contains":
        return _parse_binary_contains_expr(key, document)
    elif key == "$and":
        return _parse_logical_and_expr(key, document)
    elif key == "$or":
        return _parse_logical_or_expr(key, document)
    else:
        raise RuntimeError(f"Unknown operator: {key}")


def _parse_lit_expr(key: str, document: Dict[str, Any]) -> LiteralExpression:
    """Parse a literal expression."""
    assert key == "$lit", "Broken precondition."
    if not isinstance(document, dict):
        raise RuntimeError("Parse error at '$lit'.")
    if not len(document) == 1:
        raise RuntimeError("Parse error at '$lit'.")
    return LiteralExpression(value=document[key])


def _parse_field_expr(key: str, document: Dict[str, Any]) -> FieldExpression:
    assert key == "$field", "Broken invariant."
    return FieldExpression(field_name=document[key])


def _parse_binary_eq_expr(
    key: str, document: Dict[str, Any]
) -> BinaryEqExpression:
    """Parse a binary equality expression."""
    assert key == "$eq", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$eq'.")
    value: Set = value
    if not len(value) == 2:
        raise ParseError("Parse error at '$eq'.")

    return BinaryEqExpression(
        lhs=_parse_expression(value[0]), rhs=_parse_expression(value[1])
    )


def _parse_binary_lt_expr(
    key: str, document: Dict[str, Any]
) -> BinaryLtExpression:
    """Parse a binary less-than expression."""
    assert key == "$lt", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$lt'.")
    value: Set = value
    if not len(value) == 2:
        raise ParseError("Parse error at '$lt'.")

    return BinaryLtExpression(
        lhs=_parse_expression(value[0]), rhs=_parse_expression(value[1])
    )


def _parse_binary_lte_expr(
    key: str, document: Dict[str, Any]
) -> BinaryLteExpression:
    """Parse a binary less-than-or-equal expression."""
    assert key == "$lte", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$lte'.")
    value: Set = value
    if not len(value) == 2:
        raise ParseError("Parse error at '$lte'.")

    return BinaryLteExpression(
        lhs=_parse_expression(value[0]), rhs=_parse_expression(value[1])
    )


def _parse_binary_gt_expr(
    key: str, document: Dict[str, Any]
) -> BinaryGtExpression:
    """Parse a binary greater-than expression."""
    assert key == "$gt", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$gt'.")
    value: Set = value
    if not len(value) == 2:
        raise ParseError("Parse error at '$gt'.")

    return BinaryGtExpression(
        lhs=_parse_expression(value[0]), rhs=_parse_expression(value[1])
    )


def _parse_binary_gte_expr(
    key: str, document: Dict[str, Any]
) -> BinaryGteExpression:
    """Parse a binary greater-than-or-equal expression."""
    assert key == "$gte", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$gte'.")
    value: Set = value
    if not len(value) == 2:
        raise ParseError("Parse error at '$gte'.")

    return BinaryGteExpression(
        lhs=_parse_expression(value[0]), rhs=_parse_expression(value[1])
    )


def _parse_binary_contains_expr(
    key: str, document: Dict[str, Any]
) -> BinaryContainsExpression:
    """Parse a binary contains expression."""
    assert key == "$contains", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$contains'.")
    if not len(value) == 2:
        raise ParseError("Parse error at '$contains'.")
    return BinaryContainsExpression(
        lhs=_parse_expression(value[0]), rhs=_parse_expression(value[1])
    )


def _parse_logical_and_expr(
    key: str, document: Dict[str, Any]
) -> LogicalAndExpression:
    """Parse a logical 'and' expression."""
    assert key == "$and", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$and'.")
    return LogicalAndExpression(
        expressions=[_parse_expression(e) for e in value]
    )


def _parse_logical_or_expr(
    key: str, document: Dict[str, Any]
) -> LogicalOrExpression:
    """Parse a logical 'or' expression."""
    assert key == "$or", "Broken precondition."

    value = document[key]
    if not isinstance(value, list):
        raise ParseError("Parse error at '$or'.")
    return LogicalOrExpression(
        expressions=[_parse_expression(e) for e in value]
    )


# -----------------------------------------------------------------------------
# Parsing
# -----------------------------------------------------------------------------


def try_parse(document: Dict[str, Any]) -> bool:
    """
    Determine if a query can be parsed.

    :param document: The filter document
    :type document: Dict[str, Any]

    :return: `True` if the query parsed, `False` otherwise
    :rtype: bool
    """
    try:
        _ = FilterDocument.parse(document)
    except ParseError:
        return False
    return True
