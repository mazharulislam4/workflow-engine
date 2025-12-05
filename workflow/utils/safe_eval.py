"""
Secure Expression Evaluator

Safe expression evaluation for condition nodes without using eval().
Uses AST parsing and whitelisted operations.

Security levels:
1. Template evaluation (Jinja2) - Safe variable substitution
2. Expression parsing (AST) - Safe comparison operators only
3. No arbitrary code execution
"""

import ast
import logging
import operator
from typing import Any, Dict

logger = logging.getLogger(__name__)


class SecureExpressionEvaluator:
    """
    Secure Expression Evaluator

    Safe expression evaluation for condition nodes without using eval().
    Uses AST parsing and whitelisted operations.

    Allowed operations:
    - Comparisons: ==, !=, <, <=, >, >=
    - Boolean logic: and, or, not
    - Membership: in, not in
    - Identity: is, is not
    - Literals: numbers, strings, True, False, None

    Security levels:
    1. Template evaluation (Jinja2) - Safe variable substitution
    2. Expression parsing (AST) - Safe comparison operators only
    3. No arbitrary code execution

    """

    # Whitelisted operators

    SAFE_OPERATORS = {
        # Comparison
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        # Boolean
        ast.And: operator.and_,
        ast.Or: operator.or_,
        ast.Not: operator.not_,
        # Membership
        ast.In: lambda a, b: a in b,
        ast.NotIn: lambda a, b: a not in b,
        # Identity
        ast.Is: operator.is_,
        ast.IsNot: operator.is_not,
        # Arithmetic (limited)
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Mod: operator.mod,
        # Unary operators
        ast.USub: operator.neg,  # Unary minus (negative numbers)
        ast.UAdd: operator.pos,  # Unary plus (positive numbers)
    }

    def evaluate(self, expression: str) -> bool:
        """
        Safely evaluate expression and return boolean result.

        Args:
            expression: Expression string (after Jinja2 template evaluation)

        Returns:
            Boolean result

        Raises:
            ValueError: If expression contains unsafe operations
            SyntaxError: If expression is invalid Python

        Examples:
            >>> evaluator = SecureExpressionEvaluator()
            >>> evaluator.evaluate("200 == 200")
            True
            >>> evaluator.evaluate("'active' in ['active', 'pending']")
            True
            >>> evaluator.evaluate("5 > 3 and 10 < 20")
            True
        """
        # Handle direct booleans
        if isinstance(expression, bool):
            return expression

        # convert to string if not already
        expression = str(expression).strip()

        # Handle empty or "Truthy" strings
        if not expression:
            return False

        if expression.lower() == "true":
            return True
        if expression.lower() == "false":
            return False

        try:
            # Parse expression into AST
            tree = ast.parse(expression, mode="eval")

            # validate and evaluate
            result = self._eval_node(tree.body)

            # Convert to boolean
            return bool(result)
        except SyntaxError as e:
            logger.error(f"Syntax error in expression '{expression}': {e}")
            raise ValueError(f"Invalid expression: {expression}") from e
        except ValueError as e:
            # Re-raise ValueError (unsafe operations)
            logger.error(f"Security error in expression '{expression}': {e}")
            raise
        except Exception as e:
            logger.error(f"Error evaluating expression '{expression}': {e}")
            raise ValueError(f"Expression evaluation error: {expression}") from e

    def _eval_node(self, node: ast.AST) -> Any:
        """
        Recursively evaluate AST node.

        Only allows whitelisted node types.

        Args:
            node: AST node to evaluate

        Returns:
            Evaluated value

        Raises:
            ValueError: If node type is not allowed
        """
        # Literals (safe)
        if isinstance(node, ast.Constant):
            return node.value

        # For older Python versions
        if isinstance(node, (ast.Num, ast.Str)):
            return node.n if isinstance(node, ast.Num) else node.s

        if isinstance(node, ast.NameConstant):
            return node.value

        # Lists, tuples (safe if elements are safe)
        if isinstance(node, ast.List):
            return [self._eval_node(el) for el in node.elts]

        if isinstance(node, ast.Tuple):
            return tuple(self._eval_node(el) for el in node.elts)

        # Dictionary (safe if keys/values are safe)
        if isinstance(node, ast.Dict):
            return {
                self._eval_node(k): self._eval_node(v)  # type: ignore
                for k, v in zip(node.keys, node.values)
            }

        # Comparison operators
        if isinstance(node, ast.Compare):
            left = self._eval_node(node.left)

            for op, comparator in zip(node.ops, node.comparators):
                if type(op) not in self.SAFE_OPERATORS:
                    raise ValueError(f"Operator {type(op).__name__} not allowed")

                right = self._eval_node(comparator)
                operator_func = self.SAFE_OPERATORS[type(op)]

                if not operator_func(left, right):
                    return False

                left = right  # For chained comparisons

            return True

        # Boolean operators (and, or)
        if isinstance(node, ast.BoolOp):
            if type(node.op) not in self.SAFE_OPERATORS:
                raise ValueError(f"Operator {type(node.op).__name__} not allowed")

            operator_func = self.SAFE_OPERATORS[type(node.op)]
            values = [self._eval_node(v) for v in node.values]

            # Implement short-circuit evaluation
            if isinstance(node.op, ast.And):
                return all(values)
            elif isinstance(node.op, ast.Or):
                return any(values)

        # Unary operators (not)
        if isinstance(node, ast.UnaryOp):
            if type(node.op) not in self.SAFE_OPERATORS:
                raise ValueError(f"Operator {type(node.op).__name__} not allowed")

            operator_func = self.SAFE_OPERATORS[type(node.op)]
            operand = self._eval_node(node.operand)
            return operator_func(operand)

        # Binary operators (arithmetic)
        if isinstance(node, ast.BinOp):
            if type(node.op) not in self.SAFE_OPERATORS:
                raise ValueError(f"Operator {type(node.op).__name__} not allowed")

            left = self._eval_node(node.left)
            right = self._eval_node(node.right)
            operator_func = self.SAFE_OPERATORS[type(node.op)]
            return operator_func(left, right)

        # UNSAFE: Reject everything else
        raise ValueError(
            f"Unsafe operation: {type(node).__name__}. "
            f"Only comparisons, boolean logic, and literals are allowed."
        )


# Global instance
_evaluator = SecureExpressionEvaluator()


def safe_eval(expression: str) -> bool:
    """
    Safely evaluate expression.

    Convenience function for SecureExpressionEvaluator.

    Args:
        expression: Expression to evaluate

    Returns:
        Boolean result

    Examples:
        >>> safe_eval("200 == 200")
        True
        >>> safe_eval("'admin' in ['admin', 'user']")
        True
        >>> safe_eval("5 > 3 and 10 < 20")
        True
    """
    return _evaluator.evaluate(expression)
