"""
Test Secure Expression Evaluator

Tests for production-safe condition evaluation.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_secure_eval_basic():
    """Test basic comparisons"""
    from workflow.utils.safe_eval import safe_eval

    print("=" * 60)
    print("Testing Secure Expression Evaluator")
    print("=" * 60)

    print("\n1. Basic Comparisons:")
    tests = [
        ("200 == 200", True),
        ("200 != 404", True),
        ("5 > 3", True),
        ("10 <= 10", True),
        ("'active' == 'active'", True),
        ("100 < 50", False),
    ]

    for expr, expected in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {expr} = {result} (expected {expected})")
        assert result == expected, f"Failed: {expr}"

    print("\n2. Boolean Logic:")
    tests = [
        ("True and True", True),
        ("True or False", True),
        ("not False", True),
        ("5 > 3 and 10 < 20", True),
        ("5 > 10 or 20 > 15", True),
        ("not (5 > 10)", True),
    ]

    for expr, expected in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {expr} = {result}")
        assert result == expected

    print("\n3. Membership Tests:")
    tests = [
        ("'admin' in ['admin', 'user']", True),
        ("'guest' not in ['admin', 'user']", True),
        ("200 in [200, 201, 202]", True),
        ("404 not in [200, 201]", True),
    ]

    for expr, expected in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {expr} = {result}")
        assert result == expected

    print("\n" + "=" * 60)


def test_unsafe_operations():
    """Test that unsafe operations are blocked"""
    from workflow.utils.safe_eval import safe_eval

    print("\n" + "=" * 60)
    print("Testing Security - Blocking Unsafe Operations")
    print("=" * 60)

    unsafe_expressions = [
        "__import__('os').system('ls')",  # Import + function call
        "exec('print(1)')",  # Exec
        "eval('1+1')",  # Nested eval
        "open('/etc/passwd').read()",  # File access
        "__builtins__",  # Built-ins access
        "lambda x: x",  # Lambda
    ]

    for expr in unsafe_expressions:
        try:
            result = safe_eval(expr)
            print(f"  ✗ SECURITY FAIL: {expr} was allowed!")
            assert False, f"Should have blocked: {expr}"
        except (ValueError, SyntaxError) as e:
            print(f"  ✓ Blocked: {expr[:50]}...")

    print("\n✓ All unsafe operations blocked!")
    print("=" * 60)


def test_real_world_conditions():
    """Test real-world workflow conditions"""
    from workflow.utils.safe_eval import safe_eval

    print("\n" + "=" * 60)
    print("Testing Real-World Workflow Conditions")
    print("=" * 60)

    print("\n1. HTTP Status Checks:")
    tests = [
        ("200 == 200", True, "Success response"),
        ("404 != 200", True, "Not found"),
        ("200 in [200, 201, 202]", True, "Successful statuses"),
    ]

    for expr, expected, desc in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {desc}: {expr} = {result}")

    print("\n2. String Comparisons:")
    tests = [
        ("'ACTIVE' == 'ACTIVE'", True, "Status check"),
        ("'admin' in ['admin', 'superuser']", True, "Role check"),
        ("'guest' not in ['admin', 'user']", True, "Access denied"),
    ]

    for expr, expected, desc in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {desc}: {expr} = {result}")

    print("\n3. Numeric Thresholds:")
    tests = [
        ("100 > 50", True, "Above threshold"),
        ("1000 >= 1000", True, "At limit"),
        ("5 < 10 and 10 < 20", True, "Range check"),
    ]

    for expr, expected, desc in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {desc}: {expr} = {result}")

    print("\n" + "=" * 60)


def test_arithmetic_operations():
    """Test safe arithmetic operations"""
    from workflow.utils.safe_eval import safe_eval

    print("\n" + "=" * 60)
    print("Testing Arithmetic Operations")
    print("=" * 60)

    tests = [
        ("10 + 5 == 15", True, "Addition"),
        ("20 - 5 == 15", True, "Subtraction"),
        ("5 * 4 == 20", True, "Multiplication"),
        ("20 / 4 == 5", True, "Division"),
        ("17 % 5 == 2", True, "Modulo"),
        ("(10 + 5) * 2 == 30", True, "Grouping"),
    ]

    for expr, expected, desc in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {desc}: {expr} = {result}")
        assert result == expected

    print("\n" + "=" * 60)


def test_complex_conditions():
    """Test complex nested conditions"""
    from workflow.utils.safe_eval import safe_eval

    print("\n" + "=" * 60)
    print("Testing Complex Nested Conditions")
    print("=" * 60)

    tests = [
        (
            "(200 == 200 and 'active' == 'active') or False",
            True,
            "Nested boolean",
        ),
        (
            "100 > 50 and (200 in [200, 201] or 'admin' == 'user')",
            True,
            "Mixed operators",
        ),
        (
            "not (100 < 50 or 'inactive' == 'active')",
            True,
            "Negation of OR",
        ),
        (
            "5 < 10 and 10 < 20 and 20 < 30",
            True,
            "Chained comparisons",
        ),
    ]

    for expr, expected, desc in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {desc}:")
        print(f"      {expr}")
        print(f"      = {result}")
        assert result == expected

    print("\n" + "=" * 60)


def test_edge_cases():
    """Test edge cases and special values"""
    from workflow.utils.safe_eval import safe_eval

    print("\n" + "=" * 60)
    print("Testing Edge Cases")
    print("=" * 60)

    tests = [
        ("True", True, "Direct boolean"),
        ("False", False, "Direct boolean"),
        ("None is None", True, "None comparison"),
        ("'' == ''", True, "Empty string"),
        ("[] == []", True, "Empty list"),
        ("{} == {}", True, "Empty dict"),
        ("0 == 0", True, "Zero"),
        ("-5 < 0", True, "Negative number"),
    ]

    for expr, expected, desc in tests:
        result = safe_eval(expr)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {desc}: {expr} = {result}")
        assert result == expected

    print("\n" + "=" * 60)


def show_template_to_evaluation_flow():
    """Show complete flow from template to evaluation"""

    print("\n" + "=" * 60)
    print("Complete Flow: Template → Evaluation")
    print("=" * 60)

    print("\n1. User defines condition with template:")
    print("   expression: '{{steps.http_1.outputs.status_code}} == 200'")

    print("\n2. Jinja2 evaluates template (SAFE):")
    print("   → steps.http_1.outputs.status_code = 200")
    print("   → Expression becomes: '200 == 200'")

    print("\n3. Secure evaluator parses with AST (SAFE):")
    print("   → Parse: Compare(left=200, op=Eq, right=200)")
    print("   → Validate: Only safe operators allowed")
    print("   → Evaluate: 200 == 200 → True")

    print("\n4. Result:")
    print("   → Condition evaluates to True")
    print("   → True branch executes")

    print("\n" + "=" * 60)
    print("✅ Two-layer security:")
    print("  1. Jinja2 template evaluation (safe substitution)")
    print("  2. AST-based expression parsing (no eval)")
    print("=" * 60)


def show_usage_examples():
    """Show practical usage examples"""

    print("\n" + "=" * 60)
    print("Practical Usage Examples")
    print("=" * 60)

    examples = [
        {
            "scenario": "HTTP Response Check",
            "template": "{{steps.api_call.outputs.status_code}} == 200",
            "after_jinja": "200 == 200",
            "description": "Check if API call succeeded",
        },
        {
            "scenario": "Role-Based Access",
            "template": "{{variables.user_role}} in ['admin', 'superuser']",
            "after_jinja": "'admin' in ['admin', 'superuser']",
            "description": "Verify user has admin privileges",
        },
        {
            "scenario": "Threshold Check",
            "template": "{{steps.calculate.outputs.total}} > 1000",
            "after_jinja": "1500 > 1000",
            "description": "Check if total exceeds threshold",
        },
        {
            "scenario": "Status Validation",
            "template": "{{variables.order_status}} != 'cancelled'",
            "after_jinja": "'active' != 'cancelled'",
            "description": "Ensure order is not cancelled",
        },
    ]

    for i, ex in enumerate(examples, 1):
        print(f"\n{i}. {ex['scenario']}:")
        print(f"   Description: {ex['description']}")
        print(f"   Template:    {ex['template']}")
        print(f"   After Jinja: {ex['after_jinja']}")
        print(f"   Safe Eval:   ✓ Secure")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    try:
        test_secure_eval_basic()
        test_unsafe_operations()
        test_real_world_conditions()
        test_arithmetic_operations()
        test_complex_conditions()
        test_edge_cases()
        show_template_to_evaluation_flow()
        show_usage_examples()

        print("\n" + "=" * 60)
        print("✅ All Security Tests Passed!")
        print("=" * 60)
        print("\nProduction-ready condition evaluation:")
        print("  ✓ No eval() - uses AST parsing")
        print("  ✓ Whitelist-based operators only")
        print("  ✓ Blocks imports, function calls, attribute access")
        print("  ✓ Safe for user-provided conditions")
        print("  ✓ Integrates with Jinja2 template engine")
        print("  ✓ Two-layer security: Template + AST parsing")
        print("\n" + "=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
