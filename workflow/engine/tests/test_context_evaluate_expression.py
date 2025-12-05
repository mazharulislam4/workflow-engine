"""
Unit tests for ContextManager.evaluate_expression
"""

import sys
import unittest
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from workflow.engine.context import ContextManager


class TestContextManagerEvaluateExpression(unittest.TestCase):
    """Test cases for ContextManager.evaluate_expression method"""

    def setUp(self):
        """Set up test fixtures"""
        self.context_manager = ContextManager()

        # Set up test data
        self.context_manager.set_inputs({"user_id": "123", "email": "test@example.com"})
        self.context_manager.set_variable("api_url", "https://api.example.com")
        self.context_manager.set_variable("token", "secret_token")

        # Set up step data
        self.context_manager.set_step(
            "user_fetch",
            inputs={"user_id": "123"},
            outputs={"name": "John Doe", "age": 30, "role": "admin"},
            options={},
        )

        self.context_manager.set_step(
            "order_fetch",
            inputs={"order_id": "ORD-456"},
            outputs={"total": "199.99", "status": "completed", "items": [1, 2, 3]},
            options={},
        )

    def test_evaluate_simple_string(self):
        """Test evaluating a simple string template"""
        expression = "Hello, {{ steps.user_fetch.outputs.name }}!"
        result = self.context_manager.evaluate_expression(expression)
        self.assertEqual(result, "Hello, John Doe!")

    def test_evaluate_multiple_variables(self):
        """Test evaluating template with multiple variables"""
        expression = "User {{ steps.user_fetch.outputs.name }} has {{ steps.order_fetch.outputs['items'] | length }} items"
        result = self.context_manager.evaluate_expression(expression)
        self.assertEqual(result, "User John Doe has 3 items")

    def test_evaluate_dict_structure(self):
        """Test evaluating a dictionary with template values"""
        expression = {
            "url": "{{ variables.api_url }}/users/{{ inputs.user_id }}",
            "method": "GET",
            "headers": {
                "Authorization": "Bearer {{ variables.token }}",
                "Content-Type": "application/json",
            },
        }

        result = self.context_manager.evaluate_expression(expression)

        self.assertEqual(result["url"], "https://api.example.com/users/123")
        self.assertEqual(result["method"], "GET")
        self.assertEqual(result["headers"]["Authorization"], "Bearer secret_token")
        self.assertEqual(result["headers"]["Content-Type"], "application/json")

    def test_evaluate_list_structure(self):
        """Test evaluating a list with template values"""
        expression = [
            "{{ steps.user_fetch.outputs.name }}",
            "{{ steps.user_fetch.outputs.role }}",
            "{{ inputs.email }}",
        ]

        result = self.context_manager.evaluate_expression(expression)

        self.assertEqual(result[0], "John Doe")
        self.assertEqual(result[1], "admin")
        self.assertEqual(result[2], "test@example.com")

    def test_evaluate_nested_structure(self):
        """Test evaluating nested data structures"""
        expression = {
            "user": {
                "name": "{{ steps.user_fetch.outputs.name }}",
                "age": "{{ steps.user_fetch.outputs.age }}",
                "contact": {"email": "{{ inputs.email | to_upper }}"},
            },
            "orders": [
                "{{ steps.order_fetch.outputs.status }}",
                "{{ steps.order_fetch.outputs.total }}",
            ],
        }

        result = self.context_manager.evaluate_expression(expression)

        self.assertEqual(result["user"]["name"], "John Doe")
        self.assertEqual(result["user"]["age"], "30")
        self.assertEqual(result["user"]["contact"]["email"], "TEST@EXAMPLE.COM")
        self.assertEqual(result["orders"][0], "completed")
        self.assertEqual(result["orders"][1], "199.99")

    def test_evaluate_non_template_values(self):
        """Test that non-template values are returned as-is"""
        # Integer
        result = self.context_manager.evaluate_expression(123)
        self.assertEqual(result, 123)

        # Boolean
        result = self.context_manager.evaluate_expression(True)
        self.assertTrue(result)

        # None
        result = self.context_manager.evaluate_expression(None)
        self.assertIsNone(result)

        # Float
        result = self.context_manager.evaluate_expression(45.67)
        self.assertEqual(result, 45.67)

    def test_evaluate_with_filters(self):
        """Test evaluating templates with Jinja2 filters"""
        expression = {
            "name_upper": "{{ steps.user_fetch.outputs.name | to_upper }}",
            "name_lower": "{{ steps.user_fetch.outputs.name | to_lower }}",
            "total_float": "{{ steps.order_fetch.outputs.total | float }}",
            "age_int": "{{ steps.user_fetch.outputs.age | int }}",
        }

        result = self.context_manager.evaluate_expression(expression)

        self.assertEqual(result["name_upper"], "JOHN DOE")
        self.assertEqual(result["name_lower"], "john doe")
        self.assertEqual(result["total_float"], "199.99")
        self.assertEqual(result["age_int"], "30")

    def test_evaluate_with_conditionals(self):
        """Test evaluating templates with conditional logic"""
        expression = "{% if steps.user_fetch.outputs.role == 'admin' %}Administrator{% else %}User{% endif %}"
        result = self.context_manager.evaluate_expression(expression)
        self.assertEqual(result, "Administrator")

    def test_evaluate_with_loops(self):
        """Test evaluating templates with loops"""
        expression = "{% for item in steps.order_fetch.outputs['items'] %}{{ item }},{% endfor %}"
        result = self.context_manager.evaluate_expression(expression)
        self.assertEqual(result, "1,2,3,")

    def test_evaluate_undefined_variable_raises_error(self):
        """Test that undefined variables raise clear errors"""
        expression = "{{ steps.missing_step.outputs.data }}"

        with self.assertRaises(ValueError) as cm:
            self.context_manager.evaluate_expression(expression)

        self.assertIn("Template rendering error", str(cm.exception))

    def test_evaluate_with_default_filter(self):
        """Test using default_if_empty filter"""
        expression = "{{ steps.missing_step.outputs.value | default_if_empty('N/A') }}"

        # This should raise an error because missing_step doesn't exist
        # But if we test with an empty value:
        self.context_manager.set_step(
            "empty_step", inputs={}, outputs={"value": ""}, options={}
        )

        expression2 = "{{ steps.empty_step.outputs.value | default_if_empty('N/A') }}"
        result = self.context_manager.evaluate_expression(expression2)
        self.assertEqual(result, "N/A")

    def test_evaluate_complex_workflow_scenario(self):
        """Test a complex real-world workflow scenario"""
        # HTTP request configuration
        expression = {
            "url": "{{ variables.api_url }}/orders",
            "method": "POST",
            "headers": {
                "Authorization": "Bearer {{ variables.token }}",
                "Content-Type": "application/json",
            },
            "body": {
                "user": {
                    "id": "{{ inputs.user_id }}",
                    "name": "{{ steps.user_fetch.outputs.name }}",
                    "email": "{{ inputs.email | to_lower }}",
                },
                "order": {
                    "status": "{{ steps.order_fetch.outputs.status }}",
                    "total": "{{ steps.order_fetch.outputs.total | float }}",
                    "item_count": "{{ steps.order_fetch.outputs['items'] | length }}",
                },
            },
        }

        result = self.context_manager.evaluate_expression(expression)

        # Verify all values are correctly evaluated
        self.assertEqual(result["url"], "https://api.example.com/orders")
        self.assertEqual(result["method"], "POST")
        self.assertEqual(result["headers"]["Authorization"], "Bearer secret_token")
        self.assertEqual(result["body"]["user"]["id"], "123")
        self.assertEqual(result["body"]["user"]["name"], "John Doe")
        self.assertEqual(result["body"]["user"]["email"], "test@example.com")
        self.assertEqual(result["body"]["order"]["status"], "completed")
        self.assertEqual(result["body"]["order"]["total"], "199.99")
        self.assertEqual(result["body"]["order"]["item_count"], "3")

    def test_evaluate_static_string(self):
        """Test that static strings without templates are returned as-is"""
        expression = "This is a static string"
        result = self.context_manager.evaluate_expression(expression)
        self.assertEqual(result, "This is a static string")

    def test_evaluate_mixed_structure(self):
        """Test structure with both template and static values"""
        expression = {
            "static_key": "static_value",
            "template_key": "{{ steps.user_fetch.outputs.name }}",
            "number": 42,
            "bool": True,
            "nested": {"static": "value", "dynamic": "{{ inputs.user_id }}"},
        }

        result = self.context_manager.evaluate_expression(expression)

        self.assertEqual(result["static_key"], "static_value")
        self.assertEqual(result["template_key"], "John Doe")
        self.assertEqual(result["number"], 42)
        self.assertTrue(result["bool"])
        self.assertEqual(result["nested"]["static"], "value")
        self.assertEqual(result["nested"]["dynamic"], "123")


if __name__ == "__main__":
    unittest.main(verbosity=2)
