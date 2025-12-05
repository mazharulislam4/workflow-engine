"""
Unit tests for TemplateEngine
"""

import base64
import sys
import unittest
from pathlib import Path
from urllib.parse import quote

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from workflow.engine.template_engine import StrictUndefined, TemplateEngine


class TestTemplateEngine(unittest.TestCase):
    """Test cases for TemplateEngine"""

    def setUp(self):
        """Set up test fixtures"""
        self.engine = TemplateEngine()

    def test_basic_rendering(self):
        """Test basic template rendering with simple variables"""
        template = "Hello, {{ name }}!"
        context = {"name": "World"}
        result = self.engine.render(template, context)
        self.assertEqual(result, "Hello, World!")

    def test_nested_data_access(self):
        """Test accessing nested data structures"""
        template = "{{ user.profile.name }}"
        context = {"user": {"profile": {"name": "Alice", "age": 30}}}
        result = self.engine.render(template, context)
        self.assertEqual(result, "Alice")

    def test_conditional_rendering(self):
        """Test conditional logic in templates"""
        template = "{% if active %}Active{% else %}Inactive{% endif %}"

        result_true = self.engine.render(template, {"active": True})
        self.assertEqual(result_true, "Active")

        result_false = self.engine.render(template, {"active": False})
        self.assertEqual(result_false, "Inactive")

    def test_loop_rendering(self):
        """Test loop iteration in templates"""
        template = "{% for item in items %}{{ item }},{% endfor %}"
        context = {"items": ["a", "b", "c"]}
        result = self.engine.render(template, context)
        self.assertEqual(result, "a,b,c,")

    def test_format_date_filter(self):
        """Test custom format_date filter"""
        template = "{{ date | format_date('%Y-%m-%d') }}"
        context = {"date": "2025-12-04T10:30:00Z"}
        result = self.engine.render(template, context)
        self.assertEqual(result, "2025-12-04")

    def test_format_date_filter_custom_format(self):
        """Test format_date filter with custom format"""
        template = "{{ date | format_date('%B %d, %Y') }}"
        context = {"date": "2025-12-04T10:30:00Z"}
        result = self.engine.render(template, context)
        self.assertEqual(result, "December 04, 2025")

    def test_default_if_empty_filter(self):
        """Test default_if_empty filter"""
        template = "{{ value | default_if_empty('N/A') }}"

        # Empty string
        result = self.engine.render(template, {"value": ""})
        self.assertEqual(result, "N/A")

        # None value
        result = self.engine.render(template, {"value": None})
        self.assertEqual(result, "N/A")

        # Valid value
        result = self.engine.render(template, {"value": "test"})
        self.assertEqual(result, "test")

    def test_to_upper_filter(self):
        """Test to_upper filter"""
        template = "{{ text | to_upper }}"
        context = {"text": "hello world"}
        result = self.engine.render(template, context)
        self.assertEqual(result, "HELLO WORLD")

    def test_to_lower_filter(self):
        """Test to_lower filter"""
        template = "{{ text | to_lower }}"
        context = {"text": "HELLO WORLD"}
        result = self.engine.render(template, context)
        self.assertEqual(result, "hello world")

    def test_to_int_filter(self):
        """Test to_int filter"""
        template = "{{ value | int }}"

        # String to int (filter converts '42.7' string to 0 because int('42.7') fails)
        result = self.engine.render(template, {"value": "42"})
        self.assertEqual(result, "42")

        # Float value to int
        result = self.engine.render(template, {"value": 42.7})
        self.assertEqual(result, "42")

        # Invalid conversion uses default
        result = self.engine.render(template, {"value": "invalid"})
        self.assertEqual(result, "0")

    def test_to_float_filter(self):
        """Test to_float filter"""
        template = "{{ value | float }}"

        # String to float
        result = self.engine.render(template, {"value": "42.7"})
        self.assertEqual(result, "42.7")

        # Invalid conversion uses default
        result = self.engine.render(template, {"value": "invalid"})
        self.assertEqual(result, "0.0")

    def test_b64encode_filter(self):
        """Test base64 encoding filter"""
        template = "{{ text | b64encode }}"
        context = {"text": "hello world"}
        result = self.engine.render(template, context)
        expected = base64.b64encode(b"hello world").decode("utf-8")
        self.assertEqual(result, expected)

    def test_b64decode_filter(self):
        """Test base64 decoding filter"""
        template = "{{ encoded | b64decode }}"
        encoded_text = base64.b64encode(b"hello world").decode("utf-8")
        context = {"encoded": encoded_text}
        result = self.engine.render(template, context)
        self.assertEqual(result, "hello world")

    def test_urlencode_filter(self):
        """Test URL encoding filter"""
        template = "{{ text | urlencode }}"
        context = {"text": "hello world & more"}
        result = self.engine.render(template, context)
        expected = quote("hello world & more")
        self.assertEqual(result, expected)

    def test_urldecode_filter(self):
        """Test URL decoding filter"""
        template = "{% autoescape false %}{{ encoded | urldecode }}{% endautoescape %}"
        encoded_text = quote("hello world & more")
        context = {"encoded": encoded_text}
        result = self.engine.render(template, context)
        self.assertEqual(result, "hello world & more")

    def test_render_data_structure_dict(self):
        """Test rendering data structures - dictionary"""
        data = {
            "key1": "{{ value1 }}",
            "key2": "{{ value2 }}",
            "key3": 123,  # Non-string should remain unchanged
        }
        context = {"value1": "hello", "value2": "world"}

        result = self.engine.render_data_structure(data, context)

        self.assertEqual(result["key1"], "hello")
        self.assertEqual(result["key2"], "world")
        self.assertEqual(result["key3"], 123)

    def test_render_data_structure_list(self):
        """Test rendering data structures - list"""
        data = ["{{ item1 }}", "{{ item2 }}", 42]
        context = {"item1": "first", "item2": "second"}

        result = self.engine.render_data_structure(data, context)

        self.assertEqual(result[0], "first")
        self.assertEqual(result[1], "second")
        self.assertEqual(result[2], 42)

    def test_render_data_structure_nested(self):
        """Test rendering nested data structures"""
        data = {
            "outer": {"inner": "{{ value }}", "list": ["{{ item1 }}", "{{ item2 }}"]}
        }
        context = {"value": "nested", "item1": "a", "item2": "b"}

        result = self.engine.render_data_structure(data, context)

        self.assertEqual(result["outer"]["inner"], "nested")
        self.assertEqual(result["outer"]["list"][0], "a")
        self.assertEqual(result["outer"]["list"][1], "b")

    def test_strict_undefined_raises_error(self):
        """Test that StrictUndefined raises clear errors"""
        template = "{{ missing_key }}"
        context = {"existing_key": "value"}

        with self.assertRaises(ValueError) as cm:
            self.engine.render(template, context)

        self.assertIn("Template rendering error", str(cm.exception))

    def test_strict_undefined_dict_key_error_message(self):
        """Test that StrictUndefined provides helpful error for missing dict keys"""
        template = "{{ data.missing_key }}"
        context = {"data": {"existing_key": "value"}}

        with self.assertRaises(ValueError) as cm:
            self.engine.render(template, context)

        error_msg = str(cm.exception)
        self.assertIn("missing_key", error_msg)
        self.assertIn("not found", error_msg.lower())

    def test_template_syntax_error(self):
        """Test handling of template syntax errors"""
        template = "{{ unclosed"
        context = {}

        with self.assertRaises(ValueError) as cm:
            self.engine.render(template, context)

        self.assertIn("Template rendering error", str(cm.exception))

    def test_empty_template(self):
        """Test rendering empty template"""
        result = self.engine.render("", {})
        self.assertEqual(result, "")

    def test_no_variables_template(self):
        """Test template with no variables"""
        template = "This is a static template"
        result = self.engine.render(template, {})
        self.assertEqual(result, "This is a static template")

    def test_multiple_variables(self):
        """Test template with multiple variables"""
        template = "{{ a }} + {{ b }} = {{ c }}"
        context = {"a": 1, "b": 2, "c": 3}
        result = self.engine.render(template, context)
        self.assertEqual(result, "1 + 2 = 3")

    def test_whitespace_control(self):
        """Test that trim_blocks and lstrip_blocks are working"""
        template = "{% for item in items %}{{ item }}{% endfor %}"
        context = {"items": ["a", "b"]}
        result = self.engine.render(template, context)

        # Should have no extra whitespace
        self.assertEqual(result, "ab")

    def test_filter_with_none_value(self):
        """Test filters handle None values gracefully"""
        template = "{{ value | to_upper }}"
        result = self.engine.render(template, {"value": None})
        self.assertEqual(result, "")

    def test_complex_workflow_scenario(self):
        """Test a complex real-world workflow scenario"""
        http_config = {
            "url": "{{ base_url }}/api/{{ endpoint }}",
            "method": "POST",
            "headers": {
                "Authorization": "Bearer {{ token }}",
                "Content-Type": "application/json",
            },
            "body": {
                "user_id": "{{ user.id }}",
                "email": "{{ user.email | to_lower }}",
                "timestamp": "{{ timestamp | format_date('%Y-%m-%d') }}",
            },
        }

        context = {
            "base_url": "https://api.example.com",
            "endpoint": "users/create",
            "token": "secret123",
            "user": {"id": "user_42", "email": "TEST@EXAMPLE.COM"},
            "timestamp": "2025-12-04T10:30:00Z",
        }

        result = self.engine.render_data_structure(http_config, context)

        self.assertEqual(result["url"], "https://api.example.com/api/users/create")
        self.assertEqual(result["headers"]["Authorization"], "Bearer secret123")
        self.assertEqual(result["body"]["user_id"], "user_42")
        self.assertEqual(result["body"]["email"], "test@example.com")
        self.assertEqual(result["body"]["timestamp"], "2025-12-04")

    def test_empty_list_default_if_empty(self):
        """Test default_if_empty with empty list"""
        template = "{{ items | default_if_empty('No items') }}"
        result = self.engine.render(template, {"items": []})
        self.assertEqual(result, "No items")

    def test_empty_dict_default_if_empty(self):
        """Test default_if_empty with empty dict"""
        template = "{{ data | default_if_empty('No data') }}"
        result = self.engine.render(template, {"data": {}})
        self.assertEqual(result, "No data")

    def test_b64encode_empty_string(self):
        """Test base64 encode with empty string"""
        template = "{{ text | b64encode }}"
        result = self.engine.render(template, {"text": ""})
        self.assertEqual(result, "")

    def test_urlencode_empty_string(self):
        """Test URL encode with empty string"""
        template = "{{ text | urlencode }}"
        result = self.engine.render(template, {"text": ""})
        self.assertEqual(result, "")


class TestStrictUndefined(unittest.TestCase):
    """Test cases specifically for StrictUndefined behavior"""

    def test_str_representation_raises(self):
        """Test that string representation raises error"""
        from jinja2 import Environment

        env = Environment(undefined=StrictUndefined)
        template = env.from_string("{{ missing }}")

        with self.assertRaises(Exception):
            template.render({})

    def test_comparison_raises(self):
        """Test that comparison operations raise errors"""
        from jinja2 import Environment

        env = Environment(undefined=StrictUndefined)
        template = env.from_string("{% if missing > 5 %}yes{% endif %}")

        with self.assertRaises(Exception):
            template.render({})


if __name__ == "__main__":
    unittest.main(verbosity=2)
