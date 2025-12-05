"""
TemplateEngine Usage Examples

Demonstrates various use cases of the TemplateEngine for workflow automation.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from workflow.engine.template_engine import TemplateEngine


def example_basic_rendering():
    """Basic template rendering with simple variable substitution."""
    print("\n=== Example 1: Basic Rendering ===")

    engine = TemplateEngine()

    # Simple variable substitution
    template = "Hello, {{ name }}! Welcome to {{ company }}."
    context = {"name": "John Doe", "company": "Workflow Enterprise"}

    result = engine.render(template, context)
    print(f"Template: {template}")
    print(f"Result: {result}")
    # Output: Hello, John Doe! Welcome to Workflow Enterprise.


def example_nested_data_access():
    """Accessing nested data structures in templates."""
    print("\n=== Example 2: Nested Data Access ===")

    engine = TemplateEngine()

    template = "User: {{ user.profile.name }}, Email: {{ user.contact.email }}"
    context = {
        "user": {
            "profile": {"name": "Alice Smith", "age": 30},
            "contact": {"email": "alice@example.com", "phone": "555-1234"},
        }
    }

    result = engine.render(template, context)
    print(f"Result: {result}")
    # Output: User: Alice Smith, Email: alice@example.com


def example_conditional_logic():
    """Using conditional logic in templates."""
    print("\n=== Example 3: Conditional Logic ===")

    engine = TemplateEngine()

    template = """
    {% if status == 'active' %}
        Account is active
    {% elif status == 'pending' %}
        Account is pending approval
    {% else %}
        Account is inactive
    {% endif %}
    """

    for status in ["active", "pending", "inactive"]:
        result = engine.render(template, {"status": status})
        print(f"Status '{status}': {result.strip()}")


def example_loops():
    """Iterating over lists in templates."""
    print("\n=== Example 4: Loops ===")

    engine = TemplateEngine()

    template = """
    Tasks:
    {% for task in tasks %}
    - {{ task.name }} ({{ task.status }})
    {% endfor %}
    """

    context = {
        "tasks": [
            {"name": "Review PR", "status": "completed"},
            {"name": "Deploy to staging", "status": "in_progress"},
            {"name": "Run tests", "status": "pending"},
        ]
    }

    result = engine.render(template, context)
    print(result)


def example_custom_filters():
    """Using custom filters provided by TemplateEngine."""
    print("\n=== Example 5: Custom Filters ===")

    engine = TemplateEngine()

    # Date formatting
    template1 = "Date: {{ timestamp | format_date('%B %d, %Y') }}"
    context1 = {"timestamp": "2025-12-04T10:30:00Z"}
    print(f"Date filter: {engine.render(template1, context1)}")

    # Default if empty
    template2 = "Value: {{ value | default_if_empty('N/A') }}"
    context2 = {"value": ""}
    print(f"Default filter: {engine.render(template2, context2)}")

    # Case conversion
    template3 = "Upper: {{ text | to_upper }}, Lower: {{ text | to_lower }}"
    context3 = {"text": "Hello World"}
    print(f"Case filters: {engine.render(template3, context3)}")

    # Type conversion
    template4 = "Int: {{ value | int }}, Float: {{ value | float }}"
    context4 = {"value": "42.7"}
    print(f"Type conversion: {engine.render(template4, context4)}")


def example_encoding_filters():
    """Using encoding/decoding filters."""
    print("\n=== Example 6: Encoding Filters ===")

    engine = TemplateEngine()

    # Base64 encoding
    template1 = "Encoded: {{ text | b64encode }}"
    context1 = {"text": "secret data"}
    encoded_result = engine.render(template1, context1)
    print(f"Base64 encode: {encoded_result}")

    # Base64 decoding
    template2 = "Decoded: {{ encoded_text | b64decode }}"
    context2 = {"encoded_text": "c2VjcmV0IGRhdGE="}
    print(f"Base64 decode: {engine.render(template2, context2)}")

    # URL encoding
    template3 = "URL: https://example.com?q={{ query | urlencode }}"
    context3 = {"query": "hello world & more"}
    print(f"URL encode: {engine.render(template3, context3)}")


def example_render_data_structure():
    """Recursively rendering entire data structures."""
    print("\n=== Example 7: Render Data Structure ===")

    engine = TemplateEngine()

    # Complex nested structure with templates
    data = {
        "api_url": "https://api.example.com/{{ endpoint }}",
        "headers": {
            "Authorization": "Bearer {{ token }}",
            "Content-Type": "application/json",
        },
        "body": {"user_id": "{{ user.id }}", "action": "{{ action }}"},
        "retry_count": 3,  # Non-string values remain unchanged
    }

    context = {
        "endpoint": "users/create",
        "token": "abc123xyz",
        "user": {"id": "user_42"},
        "action": "register",
    }

    rendered = engine.render_data_structure(data, context)
    print("Rendered structure:")
    import json

    print(json.dumps(rendered, indent=2))


def example_workflow_http_request():
    """Real-world example: HTTP request node configuration."""
    print("\n=== Example 8: Workflow HTTP Request ===")

    engine = TemplateEngine()

    # HTTP request configuration with dynamic values
    http_config = {
        "url": "{{ api_base_url }}/api/v1/orders/{{ order_id }}",
        "method": "POST",
        "headers": {
            "Authorization": "Bearer {{ auth_token }}",
            "X-Request-ID": "{{ request_id }}",
            "Content-Type": "application/json",
        },
        "body": {
            "customer": {
                "name": "{{ customer.name }}",
                "email": "{{ customer.email | to_lower }}",
            },
            "items": "{{ items | tojson }}",
            "total": "{{ total | float }}",
        },
    }

    context = {
        "api_base_url": "https://api.myshop.com",
        "order_id": "ORD-12345",
        "auth_token": "secret_token_xyz",
        "request_id": "req_abc123",
        "customer": {"name": "Jane Smith", "email": "JANE@EXAMPLE.COM"},
        "items": [{"id": 1, "qty": 2}, {"id": 2, "qty": 1}],
        "total": "149.99",
    }

    rendered_config = engine.render_data_structure(http_config, context)
    print("Rendered HTTP config:")
    import json

    print(json.dumps(rendered_config, indent=2))


def example_error_handling():
    """Demonstrating error handling with StrictUndefined."""
    print("\n=== Example 9: Error Handling ===")

    engine = TemplateEngine()

    # This will raise an error because 'missing_key' doesn't exist
    template = "Value: {{ missing_key }}"
    context = {"existing_key": "value"}

    try:
        result = engine.render(template, context)
    except ValueError as e:
        print(f"Error caught: {e}")
        # Output: Template rendering error: Variable or field 'missing_key' is not defined


def example_workflow_condition():
    """Real-world example: Condition node evaluation."""
    print("\n=== Example 10: Workflow Condition Node ===")

    engine = TemplateEngine()

    # Condition expressions
    conditions = [
        "{{ order.total | float > 100 }}",
        "{{ user.role == 'admin' }}",
        "{{ items | length > 0 }}",
        "{{ status in ['approved', 'completed'] }}",
    ]

    context = {
        "order": {"total": "150.50"},
        "user": {"role": "admin"},
        "items": [1, 2, 3],
        "status": "approved",
    }

    print("Condition evaluations:")
    for condition in conditions:
        result = engine.render(condition, context)
        print(f"  {condition} => {result}")


if __name__ == "__main__":
    print("=" * 60)
    print("TemplateEngine Usage Examples")
    print("=" * 60)

    example_basic_rendering()
    example_nested_data_access()
    example_conditional_logic()
    example_loops()
    example_custom_filters()
    example_encoding_filters()
    example_render_data_structure()
    example_workflow_http_request()
    example_error_handling()
    example_workflow_condition()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)
