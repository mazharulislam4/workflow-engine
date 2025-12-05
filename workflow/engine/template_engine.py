import logging
from typing import Any, Callable, Dict, List, Tuple, Union

from jinja2 import (
    BaseLoader,
    ChainableUndefined,
    Environment,
    Template,
    TemplateSyntaxError,
    UndefinedError,
    select_autoescape,
)
from jinja2.exceptions import SecurityError

logger = logging.getLogger(__name__)


class StrictUndefined(ChainableUndefined):
    """
    Custom undefined class that raises clear errors for missing keys.
    """

    def __init__(self, hint=None, obj=None, name=None, exc=None):
        super().__init__(hint, obj, name, exc)
        self._undefined_name = name
        self._undefined_obj = obj

    def _fail_with_undefined_error(self, *args, **kwargs):
        """Raise error with clear message"""
        if self._undefined_obj is not None and self._undefined_name is not None:
            if isinstance(self._undefined_obj, dict):
                available_keys = (
                    list(self._undefined_obj.keys()) if self._undefined_obj else []
                )

                # Create a helpful error message showing available keys
                raise UndefinedError(
                    f"Key '{self._undefined_name}' not found. "
                    f"Available keys: {available_keys}. "
                    f"Check your template syntax and key names (case-sensitive)."
                )

        # Generic undefined variable
        raise UndefinedError(
            f"Variable or field '{self._undefined_name}' is not defined"
        )

    # Override methods to trigger error
    def __str__(self) -> Any:
        return str(self._fail_with_undefined_error())

    def __repr__(self) -> Any:
        return repr(self._fail_with_undefined_error())

    def __int__(self) -> Any:
        return self._fail_with_undefined_error()

    def __float__(self) -> Any:
        return self._fail_with_undefined_error()

    def __bool__(self) -> bool:
        self._fail_with_undefined_error()
        return False

    def __lt__(self, other):
        return self._fail_with_undefined_error()

    def __le__(self, other):
        return self._fail_with_undefined_error()

    def __gt__(self, other):
        return self._fail_with_undefined_error()

    def __ge__(self, other):
        return self._fail_with_undefined_error()

    def __eq__(self, other):
        return self._fail_with_undefined_error()

    def __ne__(self, other):
        return self._fail_with_undefined_error()

    def __call__(self, *args, **kwargs):
        return self._fail_with_undefined_error()


class TemplateLoader(BaseLoader):
    """
    Custom Jinja2 loader
    """

    def get_source(
        self, environment: Environment, template: str
    ) -> Tuple[str, str | None, Callable[[], bool] | None]:
        return template, None, lambda: True


class TemplateEngine:
    """
    Template engine using Jinja2 with graceful undefined handling
    """

    def __init__(self) -> None:
        self.env = Environment(
            loader=TemplateLoader(),
            autoescape=select_autoescape(["html", "xml"], default_for_string=True),
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=StrictUndefined,  # Use strict undefined that raises clear errors
        )

        self.env.filters["format_date"] = self._format_date_filter
        self.env.filters["default_if_empty"] = self._default_if_empty_filter
        self.env.filters["to_upper"] = lambda x: str(x).upper() if x else ""
        self.env.filters["to_lower"] = lambda x: str(x).lower() if x else ""
        self.env.filters["int"] = self._to_int_filter
        self.env.filters["float"] = self._to_float_filter
        self.env.filters["b64encode"] = self._b64encode_filter
        self.env.filters["b64decode"] = self._b64decode_filter
        self.env.filters["urlencode"] = self._urlencode_filter
        self.env.filters["urldecode"] = self._urldecode_filter

    def render(self, template_str: str, context: Dict[str, Any]) -> str:
        """
        Render a template string with the given context.

        Args:
            template_str (str): The template string to render.
            context (Dict[str, Any]): The context to use for rendering.

        Returns:
            str: The rendered template string.
        """
        try:
            template: Template = self.env.from_string(template_str)
            return template.render(context)
        except (TemplateSyntaxError, UndefinedError, SecurityError) as e:
            raise ValueError(f"Template rendering error: {e}") from e

    def render_data_structure(
        self, data: Union[Dict[str, Any], List[Any], str, None], context: Dict[str, Any]
    ) -> Any:
        """Recursively render all string values in a data structure."""
        if isinstance(data, dict):
            return {
                key: self.render_data_structure(value, context)
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [self.render_data_structure(item, context) for item in data]
        elif isinstance(data, str):
            return self.render(data, context)
        else:
            return data

    def _format_date_filter(self, value: str, date_format: str = "%Y-%m-%d") -> str:
        """
        Custom Jinja2 filter to format dates.

        Args:
            value (str): The date string to format.
            date_format (str): The desired date format.

        Returns:
            str: The formatted date string.
        """
        if not value:
            return ""
        try:
            from datetime import datetime

            if isinstance(value, str):
                value = value.replace("Z", "+00:00")
                dt = datetime.fromisoformat(value)
            elif isinstance(value, datetime):
                dt = value
            else:
                return value
            return dt.strftime(date_format)
        except ValueError:
            return str(value)

    def _default_if_empty_filter(self, value: Any, default: str) -> Any:
        """
        Custom Jinja2 filter to provide a default value if the input is empty.

        Args:
            value (Any): The input value to check.
            default (str): The default value to return if input is empty.

        Returns:
            Any: The original value if not empty, otherwise the default value.
        """
        if (
            value is None
            or value == ""
            or (isinstance(value, (list, dict)) and len(value) == 0)
        ):
            return default
        return value

    def _to_int_filter(self, value: Any, default: int = 0) -> int:
        """
        Convert value to integer.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            int: Converted integer value
        """
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _to_float_filter(self, value: Any, default: float = 0.0) -> float:
        """
        Convert value to float.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            float: Converted float value
        """
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _b64encode_filter(self, value: Any) -> str:
        """
        Encode value to base64.

        Args:
            value: Value to encode (string or bytes)

        Returns:
            str: Base64 encoded string
        """
        import base64

        if value is None or value == "":
            return ""
        try:
            if isinstance(value, str):
                value = value.encode("utf-8")
            return base64.b64encode(value).decode("utf-8")
        except Exception as e:
            logger.warning(f"Failed to base64 encode value: {e}")
            return str(value)

    def _b64decode_filter(self, value: Any) -> str:
        """
        Decode value from base64.

        Args:
            value: Base64 encoded string to decode

        Returns:
            str: Decoded string
        """
        import base64

        if value is None or value == "":
            return ""
        try:
            if isinstance(value, str):
                value = value.encode("utf-8")
            return base64.b64decode(value).decode("utf-8")
        except Exception as e:
            logger.warning(f"Failed to base64 decode value: {e}")
            return str(value)

    def _urlencode_filter(self, value: Any) -> str:
        """
        URL encode value.

        Args:
            value: Value to URL encode

        Returns:
            str: URL encoded string
        """
        from urllib.parse import quote

        if value is None or value == "":
            return ""
        try:
            return quote(str(value))
        except Exception as e:
            logger.warning(f"Failed to URL encode value: {e}")
            return str(value)

    def _urldecode_filter(self, value: Any) -> str:
        """
        URL decode value.

        Args:
            value: URL encoded value to decode

        Returns:
            str: Decoded string
        """
        from urllib.parse import unquote

        if value is None or value == "":
            return ""
        try:
            return unquote(str(value))
        except Exception as e:
            logger.warning(f"Failed to URL decode value: {e}")
            return str(value)
