import json
import logging
from typing import Any, Dict, Optional, Union
from venv import logger

import requests

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("http_request")
class HTTPRequestExecutor(NodeExecutor):
    """
    Executor for HTTP Request nodes.

    This executor performs an HTTP request based on the node configuration.
    It supports GET, POST, PUT, DELETE methods and allows setting headers,
    query parameters, and request body.

    The response is returned as the output of the node.
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the HTTP request.

        Returns:
            A dictionary containing the response status code and body.
        """
        config = inputs.get("config", {})
        node_id = inputs.get("node_id", "unknown")

        method = config.get("method", "GET").upper()
        url = config.get("url", "")
        headers = config.get("headers", {})
        params = config.get("params", {})
        body = config.get("body", None)
        content_type = headers.get("Content-Type", "application/json")

        logger.info(f"HTTP Request [{node_id}]: {method} {url}")
        logger.debug(
            f"HTTP Request Config:\n{json.dumps(config, indent=2, default=str)}"
        )

        if headers:
            logger.debug(f"Request Headers:\n{json.dumps(headers, indent=2)}")
        if params:
            logger.debug(f"Query Parameters:\n{json.dumps(params, indent=2)}")
        if body:
            logger.debug(f"Request Body:\n{json.dumps(body, indent=2, default=str)}")

        if not url:
            raise ValueError("URL must be provided for HTTP request.")

        # Get request-level timeout (separate from node execution timeout)
        # This is the HTTP library timeout, not the executor timeout
        request_timeout = config.get("timeout", 80)  # 30 seconds default

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=body,
                timeout=request_timeout,  # Add timeout to prevent hanging
            )

            logger.info(f"HTTP Response [{node_id}]: Status {response.status_code}")
            logger.debug(
                f"Response Headers:\n{json.dumps(dict(response.headers), indent=2)}"
            )

            if "application/json" in content_type:
                try:
                    data = response.json()
                    logger.debug(
                        f"Response Body (JSON):\n{json.dumps(data, indent=2, default=str)}"
                    )
                except json.JSONDecodeError:
                    data = response.text
                    logger.debug(
                        f"Response Body (Text): {data[:200]}..."
                        if len(data) > 200
                        else f"Response Body (Text): {data}"
                    )
            else:
                data = response.text
                logger.debug(
                    f"Response Body (Text): {data[:200]}..."
                    if len(data) > 200
                    else f"Response Body (Text): {data}"
                )

            return {
                "status_code": response.status_code,
                "result": data,
                "headers": dict(response.headers),
            }
        except requests.RequestException as e:
            logger.error(f"[FAIL] HTTP Request Failed [{node_id}]: {str(e)}")
            raise
