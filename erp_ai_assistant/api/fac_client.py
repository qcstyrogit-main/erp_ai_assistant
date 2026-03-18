import os
from typing import Any

import frappe
import requests

from .tool_registry import execute_tool as fallback_dispatch_tool
from .tool_registry import get_tool_definitions as get_fallback_tool_definitions


DEFAULT_FAC_MCP_TIMEOUT = 30.0


def _cfg_value(key: str, default: Any = None) -> Any:
    for candidate in (key, key.lower(), key.upper()):
        try:
            value = frappe.conf.get(candidate)
        except Exception:
            value = None
        if value not in (None, ""):
            return value
        value = os.getenv(candidate)
        if value not in (None, ""):
            return value
    return default


def _remote_mcp_url() -> str:
    return str(_cfg_value("ERP_AI_FAC_MCP_URL", "") or "").strip()


def _remote_mcp_timeout() -> float:
    raw = _cfg_value("ERP_AI_FAC_MCP_TIMEOUT", DEFAULT_FAC_MCP_TIMEOUT)
    try:
        return max(5.0, min(float(str(raw).strip()), 120.0))
    except Exception:
        return DEFAULT_FAC_MCP_TIMEOUT


def _remote_mcp_headers() -> dict[str, str]:
    headers = {"content-type": "application/json"}
    authorization = str(_cfg_value("ERP_AI_FAC_MCP_AUTHORIZATION", "") or "").strip()
    if authorization:
        headers["authorization"] = authorization
    return headers


def _remote_mcp_request(method: str, params: dict[str, Any] | None = None) -> Any:
    endpoint = _remote_mcp_url()
    if not endpoint:
        raise RuntimeError("FAC MCP endpoint is not configured.")

    response = requests.post(
        endpoint,
        headers=_remote_mcp_headers(),
        json={
            "jsonrpc": "2.0",
            "id": "erp-ai-assistant",
            "method": method,
            "params": params or {},
        },
        timeout=_remote_mcp_timeout(),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("FAC MCP endpoint returned an invalid payload.")
    if payload.get("error"):
        error = payload.get("error") or {}
        raise RuntimeError(str(error.get("message") or "FAC MCP request failed."))
    return payload.get("result")


def _extract_remote_tool_definitions(result: Any) -> dict[str, dict[str, Any]]:
    payload = result or {}
    tools = payload.get("tools") if isinstance(payload, dict) else None
    if not isinstance(tools, list):
        return {}

    definitions: dict[str, dict[str, Any]] = {}
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name") or "").strip()
        if not name:
            continue
        input_schema = tool.get("inputSchema")
        if not isinstance(input_schema, dict):
            input_schema = {"type": "object", "properties": {}}
        definitions[name] = {
            "description": str(tool.get("description") or "").strip() or name,
            "inputSchema": input_schema,
            "annotations": tool.get("annotations"),
        }
    return definitions


def _extract_remote_tool_result(result: Any) -> Any:
    payload = result or {}
    if not isinstance(payload, dict):
        return payload
    content = payload.get("content")
    if not isinstance(content, list) or not content:
        return payload
    first = content[0] if isinstance(content[0], dict) else None
    if not isinstance(first, dict):
        return payload
    text = str(first.get("text") or "").strip()
    if not text:
        return payload
    try:
        return frappe.parse_json(text)
    except Exception:
        return text


def _registry_tool_definitions(registry: Any) -> dict[str, dict[str, Any]]:
    try:
        tools = registry.get_available_tools(user=frappe.session.user)
    except Exception:
        return {}

    definitions: dict[str, dict[str, Any]] = {}
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name") or "").strip()
        if not name:
            continue
        input_schema = tool.get("inputSchema")
        if not isinstance(input_schema, dict):
            input_schema = {"type": "object", "properties": {}}
        definitions[name] = {
            "description": str(tool.get("description") or "").strip() or name,
            "inputSchema": input_schema,
            "annotations": tool.get("annotations"),
        }
    return definitions


def test_remote_mcp_connection() -> dict[str, Any]:
    endpoint = _remote_mcp_url()
    timeout = _remote_mcp_timeout()
    headers = _remote_mcp_headers()
    has_authorization = "authorization" in {key.lower(): value for key, value in headers.items()}
    if not endpoint:
        return {
            "ok": False,
            "type": "fac_mcp_test",
            "endpoint": "",
            "timeout": timeout,
            "has_authorization": has_authorization,
            "tool_count": 0,
            "tool_names": [],
            "message": "FAC MCP URL is not configured.",
        }

    try:
        result = _remote_mcp_request("tools/list")
        definitions = _extract_remote_tool_definitions(result)
        tool_names = sorted(definitions.keys())
        return {
            "ok": True,
            "type": "fac_mcp_test",
            "endpoint": endpoint,
            "timeout": timeout,
            "has_authorization": has_authorization,
            "tool_count": len(tool_names),
            "tool_names": tool_names[:20],
            "message": f"FAC MCP connection succeeded. tools/list returned {len(tool_names)} tool(s).",
        }
    except Exception as exc:
        return {
            "ok": False,
            "type": "fac_mcp_test",
            "endpoint": endpoint,
            "timeout": timeout,
            "has_authorization": has_authorization,
            "tool_count": 0,
            "tool_names": [],
            "message": str(exc) or "FAC MCP connection failed.",
        }


def _fac_registry():
    try:
        from frappe_assistant_core.core.tool_registry import get_tool_registry

        return get_tool_registry()
    except Exception:
        return None


def _local_fac_available() -> tuple[Any, dict[str, dict[str, Any]]]:
    registry = _fac_registry()
    if not registry:
        return None, {}
    definitions = _registry_tool_definitions(registry)
    if not definitions:
        return registry, {}
    return registry, definitions


def get_tool_definitions() -> dict[str, dict[str, Any]]:
    _registry, local_definitions = _local_fac_available()
    if local_definitions:
        return local_definitions

    try:
        remote_definitions = _extract_remote_tool_definitions(_remote_mcp_request("tools/list"))
        if remote_definitions:
            return remote_definitions
    except Exception:
        pass

    return get_fallback_tool_definitions()


def dispatch_tool(name: str, arguments: dict[str, Any]) -> Any:
    registry, local_definitions = _local_fac_available()
    if registry and local_definitions:
        try:
            return registry.execute_tool(name, arguments or {})
        except Exception:
            if name in get_fallback_tool_definitions():
                return fallback_dispatch_tool(name, arguments)
            raise

    try:
        return _extract_remote_tool_result(
            _remote_mcp_request(
                "tools/call",
                {
                    "name": name,
                    "arguments": arguments or {},
                },
            )
        )
    except Exception:
        pass

    return fallback_dispatch_tool(name, arguments)


def test_fac_connection() -> dict[str, Any]:
    registry, local_definitions = _local_fac_available()
    if registry and local_definitions:
        tool_names = sorted(local_definitions.keys())
        return {
            "ok": True,
            "type": "fac_connection_test",
            "mode": "local_registry",
            "endpoint": None,
            "timeout": None,
            "has_authorization": False,
            "tool_count": len(tool_names),
            "tool_names": tool_names[:20],
            "message": f"Connected to local Frappe Assistant Core registry. {len(tool_names)} tool(s) available.",
        }
    remote = test_remote_mcp_connection()
    remote["type"] = "fac_connection_test"
    remote["mode"] = "remote_mcp" if remote.get("ok") else "fallback_only"
    if not remote.get("ok") and registry:
        remote["message"] = (
            f"Local Frappe Assistant Core registry was found but no tools were available. {remote.get('message')}"
        ).strip()
    return remote
