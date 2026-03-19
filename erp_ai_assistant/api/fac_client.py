import os
from typing import Any

import frappe
import requests

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
        annotations = tool.get("annotations") if isinstance(tool.get("annotations"), dict) else None
        normalized_schema = _normalize_input_schema(input_schema)
        definitions[name] = {
            "description": _enriched_tool_description(
                name,
                str(tool.get("description") or "").strip() or name,
                normalized_schema,
                annotations,
            ),
            "inputSchema": normalized_schema,
            "annotations": annotations,
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


def _registry_tool_definitions(registry: Any, user: str | None = None) -> dict[str, dict[str, Any]]:
    try:
        tools = registry.get_available_tools(user=user or frappe.session.user)
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
        annotations = tool.get("annotations") if isinstance(tool.get("annotations"), dict) else None
        normalized_schema = _normalize_input_schema(input_schema)
        definitions[name] = {
            "description": _enriched_tool_description(
                name,
                str(tool.get("description") or "").strip() or name,
                normalized_schema,
                annotations,
            ),
            "inputSchema": normalized_schema,
            "annotations": annotations,
        }
    return definitions


def _normalize_input_schema(input_schema: Any) -> dict[str, Any]:
    schema = input_schema if isinstance(input_schema, dict) else {"type": "object", "properties": {}}
    if schema.get("type") != "object":
        schema = {"type": "object", "properties": {}}
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        schema = dict(schema)
        schema["properties"] = {}
    if "additionalProperties" not in schema:
        schema = dict(schema)
        schema["additionalProperties"] = False
    return schema


def _schema_summary(input_schema: dict[str, Any]) -> str:
    properties = input_schema.get("properties") or {}
    required = input_schema.get("required") or []
    if not isinstance(properties, dict):
        properties = {}
    if not isinstance(required, list):
        required = []

    property_names = [str(name).strip() for name in properties.keys() if str(name).strip()]
    required_names = [str(name).strip() for name in required if str(name).strip()]
    parts: list[str] = []
    if required_names:
        parts.append(f"required: {', '.join(required_names[:6])}")
    if property_names:
        parts.append(f"fields: {', '.join(property_names[:10])}")
    return "; ".join(parts)


def _enriched_tool_description(
    name: str,
    description: str,
    input_schema: dict[str, Any],
    annotations: dict[str, Any] | None,
) -> str:
    text = str(description or "").strip() or str(name or "").strip()
    lowered = str(name or "").strip().lower()
    schema_hint = _schema_summary(input_schema)
    annotation_hint = ""
    if annotations:
        titles = [str(value).strip() for value in annotations.values() if str(value).strip()]
        if titles:
            annotation_hint = f" annotations: {', '.join(titles[:4])}."

    workflow_hint = ""
    if lowered == "get_doctype_info":
        workflow_hint = " Use this before create_document or update_document to learn exact fieldnames, required fields, and child tables."
    elif lowered == "create_document":
        workflow_hint = (
            " Use after get_doctype_info. Pass the exact DocType in doctype and the document body in data."
            " Child tables must be arrays of objects under the correct table field."
        )
    elif lowered == "update_document":
        workflow_hint = (
            " Use after identifying the exact target document. Pass doctype, name, and only the fields that should change."
        )
    elif lowered == "search_link":
        workflow_hint = " Use this to resolve referenced records such as Item, Customer, Supplier, Warehouse, Company, or User before mutations."
    elif lowered == "list_documents":
        workflow_hint = " Use this to identify candidate records or confirm targets before update or workflow actions."
    elif lowered == "get_document":
        workflow_hint = " Use this to read the exact current document state before updating, submitting, or applying workflow actions."
    elif lowered == "export_doctype_records":
        workflow_hint = (
            " Use this to generate a real export file for a DocType."
            " Always pass doctype and a useful fields list."
            " Do not rely on the default name-only export when the user asks for a detailed file."
        )

    if schema_hint:
        text = f"{text} Input schema: {schema_hint}."
    if workflow_hint:
        text = f"{text}{workflow_hint}"
    if annotation_hint:
        text = f"{text}{annotation_hint}"
    return text.strip()


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


def _local_fac_available(user: str | None = None) -> tuple[Any, dict[str, dict[str, Any]]]:
    registry = _fac_registry()
    if not registry:
        return None, {}
    definitions = _registry_tool_definitions(registry, user=user)
    if not definitions:
        return registry, {}
    return registry, definitions


def get_tool_definitions(user: str | None = None) -> dict[str, dict[str, Any]]:
    _registry, local_definitions = _local_fac_available(user=user)
    if local_definitions:
        return local_definitions

    try:
        remote_definitions = _extract_remote_tool_definitions(_remote_mcp_request("tools/list"))
        if remote_definitions:
            return remote_definitions
    except Exception:
        pass

    return {}


def dispatch_tool(name: str, arguments: dict[str, Any], user: str | None = None) -> Any:
    registry, local_definitions = _local_fac_available(user=user)
    if registry and local_definitions:
        if user:
            frappe.set_user(user)
        return registry.execute_tool(name, arguments or {})

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
        raise RuntimeError(
            "FAC-native tool execution failed because no FAC tool backend was available for this request."
        )


def test_fac_connection() -> dict[str, Any]:
    registry, local_definitions = _local_fac_available(user=frappe.session.user)
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
