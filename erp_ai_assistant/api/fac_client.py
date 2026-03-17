from typing import Any

import frappe

from .tool_registry import execute_tool as fallback_dispatch_tool
from .tool_registry import get_tool_definitions as get_fallback_tool_definitions


def _fac_registry():
    try:
        from frappe_assistant_core.core.tool_registry import get_tool_registry

        return get_tool_registry()
    except Exception:
        return None


def get_tool_definitions() -> dict[str, dict[str, Any]]:
    registry = _fac_registry()
    if not registry:
        return get_fallback_tool_definitions()

    try:
        tools = registry.get_available_tools(user=frappe.session.user)
    except Exception:
        return get_fallback_tool_definitions()

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
    return definitions or get_fallback_tool_definitions()


def dispatch_tool(name: str, arguments: dict[str, Any]) -> Any:
    registry = _fac_registry()
    if not registry:
        return fallback_dispatch_tool(name, arguments)

    try:
        return registry.execute_tool(name, arguments or {})
    except Exception:
        if name in get_fallback_tool_definitions():
            return fallback_dispatch_tool(name, arguments)
        raise
