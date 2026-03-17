import json
from typing import Any

import frappe
from frappe import _

from .resource_registry import list_resource_specs, read_resource
from .tool_registry import execute_tool, list_tool_specs


DIRECT_METHOD_ALIASES = {
    "get_list": "list_documents",
    "get_report": "generate_report",
}


@frappe.whitelist()
def handle_mcp():
    """Minimal MCP/FAC-compatible endpoint for the assistant registries."""
    payload = frappe.request.get_json(silent=True) or {}
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}

    if not method:
        return _error_response(request_id, -32600, "Invalid Request: missing method")

    try:
        if method == "tools/list":
            return _success_response(request_id, {"tools": list_tool_specs()})

        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            result = execute_tool(name, arguments)
            return _success_response(request_id, {"content": [_text_block(result)], "isError": False})

        if method == "resources/list":
            return _success_response(request_id, {"resources": list_resource_specs()})

        if method == "resources/read":
            resource_name = str(params.get("name") or "").strip()
            if not resource_name:
                uri = str(params.get("uri") or "").strip()
                if uri.startswith("erp://resource/"):
                    resource_name = uri.replace("erp://resource/", "", 1).strip()
            result = read_resource(
                resource_name,
                context=params.get("context") or {},
                arguments=params.get("arguments") or {},
            )
            return _success_response(request_id, {"contents": [_text_block(result)]})

        tool_name = DIRECT_METHOD_ALIASES.get(method, method)
        result = execute_tool(tool_name, params)
        return _success_response(request_id, result)
    except frappe.PermissionError:
        return _error_response(request_id, -32001, _("Permission denied"))
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant FAC Proxy Error")
        return _error_response(request_id, -32000, str(exc))


def _success_response(request_id: Any, result: Any) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": result,
    }


def _error_response(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": code,
            "message": message,
        },
    }


def _text_block(result: Any) -> dict[str, str]:
    return {
        "type": "text",
        "text": json.dumps(result, indent=2, default=str),
    }
