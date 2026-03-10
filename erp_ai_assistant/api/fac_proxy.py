import json
from typing import Any, Callable

import frappe
from frappe import _


TOOL_DEFINITIONS = {
    "get_document": {
        "description": "Get a single Frappe document by DocType and name",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
                "name": {"type": "string"},
                "fields": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["doctype", "name"],
        },
    },
    "list_documents": {
        "description": "List Frappe documents with optional fields, filters, limit, and ordering",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
                "fields": {"type": "array", "items": {"type": "string"}},
                "filters": {"type": "object"},
                "limit": {"type": "integer", "default": 20},
                "order_by": {"type": "string"},
            },
            "required": ["doctype"],
        },
    },
    "create_document": {
        "description": "Create a Frappe document",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
                "data": {"type": "object"},
                "submit": {"type": "boolean", "default": False},
                "validate_only": {"type": "boolean", "default": False},
            },
            "required": ["doctype", "data"],
        },
    },
    "update_document": {
        "description": "Update a Frappe document",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
                "name": {"type": "string"},
                "data": {"type": "object"},
            },
            "required": ["doctype", "name", "data"],
        },
    },
    "delete_document": {
        "description": "Delete a Frappe document",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": ["doctype", "name"],
        },
    },
    "get_doctype_info": {
        "description": "Get DocType metadata",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
            },
            "required": ["doctype"],
        },
    },
    "search_documents": {
        "description": "Search documents using a general query or link-field lookup",
        "inputSchema": {
            "type": "object",
            "properties": {
                "doctype": {"type": "string"},
                "query": {"type": "string"},
                "txt": {"type": "string"},
                "page_length": {"type": "integer", "default": 10},
                "filters": {"type": "object"},
            },
        },
    },
    "generate_report": {
        "description": "Run a Frappe/ERPNext report",
        "inputSchema": {
            "type": "object",
            "properties": {
                "report_name": {"type": "string"},
                "filters": {"type": "object"},
                "format": {"type": "string", "default": "json"},
            },
            "required": ["report_name"],
        },
    },
}


DIRECT_METHOD_ALIASES = {
    "get_list": "list_documents",
    "get_report": "generate_report",
}


@frappe.whitelist()
def handle_mcp():
    """FAC/MCP-compatible endpoint inside ERP.

    Supports:
    - JSON-RPC `tools/list`
    - JSON-RPC `tools/call`
    - direct JSON-RPC method calls such as `get_document`, `get_list`, `get_report`
    """
    payload = frappe.request.get_json(silent=True) or {}
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}

    if not method:
        return _error_response(request_id, -32600, "Invalid Request: missing method")

    try:
        if method == "tools/list":
            return _success_response(request_id, {"tools": _build_tool_list()})

        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            result = _dispatch_tool(name, arguments)
            return _success_response(request_id, {"content": [_text_block(result)], "isError": False})

        tool_name = DIRECT_METHOD_ALIASES.get(method, method)
        result = _dispatch_tool(tool_name, params)
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


def _build_tool_list() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "description": spec["description"],
            "inputSchema": spec["inputSchema"],
        }
        for name, spec in TOOL_DEFINITIONS.items()
    ]


def _dispatch_tool(name: str, arguments: dict[str, Any]) -> Any:
    handlers: dict[str, Callable[[dict[str, Any]], Any]] = {
        "get_document": _tool_get_document,
        "list_documents": _tool_list_documents,
        "create_document": _tool_create_document,
        "update_document": _tool_update_document,
        "delete_document": _tool_delete_document,
        "get_doctype_info": _tool_get_doctype_info,
        "search_documents": _tool_search_documents,
        "generate_report": _tool_generate_report,
    }
    if name not in handlers:
        raise ValueError(f"Tool '{name}' not found. Available tools: {list(handlers.keys())}")
    return handlers[name](arguments)


def _tool_get_document(arguments: dict[str, Any]) -> dict[str, Any]:
    doc = frappe.get_doc(arguments["doctype"], arguments["name"])
    doc.check_permission("read")
    data = doc.as_dict()
    fields = arguments.get("fields")
    if fields:
        data = {field: data.get(field) for field in fields if field in data}
    return {
        "success": True,
        "doctype": arguments["doctype"],
        "name": arguments["name"],
        "data": data,
        "message": f"{arguments['doctype']} '{arguments['name']}' retrieved successfully",
    }


def _tool_list_documents(arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = arguments["doctype"]
    filters = arguments.get("filters") or {}
    fields = arguments.get("fields") or ["name"]
    limit = arguments.get("limit", 20)
    order_by = arguments.get("order_by") or "modified desc"

    data = frappe.get_all(
        doctype,
        filters=filters,
        fields=fields,
        limit_page_length=limit,
        order_by=order_by,
    )
    total_count = frappe.db.count(doctype, filters=filters)
    return {
        "success": True,
        "doctype": doctype,
        "data": data,
        "count": len(data),
        "total_count": total_count,
        "has_more": total_count > len(data),
        "filters_applied": filters,
        "message": f"Found {len(data)} {doctype} records",
    }


def _tool_create_document(arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = arguments["doctype"]
    data = dict(arguments["data"])
    data["doctype"] = doctype

    doc = frappe.get_doc(data)
    if arguments.get("validate_only"):
        doc.run_method("validate")
        return {
            "success": True,
            "doctype": doctype,
            "validated": True,
            "message": f"{doctype} validated successfully",
        }

    doc.insert()
    if arguments.get("submit") and getattr(doc, "docstatus", 0) == 0 and hasattr(doc, "submit"):
        doc.submit()

    return {
        "success": True,
        "name": doc.name,
        "doctype": doc.doctype,
        "docstatus": doc.docstatus,
        "owner": doc.owner,
        "creation": doc.creation,
        "submitted": doc.docstatus == 1,
        "can_submit": hasattr(doc, "submit"),
        "message": f"{doctype} '{doc.name}' created successfully as {'submitted' if doc.docstatus == 1 else 'draft'}",
        "next_steps": _next_steps_for_doc(doc),
    }


def _tool_update_document(arguments: dict[str, Any]) -> dict[str, Any]:
    doc = frappe.get_doc(arguments["doctype"], arguments["name"])
    doc.check_permission("write")

    for key, value in (arguments.get("data") or {}).items():
        doc.set(key, value)
    doc.save()

    return {
        "success": True,
        "name": doc.name,
        "doctype": doc.doctype,
        "modified": doc.modified,
        "modified_by": doc.modified_by,
        "message": f"{doc.doctype} '{doc.name}' updated successfully",
        "data": doc.as_dict(),
    }


def _tool_delete_document(arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = arguments["doctype"]
    name = arguments["name"]
    frappe.delete_doc(doctype, name)
    return {
        "success": True,
        "doctype": doctype,
        "name": name,
        "message": f"{doctype} '{name}' deleted successfully",
    }


def _tool_get_doctype_info(arguments: dict[str, Any]) -> dict[str, Any]:
    meta = frappe.get_meta(arguments["doctype"])
    return {
        "doctype": meta.name,
        "module": meta.module,
        "fields": [
            {
                "fieldname": field.fieldname,
                "label": field.label,
                "fieldtype": field.fieldtype,
                "reqd": field.reqd,
                "options": field.options,
            }
            for field in meta.fields
        ],
    }


def _tool_search_documents(arguments: dict[str, Any]) -> Any:
    if arguments.get("doctype") and arguments.get("txt"):
        return frappe.call(
            "frappe.desk.search.search_link",
            doctype=arguments["doctype"],
            txt=arguments["txt"],
            filters=arguments.get("filters"),
            page_length=arguments.get("page_length", 10),
        )

    query = arguments.get("query")
    if not query:
        raise ValueError("search_documents requires either (doctype + txt) or query")

    # Broad search fallback using global search.
    results = frappe.get_all(
        "Global Search",
        filters={"content": ["like", f"%{query}%"]},
        fields=["doctype", "name", "title", "content"],
        limit_page_length=arguments.get("page_length", 20),
        order_by="modified desc",
    )
    return results


def _tool_generate_report(arguments: dict[str, Any]) -> Any:
    report_name = arguments["report_name"]
    filters = arguments.get("filters") or {}
    report_data = frappe.get_attr("frappe.desk.query_report.run")(report_name=report_name, filters=filters)
    return report_data


def _next_steps_for_doc(doc) -> list[str]:
    steps = []
    if getattr(doc, "docstatus", 0) == 0:
        steps.append("Document is in draft state")
    if hasattr(doc, "submit") and getattr(doc, "docstatus", 0) == 0:
        steps.append("You can submit this document when ready")
    steps.append("You can update this document using update_document")
    return steps
