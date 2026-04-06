from typing import Any

import frappe

from ..chat import get_pending_action
from ..context_resolver import normalize_context_payload
from ..erp_tools import describe_erp_schema_internal, get_doctype_fields_internal, list_erp_doctypes_internal


def _resource_current_document(context: dict[str, Any], _arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = str(context.get("doctype") or "").strip()
    docname = str(context.get("docname") or "").strip()
    if not doctype or not docname:
        return {
            "ok": False,
            "type": "resource",
            "message": "No current document is available in the active context.",
            "data": None,
        }
    doc = frappe.get_doc(doctype, docname)
    doc.check_permission("read")
    return {
        "ok": True,
        "type": "resource",
        "resource": "current_document",
        "data": {
            "doctype": doctype,
            "docname": docname,
            "document": doc.as_dict(),
        },
    }


def _resource_doctype_schema(context: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = str(arguments.get("doctype") or context.get("doctype") or "").strip()
    if not doctype:
        return {
            "ok": False,
            "type": "resource",
            "message": "Doctype is required to read schema.",
            "data": None,
        }
    schema = describe_erp_schema_internal(doctype)
    fields = get_doctype_fields_internal(doctype, writable_only=False)
    return {
        "ok": bool(schema.get("ok")),
        "type": "resource",
        "resource": "doctype_schema",
        "data": {
            "schema": schema,
            "fields": fields,
        },
        "message": schema.get("message"),
    }


def _resource_available_doctypes(_context: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "type": "resource",
        "resource": "available_doctypes",
        "data": list_erp_doctypes_internal(
            search=arguments.get("search"),
            module=arguments.get("module"),
            limit=arguments.get("limit", 100),
        ),
    }


def _resource_current_page_context(context: dict[str, Any], _arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "type": "resource",
        "resource": "current_page_context",
        "data": context,
    }


def _resource_pending_assistant_action(context: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    conversation = str(arguments.get("conversation") or context.get("conversation") or "").strip()
    if not conversation:
        return {
            "ok": False,
            "type": "resource",
            "message": "Conversation is required to read pending assistant action.",
            "data": None,
        }
    pending = get_pending_action(conversation)
    return {
        "ok": True,
        "type": "resource",
        "resource": "pending_assistant_action",
        "data": {
            "conversation": conversation,
            "pending_action": pending,
        },
    }


INTERNAL_RESOURCE_REGISTRY: dict[str, dict[str, Any]] = {
    "current_document": {
        "title": "Current Document",
        "description": "The active ERP document from the current page context.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": _resource_current_document,
    },
    "doctype_schema": {
        "title": "DocType Schema",
        "description": "Schema and field metadata for a requested DocType or the current document DocType.",
        "inputSchema": {
            "type": "object",
            "properties": {"doctype": {"type": "string"}},
        },
        "handler": _resource_doctype_schema,
    },
    "available_doctypes": {
        "title": "Available DocTypes",
        "description": "List of available ERP DocTypes in the current site.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "search": {"type": "string"},
                "module": {"type": "string"},
                "limit": {"type": "integer", "default": 100},
            },
        },
        "handler": _resource_available_doctypes,
    },
    "current_page_context": {
        "title": "Current Page Context",
        "description": "The current route, doctype, docname, and user context from the assistant host.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": _resource_current_page_context,
    },
    "pending_assistant_action": {
        "title": "Pending Assistant Action",
        "description": "The assistant's pending clarification or continuation state for a conversation.",
        "inputSchema": {
            "type": "object",
            "properties": {"conversation": {"type": "string"}},
        },
        "handler": _resource_pending_assistant_action,
    },
}


def list_resource_specs() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "title": spec["title"],
            "description": spec["description"],
            "inputSchema": spec["inputSchema"],
            "uri": f"erp://resource/{name}",
        }
        for name, spec in INTERNAL_RESOURCE_REGISTRY.items()
    ]


def read_resource(name: str, *, context: dict[str, Any] | str | None = None, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    spec = INTERNAL_RESOURCE_REGISTRY.get(str(name or "").strip())
    if not spec:
        return {
            "ok": False,
            "type": "resource",
            "message": f"Resource '{name}' not found.",
            "data": None,
        }
    handler = spec.get("handler")
    if not callable(handler):
        return {
            "ok": False,
            "type": "resource",
            "message": f"Resource '{name}' is unavailable.",
            "data": None,
        }
    return handler(normalize_context_payload(context), arguments or {})


def get_resource_catalog_summary() -> dict[str, Any]:
    return {
        "count": len(INTERNAL_RESOURCE_REGISTRY),
        "resources": list_resource_specs(),
    }
