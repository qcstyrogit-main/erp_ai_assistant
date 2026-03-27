from typing import Any, Callable

import frappe

from .erp_tools import (
    answer_erp_query_internal,
    cancel_erp_document_internal,
    create_erp_document_internal,
    create_purchase_order_internal,
    create_quotation_internal,
    create_sales_order_internal,
    describe_erp_schema_internal,
    get_doctype_fields_internal,
    get_erp_document_internal,
    list_erp_doctypes_internal,
    list_erp_documents_internal,
    ping_assistant_internal,
    run_workflow_action_internal,
    search_erp_documents_internal,
    submit_erp_document_internal,
    update_erp_document_internal,
)
from .file_tools import export_doctype_list_excel_internal, export_employee_list_excel_internal, generate_document_pdf_internal
from .security import clamp_limit, ensure_doctype_access, ensure_document_access, permission_summary, require_destruction_confirmation


def _tool_ping_assistant(arguments: dict[str, Any]) -> dict[str, Any]:
    return ping_assistant_internal()


def _tool_answer_erp_query(arguments: dict[str, Any]) -> dict[str, Any]:
    return answer_erp_query_internal(arguments.get("question"))


def _tool_create_sales_order(arguments: dict[str, Any]) -> dict[str, Any]:
    return create_sales_order_internal(
        customer=arguments.get("customer"),
        items=arguments.get("items"),
        company=arguments.get("company"),
    )


def _tool_create_quotation(arguments: dict[str, Any]) -> dict[str, Any]:
    return create_quotation_internal(
        customer=arguments.get("customer"),
        items=arguments.get("items"),
        company=arguments.get("company"),
    )


def _tool_create_purchase_order(arguments: dict[str, Any]) -> dict[str, Any]:
    return create_purchase_order_internal(
        supplier=arguments.get("supplier"),
        items=arguments.get("items"),
        company=arguments.get("company"),
    )


def _tool_create_erp_document(arguments: dict[str, Any]) -> dict[str, Any]:
    return create_erp_document_internal(
        doctype=arguments.get("doctype"),
        values=arguments.get("values"),
    )


def _tool_update_erp_document(arguments: dict[str, Any]) -> dict[str, Any]:
    return update_erp_document_internal(
        doctype=arguments.get("doctype"),
        record=arguments.get("record"),
        field=arguments.get("field"),
        value=arguments.get("value"),
    )


def _tool_list_erp_documents(arguments: dict[str, Any]) -> dict[str, Any]:
    return list_erp_documents_internal(
        doctype=arguments.get("doctype"),
        filters=arguments.get("filters"),
        limit=arguments.get("limit", 20),
    )


def _tool_list_erp_doctypes(arguments: dict[str, Any]) -> dict[str, Any]:
    return list_erp_doctypes_internal(
        search=arguments.get("search"),
        module=arguments.get("module"),
        limit=arguments.get("limit", 100),
    )


def _tool_get_erp_document(arguments: dict[str, Any]) -> dict[str, Any]:
    return get_erp_document_internal(
        doctype=arguments.get("doctype"),
        name=arguments.get("name"),
    )


def _tool_get_doctype_fields(arguments: dict[str, Any]) -> dict[str, Any]:
    return get_doctype_fields_internal(
        doctype=arguments.get("doctype"),
        writable_only=bool(arguments.get("writable_only")),
    )


def _tool_describe_erp_schema(arguments: dict[str, Any]) -> dict[str, Any]:
    return describe_erp_schema_internal(arguments.get("doctype"))


def _tool_search_erp_documents(arguments: dict[str, Any]) -> dict[str, Any]:
    return search_erp_documents_internal(
        query=arguments.get("query"),
        doctype=arguments.get("doctype"),
        limit=arguments.get("limit", 20),
    )


def _tool_export_doctype_list_excel(arguments: dict[str, Any]) -> dict[str, Any]:
    return export_doctype_list_excel_internal(
        doctype=arguments.get("doctype"),
        filters=arguments.get("filters"),
        fields=arguments.get("fields"),
    )


def _tool_export_employee_list_excel(arguments: dict[str, Any]) -> dict[str, Any]:
    return export_employee_list_excel_internal(
        arguments.get("filters"),
        fields=arguments.get("fields"),
    )


def _tool_generate_document_pdf(arguments: dict[str, Any]) -> dict[str, Any]:
    return generate_document_pdf_internal(
        doctype=arguments.get("doctype"),
        docname=arguments.get("docname"),
        print_format=arguments.get("print_format"),
    )


def _tool_submit_erp_document(arguments: dict[str, Any]) -> dict[str, Any]:
    return submit_erp_document_internal(
        doctype=arguments.get("doctype"),
        record=arguments.get("record"),
    )


def _tool_cancel_erp_document(arguments: dict[str, Any]) -> dict[str, Any]:
    return cancel_erp_document_internal(
        doctype=arguments.get("doctype"),
        record=arguments.get("record"),
    )


def _tool_run_workflow_action(arguments: dict[str, Any]) -> dict[str, Any]:
    return run_workflow_action_internal(
        doctype=arguments.get("doctype"),
        record=arguments.get("record"),
        action=arguments.get("action"),
    )


def _tool_get_document(arguments: dict[str, Any]) -> dict[str, Any]:
    ensure_doctype_access(arguments["doctype"], "read")
    doc = frappe.get_doc(arguments["doctype"], arguments["name"])
    ensure_document_access(doc, "read")
    data = doc.as_dict()
    fields = arguments.get("fields")
    if fields:
        data = {field: data.get(field) for field in fields if field in data}
    return {
        "success": True,
        "doctype": arguments["doctype"],
        "name": arguments["name"],
        "data": data,
        "permissions": permission_summary(arguments["doctype"]),
        "message": f"{arguments['doctype']} '{arguments['name']}' retrieved successfully",
    }

def _tool_list_documents(arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = arguments["doctype"]
    ensure_doctype_access(doctype, "read")
    filters = arguments.get("filters") or {}
    fields = arguments.get("fields") or ["name", "modified"]
    limit = clamp_limit(arguments.get("limit", 20))
    order_by = arguments.get("order_by") or "modified desc"

    data = frappe.get_list(
        doctype,
        filters=filters,
        fields=fields,
        limit_page_length=limit,
        order_by=order_by,
    )
    return {
        "success": True,
        "doctype": doctype,
        "data": data,
        "count": len(data),
        "returned_limit": limit,
        "filters_applied": filters,
        "permissions": permission_summary(doctype),
        "message": f"Found {len(data)} permitted {doctype} records",
    }

def _tool_create_document(arguments: dict[str, Any]) -> dict[str, Any]:
    doctype = arguments["doctype"]
    ensure_doctype_access(doctype, "create")
    data = dict(arguments["data"])
    data["doctype"] = doctype

    if doctype == "Payment Entry":
        result = create_erp_document_internal(doctype=doctype, values=data)
        if not result.get("ok"):
            return {
                "success": False,
                "doctype": doctype,
                "message": str(result.get("message") or "Payment Entry creation failed"),
                "error": str(result.get("message") or "Payment Entry creation failed"),
            }
        return {
            "success": True,
            "name": result.get("name"),
            "doctype": result.get("doctype") or doctype,
            "docstatus": 0,
            "submitted": False,
            "can_submit": True,
            "message": f"{doctype} '{result.get('name')}' created successfully as draft",
        }

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
    submitted = False
    if arguments.get("submit") and getattr(doc, "docstatus", 0) == 0 and hasattr(doc, "submit"):
        if not arguments.get("confirmed_submit"):
            return {
                "success": True,
                "name": doc.name,
                "doctype": doc.doctype,
                "docstatus": doc.docstatus,
                "submitted": False,
                "message": f"{doctype} was created as draft. Re-run with confirmed_submit=true to submit.",
            }
        ensure_document_access(doc, "submit")
        doc.submit()
        submitted = True

    return {
        "success": True,
        "name": doc.name,
        "doctype": doc.doctype,
        "docstatus": doc.docstatus,
        "owner": doc.owner,
        "creation": doc.creation,
        "submitted": submitted,
        "can_submit": hasattr(doc, "submit"),
        "message": f"{doctype} '{doc.name}' created successfully as {'submitted' if doc.docstatus == 1 else 'draft'}",
    }

def _tool_update_document(arguments: dict[str, Any]) -> dict[str, Any]:
    ensure_doctype_access(arguments["doctype"], "write")
    doc = frappe.get_doc(arguments["doctype"], arguments["name"])
    ensure_document_access(doc, "write")
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
    ensure_doctype_access(doctype, "delete")
    doc = frappe.get_doc(doctype, name)
    ensure_document_access(doc, "delete")
    require_destruction_confirmation(arguments, action=f"Deleting {doctype} '{name}'")
    frappe.delete_doc(doctype, name)
    return {
        "success": True,
        "doctype": doctype,
        "name": name,
        "message": f"{doctype} '{name}' deleted successfully",
    }

def _tool_get_doctype_info(arguments: dict[str, Any]) -> dict[str, Any]:
    ensure_doctype_access(arguments["doctype"], "read")
    meta = frappe.get_meta(arguments["doctype"])
    return {
        "doctype": meta.name,
        "module": meta.module,
        "permissions": permission_summary(meta.name),
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
        ensure_doctype_access(arguments["doctype"], "read")
        return frappe.call(
            "frappe.desk.search.search_link",
            doctype=arguments["doctype"],
            txt=arguments["txt"],
            filters=arguments.get("filters"),
            page_length=clamp_limit(arguments.get("page_length", 10), default=10, maximum=50),
        )

    query = arguments.get("query")
    if not query:
        raise ValueError("search_documents requires either (doctype + txt) or query")

    if not arguments.get("doctype"):
        raise frappe.ValidationError("For production safety, generic global search requires a doctype.")

    ensure_doctype_access(arguments["doctype"], "read")
    return frappe.get_list(
        arguments["doctype"],
        filters=arguments.get("filters") or {},
        fields=["name", "modified"],
        limit_page_length=clamp_limit(arguments.get("page_length", 20), default=20, maximum=50),
        order_by="modified desc",
    )

def _tool_generate_report(arguments: dict[str, Any]) -> Any:
    report_name = arguments["report_name"]
    filters = arguments.get("filters") or {}
    if not frappe.has_permission("Report", ptype="read"):
        raise frappe.PermissionError("User does not have permission to run reports.")
    from frappe.desk.query_report import run as _run_report
    return _run_report(report_name=report_name, filters=filters)


INTERNAL_TOOL_REGISTRY: dict[str, dict[str, Any]] = {
    "ping_assistant": {
        "description": "Check whether the ERP AI Assistant backend is ready",
        "inputSchema": {"type": "object", "properties": {}},
        "category": "system",
        "handler": _tool_ping_assistant,
    },
    "answer_erp_query": {
        "description": "Answer safe deterministic ERP queries such as count questions",
        "inputSchema": {"type": "object", "properties": {"question": {"type": "string"}}, "required": ["question"]},
        "category": "read",
        "handler": _tool_answer_erp_query,
    },
    "create_sales_order": {
        "description": "Create a draft Sales Order from a customer and item rows",
        "inputSchema": {
            "type": "object",
            "properties": {
                "customer": {"type": "string"},
                "company": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string"},
                            "qty": {"type": "number"},
                            "rate": {"type": "number"},
                        },
                        "required": ["item_code", "qty"],
                    },
                },
            },
            "required": ["customer", "items"],
        },
        "category": "write",
        "handler": _tool_create_sales_order,
    },
    "create_quotation": {
        "description": "Create a draft Quotation from a customer and item rows",
        "inputSchema": {
            "type": "object",
            "properties": {
                "customer": {"type": "string"},
                "company": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string"},
                            "qty": {"type": "number"},
                            "rate": {"type": "number"},
                        },
                        "required": ["item_code", "qty"],
                    },
                },
            },
            "required": ["customer", "items"],
        },
        "category": "write",
        "handler": _tool_create_quotation,
    },
    "create_purchase_order": {
        "description": "Create a draft Purchase Order from a supplier and item rows",
        "inputSchema": {
            "type": "object",
            "properties": {
                "supplier": {"type": "string"},
                "company": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "item_code": {"type": "string"},
                            "qty": {"type": "number"},
                            "rate": {"type": "number"},
                        },
                        "required": ["item_code", "qty"],
                    },
                },
            },
            "required": ["supplier", "items"],
        },
        "category": "write",
        "handler": _tool_create_purchase_order,
    },
    "create_erp_document": {
        "description": "Create a document using metadata-aware field mapping for ERP doctypes permitted by site permissions",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "values": {"type": "object"}}, "required": ["doctype", "values"]},
        "category": "write",
        "handler": _tool_create_erp_document,
    },
    "update_erp_document": {
        "description": "Safely update an ERP document field using metadata-aware field resolution and site permissions",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "record": {"type": "string"}, "field": {"type": "string"}, "value": {}}, "required": ["doctype", "record", "field", "value"]},
        "category": "write",
        "handler": _tool_update_erp_document,
    },
    "list_erp_documents": {
        "description": "List ERP documents using metadata-driven fields and site permissions",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "filters": {"type": "object"}, "limit": {"type": "integer", "default": 20}}, "required": ["doctype"]},
        "category": "read",
        "handler": _tool_list_erp_documents,
    },
    "list_erp_doctypes": {
        "description": "List DocTypes available in this ERP instance",
        "inputSchema": {"type": "object", "properties": {"search": {"type": "string"}, "module": {"type": "string"}, "limit": {"type": "integer", "default": 200}}},
        "category": "resource",
        "handler": _tool_list_erp_doctypes,
    },
    "get_erp_document": {
        "description": "Get a single ERP document using site permissions",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "name": {"type": "string"}}, "required": ["doctype", "name"]},
        "category": "read",
        "handler": _tool_get_erp_document,
    },
    "get_doctype_fields": {
        "description": "Get field metadata for a DocType",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "writable_only": {"type": "boolean", "default": False}}, "required": ["doctype"]},
        "category": "resource",
        "handler": _tool_get_doctype_fields,
    },
    "describe_erp_schema": {
        "description": "Describe a DocType schema summary",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}}, "required": ["doctype"]},
        "category": "resource",
        "handler": _tool_describe_erp_schema,
    },
    "search_erp_documents": {
        "description": "Search ERP documents using metadata-driven search fields and site permissions",
        "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}, "doctype": {"type": "string"}, "limit": {"type": "integer", "default": 10}}, "required": ["query"]},
        "category": "read",
        "handler": _tool_search_erp_documents,
    },
    "export_doctype_list_excel": {
        "description": "Generate an Excel file for an ERP DocType list using metadata and site permissions",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "filters": {"type": "object"}, "fields": {"type": "array", "items": {"type": "string"}}}, "required": ["doctype"]},
        "category": "file",
        "handler": _tool_export_doctype_list_excel,
    },
    "export_employee_list_excel": {
        "description": "Generate an Employee list Excel file and save it as a File document",
        "inputSchema": {"type": "object", "properties": {"filters": {"type": "object"}, "fields": {"type": "array", "items": {"type": "string"}}}},
        "category": "file",
        "handler": _tool_export_employee_list_excel,
    },
    "generate_document_pdf": {
        "description": "Generate and save a PDF for a document using Frappe print utilities",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "docname": {"type": "string"}, "print_format": {"type": "string"}}, "required": ["doctype", "docname"]},
        "category": "file",
        "handler": _tool_generate_document_pdf,
    },
    "submit_erp_document": {
        "description": "Submit a draft ERP document that supports submission",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "record": {"type": "string"}}, "required": ["doctype", "record"]},
        "category": "workflow",
        "handler": _tool_submit_erp_document,
    },
    "cancel_erp_document": {
        "description": "Cancel a submitted ERP document that supports cancellation",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "record": {"type": "string"}}, "required": ["doctype", "record"]},
        "category": "workflow",
        "handler": _tool_cancel_erp_document,
    },
    "run_workflow_action": {
        "description": "Apply a workflow action such as Approve or Reject to an ERP document",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "record": {"type": "string"}, "action": {"type": "string"}}, "required": ["doctype", "record", "action"]},
        "category": "workflow",
        "handler": _tool_run_workflow_action,
    },
    "get_document": {
        "description": "Get a single Frappe document by DocType and name",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "name": {"type": "string"}, "fields": {"type": "array", "items": {"type": "string"}}}, "required": ["doctype", "name"]},
        "category": "read",
        "handler": _tool_get_document,
    },
    "list_documents": {
        "description": "List Frappe documents with optional fields, filters, limit, and ordering",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "fields": {"type": "array", "items": {"type": "string"}}, "filters": {"type": "object"}, "limit": {"type": "integer", "default": 20}, "order_by": {"type": "string"}}, "required": ["doctype"]},
        "category": "read",
        "handler": _tool_list_documents,
    },
    "create_document": {
        "description": "Create a Frappe document, defaulting to draft-first behavior",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "data": {"type": "object"}, "submit": {"type": "boolean", "default": False}, "confirmed_submit": {"type": "boolean", "default": False}, "validate_only": {"type": "boolean", "default": False}}, "required": ["doctype", "data"]},
        "category": "write",
        "handler": _tool_create_document,
    },
    "update_document": {
        "description": "Update a Frappe document",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "name": {"type": "string"}, "data": {"type": "object"}}, "required": ["doctype", "name", "data"]},
        "category": "write",
        "handler": _tool_update_document,
    },
    "delete_document": {
        "description": "Delete a Frappe document after explicit confirmation",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "name": {"type": "string"}, "confirmed": {"type": "boolean", "default": False}, "confirmation_text": {"type": "string"}}, "required": ["doctype", "name"]},
        "category": "destructive",
        "handler": _tool_delete_document,
    },
    "get_doctype_info": {
        "description": "Get DocType metadata",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}}, "required": ["doctype"]},
        "category": "resource",
        "handler": _tool_get_doctype_info,
    },
    "search_documents": {
        "description": "Search documents using a general query or link-field lookup",
        "inputSchema": {"type": "object", "properties": {"doctype": {"type": "string"}, "query": {"type": "string"}, "txt": {"type": "string"}, "page_length": {"type": "integer", "default": 10}, "filters": {"type": "object"}}},
        "category": "read",
        "handler": _tool_search_documents,
    },
    "generate_report": {
        "description": "Run a Frappe/ERPNext report",
        "inputSchema": {"type": "object", "properties": {"report_name": {"type": "string"}, "filters": {"type": "object"}, "format": {"type": "string", "default": "json"}}, "required": ["report_name"]},
        "category": "report",
        "handler": _tool_generate_report,
    },
}


def get_tool_definitions() -> dict[str, dict[str, Any]]:
    return {
        name: {
            "description": spec["description"],
            "inputSchema": spec["inputSchema"],
            "annotations": {
                "category": spec.get("category"),
            },
        }
        for name, spec in INTERNAL_TOOL_REGISTRY.items()
    }


def list_tool_specs(*, category: str | None = None) -> list[dict[str, Any]]:
    rows = []
    for name, spec in INTERNAL_TOOL_REGISTRY.items():
        if category and str(spec.get("category") or "").strip().lower() != str(category or "").strip().lower():
            continue
        rows.append(
            {
                "name": name,
                "description": spec["description"],
                "inputSchema": spec["inputSchema"],
                "annotations": {"category": spec.get("category")},
            }
        )
    return rows


def execute_tool(name: str, arguments: dict[str, Any] | None = None) -> Any:
    spec = INTERNAL_TOOL_REGISTRY.get(str(name or "").strip())
    if not spec:
        raise ValueError(f"Tool '{name}' not found. Available tools: {list(INTERNAL_TOOL_REGISTRY.keys())}")
    handler = spec.get("handler")
    if not callable(handler):
        raise ValueError(f"Tool '{name}' is missing a callable handler.")
    return handler(arguments or {})


def get_tool_catalog_summary() -> dict[str, Any]:
    categories: dict[str, int] = {}
    for spec in INTERNAL_TOOL_REGISTRY.values():
        category = str(spec.get("category") or "other").strip()
        categories[category] = categories.get(category, 0) + 1
    return {
        "count": len(INTERNAL_TOOL_REGISTRY),
        "categories": categories,
        "tools": list_tool_specs(),
    }
