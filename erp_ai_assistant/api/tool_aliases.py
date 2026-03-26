"""
erp_ai_assistant.api.tool_aliases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Registry-driven tool name alias resolution.

Instead of a flat TOOL_NAME_MAP dict (which silently fails on unknown names),
this module provides a versioned alias registry that:
  - resolves known legacy / FAC / provider name variants to canonical names
  - logs a warning when an unrecognised alias is encountered
  - never crashes the main tool-dispatch path

Usage:
    from .tool_aliases import resolve_tool_name
    canonical = resolve_tool_name(raw_name_from_llm)
"""

from __future__ import annotations

import frappe

# ── Alias registry ────────────────────────────────────────────────────────────
# Format:  "alias_name": "canonical_name"
# Add new aliases here as FAC evolves — never remove old ones.
_ALIAS_REGISTRY: dict[str, str] = {
    # Legacy frappe.client shims
    "frappe.get_list": "list_documents",
    "frappe.client.get_list": "list_documents",
    "frappe.get_doc": "get_document",
    "frappe.client.get": "get_document",
    "frappe.get_report": "generate_report",

    # Short internal names → canonical FAC names
    "get_list": "list_documents",
    "get_report": "generate_report",
    "get_doc": "get_document",

    # Legacy internal tool names (pre-FAC)
    "get_erp_document": "get_document",
    "list_erp_documents": "list_documents",
    "create_erp_document": "create_document",
    "update_erp_document": "update_document",
    "get_doctype_fields": "get_doctype_info",
    "describe_erp_schema": "get_doctype_info",
    "search_erp_documents": "search_documents",
    "submit_erp_document": "submit_document",
    "cancel_erp_document": "cancel_document",
    "run_workflow_action": "run_workflow",
    "answer_erp_query": "search_documents",

    # Provider-specific shims
    "list_erp_doctypes": "available_doctypes",
}

# Canonical names — these pass through unchanged
_CANONICAL_NAMES: frozenset[str] = frozenset({
    "get_document",
    "list_documents",
    "create_document",
    "update_document",
    "delete_document",
    "submit_document",
    "cancel_document",
    "search_documents",
    "search_doctype",
    "search_link",
    "get_doctype_info",
    "run_workflow",
    "generate_report",
    "report_list",
    "report_requirements",
    "export_doctype_list_excel",
    "export_employee_list_excel",
    "generate_document_pdf",
    "run_python_code",
    "analyze_business_data",
    "run_database_query",
    "create_dashboard",
    "create_dashboard_chart",
    "list_user_dashboards",
    "ping_assistant",
    "answer_erp_query",
    "create_sales_order",
    "create_purchase_order",
    "create_quotation",
    "fetch",
    "search",
    "extract_file_content",
})


def resolve_tool_name(raw_name: str) -> str:
    """
    Resolve a raw tool name (possibly an alias or legacy name) to its
    canonical name.  Logs a warning if the name is unrecognised so
    developers can add the alias to the registry.
    """
    name = str(raw_name or "").strip()
    if not name:
        return name

    # Already canonical — fast path
    if name in _CANONICAL_NAMES:
        return name

    # Known alias
    resolved = _ALIAS_REGISTRY.get(name)
    if resolved:
        return resolved

    # Unknown — log once so developers notice
    try:
        frappe.log_error(
            f"erp_ai_assistant: unrecognised tool name '{name}'. "
            "Add it to erp_ai_assistant.api.tool_aliases._ALIAS_REGISTRY "
            "or _CANONICAL_NAMES if it is a valid FAC tool.",
            "ERP AI Assistant: Unknown Tool Name",
        )
    except Exception:
        pass

    # Return as-is so dispatch can still attempt the call
    return name


def all_aliases() -> dict[str, str]:
    """Return a copy of the full alias registry (for introspection/testing)."""
    return dict(_ALIAS_REGISTRY)
