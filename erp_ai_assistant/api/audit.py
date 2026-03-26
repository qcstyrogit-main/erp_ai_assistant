"""
erp_ai_assistant.api.audit
~~~~~~~~~~~~~~~~~~~~~~~~~~
Writes AI Audit Log entries after every prompt execution.

Usage (in ai.py after _run_enqueued_prompt completes):

    from .audit import write_audit_log
    write_audit_log(
        user=user,
        conversation=conversation_name,
        prompt=prompt_text,
        tools_used=tools_list,
        affected_records=records_list,
        tokens_in=n,
        tokens_out=n,
        duration_ms=elapsed,
        provider=provider_name,
        model=model_name,
        route=route,
        doctype_context=doctype,
        docname_context=docname,
        error_message=None,
        confirmed_destructive=False,
    )
"""

from __future__ import annotations

import json
import time
from typing import Any

import frappe

AUDIT_DOCTYPE = "AI Audit Log"

# Tool names classified as destructive / write / workflow for risk assessment
_DESTRUCTIVE_TOOLS = {"delete_document", "cancel_erp_document", "cancel_document"}
_WRITE_TOOLS = {
    "create_document", "update_document", "create_erp_document",
    "update_erp_document", "submit_document", "submit_erp_document",
    "create_sales_order", "create_purchase_order", "create_quotation",
}
_WORKFLOW_TOOLS = {"run_workflow", "run_workflow_action"}
_REPORT_TOOLS = {"generate_report", "report_list", "report_requirements"}
_EXPORT_TOOLS = {"export_doctype_list_excel", "export_employee_list_excel", "generate_document_pdf"}
_ANALYSIS_TOOLS = {"run_python_code", "analyze_business_data", "run_database_query"}


def _classify_action(tools: list[str]) -> tuple[str, str]:
    """Return (action_type, risk_level) based on tools used."""
    tool_set = set(tools or [])
    if tool_set & _DESTRUCTIVE_TOOLS:
        return "destructive", "high"
    if tool_set & _WRITE_TOOLS:
        return "write", "medium"
    if tool_set & _WORKFLOW_TOOLS:
        return "workflow", "medium"
    if tool_set & _REPORT_TOOLS:
        return "report", "low"
    if tool_set & _EXPORT_TOOLS:
        return "export", "low"
    if tool_set & _ANALYSIS_TOOLS:
        return "analysis", "low"
    return "query", "low"


def _audit_ready() -> bool:
    try:
        return bool(frappe.db.exists("DocType", AUDIT_DOCTYPE) and frappe.db.table_exists(AUDIT_DOCTYPE))
    except Exception:
        return False


def write_audit_log(
    *,
    user: str,
    conversation: str | None = None,
    prompt: str = "",
    tools_used: list[str] | None = None,
    affected_records: list[dict[str, Any]] | None = None,
    tokens_in: int = 0,
    tokens_out: int = 0,
    duration_ms: int = 0,
    provider: str = "",
    model: str = "",
    route: str = "",
    doctype_context: str = "",
    docname_context: str = "",
    error_message: str = "",
    confirmed_destructive: bool = False,
) -> None:
    """
    Write one AI Audit Log entry. Silently swallows all errors so it
    never interrupts the main assistant flow.
    """
    if not _audit_ready():
        return

    try:
        tools = list(tools_used or [])
        action_type, risk_level = _classify_action(tools)

        doc = frappe.get_doc({
            "doctype": AUDIT_DOCTYPE,
            "user": user or frappe.session.user,
            "conversation": conversation,
            "prompt_preview": (prompt or "")[:500],
            "action_type": action_type,
            "risk_level": risk_level,
            "provider": provider,
            "model": model,
            "tools_used": json.dumps(tools, ensure_ascii=False) if tools else "[]",
            "affected_records": json.dumps(affected_records or [], ensure_ascii=False),
            "tokens_in": int(tokens_in or 0),
            "tokens_out": int(tokens_out or 0),
            "duration_ms": int(duration_ms or 0),
            "route": route or "",
            "doctype_context": doctype_context or "",
            "docname_context": docname_context or "",
            "error_message": (error_message or "")[:1000],
            "confirmed_destructive": int(bool(confirmed_destructive)),
        })
        doc.insert(ignore_permissions=True)
        frappe.db.commit()
    except Exception:
        # Audit must never crash the assistant
        pass


class PromptTimer:
    """Context manager that tracks elapsed time for a prompt execution."""

    def __init__(self) -> None:
        self._start: float = 0.0
        self.elapsed_ms: int = 0

    def __enter__(self) -> "PromptTimer":
        self._start = time.monotonic()
        return self

    def __exit__(self, *_: Any) -> None:
        self.elapsed_ms = int((time.monotonic() - self._start) * 1000)
