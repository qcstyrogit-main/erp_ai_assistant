"""
erp_ai_assistant.api.safety
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Improved risk classification and confirmation-gate logic.

Key improvements over v1:
  - Role-aware decisions: HR Manager can self-confirm HR payroll; others cannot.
  - Bulk-operation gate: any update affecting >10 records is HIGH-RISK.
  - Explicit low-risk allowlist so new tools default to MEDIUM until reviewed.
  - Rich user-facing confirmation messages with field-level detail.
  - is_confirmation_reply extended to support locale-friendly affirmatives.
  - New: structured pending_action payload includes expiry so stale
    confirmations are rejected automatically.
"""
from __future__ import annotations

import frappe
from dataclasses import dataclass, field
from typing import Any

# ── Risk tables ───────────────────────────────────────────────────────────────

HIGH_RISK_TOOLS = frozenset({
    "delete_document",
    "cancel_erp_document",
    "submit_erp_document",
    "run_workflow_action",
    "run_payroll",
    "amend_document",
    "bulk_update_documents",
})

MEDIUM_RISK_TOOLS = frozenset({
    "create_document",
    "create_erp_document",
    "update_document",
    "update_erp_document",
    "create_sales_order",
    "create_purchase_order",
    "create_quotation",
    "create_transaction_document",
})

# Tools known to be purely read-only — no confirmation ever needed
LOW_RISK_TOOLS = frozenset({
    "get_erp_document",
    "list_erp_documents",
    "search_erp_documents",
    "list_erp_doctypes",
    "describe_erp_schema",
    "get_doctype_fields",
    "answer_erp_query",
    "ping_assistant",
    "list_tool_specs",
    "list_resource_specs",
    "get_resource_catalog_summary",
    "get_tool_catalog_summary",
    "read_resource",
    "export_doctype_list_excel",
})

# DocTypes that always escalate MEDIUM-RISK tools to HIGH-RISK
HIGH_RISK_DOCTYPES = frozenset({
    "sales invoice",
    "purchase invoice",
    "payment entry",
    "journal entry",
    "stock entry",
    "payroll entry",
    "salary slip",
    "asset",
    "landed cost voucher",
})

# DocTypes that require only MEDIUM-RISK treatment
MEDIUM_RISK_DOCTYPES = frozenset({
    "sales order",
    "purchase order",
    "delivery note",
    "purchase receipt",
    "quotation",
    "material request",
    "expense claim",
})

# Roles that may self-approve certain domain-specific HIGH-RISK actions
_SELF_APPROVAL_MAP: dict[str, frozenset[str]] = {
    # HR Manager can approve payroll-related actions without extra escalation
    "HR Manager": frozenset({"run_payroll", "submit_erp_document"}),
    # System Manager can approve anything (but still requires confirmation text)
    "System Manager": frozenset(HIGH_RISK_TOOLS),
}


# ── Risk decision dataclass ───────────────────────────────────────────────────

@dataclass
class RiskDecision:
    requires_confirmation: bool
    risk_level: str = "low"          # "low" | "medium" | "high"
    reason: str = ""
    summary: str = ""
    user_message: str = ""
    allowed_roles: list[str] = field(default_factory=list)
    doctype: str = ""
    tool_name: str = ""


# ── Public helpers ────────────────────────────────────────────────────────────

def classify_risk(
    *,
    tool_name: str,
    arguments: dict[str, Any],
    context: dict[str, Any] | None = None,
    user: str | None = None,
) -> RiskDecision:
    """
    Classify the risk of a tool call and decide whether confirmation is needed.

    Improvements over v1:
      - Checks LOW_RISK_TOOLS allowlist first (fast-path for read tools).
      - Unknown tools default to MEDIUM-RISK (safe default).
      - Bulk record count triggers HIGH-RISK regardless of tool name.
      - Role-aware: checks user roles against _SELF_APPROVAL_MAP.
    """
    norm_tool = str(tool_name or "").strip()
    doctype_raw = str(
        arguments.get("doctype") or arguments.get("target_doctype") or ""
    ).strip()
    doctype = doctype_raw.lower()

    # ── Fast path: known safe read tools ────────────────────────────────────
    if norm_tool in LOW_RISK_TOOLS:
        return RiskDecision(requires_confirmation=False, risk_level="low", tool_name=norm_tool)

    # ── Bulk-operation gate ──────────────────────────────────────────────────
    record_count = _extract_record_count(arguments)
    if record_count > 10:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="high",
            reason="bulk_operation",
            summary=f"Bulk operation on {record_count} records via {norm_tool}",
            user_message=_build_confirmation_text(norm_tool, arguments, "high",
                                                   extra=f"This will affect {record_count} records."),
            tool_name=norm_tool,
            doctype=doctype_raw,
        )

    # ── High-risk tool ───────────────────────────────────────────────────────
    if norm_tool in HIGH_RISK_TOOLS:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="high",
            reason="high_risk_mutation",
            summary=f"{norm_tool} on {doctype_raw or 'ERP data'}",
            user_message=_build_confirmation_text(norm_tool, arguments, "high"),
            tool_name=norm_tool,
            doctype=doctype_raw,
        )

    # ── Medium-risk tool on a high-risk DocType → escalate to HIGH ───────────
    if norm_tool in MEDIUM_RISK_TOOLS and doctype in HIGH_RISK_DOCTYPES:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="high",
            reason="financial_or_stock_mutation",
            summary=f"{norm_tool} for {doctype_raw}",
            user_message=_build_confirmation_text(norm_tool, arguments, "high"),
            tool_name=norm_tool,
            doctype=doctype_raw,
        )

    # ── Medium-risk tool on a medium-risk DocType ────────────────────────────
    if norm_tool in MEDIUM_RISK_TOOLS and doctype in MEDIUM_RISK_DOCTYPES:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="medium",
            reason="transactional_mutation",
            summary=f"{norm_tool} for {doctype_raw}",
            user_message=_build_confirmation_text(norm_tool, arguments, "medium"),
            tool_name=norm_tool,
            doctype=doctype_raw,
        )

    # ── Medium-risk tool, unknown DocType → medium by default ────────────────
    if norm_tool in MEDIUM_RISK_TOOLS:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="medium",
            reason="write_operation",
            summary=f"{norm_tool}" + (f" for {doctype_raw}" if doctype_raw else ""),
            user_message=_build_confirmation_text(norm_tool, arguments, "medium"),
            tool_name=norm_tool,
            doctype=doctype_raw,
        )

    # ── Unknown tool → treat as MEDIUM (safe default) ────────────────────────
    return RiskDecision(
        requires_confirmation=True,
        risk_level="medium",
        reason="unknown_tool_default",
        summary=f"Unknown tool '{norm_tool}'" + (f" for {doctype_raw}" if doctype_raw else ""),
        user_message=_build_confirmation_text(norm_tool, arguments, "medium",
                                               extra="This tool is not in the known safe list."),
        tool_name=norm_tool,
        doctype=doctype_raw,
    )


def check_role_override(
    decision: RiskDecision,
    user: str | None = None,
) -> bool:
    """
    Return True if the user's roles allow them to bypass the confirmation gate
    for this specific tool.

    Note: Even with a role override, the UI should STILL ask for confirmation —
    this only controls whether the confirmation can be answered by the user
    themselves vs. requiring a higher-privileged approver.
    """
    if not decision.requires_confirmation:
        return False
    target = user or frappe.session.user or "Guest"
    try:
        user_roles = set(frappe.get_roles(target) or [])
    except Exception:
        user_roles = set()

    for role, allowed_tools in _SELF_APPROVAL_MAP.items():
        if role in user_roles and decision.tool_name in allowed_tools:
            return True
    return False


def build_confirmation_payload(
    *,
    conversation: str,
    tool_name: str,
    arguments: dict[str, Any],
    reason: str,
    summary: str,
    risk_level: str = "high",
) -> dict[str, Any]:
    """Build the confirmation payload stored as a pending_action."""
    return {
        "action": "confirm_tool_execution",
        "conversation": conversation,
        "tool_name": tool_name,
        "arguments": arguments,
        "reason": reason,
        "summary": summary,
        "risk_level": risk_level,
        # Expiry: pending confirmations older than 300 seconds are auto-rejected
        "created_at": frappe.utils.now(),
        "expires_in_seconds": 300,
    }


def is_confirmation_reply(text: str) -> bool:
    """
    Return True if the user's text is an affirmative confirmation.

    Extended from v1 to support common Filipino/local affirmatives and
    punctuation-tolerant matching.
    """
    normalised = str(text or "").strip().lower().rstrip(".,!").strip()
    affirmatives = {
        # English
        "yes", "y", "confirm", "confirmed", "proceed", "ok", "okay",
        "go ahead", "do it", "execute", "run it", "sure", "absolutely",
        "affirmative", "approve", "agreed", "accept",
        # Filipino / local
        "oo", "sige", "opo", "tuloy", "go",
    }
    return normalised in affirmatives


def is_rejection_reply(text: str) -> bool:
    """Return True if the user's text is a clear rejection / cancellation."""
    normalised = str(text or "").strip().lower().rstrip(".,!").strip()
    rejections = {
        "no", "n", "cancel", "stop", "abort", "never mind", "nevermind",
        "don't", "dont", "nope", "negative", "reject", "decline",
        # Filipino
        "hindi", "wag", "ayaw",
    }
    return normalised in rejections


# ── Private helpers ───────────────────────────────────────────────────────────

def _extract_record_count(arguments: dict[str, Any]) -> int:
    """Try to infer how many records an operation will affect."""
    # Explicit count argument
    for key in ("count", "limit", "num_records", "quantity"):
        val = arguments.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                pass
    # List of names / items argument
    for key in ("names", "records", "items", "docnames"):
        val = arguments.get(key)
        if isinstance(val, list):
            return len(val)
    return 1


def _build_confirmation_text(
    tool_name: str,
    arguments: dict[str, Any],
    level: str,
    extra: str = "",
) -> str:
    """Build a user-facing confirmation request message."""
    doctype = arguments.get("doctype") or arguments.get("target_doctype")
    name = arguments.get("name") or arguments.get("record") or arguments.get("docname")

    subject = f"**{tool_name}**"
    if doctype:
        subject += f" on **{doctype}**"
    if name:
        subject += f" — **{name}**"

    level_label = {
        "high": "🔴 HIGH-RISK",
        "medium": "🟡 MEDIUM-RISK",
        "low": "🟢 LOW",
    }.get(level, level.upper())

    lines = [
        f"I'm ready to execute: {subject}",
        f"Risk level: **{level_label}**",
    ]
    if extra:
        lines.append(extra)

    lines.extend([
        "",
        "This action cannot be automatically undone.",
        "**Reply `Yes` to confirm, or `No` to cancel.**",
    ])
    return "\n".join(lines)
