from __future__ import annotations

from dataclasses import dataclass
from typing import Any


HIGH_RISK_TOOLS = {
    "delete_document",
    "cancel_erp_document",
    "submit_erp_document",
    "run_workflow_action",
}

MEDIUM_RISK_TOOLS = {
    "create_document",
    "create_erp_document",
    "update_document",
    "update_erp_document",
    # Transaction-creating tools — have financial/stock impact; require doctype-level risk gate
    "create_sales_order",
    "create_purchase_order",
    "create_quotation",
}

HIGH_RISK_DOCTYPES = {
    "sales invoice",
    "purchase invoice",
    "payment entry",
    "journal entry",
    "stock entry",
}


@dataclass
class RiskDecision:
    requires_confirmation: bool
    risk_level: str = "low"
    reason: str = ""
    summary: str = ""
    user_message: str = ""


def classify_risk(*, tool_name: str, arguments: dict[str, Any], context: dict[str, Any] | None = None) -> RiskDecision:
    normalized_tool = str(tool_name or "").strip()
    doctype = str(arguments.get("doctype") or arguments.get("target_doctype") or "").strip().lower()

    if normalized_tool in HIGH_RISK_TOOLS:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="high",
            reason="high_risk_mutation",
            summary=f"{normalized_tool} on ERP data",
            user_message=_build_confirmation_text(normalized_tool, arguments, "high"),
        )

    if normalized_tool in MEDIUM_RISK_TOOLS and doctype in HIGH_RISK_DOCTYPES:
        return RiskDecision(
            requires_confirmation=True,
            risk_level="medium",
            reason="financial_or_stock_mutation",
            summary=f"{normalized_tool} for {doctype}",
            user_message=_build_confirmation_text(normalized_tool, arguments, "medium"),
        )

    return RiskDecision(requires_confirmation=False)


def build_confirmation_payload(*, conversation: str, tool_name: str, arguments: dict[str, Any], reason: str, summary: str) -> dict[str, Any]:
    return {
        "action": "confirm_tool_execution",
        "conversation": conversation,
        "tool_name": tool_name,
        "arguments": arguments,
        "reason": reason,
        "summary": summary,
    }


def is_confirmation_reply(text: str) -> bool:
    return str(text or "").strip().lower() in {"yes", "y", "confirm", "confirmed", "proceed", "ok", "go ahead"}


def _build_confirmation_text(tool_name: str, arguments: dict[str, Any], level: str) -> str:
    doctype = arguments.get("doctype")
    name = arguments.get("name") or arguments.get("record")
    return (
        f"I’m ready to run **{tool_name}**"
        f"{f' for **{doctype}**' if doctype else ''}"
        f"{f' on **{name}**' if name else ''}. "
        f"This is a **{level}-risk ERP action**. Reply **Yes** to continue or **No** to cancel."
    )
