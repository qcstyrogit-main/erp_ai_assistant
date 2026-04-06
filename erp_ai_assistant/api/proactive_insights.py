"""
erp_ai_assistant.api.proactive_insights
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NEW v3 MODULE — Proactive Intelligence Engine.

This module analyses tool results and session context to generate proactive
insights, anomaly flags, and next-step suggestions — without the user having
to ask.

This is what makes the assistant feel like Claude rather than a simple query
tool: it anticipates what the user needs to know next.

Usage (called from orchestrator after every tool result):
    from .proactive_insights import (
        analyse_tool_result,
        generate_next_steps,
        check_compliance_deadlines,
    )

    insights = analyse_tool_result(tool_name, result, context)
    next_steps = generate_next_steps(intent, doctype, result)
    deadlines = check_compliance_deadlines()
"""
from __future__ import annotations

import math
from typing import Any
from datetime import date
import frappe
from frappe.utils import nowdate, getdate


# ─────────────────────────────────────────────────────────────────────────────
# Anomaly detection thresholds
# ─────────────────────────────────────────────────────────────────────────────

_LARGE_AMOUNT_THRESHOLD = 10_000_000   # ₱10M — flag for verification
_ZERO_AMOUNT_DOCTYPES = frozenset({
    "sales invoice", "purchase invoice", "payment entry",
    "salary slip", "purchase order", "sales order",
})
_COLLECTION_RISK_THRESHOLD = 3         # 3+ overdue invoices = flag customer
_SUPPLY_CHAIN_RISK_THRESHOLD = 2       # 2+ delayed POs = flag supplier


# ─────────────────────────────────────────────────────────────────────────────
# BIR filing calendar (Philippine-specific)
# ─────────────────────────────────────────────────────────────────────────────

_BIR_DEADLINES = [
    {"day": 10, "form": "BIR 0619-E / 0619-F", "description": "Monthly withholding tax"},
    {"day": 20, "form": "BIR 2550M",             "description": "Monthly VAT declaration"},
    {"day": 25, "description": "Quarterly VAT (2550Q) — check if quarter end"},
]

_QUARTER_END_MONTHS = {3, 6, 9, 12}


# ─────────────────────────────────────────────────────────────────────────────
# Next-step suggestion templates per DocType
# ─────────────────────────────────────────────────────────────────────────────

_NEXT_STEPS = {
    "Sales Invoice": [
        ("💳", "Create a Payment Entry for this invoice"),
        ("📧", "Draft a payment reminder email for the customer"),
        ("📊", "Run the Accounts Receivable Summary report"),
    ],
    "Purchase Invoice": [
        ("💳", "Create a Payment Entry to pay this supplier"),
        ("📊", "Run the Accounts Payable report for this supplier"),
        ("📦", "Check if goods were received (Purchase Receipt)"),
    ],
    "Sales Order": [
        ("🚚", "Create a Delivery Note to fulfil this order"),
        ("🧾", "Create a Sales Invoice for this order"),
        ("📊", "Check stock availability for ordered items"),
    ],
    "Purchase Order": [
        ("📦", "Create a Purchase Receipt when goods arrive"),
        ("📊", "Compare with Supplier Quotation"),
        ("✉️", "Check if the supplier has confirmed this PO"),
    ],
    "Quotation": [
        ("✅", "Convert this Quotation to a Sales Order"),
        ("📧", "Follow up with the customer on this quotation"),
        ("📊", "Check lost quotations for this customer"),
    ],
    "Payment Entry": [
        ("🔗", "Reconcile with the bank statement"),
        ("📊", "Run Bank Reconciliation Statement"),
        ("✅", "Submit this payment entry"),
    ],
    "Stock Entry": [
        ("📊", "Check updated Stock Balance after this entry"),
        ("🔍", "Verify stock valuation impact"),
        ("📋", "Review related Material Request"),
    ],
    "Employee": [
        ("💰", "Check this employee's salary slip"),
        ("📅", "Review leave balance"),
        ("📊", "Run Monthly Attendance Sheet"),
    ],
    "Salary Slip": [
        ("✅", "Submit this salary slip"),
        ("📊", "Run Salary Register for the period"),
        ("🏦", "Generate Bank Remittance file"),
    ],
    "default": [
        ("📊", "Export this data to Excel"),
        ("🔍", "Search for related documents"),
        ("📋", "Run a summary report for this module"),
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def analyse_tool_result(
    tool_name: str,
    result: Any,
    context: dict[str, Any] | None = None,
) -> list[str]:
    """
    Analyse a tool result and return a list of proactive insight strings.
    These are injected into the AI response as additional observations.

    Returns: list of insight strings (empty list = no anomalies found)
    """
    insights: list[str] = []
    if not result:
        return insights

    ctx = context or {}

    if isinstance(result, dict):
        insights.extend(_check_document_anomalies(tool_name, result))
    elif isinstance(result, list):
        insights.extend(_check_list_anomalies(tool_name, result, ctx))

    return insights


def generate_next_steps(
    intent: str,
    doctype: str | None,
    result: Any,
    context: dict[str, Any] | None = None,
) -> str:
    """
    Generate formatted next-step suggestions based on what just happened.
    Returns a Markdown-formatted string to append to the AI response.
    """
    # ── Check if the last action failed ───────────────────────────────────────
    is_error = False
    if isinstance(result, dict):
        tool_res = result.get("result", {})
        if isinstance(tool_res, dict):
            _val = tool_res.get("_validation", {})
            if isinstance(_val, dict) and _val.get("quality") == "error":
                is_error = True
            elif "error" in tool_res or tool_res.get("success") is False:
                is_error = True
        elif "error" in result or "exc" in result or result.get("success") is False:
            is_error = True

    if is_error:
        return "\n".join([
            "",
            "---",
            "**How would you like to resolve this issue?**",
            "- 🛠️ Provide the missing information to try again",
            "- ❌ Cancel this operation",
            "- ❓ Ask me to explain the error in detail",
        ])

    dt = str(doctype or "").strip()
    steps = _NEXT_STEPS.get(dt, _NEXT_STEPS["default"])

    # For write intents, suggest the workflow next step
    if intent in {"create", "update", "workflow"}:
        workflow_steps = _workflow_next_steps(dt, result)
        if workflow_steps:
            steps = workflow_steps + steps[:1]

    lines = ["", "---", "**What would you like to do next?**"]
    for emoji, text in steps[:3]:
        lines.append(f"- {emoji} {text}")

    return "\n".join(lines)


def check_compliance_deadlines() -> list[str]:
    """
    Check if today is near a BIR filing deadline.
    Returns list of warning strings.
    """
    warnings: list[str] = []
    try:
        today = getdate(nowdate())
        day = today.day
        month = today.month

        for deadline in _BIR_DEADLINES:
            deadline_day = deadline.get("day", 0)
            days_until = deadline_day - day

            if 0 <= days_until <= 3:
                form = deadline.get("form", "")
                desc = deadline.get("description", "")
                if days_until == 0:
                    warnings.append(
                        f"🔴 BIR DEADLINE TODAY: {form} ({desc}) is due today!"
                    )
                else:
                    warnings.append(
                        f"⚠️ BIR Reminder: {form} ({desc}) is due in {days_until} day(s)."
                    )

        # April ITR deadline
        if month == 4 and day >= 12 and day <= 15:
            warnings.append(
                f"⚠️ Annual ITR (1701/1702) deadline is April 15 "
                f"({'TODAY' if day == 15 else f'in {15 - day} days'})."
            )

        # Quarter-end reminders
        if month in _QUARTER_END_MONTHS and day >= 25:
            warnings.append(
                "📅 Quarter end approaching — prepare Quarterly VAT (2550Q) "
                "and Alphalist of Withholding Taxes (SAWT)."
            )

    except Exception:
        pass

    return warnings


def detect_collection_risks(records: list[dict[str, Any]]) -> list[str]:
    """
    Given a list of invoice records, detect customers with multiple overdue
    invoices (collection risk).
    """
    insights: list[str] = []
    customer_overdue: dict[str, int] = {}
    customer_amount: dict[str, float] = {}

    for record in records:
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "").lower()
        customer = str(record.get("customer") or "")
        if status == "overdue" and customer:
            customer_overdue[customer] = customer_overdue.get(customer, 0) + 1
            try:
                amt = float(record.get("outstanding_amount") or 0)
                customer_amount[customer] = customer_amount.get(customer, 0) + amt
            except (TypeError, ValueError):
                pass

    for customer, count in customer_overdue.items():
        if count >= _COLLECTION_RISK_THRESHOLD:
            total = customer_amount.get(customer, 0)
            insights.append(
                f"⚠️ **Collection Risk**: {customer} has {count} overdue invoice(s) "
                f"totalling ₱{total:,.2f}. Consider escalating."
            )

    return insights


def detect_supply_chain_risks(records: list[dict[str, Any]]) -> list[str]:
    """
    Given a list of PO records, detect suppliers with multiple delayed orders.
    """
    insights: list[str] = []
    supplier_delayed: dict[str, int] = {}

    for record in records:
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "").lower()
        supplier = str(record.get("supplier") or "")
        if status in {"to receive and bill", "to bill"} and supplier:
            supplier_delayed[supplier] = supplier_delayed.get(supplier, 0) + 1

    for supplier, count in supplier_delayed.items():
        if count >= _SUPPLY_CHAIN_RISK_THRESHOLD:
            insights.append(
                f"⚠️ **Supply Chain Risk**: {supplier} has {count} open/delayed "
                f"Purchase Order(s). Consider following up."
            )

    return insights


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _check_document_anomalies(tool_name: str, doc: dict[str, Any]) -> list[str]:
    """Check a single document for financial anomalies."""
    anomalies: list[str] = []
    doctype = str(doc.get("doctype") or "").lower()
    docstatus = doc.get("docstatus")
    is_submitted = docstatus == 1

    # Zero grand_total on submitted financial document
    grand_total = doc.get("grand_total")
    if grand_total is not None and doctype in _ZERO_AMOUNT_DOCTYPES:
        try:
            fval = float(grand_total)
            if fval == 0.0 and is_submitted:
                anomalies.append(
                    f"⚠️ **Anomaly**: This submitted {doctype} has a grand total of ₱0.00. "
                    f"This is unusual — please verify."
                )
        except (TypeError, ValueError):
            pass

    # Very large amounts
    for field in ("grand_total", "net_total", "outstanding_amount"):
        val = doc.get(field)
        if val is not None:
            try:
                fval = float(val)
                if fval > _LARGE_AMOUNT_THRESHOLD:
                    anomalies.append(
                        f"💡 **Large Amount**: {field} = ₱{fval:,.2f} "
                        f"— please confirm this amount is correct before submitting."
                    )
                    break
            except (TypeError, ValueError):
                pass

    # Negative outstanding amount on unpaid invoice
    status = str(doc.get("status") or "").lower()
    outstanding = doc.get("outstanding_amount")
    if status == "unpaid" and outstanding is not None:
        try:
            fval = float(outstanding)
            if fval < 0:
                anomalies.append(
                    f"⚠️ **Data Issue**: This unpaid invoice has a negative outstanding "
                    f"amount (₱{fval:,.2f}). This may indicate a duplicate payment."
                )
        except (TypeError, ValueError):
            pass

    return anomalies


def _check_list_anomalies(
    tool_name: str,
    records: list[Any],
    context: dict[str, Any],
) -> list[str]:
    """Check a list of records for patterns worth flagging."""
    anomalies: list[str] = []

    if not records:
        return anomalies

    # Detect collection risks from invoice lists
    if any(
        isinstance(r, dict) and r.get("customer") and r.get("outstanding_amount")
        for r in records
    ):
        collection_risks = detect_collection_risks(records)  # type: ignore[arg-type]
        anomalies.extend(collection_risks)

    # Detect supply chain risks from PO lists
    if any(
        isinstance(r, dict) and r.get("supplier")
        for r in records
    ):
        supply_risks = detect_supply_chain_risks(records)  # type: ignore[arg-type]
        anomalies.extend(supply_risks)

    # Large number of overdue records
    overdue_count = sum(
        1 for r in records
        if isinstance(r, dict) and str(r.get("status") or "").lower() == "overdue"
    )
    total = len(records)
    if overdue_count > 0 and total > 0:
        pct = (overdue_count / total) * 100
        if pct > 50:
            anomalies.append(
                f"⚠️ **High Overdue Rate**: {overdue_count} of {total} records "
                f"({pct:.0f}%) are overdue. This may require management attention."
            )

    return anomalies


def _workflow_next_steps(
    doctype: str, result: Any
) -> list[tuple[str, str]]:
    """Return workflow-specific next steps based on what was just created/updated."""
    pipeline_next: dict[str, tuple[str, str]] = {
        "Quotation":          ("✅", "Convert to Sales Order"),
        "Sales Order":        ("🚚", "Create Delivery Note"),
        "Delivery Note":      ("🧾", "Create Sales Invoice"),
        "Sales Invoice":      ("💳", "Create Payment Entry"),
        "Material Request":   ("📋", "Create Purchase Order"),
        "Purchase Order":     ("📦", "Create Purchase Receipt"),
        "Purchase Receipt":   ("🧾", "Create Purchase Invoice"),
        "Purchase Invoice":   ("💳", "Create Payment Entry"),
        "Salary Slip":        ("✅", "Submit Salary Slip"),
        "Payroll Entry":      ("💰", "Submit and create Salary Slips"),
    }
    step = pipeline_next.get(doctype)
    return [step] if step else []
