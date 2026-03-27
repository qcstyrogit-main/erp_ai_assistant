from __future__ import annotations

from typing import Any
import re
import frappe
from frappe.utils import getdate, nowdate, formatdate, fmt_money


COMMON_SUMMARY_FIELDS = [
    "status",
    "docstatus",
    "customer",
    "supplier",
    "party_name",
    "company",
    "posting_date",
    "due_date",
    "grand_total",
    "rounded_total",
    "outstanding_amount",
    "workflow_state",
]


MODULE_SUGGESTIONS = {
    "Sales": [
        "Show overdue sales invoices.",
        "Create a draft payment follow-up plan for this customer.",
        "Export the visible data to Excel and PDF.",
    ],
    "Accounts": [
        "Show due and overdue receivables for this customer.",
        "Create a draft payment entry for this invoice.",
        "Export this result to Excel.",
    ],
    "Stock": [
        "Show low-stock items that need attention.",
        "Create a draft material request from the shortages.",
        "Export the list to Excel.",
    ],
    "General": [
        "Summarize what needs attention today.",
        "Show my pending approvals.",
        "Export the current results to Excel.",
    ],
}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _as_money(value: Any, currency: str | None = None) -> str:
    if value in (None, ""):
        return "—"
    try:
        number = float(value)
        site_currency = currency or frappe.db.get_default("currency") or "USD"
        return fmt_money(number, currency=site_currency)
    except Exception:
        return str(value)


def _fmt_date(value: Any) -> str:
    try:
        if not value:
            return "—"
        return formatdate(value)
    except Exception:
        return str(value)


def _safe_doc(doctype: str | None, name: str | None):
    if not doctype or not name:
        return None
    try:
        return frappe.get_doc(doctype, name)
    except Exception:
        return None




def _guess_doc_from_payload_or_text(payload: Any = None, reply_text: str | None = None) -> tuple[str, str]:
    doctype = ""
    name = ""
    if isinstance(payload, dict):
        doctype = _clean_text(payload.get("doctype") or payload.get("ref_doctype") or payload.get("document_type"))
        name = _clean_text(payload.get("name") or payload.get("document_name") or payload.get("docname"))
        rows = payload.get("data")
        if (not doctype or not name) and isinstance(rows, list) and rows:
            first = rows[0] if isinstance(rows[0], dict) else {}
            if isinstance(first, dict):
                doctype = doctype or _clean_text(first.get("doctype"))
                name = name or _clean_text(first.get("name"))
    if doctype and name:
        return doctype, name
    text = _clean_text(reply_text)
    patterns = [
        r"Sales Invoice\s+([A-Z0-9-]+)",
        r"Purchase Invoice\s+([A-Z0-9-]+)",
        r"Invoice:\s*\*\*([A-Z0-9-]+)\*\*",
        r"Invoice\s+\*\*([A-Z0-9-]+)\*\*",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        found = _clean_text(match.group(1))
        if not found:
            continue
        if 'purchase invoice' in pattern.lower():
            return 'Purchase Invoice', found
        return 'Sales Invoice', found
    return '', ''

def _doc_summary_fields(doc: Any) -> list[dict[str, str]]:
    fields: list[dict[str, str]] = []
    for key in COMMON_SUMMARY_FIELDS:
        value = getattr(doc, key, None)
        if value in (None, ""):
            continue
        label = key.replace("_", " ").title()
        if key in {"grand_total", "rounded_total", "outstanding_amount"}:
            shown = _as_money(value)
        elif key in {"posting_date", "due_date"}:
            shown = _fmt_date(value)
        else:
            shown = str(value)
        fields.append({"label": label, "value": shown})
    return fields[:8]


def _invoice_analysis(doc: Any) -> dict[str, Any]:
    issues: list[str] = []
    actions: list[dict[str, Any]] = []
    suggestions: list[str] = []
    insights: list[str] = []

    status = _clean_text(getattr(doc, "status", ""))
    outstanding = float(getattr(doc, "outstanding_amount", 0) or 0)
    grand_total = float(getattr(doc, "grand_total", 0) or 0)
    due_date = getattr(doc, "due_date", None)
    posting_date = getattr(doc, "posting_date", None)
    creation = getattr(doc, "creation", None)
    customer = _clean_text(getattr(doc, "customer", ""))
    payments = getattr(doc, "payments", None) or []

    if outstanding > 0:
        issues.append(f"Outstanding amount remains {_as_money(outstanding)}.")
    if status.lower() == "overdue" and due_date:
        try:
            days_overdue = (getdate(nowdate()) - getdate(due_date)).days
            if days_overdue > 0:
                issues.append(f"Invoice is overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}.")
        except Exception:
            issues.append("Invoice is marked overdue.")
    elif status:
        issues.append(f"Invoice status is {status}.")

    if not payments and outstanding >= grand_total > 0:
        issues.append("No payments are linked yet.")

    try:
        if posting_date and creation and getdate(posting_date) < getdate(creation):
            delta = (getdate(creation) - getdate(posting_date)).days
            if delta > 30:
                issues.append("Posting date is much earlier than the creation date. Please verify the date.")
        # guard when creation contains timestamp strings
    except Exception:
        pass

    if outstanding > 0:
        actions.append({
            "label": "Create Payment Entry",
            "prompt": f"Create a draft Payment Entry for {doc.doctype} {doc.name}. Do not submit anything yet.",
            "style": "primary",
        })
        actions.append({
            "label": "Send Reminder",
            "prompt": f"Draft a payment reminder email for customer {customer or 'this customer'} for {doc.doctype} {doc.name}.",
        })
        suggestions.append("Show all overdue invoices for this customer.")
        suggestions.append("Export this invoice analysis to Word or PDF.")

    if any("Posting date" in issue for issue in issues):
        actions.append({
            "label": "Fix Posting Date",
            "prompt": f"Check the correct posting date for {doc.doctype} {doc.name} and prepare the safest update. Do not change anything until I confirm.",
        })

    if issues:
        insights.append("This invoice needs action because there is still receivable exposure and no completed payment recorded.")
    if status.lower() == "overdue":
        insights.append("Start with customer follow-up, then prepare payment handling or internal escalation.")

    return {
        "summary": {
            "title": f"{doc.doctype}: {doc.name}",
            "badge": status or "Open",
            "rows": [
                {"label": "Customer", "value": customer or "—"},
                {"label": "Outstanding", "value": _as_money(outstanding)},
                {"label": "Due Date", "value": _fmt_date(due_date)},
                {"label": "Posting Date", "value": _fmt_date(posting_date)},
            ],
        },
        "issues": issues[:5],
        "actions": actions[:4],
        "insights": insights[:3],
        "suggestions": suggestions[:4],
    }


def _dataset_summary(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("data")
    if not isinstance(rows, list):
        return None
    count = payload.get("count") if payload.get("count") not in (None, "") else len(rows)
    doctype = _clean_text(payload.get("doctype"))
    filters = payload.get("filters_applied") if isinstance(payload.get("filters_applied"), dict) else None
    subtitle = f"Showing {count} row{'s' if int(count or 0) != 1 else ''}"
    if doctype:
        subtitle += f" from {doctype}"
    issues = []
    if count and int(count) >= int(payload.get("returned_limit") or count):
        issues.append("The result may be capped by the current limit. Export the data if you need the full set offline.")
    if filters:
        pieces = [f"{key}: {value}" for key, value in list(filters.items())[:4]]
        if pieces:
            issues.append("Filters applied — " + ", ".join(pieces))
    return {
        "summary": {
            "title": doctype or "Dataset",
            "badge": f"{count} rows",
            "rows": [
                {"label": "Returned", "value": str(count)},
                {"label": "Limit", "value": str(payload.get("returned_limit") or len(rows))},
            ],
        },
        "issues": issues[:3],
        "actions": [
            {"label": "Show All Rows", "prompt": "Show all rows from the current result."},
            {"label": "Export Excel", "prompt": "Export the current result to Excel."},
            {"label": "Export PDF", "prompt": "Export the current result to PDF."},
        ],
        "insights": [subtitle],
        "suggestions": ["Summarize the most important rows.", "Group this data by status."]
    }


def build_copilot_package(*, prompt: str | None, context: dict[str, Any] | None, payload: Any = None, reply_text: str | None = None) -> dict[str, Any]:
    context = context or {}
    doctype = _clean_text(context.get("doctype") or (payload.get("doctype") if isinstance(payload, dict) else ""))
    docname = _clean_text(context.get("docname") or (payload.get("name") if isinstance(payload, dict) else ""))
    if not (doctype and docname):
        guessed_doctype, guessed_docname = _guess_doc_from_payload_or_text(payload=payload, reply_text=reply_text)
        doctype = doctype or guessed_doctype
        docname = docname or guessed_docname
    route_module = _clean_text(context.get("target_module") or context.get("route_module") or context.get("module")) or "General"
    doc = _safe_doc(doctype, docname)

    package: dict[str, Any] = {
        "version": 2,
        "mode": "copilot",
        "actions": [],
        "issues": [],
        "insights": [],
        "suggestions": list(MODULE_SUGGESTIONS.get(route_module, MODULE_SUGGESTIONS["General"])),
    }

    if doc and doctype in {"Sales Invoice", "Purchase Invoice"}:
        package.update(_invoice_analysis(doc))
    elif doc:
        package["summary"] = {
            "title": f"{doc.doctype}: {doc.name}",
            "badge": _clean_text(getattr(doc, "status", "")) or _clean_text(getattr(doc, "workflow_state", "")) or "Record",
            "rows": _doc_summary_fields(doc),
        }
        package["actions"] = [
            {"label": "Summarize", "prompt": f"Summarize {doc.doctype} {doc.name} and highlight what needs attention today.", "style": "primary"},
            {"label": "Explain Status", "prompt": f"Explain the current status of {doc.doctype} {doc.name} and what is blocking the next step."},
            {"label": "Draft Next Step", "prompt": f"Create the safest draft next step for {doc.doctype} {doc.name}. Do not submit anything yet."},
        ]
    else:
        dataset = _dataset_summary(payload)
        if dataset:
            package.update(dataset)
        else:
            package["summary"] = {
                "title": doctype or route_module or "ERP Copilot",
                "badge": "Context",
                "rows": [
                    {"label": "DocType", "value": doctype or "—"},
                    {"label": "Document", "value": docname or "—"},
                    {"label": "Module", "value": route_module or "General"},
                ],
            }

    lowered_prompt = _clean_text(prompt).lower()
    if "export" in lowered_prompt and not any(action.get("label") == "Export Excel" for action in package.get("actions", [])):
        package.setdefault("actions", []).append({"label": "Export Excel", "prompt": "Export the current result to Excel.", "style": "primary"})
    if reply_text and not package.get("insights"):
        package["insights"] = [str(reply_text).splitlines()[0][:240]]

    package["actions"] = [row for row in package.get("actions", []) if isinstance(row, dict) and row.get("label") and row.get("prompt")][:5]
    package["issues"] = [str(item) for item in package.get("issues", []) if str(item).strip()][:5]
    package["insights"] = [str(item) for item in package.get("insights", []) if str(item).strip()][:4]
    package["suggestions"] = [str(item) for item in package.get("suggestions", []) if str(item).strip()][:5]
    return package
