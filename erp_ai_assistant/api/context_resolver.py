"""
erp_ai_assistant.api.context_resolver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Improved request context builder for the ERP AI Assistant.

Key improvements over v1:
  - Auto-derives the ERP module from the doctype when target_module is not
    explicitly supplied (so the LLM always has module context).
  - Injects user roles into the context dict so downstream prompt_builder
    and orchestrator can make permission-aware decisions.
  - Sanitises and validates all context values (no raw None or empty strings
    passed downstream).
  - Backwards-compatible: all v1 callers work unchanged.
"""
from __future__ import annotations

import json
from typing import Any

import frappe


# ── DocType → ERP Module mapping ──────────────────────────────────────────────
# Covers the most common Frappe/ERPNext DocTypes. The orchestrator and
# intent_detector maintain their own module-signal tables; this mapping is
# authoritative for the context dict.

_DOCTYPE_MODULE_MAP: dict[str, str] = {
    # Finance
    "Account": "Finance",
    "Cost Center": "Finance",
    "Budget": "Finance",
    "Journal Entry": "Finance",
    "Payment Entry": "Finance",
    "Payment Request": "Finance",
    "Sales Invoice": "Finance",
    "Purchase Invoice": "Finance",
    "Expense Claim": "Finance",
    "Bank Statement": "Finance",
    "Landed Cost Voucher": "Finance",
    # Inventory
    "Item": "Inventory",
    "Item Price": "Inventory",
    "Bin": "Inventory",
    "Warehouse": "Inventory",
    "Stock Entry": "Inventory",
    "Stock Reconciliation": "Inventory",
    "Purchase Receipt": "Inventory",
    "Delivery Note": "Inventory",
    "Material Request": "Inventory",
    "Item Group": "Inventory",
    # Sales
    "Lead": "Sales",
    "Opportunity": "Sales",
    "Quotation": "Sales",
    "Sales Order": "Sales",
    "Delivery Note": "Sales",
    "Customer": "Sales",
    "Contact": "Sales",
    "Address": "Sales",
    "Territory": "Sales",
    # Purchasing
    "Supplier Quotation": "Purchasing",
    "Purchase Order": "Purchasing",
    "Supplier": "Purchasing",
    "Request for Quotation": "Purchasing",
    # HR
    "Employee": "HR",
    "Department": "HR",
    "Designation": "HR",
    "Salary Structure": "HR",
    "Salary Slip": "HR",
    "Payroll Entry": "HR",
    "Leave Application": "HR",
    "Leave Allocation": "HR",
    "Attendance": "HR",
    "Appraisal": "HR",
    "Employee Transfer": "HR",
    # Manufacturing
    "BOM": "Manufacturing",
    "Work Order": "Manufacturing",
    "Job Card": "Manufacturing",
    "Production Plan": "Manufacturing",
    "Operation": "Manufacturing",
    "Routing": "Manufacturing",
    "Workstation": "Manufacturing",
    # Projects
    "Project": "Projects",
    "Task": "Projects",
    "Timesheet": "Projects",
    "Activity Type": "Projects",
    "Project Template": "Projects",
    # Assets
    "Asset": "Assets",
    "Asset Category": "Assets",
    "Asset Maintenance": "Assets",
    "Asset Movement": "Assets",
    # CRM
    "Campaign": "CRM",
    "Prospect": "CRM",
    "CRM Action": "CRM",
}


def _doctype_to_module(doctype: str | None) -> str | None:
    """Return the ERP module for a given DocType, or None if not mapped."""
    if not doctype:
        return None
    return _DOCTYPE_MODULE_MAP.get(str(doctype).strip())


def _get_user_roles(user: str | None) -> list[str]:
    """Return the user's current ERP roles, sorted."""
    target = user or frappe.session.user or "Guest"
    try:
        roles = frappe.get_roles(target) or []
        return sorted({str(r).strip() for r in roles if str(r or "").strip()})
    except Exception:
        return ["Guest"]


def normalize_context_payload(context: dict[str, Any] | str | None) -> dict[str, Any]:
    """
    Parse and normalise a raw context payload (may arrive as a JSON string).
    Returns a clean dict with no None values.
    """
    parsed = context
    if isinstance(context, str):
        try:
            parsed = json.loads(context)
        except Exception:
            parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    # Strip None / empty values so downstream code can use .get() safely
    return {k: v for k, v in parsed.items() if v not in (None, "", [], {})}


def build_request_context(
    *,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    user: str | None = None,
    target_module: str | None = None,
    base: dict[str, Any] | None = None,
    inject_roles: bool = True,
) -> dict[str, Any]:
    """
    Build the canonical request context dict for a prompt turn.

    Improvements over v1:
      - Auto-derives module from doctype if target_module is not supplied.
      - Injects user roles (as a list) into context["roles"] when
        inject_roles=True (default on).
      - Sanitises all values.
    """
    context = dict(base or {})

    # ── Explicit overrides ────────────────────────────────────────────────────
    if doctype is not None:
        context["doctype"] = str(doctype).strip() or None
    if docname is not None:
        context["docname"] = str(docname).strip() or None
    if route is not None:
        context["route"] = str(route).strip() or None
    if user is not None:
        context["user"] = str(user).strip() or None

    # ── Module auto-derivation ────────────────────────────────────────────────
    effective_module = target_module or context.get("target_module") or context.get("module")
    if not effective_module:
        # Try to derive from doctype
        effective_module = _doctype_to_module(context.get("doctype"))
    if effective_module:
        context["target_module"] = str(effective_module).strip()
        context["module"] = context["target_module"]

    # ── Role injection ────────────────────────────────────────────────────────
    if inject_roles:
        effective_user = context.get("user") or frappe.session.user
        roles = _get_user_roles(effective_user)
        context["roles"] = roles
        context["user"] = effective_user

    # ── Strip falsy values ────────────────────────────────────────────────────
    return {k: v for k, v in context.items() if v not in (None, "", [], {})}
