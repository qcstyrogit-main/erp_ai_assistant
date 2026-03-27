import re
from typing import Any

import frappe


SAFE_ERP_DOCTYPES: dict[str, dict[str, Any]] = {
    "employee": {
        "doctype": "Employee",
        "title": "Employee List",
        "fields": ["name", "employee_name", "department", "designation", "status", "company"],
        "filter_fields": {"status", "company", "department", "designation"},
        "search_fields": ["name", "employee_name", "company_email", "cell_number"],
    },
    "customer": {
        "doctype": "Customer",
        "title": "Customer List",
        "fields": ["name", "customer_name", "customer_type", "customer_group", "territory", "disabled"],
        "filter_fields": {"customer_type", "customer_group", "territory", "disabled"},
        "search_fields": ["name", "customer_name", "territory"],
    },
    "item": {
        "doctype": "Item",
        "title": "Item List",
        "fields": ["name", "item_name", "item_group", "stock_uom", "disabled"],
        "filter_fields": {"item_group", "stock_uom", "disabled"},
        "search_fields": ["name", "item_name", "item_group", "description"],
    },
    "supplier": {
        "doctype": "Supplier",
        "title": "Supplier List",
        "fields": ["name", "supplier_name", "supplier_group", "supplier_type", "country", "disabled"],
        "filter_fields": {"supplier_group", "supplier_type", "country", "disabled"},
        "search_fields": ["name", "supplier_name", "supplier_group"],
    },
    "lead": {
        "doctype": "Lead",
        "title": "Lead List",
        "fields": ["name", "lead_name", "company_name", "status", "source"],
        "filter_fields": {"status", "source"},
        "search_fields": ["name", "lead_name", "company_name", "email_id"],
    },
    "opportunity": {
        "doctype": "Opportunity",
        "title": "Opportunity List",
        "fields": ["name", "party_name", "status", "opportunity_type", "company"],
        "filter_fields": {"status", "opportunity_type", "company"},
        "search_fields": ["name", "party_name", "title"],
    },
    "sales order": {
        "doctype": "Sales Order",
        "title": "Sales Order List",
        "fields": ["name", "customer", "transaction_date", "delivery_date", "status", "company", "grand_total"],
        "filter_fields": {"customer", "status", "company", "grand_total", "base_grand_total"},
        "search_fields": ["name", "customer", "customer_name", "company"],
        "party_field": "customer",
        "company_field": "company",
    },
    "quotation": {
        "doctype": "Quotation",
        "title": "Quotation List",
        "fields": ["name", "party_name", "transaction_date", "status", "company", "grand_total"],
        "filter_fields": {"party_name", "status", "company", "grand_total", "base_grand_total"},
        "search_fields": ["name", "party_name", "company"],
        "party_field": "party_name",
        "company_field": "company",
    },
    "sales invoice": {
        "doctype": "Sales Invoice",
        "title": "Sales Invoice List",
        "fields": ["name", "customer", "posting_date", "status", "company", "grand_total", "outstanding_amount"],
        "filter_fields": {"customer", "status", "company", "grand_total", "base_grand_total", "outstanding_amount"},
        "search_fields": ["name", "customer", "customer_name", "company"],
    },
    "purchase order": {
        "doctype": "Purchase Order",
        "title": "Purchase Order List",
        "fields": ["name", "supplier", "transaction_date", "status", "company", "grand_total"],
        "filter_fields": {"supplier", "status", "company", "grand_total", "base_grand_total"},
        "search_fields": ["name", "supplier", "company"],
        "party_field": "supplier",
        "company_field": "company",
    },
    "purchase invoice": {
        "doctype": "Purchase Invoice",
        "title": "Purchase Invoice List",
        "fields": ["name", "supplier", "posting_date", "status", "company", "grand_total", "outstanding_amount"],
        "filter_fields": {"supplier", "status", "company", "grand_total", "base_grand_total", "outstanding_amount"},
        "search_fields": ["name", "supplier", "company"],
    },
    "delivery note": {
        "doctype": "Delivery Note",
        "title": "Delivery Note List",
        "fields": ["name", "customer", "posting_date", "status", "company", "grand_total"],
        "filter_fields": {"customer", "status", "company", "grand_total", "base_grand_total"},
        "search_fields": ["name", "customer", "customer_name", "company"],
    },
    "purchase receipt": {
        "doctype": "Purchase Receipt",
        "title": "Purchase Receipt List",
        "fields": ["name", "supplier", "posting_date", "status", "company", "grand_total"],
        "filter_fields": {"supplier", "status", "company", "grand_total", "base_grand_total"},
        "search_fields": ["name", "supplier", "company"],
    },
}


SAFE_ERP_ALIASES = {
    "employees": "employee",
    "employee": "employee",
    "customers": "customer",
    "customer": "customer",
    "items": "item",
    "item": "item",
    "suppliers": "supplier",
    "supplier": "supplier",
    "leads": "lead",
    "lead": "lead",
    "opportunities": "opportunity",
    "opportunity": "opportunity",
    "sales orders": "sales order",
    "sales order": "sales order",
    "sales quotations": "quotation",
    "sales quotation": "quotation",
    "quotations": "quotation",
    "quotation": "quotation",
    "sales invoices": "sales invoice",
    "sales invoice": "sales invoice",
    "purchase orders": "purchase order",
    "purchase order": "purchase order",
    "purchase invoices": "purchase invoice",
    "purchase invoice": "purchase invoice",
    "delivery notes": "delivery note",
    "delivery note": "delivery note",
    "purchase receipts": "purchase receipt",
    "purchase receipt": "purchase receipt",
}


def get_safe_doctype_config(target: str | None) -> dict[str, Any] | None:
    normalized = str(target or "").strip().lower()
    if not normalized:
        return None
    key = SAFE_ERP_ALIASES.get(normalized, normalized)
    return SAFE_ERP_DOCTYPES.get(key)


def resolve_safe_doctype_from_text(text: str | None) -> tuple[str, dict[str, Any]] | None:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return None
    aliases = sorted(SAFE_ERP_ALIASES.keys(), key=len, reverse=True)
    for alias in aliases:
        if re.search(rf"\b{re.escape(alias)}\b", normalized):
            key = SAFE_ERP_ALIASES[alias]
            config = SAFE_ERP_DOCTYPES.get(key)
            if config:
                return key, config
    return None


def get_available_safe_doctypes() -> list[dict[str, str]]:
    rows = []
    for key, config in SAFE_ERP_DOCTYPES.items():
        doctype = str(config.get("doctype") or "").strip()
        if doctype and frappe.db.exists("DocType", doctype):
            rows.append({"key": key, "doctype": doctype})
    return rows
