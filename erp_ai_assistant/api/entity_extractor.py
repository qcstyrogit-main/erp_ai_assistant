import re
from typing import Any

from .catalog import resolve_safe_doctype_from_text
from .intent_detector import normalize_prompt


def extract_natural_filters(text: str, config: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    lowered = str(text or "").lower()
    context = context or {}
    filter_fields = set(config.get("filter_fields") or set())
    filters: dict[str, Any] = {}

    if "status" in filter_fields and " active " in f" {lowered} ":
        filters["status"] = "Active"

    year_match = re.search(r"\b(20\d{2}|19\d{2})\b", str(text or ""))
    if year_match:
        year = str(year_match.group(1))
        for date_field in ("posting_date", "transaction_date"):
            if date_field in filter_fields:
                filters[date_field] = ["between", [f"{year}-01-01", f"{year}-12-31"]]
                break

    customer_match = re.search(
        r"(?:for|of)\s+(?:customer\s+)?(?P<value>.+?)(?:\s+to\s+(?:excel|xlsx|csv|spreadsheet)|\s+(?:excel|xlsx|csv|spreadsheet)\b|$)",
        str(text or ""),
        re.IGNORECASE,
    )
    supplier_match = re.search(
        r"(?:for|of)\s+(?:supplier\s+)?(?P<value>.+?)(?:\s+to\s+(?:excel|xlsx|csv|spreadsheet)|\s+(?:excel|xlsx|csv|spreadsheet)\b|$)",
        str(text or ""),
        re.IGNORECASE,
    )
    customer_value = str(customer_match.group("value") or "").strip(" .?") if customer_match else ""
    supplier_value = str(supplier_match.group("value") or "").strip(" .?") if supplier_match else ""
    if customer_value.lower() in {"a customer", "the customer", "customer", "this customer"}:
        customer_value = ""
    if supplier_value.lower() in {"a supplier", "the supplier", "supplier", "this supplier"}:
        supplier_value = ""

    if "customer" in filter_fields:
        if not customer_value and str(context.get("doctype") or "").strip() == "Customer":
            customer_value = str(context.get("docname") or "").strip()
        if customer_value and not re.fullmatch(r"20\d{2}|19\d{2}", customer_value):
            filters["customer"] = customer_value

    if "supplier" in filter_fields:
        if not supplier_value and str(context.get("doctype") or "").strip() == "Supplier":
            supplier_value = str(context.get("docname") or "").strip()
        if supplier_value and not re.fullmatch(r"20\d{2}|19\d{2}", supplier_value):
            filters["supplier"] = supplier_value

    return filters


def extract_prompt_entities(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = normalize_prompt(prompt)
    safe_target = resolve_safe_doctype_from_text(normalized)
    target_key = safe_target[0] if safe_target else None
    target_config = safe_target[1] if safe_target else None
    target_doctype = str(target_config.get("doctype") or "").strip() if isinstance(target_config, dict) else None
    filters = extract_natural_filters(normalized, target_config or {}, context=context or {}) if target_config else {}
    export_requested = any(term in normalized.lower() for term in ("excel", "xlsx", "spreadsheet", "csv", "download file", "make sheet", "save as excel"))
    return {
        "normalized_prompt": normalized,
        "target_key": target_key,
        "target_doctype": target_doctype,
        "filters": filters,
        "export_requested": export_requested,
        "context_doctype": str((context or {}).get("doctype") or "").strip() or None,
        "context_docname": str((context or {}).get("docname") or "").strip() or None,
    }
