import re
from typing import Any

from .context_resolver import normalize_context_payload
from .intent_detector import normalize_prompt as _base_normalize_prompt


DOCTYPE_ALIASES = {
    "employees": "Employee",
    "employee": "Employee",
    "customers": "Customer",
    "customer": "Customer",
    "items": "Item",
    "item": "Item",
    "sales order": "Sales Order",
    "sales orders": "Sales Order",
    "so": "Sales Order",
    "sales invoice": "Sales Invoice",
    "sales invoices": "Sales Invoice",
    "invoice": "Sales Invoice",
    "invoices": "Sales Invoice",
    "quotation": "Quotation",
    "quotations": "Quotation",
    "quote": "Quotation",
    "quotes": "Quotation",
}

ACTION_ALIASES = {
    "create": {"create", "make", "add", "generate"},
    "export": {"export", "download"},
    "pdf": {"pdf", "print", "printable"},
    "count": {"count", "how many", "number of", "total"},
}

EXPORT_FIELD_MARKERS = (
    "with fields",
    "with field",
    "including fields",
    "including field",
    "including",
    "include fields",
    "include field",
    "include",
    "columns",
    "column",
    "only fields",
    "only field",
    "only",
)


def _empty_parse(normalized_prompt: str, *, ok: bool = False, source: str = "rules", message: str | None = None) -> dict[str, Any]:
    return {
        "ok": ok,
        "intent": "unknown" if not ok else "unknown",
        "source": source,
        "confidence": 0.0,
        "normalized_prompt": normalized_prompt,
        "doctype": None,
        "docname": None,
        "customer": None,
        "items": [],
        "filters": {},
        "count_target": None,
        "report_name": None,
        "needs_clarification": False,
        "clarification_question": None,
        "message": message,
    }


def normalize_prompt(prompt: str) -> str:
    text = _base_normalize_prompt(prompt)
    replacements = (
        (r"\bordr\b", "order"),
        (r"\bgen\b", "generate"),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    if re.search(r"\b(emp|emps)\b", text, re.IGNORECASE):
        text = re.sub(r"\bemps\b", "employees", text, flags=re.IGNORECASE)
        text = re.sub(r"\bemp\b", "employee", text, flags=re.IGNORECASE)

    if re.search(r"\bso\b", text, re.IGNORECASE) and any(
        word in text.lower() for word in ("create", "make", "generate", "new", "add")
    ):
        text = re.sub(r"\bso\b", "sales order", text, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", text).strip()


def _normalize_count_alias(label: str) -> str | None:
    lowered = str(label or "").strip().lower()
    mapping = {
        "employees": "employees",
        "employee": "employees",
        "active employees": "active employees",
        "active employee": "active employees",
        "customers": "customers",
        "customer": "customers",
        "items": "items",
        "item": "items",
    }
    return mapping.get(lowered)


def extract_customer(prompt: str) -> str | None:
    text = normalize_prompt(prompt)
    patterns = (
        r"(?:for|customer)\s+(?:customer\s+)?(?P<customer>.+?)(?:\s+with\s+items|\s+items|$)",
        r"sales order for (?P<customer>.+?)(?:\s+with\s+items|\s+items|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = str(match.group("customer") or "").strip(" .")
            if value:
                return value
    return None


def extract_items(prompt: str) -> list[dict[str, Any]]:
    text = normalize_prompt(prompt)
    items_clause = ""
    for marker in (" with items ", " items ", " with item "):
        lowered = text.lower()
        if marker in lowered:
            index = lowered.find(marker)
            items_clause = text[index + len(marker):].strip()
            break
    if not items_clause:
        return []

    rows: list[dict[str, Any]] = []
    for raw_part in re.split(r"\s*,\s*", items_clause):
        part = raw_part.strip()
        if not part:
            continue
        match = re.match(
            r"(?P<item_code>[\w./-]+)\s*(?:(?:qty|quantity)\s*(?P<qty_a>\d+(?:\.\d+)?)|x\s*(?P<qty_b>\d+(?:\.\d+)?))?(?:\s*@\s*(?P<rate>\d+(?:\.\d+)?))?$",
            part,
            re.IGNORECASE,
        )
        if not match:
            continue
        qty = match.group("qty_a") or match.group("qty_b") or "1"
        row = {
            "item_code": str(match.group("item_code") or "").strip(),
            "qty": float(qty),
        }
        if match.group("rate"):
            row["rate"] = float(match.group("rate"))
        if row["item_code"]:
            rows.append(row)
    return rows


def extract_doctype_and_docname(prompt: str) -> tuple[str | None, str | None]:
    text = normalize_prompt(prompt)
    patterns = [
        (r"(?:generate|create|export|download|print)\s+pdf\s+(?:for|of)?\s*(sales invoice|quotation|sales order)\s+(?P<docname>[A-Za-z0-9./_-]+)", True),
        (r"(?:print|open|show|get|view)\s+(sales invoice|quotation|sales order)\s+(?P<docname>[A-Za-z0-9./_-]+)", True),
    ]
    for pattern, uses_group1 in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        raw_doctype = str(match.group(1) or "").strip().lower() if uses_group1 else ""
        docname = str(match.group("docname") or "").strip()
        return DOCTYPE_ALIASES.get(raw_doctype, raw_doctype.title() if raw_doctype else None), docname or None
    return None, None


def extract_count_target(prompt: str) -> str | None:
    text = normalize_prompt(prompt)
    lowered = text.lower()
    patterns = (
        r"^how many (?P<label>.+)$",
        r"^count (?P<label>.+)$",
        r"^number of (?P<label>.+)$",
        r"^total (?P<label>.+)$",
    )
    for pattern in patterns:
        match = re.match(pattern, lowered)
        if match:
            return _normalize_count_alias(str(match.group("label") or "").strip(" ?."))
    for label in ("active employees", "employees", "customers", "items"):
        if label in lowered:
            return label
    return None


def extract_export_doctype(prompt: str) -> str | None:
    text = normalize_prompt(prompt).lower()
    for alias in sorted(DOCTYPE_ALIASES.keys(), key=len, reverse=True):
        if re.search(rf"\b{re.escape(alias)}\b", text):
            return DOCTYPE_ALIASES[alias]
    return None


def extract_requested_fields(prompt: str) -> list[str]:
    text = normalize_prompt(prompt)
    lowered = text.lower()
    clause = ""
    for marker in EXPORT_FIELD_MARKERS:
        token = f" {marker} "
        if token in f" {lowered} ":
            start = lowered.find(marker) + len(marker)
            clause = text[start:].strip(" .")
            break
    if not clause:
        return []

    clause = re.sub(
        r"\b(?:to|as)\s+(?:excel|xlsx|csv|spreadsheet)\b.*$",
        "",
        clause,
        flags=re.IGNORECASE,
    ).strip(" .,")
    clause = re.sub(r"\b(?:excel|xlsx|csv|spreadsheet)\b$", "", clause, flags=re.IGNORECASE).strip(" .,")
    if not clause:
        return []

    parts = [
        re.sub(r"^(?:and|with)\s+", "", chunk.strip(), flags=re.IGNORECASE)
        for chunk in re.split(r"\s*,\s*|\s+and\s+", clause)
    ]
    return [part for part in parts if part]


def resolve_context(parsed: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    current = normalize_context_payload(context)
    intent = str(parsed.get("intent") or "").strip().lower()
    if intent == "generate_pdf":
        if not parsed.get("doctype"):
            parsed["doctype"] = current.get("doctype")
        if not parsed.get("docname"):
            parsed["docname"] = current.get("docname")
    return parsed


def apply_clarification_rules(parsed: dict[str, Any]) -> dict[str, Any]:
    intent = str(parsed.get("intent") or "").strip().lower()
    if intent == "create_sales_order":
        missing = []
        if not parsed.get("customer"):
            missing.append("customer")
        if not parsed.get("items"):
            missing.append("items")
        if missing:
            parsed["needs_clarification"] = True
            parsed["clarification_question"] = (
                "Please provide the customer and at least one item for the Sales Order."
                if len(missing) > 1
                else f"Please provide the missing {missing[0]} for the Sales Order."
            )
    elif intent == "generate_pdf":
        missing = []
        if not parsed.get("doctype"):
            missing.append("doctype")
        if not parsed.get("docname"):
            missing.append("document name")
        if missing:
            parsed["needs_clarification"] = True
            parsed["clarification_question"] = "Please specify which document PDF you want, for example: generate pdf for sales invoice SINV-0001."
    return parsed


def fallback_parse_prompt(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = normalize_prompt(prompt)
    # Future LLM/Ollama/FAC/MCP JSON parser plugs in here.
    return _empty_parse(normalized, ok=False, source="fallback", message="Could not parse prompt.")


def parse_prompt(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = normalize_prompt(prompt)
    parsed = _empty_parse(normalized, ok=False, source="rules", message="Could not parse prompt.")
    current = normalize_context_payload(context)
    lowered = normalized.lower()

    count_target = extract_count_target(normalized)
    if count_target:
        parsed.update(
            {
                "ok": True,
                "intent": "count_records",
                "source": "rules",
                "confidence": 0.95,
                "count_target": count_target,
                "message": None,
            }
        )
        return apply_clarification_rules(resolve_context(parsed, current))

    if any(term in lowered for term in ("excel", "xlsx", "spreadsheet", "download", "csv", "sheet")):
        export_doctype = extract_export_doctype(normalized)
        if export_doctype:
            export_intent = "export_employee_excel" if export_doctype == "Employee" else "export_doctype_excel"
            parsed.update(
                {
                    "ok": True,
                    "intent": export_intent,
                    "source": "rules",
                    "confidence": 0.94,
                    "doctype": export_doctype,
                    "filters": {"status": "Active"} if export_doctype == "Employee" and "active" in lowered else {},
                    "fields": extract_requested_fields(normalized),
                    "message": None,
                }
            )
            return apply_clarification_rules(resolve_context(parsed, current))

    if any(term in lowered for term in ("employee", "employees")) and any(term in lowered for term in ("excel", "xlsx", "spreadsheet", "download")):
        parsed.update(
            {
                "ok": True,
                "intent": "export_employee_excel",
                "source": "rules",
                "confidence": 0.94,
                "doctype": "Employee",
                "filters": {"status": "Active"} if "active" in lowered else {},
                "fields": extract_requested_fields(normalized),
                "message": None,
            }
        )
        return apply_clarification_rules(resolve_context(parsed, current))

    if any(term in lowered for term in ("pdf", "print", "printable")):
        doctype, docname = extract_doctype_and_docname(normalized)
        parsed.update(
            {
                "ok": True,
                "intent": "generate_pdf",
                "source": "rules",
                "confidence": 0.9,
                "doctype": doctype,
                "docname": docname,
                "message": None,
            }
        )
        return apply_clarification_rules(resolve_context(parsed, current))

    if "sales order" in lowered and any(word in lowered for word in ("create", "make", "add", "generate")):
        parsed.update(
            {
                "ok": True,
                "intent": "create_sales_order",
                "source": "rules",
                "confidence": 0.93,
                "doctype": "Sales Order",
                "customer": extract_customer(normalized),
                "items": extract_items(normalized),
                "message": None,
            }
        )
        return apply_clarification_rules(resolve_context(parsed, current))

    fallback = fallback_parse_prompt(prompt, current)
    if fallback.get("ok"):
        return apply_clarification_rules(resolve_context(fallback, current))
    return fallback
