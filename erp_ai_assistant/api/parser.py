import json
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
        "fields": [],
        "values": {},
        "count_target": None,
        "report_name": None,
        "needs_clarification": False,
        "clarification_question": None,
        "message": message,
    }


ALLOWED_INTENTS = {
    "create",
    "update",
    "read",
    "search",
    "count",
    "export",
    "workflow",
    "answer",
    "create_sales_order",
    "generate_pdf",
    "count_records",
    "export_employee_excel",
    "export_doctype_excel",
    "unknown",
}

INTENT_CANONICAL_MAP = {
    "create": "create",
    "new": "create",
    "add": "create",
    "make": "create",
    "update": "update",
    "change": "update",
    "modify": "update",
    "set": "update",
    "read": "read",
    "show": "read",
    "view": "read",
    "open": "read",
    "get": "read",
    "search": "search",
    "find": "search",
    "count": "count_records",
    "count_records": "count_records",
    "export": "export_doctype_excel",
    "download": "export_doctype_excel",
    "pdf": "generate_pdf",
    "generate_pdf": "generate_pdf",
    "workflow": "workflow",
    "answer": "answer",
    "create_sales_order": "create_sales_order",
    "export_employee_excel": "export_employee_excel",
    "export_doctype_excel": "export_doctype_excel",
    "unknown": "unknown",
}

DEFAULT_PARSE_SCHEMA = {
    "intent": "unknown",
    "doctype": None,
    "docname": None,
    "customer": None,
    "items": [],
    "filters": {},
    "fields": [],
    "values": {},
    "count_target": None,
    "report_name": None,
    "needs_clarification": False,
    "clarification_question": None,
    "confidence": 0.0,
    "message": None,
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


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_json_loads(text: str) -> dict[str, Any] | None:
    text = str(text or "").strip()
    if not text:
        return None
    try:
        loaded = json.loads(text)
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            loaded = json.loads(match.group(0))
            if isinstance(loaded, dict):
                return loaded
        except Exception:
            return None
    return None


def _normalize_doctype(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    lowered = raw.lower()
    return DOCTYPE_ALIASES.get(lowered, raw)


def _normalize_intent(value: Any) -> str:
    raw = str(value or "").strip().lower()
    intent = INTENT_CANONICAL_MAP.get(raw, raw)
    return intent if intent in ALLOWED_INTENTS else "unknown"


def _normalize_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        item_code = str(row.get("item_code") or row.get("item") or row.get("item_name") or "").strip()
        if not item_code:
            continue
        qty = row.get("qty", row.get("quantity", 1))
        rate = row.get("rate")
        normalized_row: dict[str, Any] = {
            "item_code": item_code,
            "qty": _coerce_float(qty, 1.0) or 1.0,
        }
        if rate not in (None, ""):
            normalized_row["rate"] = _coerce_float(rate, 0.0)
        normalized.append(normalized_row)
    return normalized


def _normalize_fields(fields: Any) -> list[str]:
    if not isinstance(fields, list):
        return []
    out: list[str] = []
    for field in fields:
        value = str(field or "").strip()
        if value:
            out.append(value)
    return out


def _normalize_filters(filters: Any) -> dict[str, Any]:
    return filters if isinstance(filters, dict) else {}


def _normalize_values(values: Any) -> dict[str, Any]:
    return values if isinstance(values, dict) else {}


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


def _postprocess_sales_order(parsed: dict[str, Any]) -> dict[str, Any]:
    lowered_doctype = str(parsed.get("doctype") or "").strip().lower()
    if parsed["intent"] == "create" and lowered_doctype == "sales order":
        parsed["intent"] = "create_sales_order"
    if parsed["intent"] == "create_sales_order":
        parsed["doctype"] = "Sales Order"
    customer = parsed.get("customer")
    if not customer and isinstance(parsed.get("values"), dict):
        customer = parsed["values"].get("customer")
        if customer:
            parsed["customer"] = customer
    return parsed


def _postprocess_exports(parsed: dict[str, Any], normalized_prompt: str) -> dict[str, Any]:
    if parsed["intent"] == "export_doctype_excel" and parsed.get("doctype") == "Employee":
        parsed["intent"] = "export_employee_excel"
    if parsed["intent"] in {"export", "create"} and any(
        term in normalized_prompt.lower() for term in ("excel", "xlsx", "csv", "spreadsheet")
    ):
        parsed["intent"] = "export_doctype_excel"
        if parsed.get("doctype") == "Employee":
            parsed["intent"] = "export_employee_excel"
    return parsed


def _postprocess_pdf(parsed: dict[str, Any], normalized_prompt: str) -> dict[str, Any]:
    if parsed["intent"] == "generate_pdf":
        return parsed
    if any(term in normalized_prompt.lower() for term in ("pdf", "print", "printable")):
        if parsed.get("doctype") or parsed.get("docname"):
            parsed["intent"] = "generate_pdf"
    return parsed


def _finalize_llm_parse(raw: dict[str, Any], normalized_prompt: str) -> dict[str, Any]:
    parsed = dict(DEFAULT_PARSE_SCHEMA)
    parsed["intent"] = _normalize_intent(raw.get("intent"))
    parsed["doctype"] = _normalize_doctype(raw.get("doctype"))
    parsed["docname"] = str(raw.get("docname") or "").strip() or None
    parsed["customer"] = str(raw.get("customer") or "").strip() or None
    parsed["items"] = _normalize_items(raw.get("items"))
    parsed["filters"] = _normalize_filters(raw.get("filters"))
    parsed["fields"] = _normalize_fields(raw.get("fields"))
    parsed["values"] = _normalize_values(raw.get("values"))
    parsed["count_target"] = str(raw.get("count_target") or "").strip() or None
    parsed["report_name"] = str(raw.get("report_name") or "").strip() or None
    parsed["needs_clarification"] = bool(raw.get("needs_clarification", False))
    parsed["clarification_question"] = str(raw.get("clarification_question") or "").strip() or None
    parsed["confidence"] = max(0.0, min(_coerce_float(raw.get("confidence"), 0.0), 1.0))
    parsed["message"] = str(raw.get("message") or "").strip() or None

    parsed = _postprocess_sales_order(parsed)
    parsed = _postprocess_exports(parsed, normalized_prompt)
    parsed = _postprocess_pdf(parsed, normalized_prompt)

    if not parsed.get("customer"):
        parsed["customer"] = extract_customer(normalized_prompt)
    if not parsed.get("items"):
        parsed["items"] = extract_items(normalized_prompt)
    if not parsed.get("doctype") and parsed["intent"] in {"generate_pdf", "export_doctype_excel", "export_employee_excel"}:
        parsed["doctype"] = extract_export_doctype(normalized_prompt)
    if parsed["intent"] == "generate_pdf" and not parsed.get("docname"):
        doctype, docname = extract_doctype_and_docname(normalized_prompt)
        parsed["doctype"] = parsed.get("doctype") or doctype
        parsed["docname"] = parsed.get("docname") or docname
    if parsed["intent"] == "count_records" and not parsed.get("count_target"):
        parsed["count_target"] = extract_count_target(normalized_prompt)
    return parsed


def _build_llm_parse_prompt(prompt: str, context: dict[str, Any] | None = None) -> str:
    current = context or {}
    allowed_doctypes = sorted(set(DOCTYPE_ALIASES.values()))
    return f"""
You are an ERPNext intent parser.

Convert the user's message into STRICT JSON only.
Do not wrap the JSON in markdown.
Do not add commentary.
Return exactly one JSON object.

Allowed intents:
- create
- update
- read
- search
- count_records
- export_doctype_excel
- export_employee_excel
- generate_pdf
- workflow
- answer
- create_sales_order
- unknown

Known ERP doctypes:
{json.dumps(allowed_doctypes)}

Current UI context:
{json.dumps(current, ensure_ascii=False)}

User prompt:
{json.dumps(prompt, ensure_ascii=False)}

Return schema:
{{
  "intent": "unknown",
  "doctype": null,
  "docname": null,
  "customer": null,
  "items": [],
  "filters": {{}},
  "fields": [],
  "values": {{}},
  "count_target": null,
  "report_name": null,
  "needs_clarification": false,
  "clarification_question": null,
  "confidence": 0.0,
  "message": null
}}

Rules:
1. Prefer create_sales_order for Sales Order creation requests.
2. Prefer generate_pdf for print/pdf requests.
3. Prefer export_doctype_excel or export_employee_excel for excel/xlsx/csv/spreadsheet requests.
4. Use count_records only for counting requests.
5. Never invent document names.
6. Never invent item codes.
7. If required information is missing, set needs_clarification=true.
8. confidence must be between 0 and 1.
9. Output valid JSON only.
""".strip()


def _call_llm_json_parser(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        from . import ai as ai_api

        if not ai_api._llm_chat_configured():
            return None
        raw = ai_api.parse_prompt_with_model(_build_llm_parse_prompt(prompt, context=context))
        return _safe_json_loads(raw)
    except Exception:
        return None


def fallback_parse_prompt(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = normalize_prompt(prompt)
    current = normalize_context_payload(context)
    raw = _call_llm_json_parser(normalized, current)
    if not raw:
        return _empty_parse(normalized, ok=False, source="fallback", message="Could not parse prompt.")

    parsed = _finalize_llm_parse(raw, normalized)
    parsed["normalized_prompt"] = normalized
    parsed["source"] = "llm"

    if parsed["intent"] == "unknown":
        parsed["ok"] = False
        parsed["message"] = parsed.get("message") or "Could not confidently understand the request."
        return parsed

    if parsed["confidence"] < 0.65:
        parsed["ok"] = False
        parsed["needs_clarification"] = True
        parsed["clarification_question"] = (
            parsed.get("clarification_question")
            or "Please rephrase or add the missing ERP details so I can do this correctly."
        )
        parsed["message"] = parsed.get("message") or "Low confidence parse."
        return parsed

    parsed["ok"] = True
    parsed["message"] = None
    return parsed


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
