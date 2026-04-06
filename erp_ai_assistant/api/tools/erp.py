import json
import os
import re
from typing import Any

import frappe
from frappe import _
from frappe.utils.data import get_url_to_form
from frappe.utils import getdate, nowdate

from ..catalog import get_safe_doctype_config, resolve_safe_doctype_from_text


SAFE_COUNT_QUERIES = {
    "active employees": ("Employee", {"status": "Active"}, "active employees"),
    "employees": ("Employee", None, "employees"),
    "customers": ("Customer", None, "customers"),
    "items": ("Item", None, "items"),
    "suppliers": ("Supplier", None, "suppliers"),
    "sales orders": ("Sales Order", None, "sales orders"),
    "sales invoices": ("Sales Invoice", None, "sales invoices"),
}


def _error(message: str, *, error_type: str = "validation_error", extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {"ok": False, "type": "error", "error_type": error_type, "message": message}
    if extra:
        payload.update(extra)
    return payload


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"", "0", "false", "no", "off"}


def _normalized_match_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _candidate_match_score(hint: str, row: dict[str, Any], search_fields: list[str] | None = None) -> int:
    normalized_hint = _normalized_match_text(hint)
    if not normalized_hint:
        return 0
    fields = ["name"] + list(search_fields or [])
    values: list[str] = []
    seen: set[str] = set()
    for fieldname in fields:
        value = str((row or {}).get(fieldname) or "").strip()
        if not value:
            continue
        normalized = _normalized_match_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)

    best = 0
    for value in values:
        if value == normalized_hint:
            best = max(best, 400 if value == _normalized_match_text((row or {}).get("name")) else 320)
            continue
        if value.startswith(normalized_hint):
            best = max(best, 240 if value == _normalized_match_text((row or {}).get("name")) else 180)
            continue
        if normalized_hint in value:
            best = max(best, 120 if value == _normalized_match_text((row or {}).get("name")) else 90)
    return best


def _pick_best_candidate(hint: str, candidates: list[dict[str, Any]], search_fields: list[str] | None = None) -> dict[str, Any] | None:
    if not _env_flag("ERP_AI_AUTO_SELECT_LINKS", True):
        return None
    if not candidates:
        return None
    scored: list[tuple[int, dict[str, Any]]] = []
    for row in candidates:
        score = _candidate_match_score(hint, row, search_fields=search_fields)
        if score > 0:
            scored.append((score, row))
    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    top_score, top_row = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else -1
    if top_score >= 300:
        return top_row
    if top_score >= 180 and second_score < top_score:
        return top_row
    if top_score >= 120 and second_score <= 0 and len(scored) == 1:
        return top_row
    return None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_bank_account_value(value: Any) -> str | None:
    account = str(value or "").strip()
    if not account:
        return None
    if frappe.db.exists("Bank Account", account):
        linked_account = frappe.db.get_value("Bank Account", account, "account")
        return str(linked_account or "").strip() or None
    if frappe.db.exists("Account", account):
        return account
    return None


def _is_bank_or_cash_account(account: Any) -> bool:
    resolved = _resolve_bank_account_value(account)
    if not resolved:
        return False
    account_type = str(frappe.db.get_value("Account", resolved, "account_type") or "").strip()
    return account_type in {"Bank", "Cash"}


def _extract_payment_entry_reference(values: dict[str, Any]) -> tuple[str, str]:
    references = values.get("references")
    if isinstance(references, list):
        for row in references:
            if not isinstance(row, dict):
                continue
            ref_doctype = str(row.get("reference_doctype") or row.get("voucher_type") or "").strip()
            ref_name = str(row.get("reference_name") or row.get("voucher_no") or "").strip()
            if ref_doctype and ref_name:
                return ref_doctype, ref_name

    explicit_doctype = str(
        values.get("reference_doctype")
        or values.get("against_voucher_type")
        or values.get("source_doctype")
        or values.get("linked_invoice_doctype")
        or ""
    ).strip()
    explicit_name = str(
        values.get("reference_name")
        or values.get("against_voucher")
        or values.get("source_name")
        or values.get("linked_invoice")
        or ""
    ).strip()
    if explicit_doctype and explicit_name:
        return explicit_doctype, explicit_name

    for key, doctype in (
        ("sales_invoice", "Sales Invoice"),
        ("purchase_invoice", "Purchase Invoice"),
        ("sales_order", "Sales Order"),
        ("purchase_order", "Purchase Order"),
    ):
        name = str(values.get(key) or "").strip()
        if name:
            return doctype, name

    fallback_name = str(values.get("invoice") or values.get("invoice_name") or "").strip()
    if fallback_name:
        party_type = str(values.get("party_type") or "").strip().lower()
        payment_type = str(values.get("payment_type") or "").strip().lower()
        if party_type == "supplier" or payment_type == "pay":
            return "Purchase Invoice", fallback_name
        return "Sales Invoice", fallback_name

    return "", ""


def _extract_payment_entry_bank_account(values: dict[str, Any], payment_type: str) -> str | None:
    candidates: list[Any] = [
        values.get("bank_account"),
        values.get("bank"),
        values.get("company_bank_account"),
    ]
    if payment_type == "Receive":
        candidates.extend([values.get("paid_to"), values.get("paid_from")])
    elif payment_type == "Pay":
        candidates.extend([values.get("paid_from"), values.get("paid_to")])
    else:
        candidates.extend([values.get("paid_to"), values.get("paid_from")])

    for candidate in candidates:
        if _is_bank_or_cash_account(candidate):
            return _resolve_bank_account_value(candidate)

    for candidate in candidates:
        resolved = _resolve_bank_account_value(candidate)
        if resolved:
            return resolved
    return None


def _create_payment_entry_from_reference(values: dict[str, Any]) -> dict[str, Any] | None:
    reference_doctype, reference_name = _extract_payment_entry_reference(values)
    if not reference_doctype or not reference_name:
        return None

    from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry

    payment_type = str(values.get("payment_type") or "").strip() or None
    paid_amount = _as_float(values.get("paid_amount"))
    if paid_amount is None:
        paid_amount = _as_float(values.get("received_amount"))
    bank_account = _extract_payment_entry_bank_account(values, payment_type or "")
    reference_date = values.get("reference_date") or values.get("posting_date") or nowdate()

    pe = get_payment_entry(
        reference_doctype,
        reference_name,
        party_amount=paid_amount,
        bank_account=bank_account,
        payment_type=payment_type,
        reference_date=reference_date,
    )

    posting_date = values.get("posting_date")
    if posting_date:
        pe.posting_date = getdate(posting_date)
    if values.get("reference_date"):
        pe.reference_date = getdate(values.get("reference_date"))
    if values.get("mode_of_payment"):
        pe.mode_of_payment = values.get("mode_of_payment")
    if values.get("reference_no"):
        pe.reference_no = str(values.get("reference_no")).strip()
    if values.get("remarks"):
        pe.remarks = str(values.get("remarks")).strip()

    pe.insert()
    return {
        "ok": True,
        "type": "document",
        "doctype": pe.doctype,
        "name": pe.name,
        "url": get_url_to_form(pe.doctype, pe.name),
        "message": f"{pe.doctype} created successfully",
        "data": {
            "reference_doctype": reference_doctype,
            "reference_name": reference_name,
            "payment_type": pe.payment_type,
            "paid_from": pe.paid_from,
            "paid_to": pe.paid_to,
        },
    }


def _parse_json_arg(value: Any, default: Any) -> Any:
    if value in (None, "", []):
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _normalize_question(question: str) -> str:
    return re.sub(r"\s+", " ", str(question or "").strip().lower())


def _match_count_query(question: str) -> tuple[str, dict[str, Any] | None, str] | None:
    text = _normalize_question(question)
    if not text:
        return None

    ordered_aliases = sorted(SAFE_COUNT_QUERIES.keys(), key=len, reverse=True)
    for alias in ordered_aliases:
        if alias in text:
            return SAFE_COUNT_QUERIES[alias]

    count_patterns = (
        r"^how many (?P<label>.+)$",
        r"^count (?P<label>.+)$",
        r"^what is the count of (?P<label>.+)$",
        r"^show (?P<label>.+) count$",
    )
    for pattern in count_patterns:
        match = re.match(pattern, text)
        if not match:
            continue
        label = str(match.group("label") or "").strip(" ?.")
        if label in SAFE_COUNT_QUERIES:
            return SAFE_COUNT_QUERIES[label]
    return None


def _require_doctype_permission(doctype: str, ptype: str) -> None:
    if not frappe.has_permission(doctype, ptype=ptype):
        raise frappe.PermissionError(_("Not permitted to {0} {1}").format(ptype, doctype))


def _sanitize_sales_order_items(items: Any) -> tuple[list[dict[str, Any]], str | None]:
    rows = _parse_json_arg(items, items)
    if not isinstance(rows, list) or not rows:
        return [], _("Items must be a non-empty list")

    cleaned: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            return [], _("Item row {0} must be an object").format(index)

        item_code = str(row.get("item_code") or "").strip()
        if not item_code:
            return [], _("Item row {0} is missing item_code").format(index)

        try:
            qty = float(row.get("qty") or 0)
        except (TypeError, ValueError):
            return [], _("Item row {0} has an invalid qty").format(index)
        if qty <= 0:
            return [], _("Item row {0} qty must be greater than zero").format(index)

        normalized = {"item_code": item_code, "qty": qty}
        if row.get("rate") not in (None, ""):
            try:
                normalized["rate"] = float(row.get("rate"))
            except (TypeError, ValueError):
                return [], _("Item row {0} has an invalid rate").format(index)
        cleaned.append(normalized)
    return cleaned, None


COMMON_FIELD_ALIASES = {
    "birthday": "date_of_birth",
    "birth date": "date_of_birth",
    "dob": "date_of_birth",
    "employee birthday": "date_of_birth",
    "customer name": "customer_name",
    "supplier name": "supplier_name",
    "item name": "item_name",
    "status": "status",
    "department": "department",
    "designation": "designation",
    "email": "email_id",
    "company email": "company_email",
    "mobile": "mobile_no",
    "phone": "phone",
    "territory": "territory",
}

DOCTYPE_SPECIFIC_FIELD_ALIASES = {
    "Customer": {
        "name": "customer_name",
        "customer": "customer_name",
    },
    "Supplier": {
        "name": "supplier_name",
        "supplier": "supplier_name",
    },
    "Employee": {
        "name": "employee_name",
        "employee": "employee_name",
    },
    "Item": {
        "name": "item_name",
        "item": "item_name",
    },
    "Lead": {
        "name": "lead_name",
        "lead": "lead_name",
    },
}

MUTATION_DOCTYPE_BLOCKLIST = {
    "DocType",
    "DocField",
    "Custom Field",
    "Property Setter",
    "Client Script",
    "Server Script",
    "Patch Log",
    "Scheduled Job Type",
    "Access Log",
    "Version",
    "File",
    "Error Log",
    "Desktop Icon",
}


def _safe_doctype_config(doctype_or_hint: str) -> dict[str, Any] | None:
    return get_safe_doctype_config(doctype_or_hint)


def resolve_doctype_name_internal(doctype_or_hint: str) -> str | None:
    hint = str(doctype_or_hint or "").strip()
    if not hint:
        return None
    if frappe.db.exists("DocType", hint):
        return hint
    config = _safe_doctype_config(hint)
    if config:
        return str(config.get("doctype") or "").strip() or None
    matched = resolve_safe_doctype_from_text(hint)
    if matched:
        _key, resolved_config = matched
        return str(resolved_config.get("doctype") or "").strip() or None
    rows = frappe.get_all(
        "DocType",
        filters={"istable": 0},
        or_filters=[
            ["DocType", "name", "like", hint],
            ["DocType", "name", "like", f"%{hint}%"],
        ],
        fields=["name"],
        limit_page_length=2,
        order_by="name asc",
    )
    if len(rows) == 1:
        return str(rows[0].get("name") or "").strip() or None
    return None


def _resolve_safe_doctype(doctype_or_hint: str) -> tuple[str, dict[str, Any]] | None:
    config = _safe_doctype_config(doctype_or_hint)
    if config:
        return str(config.get("doctype") or "").strip(), config
    matched = resolve_safe_doctype_from_text(doctype_or_hint)
    if matched:
        _key, resolved_config = matched
        return str(resolved_config.get("doctype") or "").strip(), resolved_config
    resolved_doctype = resolve_doctype_name_internal(str(doctype_or_hint or "").strip())
    if resolved_doctype and _is_general_mutation_allowed(resolved_doctype):
        return resolved_doctype, _dynamic_doctype_config(resolved_doctype)
    return None


def _listable_meta_fields(doctype: str) -> list[str]:
    preferred = [
        "name",
        "title",
        "status",
        "docstatus",
        "company",
        "posting_date",
        "transaction_date",
        "delivery_date",
        "supplier",
        "customer",
        "party_name",
        "employee",
        "employee_name",
        "item_code",
        "item_name",
        "warehouse",
        "set_warehouse",
        "territory",
        "outstanding_amount",
        "grand_total",
        "modified",
    ]
    meta = frappe.get_meta(doctype)
    fields: list[str] = []
    title_field = str(getattr(meta, "title_field", "") or "").strip()
    if title_field:
        preferred.insert(1, title_field)
    blocked_types = {
        "Section Break",
        "Column Break",
        "Tab Break",
        "HTML",
        "Button",
        "Image",
        "Attach Image",
        "Table",
        "Table MultiSelect",
        "Fold",
        "Heading",
        "Password",
        "Code",
        "Text Editor",
        "Long Text",
        "Small Text",
    }
    available_fields = {
        str(field.fieldname or "").strip(): str(field.fieldtype or "").strip()
        for field in meta.fields
        if str(field.fieldname or "").strip()
    }
    for fieldname in preferred:
        if fieldname == "name" and fieldname not in fields:
            fields.append(fieldname)
            continue
        if fieldname in available_fields and available_fields.get(fieldname) not in blocked_types and fieldname not in fields:
            fields.append(fieldname)
        if len(fields) >= 8:
            return fields
    for fieldname, fieldtype in available_fields.items():
        if fieldtype in blocked_types or fieldname in fields:
            continue
        fields.append(fieldname)
        if len(fields) >= 8:
            break
    return fields or ["name"]


def _filterable_fields_for_doctype(doctype: str) -> set[str]:
    meta = frappe.get_meta(doctype)
    allowed_types = {
        "Data",
        "Link",
        "Select",
        "Date",
        "Datetime",
        "Check",
        "Int",
        "Long Int",
        "Float",
        "Currency",
        "Percent",
        "Small Text",
        "Text",
        "Read Only",
    }
    fields = {"name", "owner", "modified_by", "creation", "modified", "docstatus", "status"}
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        fieldtype = str(field.fieldtype or "").strip()
        if fieldname and fieldtype in allowed_types:
            fields.add(fieldname)
    return fields


def _dynamic_doctype_config(doctype: str) -> dict[str, Any]:
    return {
        "doctype": doctype,
        "title": f"{doctype} List",
        "fields": _listable_meta_fields(doctype),
        "filter_fields": _filterable_fields_for_doctype(doctype),
        "search_fields": _searchable_fields_for_doctype(doctype, {}),
    }


def _resolve_doctype_from_query_text(text: str) -> tuple[str, dict[str, Any]] | None:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return None
    matched = resolve_safe_doctype_from_text(normalized)
    if matched:
        _key, resolved_config = matched
        resolved_doctype = str(resolved_config.get("doctype") or "").strip()
        if resolved_doctype:
            return resolved_doctype, resolved_config
    rows = frappe.get_all(
        "DocType",
        filters={"istable": 0},
        fields=["name"],
        limit_page_length=20,
        order_by="name asc",
    )
    best_match = None
    for row in rows:
        doctype = str(row.get("name") or "").strip()
        if not doctype or doctype in MUTATION_DOCTYPE_BLOCKLIST:
            continue
        if re.search(rf"\b{re.escape(doctype.lower())}\b", normalized):
            if best_match is None or len(doctype) > len(best_match):
                best_match = doctype
    if best_match:
        return best_match, _dynamic_doctype_config(best_match)
    return None


def _sanitize_filters(filters: Any, allowed_fields: set[str] | None = None) -> dict[str, Any]:
    parsed = _parse_json_arg(filters, {}) or {}
    if not isinstance(parsed, dict):
        return {}
    if not allowed_fields:
        return {key: value for key, value in parsed.items() if value not in (None, "", [])}
    return {key: value for key, value in parsed.items() if key in allowed_fields and value not in (None, "", [])}


def _searchable_fields_for_doctype(doctype: str, config: dict[str, Any]) -> list[str]:
    candidates = list(config.get("search_fields") or [])
    meta = frappe.get_meta(doctype)
    search_fields_raw = str(getattr(meta, "search_fields", "") or "").strip()
    if search_fields_raw:
        candidates.extend([part.strip() for part in search_fields_raw.split(",") if part.strip()])
    title_field = str(getattr(meta, "title_field", "") or "").strip()
    if title_field:
        candidates.append(title_field)
    candidates.extend(
        [
            "name",
            "title",
            "customer",
            "customer_name",
            "supplier",
            "supplier_name",
            "party_name",
            "item_code",
            "item_name",
            "employee",
            "employee_name",
            "company",
            "remarks",
            "description",
            "subject",
        ]
    )
    unique: list[str] = []
    for fieldname in candidates:
        if fieldname and fieldname not in unique and meta.has_field(fieldname):
            unique.append(fieldname)
        elif fieldname == "name" and fieldname not in unique:
            unique.append(fieldname)
    return unique


def _find_record_candidates(doctype: str, record_hint: str, config: dict[str, Any], *, limit: int = 5) -> list[dict[str, Any]]:
    hint = str(record_hint or "").strip()
    if not hint:
        return []
    search_fields = _searchable_fields_for_doctype(doctype, config)
    rows_by_name: dict[str, dict[str, Any]] = {}

    if frappe.db.exists(doctype, hint):
        rows_by_name[hint] = {"name": hint}

    for fieldname in search_fields:
        if fieldname == "name":
            continue
        exact = frappe.get_all(
            doctype,
            filters={fieldname: hint},
            fields=["name"] + [field for field in search_fields if field != "name"][:2],
            limit_page_length=limit,
            order_by="modified desc",
        )
        for row in exact:
            name = str(row.get("name") or "").strip()
            if name:
                rows_by_name[name] = row

    or_filters = [[doctype, fieldname, "like", f"%{hint}%"] for fieldname in search_fields]
    like_rows = frappe.get_all(
        doctype,
        or_filters=or_filters,
        fields=["name"] + [field for field in search_fields if field != "name"][:2],
        order_by="modified desc",
        limit_page_length=max(limit, 10),
    )
    for row in like_rows:
        name = str(row.get("name") or "").strip()
        if name and name not in rows_by_name:
            rows_by_name[name] = row

    return list(rows_by_name.values())[:limit]


def _resolve_record_name_detailed(doctype: str, record_hint: str, config: dict[str, Any]) -> dict[str, Any]:
    candidates = _find_record_candidates(doctype, record_hint, config, limit=5)
    if not candidates:
        return {"status": "not_found", "name": None, "candidates": []}
    if len(candidates) == 1:
        return {"status": "resolved", "name": str(candidates[0].get("name") or "").strip(), "candidates": candidates}
    exact_name = next((row for row in candidates if str(row.get("name") or "").strip() == str(record_hint or "").strip()), None)
    if exact_name:
        return {"status": "resolved", "name": str(exact_name.get("name") or "").strip(), "candidates": candidates}
    best = _pick_best_candidate(record_hint, candidates, search_fields=(config or {}).get("search_fields") or [])
    if best:
        return {"status": "resolved", "name": str(best.get("name") or "").strip(), "candidates": candidates}
    return {"status": "ambiguous", "name": None, "candidates": candidates}


def _resolve_record_name(doctype: str, record_hint: str, config: dict[str, Any]) -> str | None:
    details = _resolve_record_name_detailed(doctype, record_hint, config)
    return details.get("name") if details.get("status") == "resolved" else None


def _writable_meta_fields(doctype: str) -> dict[str, Any]:
    meta = frappe.get_meta(doctype)
    blocked_types = {
        "Section Break",
        "Column Break",
        "Tab Break",
        "HTML",
        "Button",
        "Image",
        "Fold",
        "Heading",
        "Table",
        "Table MultiSelect",
        "Attach",
        "Attach Image",
        "Read Only",
    }
    fields: dict[str, Any] = {}
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        if not fieldname or field.fieldtype in blocked_types:
            continue
        if getattr(field, "read_only", 0):
            continue
        if getattr(field, "set_only_once", 0):
            continue
        fields[fieldname] = field
    return fields


def _table_meta_fields(doctype: str) -> dict[str, Any]:
    meta = frappe.get_meta(doctype)
    fields: dict[str, Any] = {}
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        if not fieldname or str(field.fieldtype or "").strip() not in {"Table", "Table MultiSelect"}:
            continue
        fields[fieldname] = field
    return fields


def _table_alias_map(doctype: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for fieldname, field in _table_meta_fields(doctype).items():
        label = str(field.label or fieldname).strip().lower()
        normalized_fieldname = str(fieldname or "").strip().lower()
        if label:
            mapping[label] = fieldname
            singular = label[:-1] if label.endswith("s") else label
            mapping[singular] = fieldname
        mapping[normalized_fieldname] = fieldname
        mapping[normalized_fieldname.replace("_", " ")] = fieldname
    return mapping


def _is_general_mutation_allowed(doctype: str) -> bool:
    if not doctype or doctype in MUTATION_DOCTYPE_BLOCKLIST:
        return False
    if not frappe.db.exists("DocType", doctype):
        return False
    meta = frappe.get_meta(doctype)
    return not bool(getattr(meta, "istable", 0))


def _field_alias_map(doctype: str) -> dict[str, str]:
    mapping = dict(COMMON_FIELD_ALIASES)
    mapping.update(DOCTYPE_SPECIFIC_FIELD_ALIASES.get(doctype, {}))
    for fieldname, field in _writable_meta_fields(doctype).items():
        label = str(field.label or "").strip().lower()
        normalized_fieldname = str(fieldname or "").strip().lower()
        if label:
            mapping[label] = fieldname
        mapping[normalized_fieldname.replace("_", " ")] = fieldname
        mapping[normalized_fieldname] = fieldname
    return mapping


def _resolve_fieldname(doctype: str, field_hint: str) -> tuple[str | None, Any | None]:
    hint = str(field_hint or "").strip().lower()
    if not hint:
        return None, None
    fields = _writable_meta_fields(doctype)
    aliases = _field_alias_map(doctype)
    fieldname = aliases.get(hint)
    if not fieldname:
        aliases_by_length = sorted(aliases.keys(), key=len, reverse=True)
        for alias in aliases_by_length:
            if alias == hint or alias.endswith(hint) or hint.endswith(alias):
                fieldname = aliases[alias]
                break
    if not fieldname or fieldname not in fields:
        return None, None
    return fieldname, fields[fieldname]


def _coerce_field_value(field, value: Any) -> Any:
    raw = str(value or "").strip()
    fieldtype = str(getattr(field, "fieldtype", "") or "").strip()
    if fieldtype == "Date":
        return str(getdate(raw))
    if fieldtype == "Check":
        lowered = raw.lower()
        return 1 if lowered in {"1", "true", "yes", "on", "active"} else 0
    if fieldtype in {"Int", "Long Int"}:
        return int(float(raw))
    if fieldtype in {"Float", "Currency", "Percent"}:
        return float(raw)
    return raw


def _parse_update_value(raw_value: str) -> str:
    return str(raw_value or "").strip().strip("\"'")


def _extract_named_value_segments(doctype: str, raw_text: str) -> dict[str, Any]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {}
    aliases = _field_alias_map(resolved_doctype)
    fields = _writable_meta_fields(resolved_doctype)
    if not aliases or not fields:
        return {}
    pattern = re.compile(
        r"(?P<alias>" + "|".join(re.escape(alias) for alias in sorted(aliases.keys(), key=len, reverse=True)) + r")\s*(?:is|=|to|as|:)?\s*",
        re.IGNORECASE,
    )
    matches = list(pattern.finditer(str(raw_text or "").strip()))
    values: dict[str, Any] = {}
    if not matches:
        return values
    for index, match in enumerate(matches):
        alias = str(match.group("alias") or "").strip().lower()
        fieldname = aliases.get(alias)
        meta_field = fields.get(fieldname or "")
        if not fieldname or not meta_field:
            continue
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(str(raw_text or "").strip())
        raw_value = str(raw_text or "").strip()[start:end].strip(" ,;")
        if not raw_value:
            continue
        try:
            values[fieldname] = _coerce_field_value(meta_field, _parse_update_value(raw_value))
        except Exception:
            values[fieldname] = _parse_update_value(raw_value)
    return values


def extract_field_value_pairs_internal(doctype: str, raw_text: str) -> dict[str, Any]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {}
    clauses = [part.strip() for part in re.split(r"\s*[;,]\s*", str(raw_text or "").strip()) if part.strip()]
    values: dict[str, Any] = {}
    for clause in clauses:
        values.update(_extract_named_value_segments(resolved_doctype, clause))
    if not values:
        values.update(_extract_named_value_segments(resolved_doctype, raw_text))
    return values


def extract_child_table_rows_internal(doctype: str, raw_text: str) -> dict[str, list[dict[str, Any]]]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {}
    text = str(raw_text or "").strip()
    if not text:
        return {}
    table_aliases = _table_alias_map(resolved_doctype)
    table_fields = _table_meta_fields(resolved_doctype)
    rows_by_field: dict[str, list[dict[str, Any]]] = {}
    for alias in sorted(table_aliases.keys(), key=len, reverse=True):
        pattern = rf"\b(?:with|and)\s+{re.escape(alias)}\s+(?P<body>.+?)(?=(?:\s+\b(?:with|and)\b\s+[a-z])|$)"
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        fieldname = table_aliases[alias]
        table_field = table_fields.get(fieldname)
        child_doctype = str(getattr(table_field, "options", "") or "").strip()
        if not child_doctype:
            continue
        if fieldname == "items" or "item_code" in {f.fieldname for f in frappe.get_meta(child_doctype).fields}:
            parsed_rows = _parse_sales_order_items(match.group("body") or "")
        else:
            parsed_rows = []
            for raw_row in re.split(r"\s*;\s*", str(match.group("body") or "").strip()):
                parsed = extract_field_value_pairs_internal(child_doctype, raw_row)
                if parsed:
                    parsed_rows.append(parsed)
        if parsed_rows:
            rows_by_field[fieldname] = parsed_rows
    return rows_by_field


def extract_child_row_updates_internal(
    doctype: str,
    raw_text: str,
    *,
    default_table_fieldname: str | None = None,
) -> dict[str, dict[int, dict[str, Any]]]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {}
    text = str(raw_text or "").strip()
    if not text:
        return {}
    table_aliases = _table_alias_map(resolved_doctype)
    table_fields = _table_meta_fields(resolved_doctype)
    default_aliases = [alias for alias, fieldname in table_aliases.items() if fieldname == default_table_fieldname]
    updates: dict[str, dict[int, dict[str, Any]]] = {}
    clauses = [part.strip() for part in re.split(r"\s*;\s*", text) if part.strip()]
    for clause in clauses:
        matched = False
        for alias in sorted(table_aliases.keys(), key=len, reverse=True):
            pattern = rf"^(?:{re.escape(alias)}\s+)?row\s+(?P<index>\d+)\s+(?P<body>.+)$"
            match = re.match(pattern, clause, re.IGNORECASE)
            if not match:
                continue
            table_fieldname = table_aliases[alias]
            child_doctype = str(getattr(table_fields.get(table_fieldname), "options", "") or "").strip()
            row_values = extract_field_value_pairs_internal(child_doctype, str(match.group("body") or "").strip())
            if row_values:
                updates.setdefault(table_fieldname, {})[max(int(match.group("index") or 1) - 1, 0)] = row_values
            matched = True
            break
        if matched:
            continue
        if default_table_fieldname and default_aliases:
            match = re.match(r"^row\s+(?P<index>\d+)\s+(?P<body>.+)$", clause, re.IGNORECASE)
            if match:
                child_doctype = str(getattr(table_fields.get(default_table_fieldname), "options", "") or "").strip()
                row_values = extract_field_value_pairs_internal(child_doctype, str(match.group("body") or "").strip())
                if row_values:
                    updates.setdefault(default_table_fieldname, {})[max(int(match.group("index") or 1) - 1, 0)] = row_values
    return updates


def extract_update_instruction_internal(doctype: str, body: str, value: str) -> dict[str, str] | None:
    resolved = _resolve_safe_doctype(doctype)
    if not resolved:
        return None
    resolved_doctype, _config = resolved
    normalized_body = str(body or "").strip()
    if not normalized_body:
        return None
    aliases = _field_alias_map(resolved_doctype)
    body_lower = normalized_body.lower()
    for alias in sorted(aliases.keys(), key=len, reverse=True):
        if body_lower == alias:
            return {"record": "", "field": alias, "value": _parse_update_value(value)}
        if body_lower.endswith(f" {alias}"):
            record = normalized_body[: len(normalized_body) - len(alias)].strip(" ,-")
            if record:
                return {"record": record, "field": alias, "value": _parse_update_value(value)}
    return None


def _list_rows_to_answer(doctype: str, rows: list[dict[str, Any]], heading: str | None = None) -> str:
    title = heading or f"{doctype} list"
    if not rows:
        return f"## {title}\n\nNo records found."

    common_columns = [
        "name", "title", "status", "workflow_state", "posting_date", "transaction_date",
        "due_date", "customer", "supplier", "party_name", "employee_name", "item_name",
        "company", "grand_total", "outstanding_amount", "currency",
    ]
    columns: list[str] = []
    for column in common_columns:
        if any(isinstance(row, dict) and row.get(column) not in (None, "", [], {}) for row in rows):
            columns.append(column)
        if len(columns) >= 6:
            break
    if not columns:
        first = rows[0] if isinstance(rows[0], dict) else {}
        columns = [key for key in list(first.keys())[:6] if key]

    lines = [f"## {title}", "", f"Found **{len(rows)}** record{'s' if len(rows) != 1 else ''}.", ""]
    if columns:
        header = "| # | " + " | ".join(key.replace("_", " ").title() for key in columns) + " |"
        divider = "|---|" + "|".join(["---"] * len(columns)) + "|"
        lines.extend([header, divider])
        for index, row in enumerate(rows[:10], start=1):
            if not isinstance(row, dict):
                cell = str(row).replace("|", "\\|")
                lines.append(f"| {index} | {cell} |")
                continue
            cells = []
            for key in columns:
                value = row.get(key)
                cells.append("—" if value in (None, "", [], {}) else str(value).replace("|", "\\|"))
            lines.append(f"| {index} | " + " | ".join(cells) + " |")
        if len(rows) > 10:
            lines.extend(["", f"Showing first **10** of **{len(rows)}** records."])
    else:
        for index, row in enumerate(rows[:10], start=1):
            lines.append(f"{index}. {row}")
    return "\n".join(lines)


def _document_to_answer(doctype: str, payload: dict[str, Any]) -> str:
    name = str(payload.get("name") or "").strip()
    lines = [f"## {doctype}{f' — {name}' if name else ''}", ""]

    priority_fields = [
        "status", "workflow_state", "docstatus", "posting_date", "transaction_date", "due_date",
        "customer", "supplier", "party_name", "company", "currency", "grand_total",
        "rounded_total", "outstanding_amount",
    ]
    summary_rows: list[tuple[str, str]] = []
    for key in priority_fields:
        value = payload.get(key)
        if value in (None, "", [], {}):
            continue
        summary_rows.append((key.replace("_", " ").title(), str(value)))
        if len(summary_rows) >= 8:
            break

    if summary_rows:
        lines.extend(["### Summary", ""])
        for label, value in summary_rows:
            lines.append(f"- **{label}:** {value}")
        lines.append("")

    lines.extend(["### Details", ""])
    shown = 0
    for key, value in payload.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (dict, list)):
            continue
        lines.append(f"- **{key.replace('_', ' ').title()}:** {value}")
        shown += 1
        if shown >= 12:
            break
    return "\n".join(lines)


def _count_query_label(doctype: str, filters: dict[str, Any]) -> str:
    if doctype == "Employee" and filters.get("status") == "Active":
        return "active employees"
    return f"{doctype.lower()} records"


def _metadata_field_rows(doctype: str, *, writable_only: bool = False) -> list[dict[str, Any]]:
    meta = frappe.get_meta(doctype)
    blocked_types = {"Section Break", "Column Break", "Tab Break", "HTML", "Button", "Image", "Fold", "Heading"}
    rows: list[dict[str, Any]] = []
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        if not fieldname or field.fieldtype in blocked_types:
            continue
        if writable_only and (
            getattr(field, "read_only", 0)
            or getattr(field, "set_only_once", 0)
            or field.fieldtype in {"Table", "Table MultiSelect", "Attach", "Attach Image", "Read Only"}
        ):
            continue
        rows.append(
            {
                "fieldname": fieldname,
                "label": str(field.label or fieldname).strip(),
                "fieldtype": str(field.fieldtype or "").strip(),
                "reqd": int(getattr(field, "reqd", 0) or 0),
                "read_only": int(getattr(field, "read_only", 0) or 0),
                "options": str(getattr(field, "options", "") or "").strip(),
            }
        )
    return rows


def _metadata_rows_to_answer(rows: list[dict[str, Any]], title: str) -> str:
    if not rows:
        return f"{title}\n\nNo fields found."
    lines = [title, ""]
    for row in rows[:40]:
        label = row.get("label") or row.get("fieldname")
        fieldtype = row.get("fieldtype") or "Data"
        fieldname = row.get("fieldname") or ""
        reqd = " required" if row.get("reqd") else ""
        lines.append(f"- {label} ({fieldname}, {fieldtype}{reqd})")
    if len(rows) > 40:
        lines.append(f"...and {len(rows) - 40} more fields.")
    return "\n".join(lines)


def _required_meta_fields(doctype: str) -> list[Any]:
    required: list[Any] = []
    meta = frappe.get_meta(doctype)
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        if not fieldname:
            continue
        if getattr(field, "reqd", 0) != 1:
            continue
        if getattr(field, "read_only", 0):
            continue
        if field.fieldtype in {"Section Break", "Column Break", "Tab Break", "HTML", "Button", "Image", "Fold", "Heading"}:
            continue
        required.append(field)
    return required


def _is_empty_value(value: Any) -> bool:
    return value in (None, "", [], {})


def _missing_required_fields(doctype: str, values: dict[str, Any]) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    payload = values or {}
    for field in _required_meta_fields(doctype):
        fieldname = str(field.fieldname or "").strip()
        if not fieldname:
            continue
        if not _is_empty_value(payload.get(fieldname)):
            continue
        default_value = str(getattr(field, "default", "") or "").strip()
        if default_value:
            continue
        missing.append(
            {
                "fieldname": fieldname,
                "label": str(field.label or fieldname).strip(),
                "fieldtype": str(field.fieldtype or "").strip(),
                "options": str(getattr(field, "options", "") or "").strip(),
            }
        )
    return missing


def get_required_doctype_fields_internal(doctype: str) -> list[dict[str, Any]]:
    doctype_name = resolve_doctype_name_internal(doctype)
    if not doctype_name or not frappe.db.exists("DocType", doctype_name):
        return []
    return _missing_required_fields(doctype_name, {})


def _missing_child_table_fields(doctype: str, values: dict[str, Any]) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    table_fields = _table_meta_fields(doctype)
    payload = values or {}
    for table_fieldname, table_field in table_fields.items():
        rows = payload.get(table_fieldname)
        if not isinstance(rows, list) or not rows:
            continue
        child_doctype = str(getattr(table_field, "options", "") or "").strip()
        for index, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            child_missing = _missing_required_fields(child_doctype, row)
            if child_missing:
                missing.append(
                    {
                        "table_fieldname": table_fieldname,
                        "table_label": str(table_field.label or table_fieldname).strip(),
                        "child_doctype": child_doctype,
                        "row_index": index,
                        "fields": child_missing,
                    }
                )
    return missing


def _build_missing_child_rows_message(doctype: str, missing_rows: list[dict[str, Any]], *, action: str) -> str:
    parts = []
    for row in missing_rows[:3]:
        labels = ", ".join(str(field.get("label") or field.get("fieldname") or "").strip() for field in row.get("fields") or [] if field)
        parts.append(f"{row.get('table_label')} row {row.get('row_index')}: {labels}")
    lead = f"I can {action} {doctype}, but some child rows are incomplete: " + "; ".join(parts) + "."
    if missing_rows:
        table_label = str(missing_rows[0].get("table_label") or "rows").strip().lower()
        lead += f" Example: reply with `{table_label} row 1 field value; {table_label} row 2 field value` or resend the full `{table_label}` rows."
    return lead


def _build_missing_fields_message(doctype: str, missing_fields: list[dict[str, Any]], *, action: str) -> str:
    labels = [str(row.get("label") or row.get("fieldname") or "").strip() for row in missing_fields if row]
    lead = f"I can {action} {doctype}, but I still need: {', '.join(labels[:6])}."
    if len(labels) > 6:
        lead += f" and {len(labels) - 6} more fields."
    example_parts = []
    for row in missing_fields[:3]:
        label = str(row.get("label") or row.get("fieldname") or "").strip().lower()
        example_parts.append(f"{label} <value>")
    if example_parts:
        lead += f" Example: {action} {doctype} with " + ", ".join(example_parts)
    return lead


def _build_pending_create_action(doctype: str, values: dict[str, Any], missing_fields: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "action": "create_erp_document",
        "doctype": doctype,
        "values": values,
        "missing_fields": missing_fields,
    }


def _build_pending_create_child_rows_action(doctype: str, values: dict[str, Any], missing_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "action": "create_erp_document",
        "doctype": doctype,
        "values": values,
        "missing_child_rows": missing_rows,
    }


def _merge_child_row_updates(
    existing_values: dict[str, Any],
    row_updates: dict[str, dict[int, dict[str, Any]]],
) -> dict[str, Any]:
    merged_values = dict(existing_values or {})
    for table_fieldname, indexed_rows in row_updates.items():
        current_rows = merged_values.get(table_fieldname)
        rows = [dict(row) if isinstance(row, dict) else {} for row in current_rows] if isinstance(current_rows, list) else []
        for index, row_values in indexed_rows.items():
            while len(rows) <= index:
                rows.append({})
            rows[index].update(row_values or {})
        merged_values[table_fieldname] = rows
    return merged_values


def _candidate_choice_rows(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in candidates[:5]:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "name": str(row.get("name") or "").strip(),
                "label": ", ".join(str(value) for key, value in row.items() if key != "name" and value not in (None, "", [], {}))[:120],
            }
        )
    return rows


def _build_ambiguous_message(subject: str, hint: str, candidates: list[dict[str, Any]]) -> str:
    lines = [f"I found multiple {subject} matches for `{hint}`. Please reply with the exact record name:", ""]
    for row in _candidate_choice_rows(candidates):
        label = f" ({row['label']})" if row.get("label") else ""
        lines.append(f"- {row['name']}{label}")
    return "\n".join(lines)


def _build_pending_update_action(doctype: str, record_hint: str, field: str, value: Any, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "action": "update_erp_document",
        "doctype": doctype,
        "record_hint": record_hint,
        "field": field,
        "value": value,
        "candidates": _candidate_choice_rows(candidates),
    }


def _build_pending_create_link_choice(doctype: str, values: dict[str, Any], fieldname: str, link_doctype: str, hint: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "action": "create_erp_document",
        "doctype": doctype,
        "values": values,
        "ambiguous_link": {
            "fieldname": fieldname,
            "link_doctype": link_doctype,
            "hint": hint,
            "candidates": _candidate_choice_rows(candidates),
        },
    }


def _resolve_link_value_detailed(link_doctype: str, raw_value: Any) -> dict[str, Any]:
    hint = str(raw_value or "").strip()
    if not hint:
        return {"status": "not_found", "name": None, "candidates": []}
    if frappe.db.exists(link_doctype, hint):
        return {"status": "resolved", "name": hint, "candidates": [{"name": hint}]}
    meta = frappe.get_meta(link_doctype)
    candidates = []
    title_field = str(getattr(meta, "title_field", "") or "").strip()
    if title_field:
        candidates.append(title_field)
    candidates.append("name")
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        if fieldname and field.fieldtype in {"Data", "Link", "Dynamic Link", "Small Text", "Text"} and fieldname not in candidates:
            candidates.append(fieldname)
    for fieldname in candidates[:6]:
        if fieldname != "name" and meta.has_field(fieldname):
            exact = frappe.db.get_value(link_doctype, {fieldname: hint}, "name")
            if exact:
                return {"status": "resolved", "name": str(exact), "candidates": [{"name": str(exact)}]}
    result_fields = ["name"] + [field for field in candidates if field != "name"][:2]
    or_filters = [[link_doctype, fieldname, "like", f"%{hint}%"] for fieldname in candidates[:4]]
    rows = frappe.get_all(link_doctype, or_filters=or_filters, fields=result_fields, limit_page_length=5, order_by="modified desc")
    if len(rows) == 1:
        return {"status": "resolved", "name": str(rows[0].get("name") or "").strip() or None, "candidates": rows}
    if len(rows) > 1:
        best = _pick_best_candidate(hint, rows, search_fields=result_fields[1:])
        if best:
            return {"status": "resolved", "name": str(best.get("name") or "").strip() or None, "candidates": rows}
        return {"status": "ambiguous", "name": None, "candidates": rows}
    return {"status": "not_found", "name": None, "candidates": []}


def _resolve_link_value(link_doctype: str, raw_value: Any) -> str | None:
    details = _resolve_link_value_detailed(link_doctype, raw_value)
    return details.get("name") if details.get("status") == "resolved" else None


def _normalize_mutation_values(doctype: str, parsed_values: dict[str, Any]) -> tuple[dict[str, Any], list[str], dict[str, Any] | None]:
    writable_fields = _writable_meta_fields(doctype)
    table_fields = _table_meta_fields(doctype)
    payload: dict[str, Any] = {}
    ignored: list[str] = []
    ambiguous: dict[str, Any] | None = None
    for key, raw_value in parsed_values.items():
        table_fieldname = _table_alias_map(doctype).get(str(key or "").strip().lower())
        if table_fieldname and table_fieldname in table_fields and isinstance(raw_value, list):
            child_doctype = str(getattr(table_fields[table_fieldname], "options", "") or "").strip()
            child_rows: list[dict[str, Any]] = []
            for row in raw_value:
                if not isinstance(row, dict):
                    continue
                normalized_child, _, child_ambiguous = _normalize_mutation_values(child_doctype, row)
                if child_ambiguous:
                    ambiguous = child_ambiguous
                    continue
                if normalized_child:
                    child_rows.append(normalized_child)
            if child_rows:
                payload[table_fieldname] = child_rows
            else:
                ignored.append(str(key))
            continue
        fieldname, meta_field = _resolve_fieldname(doctype, str(key))
        if not fieldname or not meta_field or fieldname not in writable_fields:
            ignored.append(str(key))
            continue
        try:
            coerced_value = _coerce_field_value(meta_field, raw_value)
        except Exception:
            ignored.append(str(key))
            continue
        if str(getattr(meta_field, "fieldtype", "") or "").strip() == "Link":
            options = str(getattr(meta_field, "options", "") or "").strip()
            link_result = _resolve_link_value_detailed(options, coerced_value) if options else {"status": "resolved", "name": coerced_value, "candidates": []}
            if options and link_result.get("status") == "ambiguous":
                ambiguous = {
                    "fieldname": fieldname,
                    "link_doctype": options,
                    "hint": str(raw_value),
                    "candidates": link_result.get("candidates") or [],
                }
                continue
            resolved_link = link_result.get("name") if options else coerced_value
            if options and not resolved_link:
                ignored.append(str(key))
                continue
            coerced_value = resolved_link or coerced_value
        payload[fieldname] = coerced_value
    return payload, ignored, ambiguous


def list_erp_doctypes_internal(search: str | None = None, module: str | None = None, limit: int = 100) -> dict[str, Any]:
    if not frappe.has_permission("DocType", ptype="read"):
        raise frappe.PermissionError(_("Not permitted to read DocType metadata"))

    filters: dict[str, Any] = {"istable": 0}
    if module:
        filters["module"] = str(module).strip()
    rows = frappe.get_all(
        "DocType",
        filters=filters,
        fields=["name", "module", "custom", "issingle", "is_submittable"],
        order_by="modified desc",
        limit_page_length=max(1, min(int(limit or 20), 500)),
    )
    search_text = str(search or "").strip().lower()
    if search_text:
        rows = [row for row in rows if search_text in str(row.get("name") or "").lower() or search_text in str(row.get("module") or "").lower()]

    answer_lines = ["Available DocTypes", ""]
    for row in rows:
        answer_lines.append(f"- {row.get('name')} ({row.get('module') or 'Core'})")
    return {
        "ok": True,
        "type": "answer",
        "answer": "\n".join(answer_lines),
        "data": rows,
        "meta": {"search": search_text, "module": module or "", "count": len(rows)},
    }


def get_doctype_fields_internal(doctype: str, writable_only: bool = False) -> dict[str, Any]:
    doctype_name = str(doctype or "").strip()
    if not doctype_name:
        return _error(_("Doctype is required"))
    if not frappe.db.exists("DocType", doctype_name):
        return _error(_("{0} does not exist.").format(doctype_name), error_type="not_found")
    if not frappe.has_permission("DocType", ptype="read"):
        raise frappe.PermissionError(_("Not permitted to read DocType metadata"))

    meta = frappe.get_meta(doctype_name)
    rows = _metadata_field_rows(doctype_name, writable_only=writable_only)
    return {
        "ok": True,
        "type": "answer",
        "answer": _metadata_rows_to_answer(rows, f"{doctype_name} fields"),
        "data": rows,
        "meta": {
            "doctype": doctype_name,
            "module": str(getattr(meta, "module", "") or "").strip(),
            "title_field": str(getattr(meta, "title_field", "") or "").strip(),
            "search_fields": str(getattr(meta, "search_fields", "") or "").strip(),
            "writable_only": int(bool(writable_only)),
        },
    }


def describe_erp_schema_internal(doctype: str) -> dict[str, Any]:
    details = get_doctype_fields_internal(doctype, writable_only=False)
    if not details.get("ok"):
        return details
    meta = details.get("meta") or {}
    lines = [
        f"Schema for {doctype}",
        "",
        f"Module: {meta.get('module') or 'Unknown'}",
        f"Title field: {meta.get('title_field') or 'None'}",
        f"Search fields: {meta.get('search_fields') or 'None'}",
        "",
        "Use `show fields for <doctype>` for the field list.",
    ]
    return {
        "ok": True,
        "type": "answer",
        "answer": "\n".join(lines),
        "data": {"doctype": doctype, "meta": meta, "field_count": len(details.get('data') or [])},
    }


def create_erp_document_internal(doctype: str, values: dict[str, Any] | str) -> dict[str, Any]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {"ok": False, "type": "document", "message": _("Unsupported ERP document type.")}
    if not _is_general_mutation_allowed(resolved_doctype):
        return {"ok": False, "type": "document", "message": _("Create is not enabled for {0}.").format(resolved_doctype)}

    _require_doctype_permission(resolved_doctype, "create")
    parsed_values = _parse_json_arg(values, values)
    if not isinstance(parsed_values, dict) or not parsed_values:
        return {"ok": False, "type": "document", "message": _("Field values are required to create {0}.").format(resolved_doctype)}

    if resolved_doctype == "Payment Entry":
        payment_entry_result = _create_payment_entry_from_reference(parsed_values)
        if payment_entry_result is not None:
            return payment_entry_result

    payload, ignored_fields, ambiguous_link = _normalize_mutation_values(resolved_doctype, parsed_values)
    payload["doctype"] = resolved_doctype
    if len(payload) <= 1:
        return {"ok": False, "type": "document", "message": _("No supported fields were provided for {0}.").format(resolved_doctype)}

    if ambiguous_link:
        return {
            "ok": False,
            "type": "document",
            "message": _build_ambiguous_message(
                ambiguous_link["link_doctype"],
                ambiguous_link["hint"],
                ambiguous_link["candidates"],
            ),
            "error_type": "ambiguous_link",
            "pending_action": _build_pending_create_link_choice(
                resolved_doctype,
                {key: value for key, value in payload.items() if key != "doctype"},
                ambiguous_link["fieldname"],
                ambiguous_link["link_doctype"],
                ambiguous_link["hint"],
                ambiguous_link["candidates"],
            ),
            "data": {
                "doctype": resolved_doctype,
                "fieldname": ambiguous_link["fieldname"],
                "link_doctype": ambiguous_link["link_doctype"],
                "ignored_fields": ignored_fields,
            },
        }

    missing_fields = _missing_required_fields(resolved_doctype, payload)
    if missing_fields:
        return {
            "ok": False,
            "type": "document",
            "message": _build_missing_fields_message(resolved_doctype, missing_fields, action="create"),
            "error_type": "missing_fields",
            "missing_fields": missing_fields,
            "pending_action": _build_pending_create_action(
                resolved_doctype,
                {key: value for key, value in payload.items() if key != "doctype"},
                missing_fields,
            ),
            "data": {
                "doctype": resolved_doctype,
                "provided_fields": [key for key in payload.keys() if key != "doctype"],
                "ignored_fields": ignored_fields,
            },
        }

    missing_child_rows = _missing_child_table_fields(resolved_doctype, payload)
    if missing_child_rows:
        return {
            "ok": False,
            "type": "document",
            "message": _build_missing_child_rows_message(resolved_doctype, missing_child_rows, action="create"),
            "error_type": "missing_child_rows",
            "missing_child_rows": missing_child_rows,
            "pending_action": _build_pending_create_child_rows_action(
                resolved_doctype,
                {key: value for key, value in payload.items() if key != "doctype"},
                missing_child_rows,
            ),
            "data": {
                "doctype": resolved_doctype,
                "provided_fields": [key for key in payload.keys() if key != "doctype"],
                "ignored_fields": ignored_fields,
            },
        }

    doc = frappe.get_doc(payload)
    try:
        doc.insert()
    except frappe.MandatoryError:
        missing_fields = _missing_required_fields(resolved_doctype, payload)
        return {
            "ok": False,
            "type": "document",
            "message": _build_missing_fields_message(resolved_doctype, missing_fields, action="create"),
            "error_type": "missing_fields",
            "missing_fields": missing_fields,
            "pending_action": _build_pending_create_action(
                resolved_doctype,
                {key: value for key, value in payload.items() if key != "doctype"},
                missing_fields,
            ),
            "data": {
                "doctype": resolved_doctype,
                "provided_fields": [key for key in payload.keys() if key != "doctype"],
                "ignored_fields": ignored_fields,
            },
        }
    except Exception as exc:
        child_missing_after_insert = _missing_child_table_fields(resolved_doctype, payload)
        if child_missing_after_insert:
            return {
                "ok": False,
                "type": "document",
                "message": _build_missing_child_rows_message(resolved_doctype, child_missing_after_insert, action="create"),
                "error_type": "missing_child_rows",
                "missing_child_rows": child_missing_after_insert,
                "pending_action": _build_pending_create_child_rows_action(
                    resolved_doctype,
                    {key: value for key, value in payload.items() if key != "doctype"},
                    child_missing_after_insert,
                ),
            }
        return {"ok": False, "type": "document", "message": str(exc)}
    return {
        "ok": True,
        "type": "document",
        "doctype": resolved_doctype,
        "name": doc.name,
        "url": get_url_to_form(resolved_doctype, doc.name),
        "message": f"{resolved_doctype} created successfully",
        "data": {
            "values": {key: value for key, value in payload.items() if key != "doctype"},
            "ignored_fields": ignored_fields,
        },
    }


def continue_pending_action_internal(pending_action: dict[str, Any], prompt: str) -> dict[str, Any] | None:
    if not isinstance(pending_action, dict):
        return None
    action = str(pending_action.get("action") or "").strip()
    prompt_text = str(prompt or "").strip()
    if not prompt_text:
        return None

    if action == "create_erp_document":
        doctype = str(pending_action.get("doctype") or "").strip()
        if not doctype:
            return None
        existing_values = pending_action.get("values") or {}
        if not isinstance(existing_values, dict):
            existing_values = {}
        confirmation_only = _is_confirmation_reply(prompt_text)
        missing_fields = pending_action.get("missing_fields") or []

        ambiguous_link = pending_action.get("ambiguous_link") or {}
        if isinstance(ambiguous_link, dict) and ambiguous_link.get("fieldname"):
            if confirmation_only:
                return None
            fieldname = str(ambiguous_link.get("fieldname") or "").strip()
            if fieldname:
                merged_values = dict(existing_values)
                merged_values[fieldname] = prompt_text.replace("use ", "", 1).strip()
                return create_erp_document_internal(doctype, merged_values)

        missing_child_rows = pending_action.get("missing_child_rows") or []
        default_table_fieldname = ""
        if isinstance(missing_child_rows, list) and missing_child_rows:
            default_table_fieldname = str(missing_child_rows[0].get("table_fieldname") or "").strip()
        row_updates = extract_child_row_updates_internal(
            doctype,
            prompt_text,
            default_table_fieldname=default_table_fieldname or None,
        )
        if row_updates:
            merged_values = _merge_child_row_updates(existing_values, row_updates)
            return create_erp_document_internal(doctype, merged_values)

        additional_values = extract_field_value_pairs_internal(doctype, prompt_text)
        if confirmation_only and not missing_child_rows and not missing_fields:
            return create_erp_document_internal(doctype, existing_values)
        if not additional_values and not confirmation_only and isinstance(missing_fields, list) and len(missing_fields) == 1:
            fieldname = str(missing_fields[0].get("fieldname") or "").strip()
            if fieldname:
                additional_values[fieldname] = prompt_text
        additional_child_rows = extract_child_table_rows_internal(doctype, prompt_text)
        if additional_child_rows:
            for table_field, rows in additional_child_rows.items():
                current_rows = existing_values.get(table_field)
                if isinstance(current_rows, list) and current_rows and len(current_rows) == len(rows):
                    merged_rows = []
                    for index, row in enumerate(rows):
                        base_row = current_rows[index] if index < len(current_rows) and isinstance(current_rows[index], dict) else {}
                        merged = dict(base_row)
                        if isinstance(row, dict):
                            merged.update(row)
                        merged_rows.append(merged)
                    additional_values[table_field] = merged_rows
                else:
                    additional_values[table_field] = rows
        if not additional_values:
            return None
        merged_values = dict(existing_values)
        merged_values.update(additional_values)
        return create_erp_document_internal(doctype, merged_values)

    if action == "update_erp_document":
        doctype = str(pending_action.get("doctype") or "").strip()
        field = str(pending_action.get("field") or "").strip()
        value = pending_action.get("value")
        record = str(pending_action.get("record_hint") or "").strip()
        if not doctype or not field:
            return None
        chosen = _extract_pending_choice(pending_action, prompt_text)
        if not chosen:
            return None
        return update_erp_document_internal(doctype=doctype, record=chosen or record, field=field, value=value)

    if action == "submit_erp_document":
        doctype = str(pending_action.get("doctype") or "").strip()
        record = str(pending_action.get("record_hint") or "").strip()
        if not doctype:
            return None
        chosen = _extract_pending_choice(pending_action, prompt_text)
        if not chosen:
            return None
        return submit_erp_document_internal(doctype=doctype, record=chosen or record)

    if action == "cancel_erp_document":
        doctype = str(pending_action.get("doctype") or "").strip()
        record = str(pending_action.get("record_hint") or "").strip()
        if not doctype:
            return None
        chosen = _extract_pending_choice(pending_action, prompt_text)
        if not chosen:
            return None
        return cancel_erp_document_internal(doctype=doctype, record=chosen or record)

    if action == "run_workflow_action":
        doctype = str(pending_action.get("doctype") or "").strip()
        record = str(pending_action.get("record_hint") or "").strip()
        workflow_action = str(pending_action.get("workflow_action") or "").strip()
        if not doctype or not workflow_action:
            return None
        chosen = _extract_pending_choice(pending_action, prompt_text)
        if not chosen:
            return None
        return run_workflow_action_internal(doctype=doctype, record=chosen or record, action=workflow_action)

    return None


def _is_confirmation_reply(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    confirmations = {
        "yes",
        "yes please",
        "yes proceed",
        "proceed",
        "continue",
        "go ahead",
        "create it",
        "yes create it",
        "yes proceed creating",
        "confirm",
        "confirmed",
        "okay proceed",
        "ok proceed",
    }
    if text in confirmations:
        return True
    return text.startswith("yes proceed") or text.startswith("go ahead") or text.startswith("please proceed")


def _extract_pending_choice(pending_action: dict[str, Any], prompt_text: str) -> str:
    text = str(prompt_text or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith("use "):
        return text[4:].strip()
    candidates = pending_action.get("candidates") or []
    if isinstance(candidates, list):
        candidate_names = {
            str(row.get("name") or "").strip().lower(): str(row.get("name") or "").strip()
            for row in candidates
            if isinstance(row, dict) and str(row.get("name") or "").strip()
        }
        if lowered in candidate_names:
            return candidate_names[lowered]
    return ""


def count_erp_documents_internal(doctype: str, filters: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = _resolve_safe_doctype(doctype)
    if not resolved:
        return {"ok": False, "type": "answer", "message": _("I could not understand the ERP question yet.")}
    resolved_doctype, config = resolved
    _require_doctype_permission(resolved_doctype, "read")
    safe_filters = _sanitize_filters(filters, set(config.get("filter_fields") or set()))
    count = frappe.db.count(resolved_doctype, filters=safe_filters)
    label = _count_query_label(resolved_doctype, safe_filters)
    return {
        "ok": True,
        "type": "answer",
        "answer": f"There are {count} {label}.",
        "data": {
            "query_type": label,
            "doctype": resolved_doctype,
            "count": count,
            "filters": safe_filters,
        },
    }


def list_erp_documents_internal(doctype: str, filters: dict[str, Any] | None = None, limit: int = 20) -> dict[str, Any]:
    resolved = _resolve_safe_doctype(doctype)
    if not resolved:
        return _error(_("Unsupported ERP document type."), error_type="unsupported_doctype")
    resolved_doctype, config = resolved
    _require_doctype_permission(resolved_doctype, "read")
    safe_filters = _sanitize_filters(filters, set(config.get("filter_fields") or set()))
    fields = list(config.get("fields") or ["name"])
    page_limit = max(1, min(int(limit or 20), 5000))
    rows = frappe.get_all(
        resolved_doctype,
        filters=safe_filters,
        fields=fields,
        order_by="modified desc",
        limit_page_length=page_limit,
    )
    # Get total count for pagination awareness
    try:
        total_count = frappe.db.count(resolved_doctype, filters=safe_filters)
    except Exception:
        total_count = len(rows)
    return {
        "ok": True,
        "type": "answer",
        "answer": _list_rows_to_answer(resolved_doctype, rows),
        "data": rows,
        "meta": {
            "doctype": resolved_doctype,
            "filters": safe_filters,
            "returned": len(rows),
            "total_count": total_count,
            "has_more": total_count > len(rows),
        },
    }


def get_erp_document_internal(doctype: str, name: str) -> dict[str, Any]:
    resolved = _resolve_safe_doctype(doctype)
    if not resolved:
        return _error(_("Unsupported ERP document type."), error_type="unsupported_doctype")
    resolved_doctype, config = resolved
    resolved_name = _resolve_record_name(resolved_doctype, name, config)
    if not resolved_name:
        return _error(_("{0} {1} does not exist.").format(resolved_doctype, name), error_type="not_found")
    doc = frappe.get_doc(resolved_doctype, resolved_name)
    doc.check_permission("read")
    data = doc.as_dict()
    trimmed = {key: value for key, value in data.items() if value not in (None, "", [], {})}
    return {
        "ok": True,
        "type": "answer",
        "answer": _document_to_answer(resolved_doctype, trimmed),
        "data": trimmed,
        "doctype": resolved_doctype,
        "name": resolved_name,
        "url": get_url_to_form(resolved_doctype, resolved_name),
    }


def search_erp_documents_internal(query: str, doctype: str | None = None, limit: int = 10) -> dict[str, Any]:
    query_text = str(query or "").strip()
    if not query_text:
        return _error(_("Search query is required"))
    if doctype:
        resolved = _resolve_safe_doctype(doctype)
    else:
        resolved = _resolve_doctype_from_query_text(query_text)
    if not resolved:
        return _error(_("Unsupported ERP document type."), error_type="unsupported_doctype")
    resolved_doctype, config = resolved
    _require_doctype_permission(resolved_doctype, "read")

    search_fields = list(config.get("search_fields") or ["name"])
    base_fields = list(dict.fromkeys(["name"] + search_fields[:3]))
    lowered = query_text.lower()
    for alias in sorted([doctype or ""], key=len, reverse=True):
        lowered = lowered.replace(alias.lower(), " ")
    cleaned_query = re.sub(r"\b(show|open|get|view|find|search|for|me)\b", " ", lowered)
    cleaned_query = re.sub(r"\s+", " ", cleaned_query).strip() or query_text

    filters = []
    for fieldname in search_fields[:3]:
        filters.append([resolved_doctype, fieldname, "like", f"%{cleaned_query}%"])
    rows = frappe.get_all(
        resolved_doctype,
        or_filters=filters,
        fields=base_fields,
        order_by="modified desc",
        limit_page_length=max(1, min(int(limit or 20), 1000)),
    )
    return {
        "ok": True,
        "type": "answer",
        "answer": _list_rows_to_answer(resolved_doctype, rows, heading="Search results"),
        "data": rows,
        "meta": {"doctype": resolved_doctype, "query": cleaned_query},
    }


def get_person_details_internal(record_hint: str) -> dict[str, Any]:
    hint = str(record_hint or "").strip()
    if not hint:
        return _error(_("Name is required."), error_type="missing_name")

    search_targets = [
        ("Employee", {"search_fields": ["name", "employee_name", "company_email", "cell_number"]}),
        ("Contact", {"search_fields": ["name", "full_name", "first_name", "last_name", "email_id", "mobile_no"]}),
        ("Customer", {"search_fields": ["name", "customer_name"]}),
        ("Supplier", {"search_fields": ["name", "supplier_name"]}),
        ("Lead", {"search_fields": ["name", "lead_name", "company_name", "email_id"]}),
    ]
    matches: list[dict[str, Any]] = []
    for doctype, config in search_targets:
        if not frappe.db.exists("DocType", doctype):
            continue
        try:
            _require_doctype_permission(doctype, "read")
        except frappe.PermissionError:
            continue
        resolution = _resolve_record_name_detailed(doctype, hint, config)
        if resolution.get("status") == "resolved":
            resolved_name = str(resolution.get("name") or "").strip()
            if resolved_name:
                result = get_erp_document_internal(doctype, resolved_name)
                if result.get("ok"):
                    return result
        for row in resolution.get("candidates") or []:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or "").strip()
            if not name:
                continue
            label_parts = []
            for key, value in row.items():
                if key == "name" or value in (None, "", [], {}):
                    continue
                label_parts.append(str(value))
            matches.append(
                {
                    "doctype": doctype,
                    "name": name,
                    "label": ", ".join(label_parts[:2]),
                }
            )

    unique_matches: list[dict[str, Any]] = []
    seen = set()
    for row in matches:
        key = (row.get("doctype"), row.get("name"))
        if key in seen:
            continue
        seen.add(key)
        unique_matches.append(row)

    if len(unique_matches) == 1:
        only = unique_matches[0]
        return get_erp_document_internal(str(only.get("doctype") or ""), str(only.get("name") or ""))

    if unique_matches:
        return {
            "ok": False,
            "type": "answer",
            "message": _build_ambiguous_message("record", hint, unique_matches),
            "error_type": "ambiguous_record",
            "candidates": unique_matches[:8],
        }

    return _error(_("I could not find a matching Employee, Contact, Customer, Supplier, or Lead for {0}.").format(hint), error_type="not_found")


def ping_assistant_internal() -> dict[str, Any]:
    return {
        "ok": True,
        "type": "status",
        "message": "ERP AI Assistant is ready",
    }


@frappe.whitelist()
def ping_assistant() -> dict[str, Any]:
    return ping_assistant_internal()


def answer_erp_query_internal(question: str) -> dict[str, Any]:
    matched = _match_count_query(question)
    if not matched:
        generic_match = resolve_safe_doctype_from_text(question)
        if generic_match:
            _key, config = generic_match
            return count_erp_documents_internal(str(config.get("doctype") or "").strip())
        return {"ok": False, "type": "answer", "message": "I could not understand the ERP question yet."}

    doctype, filters, label = matched
    _require_doctype_permission(doctype, "read")
    count = frappe.db.count(doctype, filters=filters or {})
    answer = f"There are {count} {label}."
    return {
        "ok": True,
        "type": "answer",
        "answer": answer,
        "data": {
            "query_type": label,
            "count": count,
        },
    }


def _resolve_company_for_party(party_doctype: str, party_name: str, company: str | None = None) -> tuple[str | None, dict[str, Any] | None]:
    company_name = str(company or "").strip()
    if company_name:
        if not frappe.db.exists("Company", company_name):
            return None, _error(_("Company {0} does not exist.").format(company_name), error_type="not_found")
        return company_name, None
    default_company = frappe.db.get_value(party_doctype, party_name, "default_company")
    company_name = str(default_company or frappe.defaults.get_user_default("Company") or "").strip()
    if not company_name:
        return None, _error(_("Company is required. Set a default company or pass company explicitly."))
    return company_name, None


def _transaction_config_for_doctype(doctype: str) -> dict[str, Any] | None:
    resolved = _resolve_safe_doctype(doctype)
    if not resolved:
        return None
    resolved_doctype, config = resolved
    party_field = str(config.get("party_field") or "").strip()
    if not party_field:
        return None
    if party_field in {"customer", "party_name"}:
        party_doctype = "Customer"
    elif party_field == "supplier":
        party_doctype = "Supplier"
    else:
        return None
    return {
        "doctype": resolved_doctype,
        "party_field": party_field,
        "party_doctype": party_doctype,
    }


def _create_transaction_document(
    doctype: str,
    party_doctype: str,
    party_field: str,
    party_name: str,
    items: list[dict[str, Any]] | str,
    company: str | None = None,
) -> dict[str, Any]:
    _require_doctype_permission(doctype, "create")
    _require_doctype_permission(party_doctype, "read")
    _require_doctype_permission("Item", "read")
    if not party_name:
        return _error(_("{0} is required").format(party_doctype))
    if not frappe.db.exists(party_doctype, party_name):
        return _error(_("{0} {1} does not exist.").format(party_doctype, party_name), error_type="not_found")
    sanitized_items, item_error = _sanitize_sales_order_items(items)
    if item_error:
        return _error(item_error)
    for row in sanitized_items:
        if not frappe.db.exists("Item", row["item_code"]):
            return _error(_("Item {0} does not exist.").format(row["item_code"]), error_type="not_found")
    company_name, error = _resolve_company_for_party(party_doctype, party_name, company=company)
    if error:
        return error

    posting_date = nowdate()
    payload = {
        "doctype": doctype,
        party_field: party_name,
        "company": company_name,
        "transaction_date": posting_date,
        "items": [dict(row) for row in sanitized_items],
    }
    if doctype == "Sales Order":
        payload["delivery_date"] = posting_date
        payload["items"] = [{**row, "delivery_date": posting_date} for row in sanitized_items]
    elif doctype == "Purchase Order":
        payload["schedule_date"] = posting_date
        payload["items"] = [{**row, "schedule_date": posting_date} for row in sanitized_items]
    elif doctype == "Quotation":
        payload["items"] = [{**row} for row in sanitized_items]
    if doctype == "Quotation":
        payload["quotation_to"] = "Customer"
    doc = frappe.get_doc(payload)
    doc.insert()
    return {
        "ok": True,
        "type": "document",
        "doctype": doctype,
        "name": doc.name,
        "url": get_url_to_form(doctype, doc.name),
        "message": f"{doctype} created successfully",
        "data": {
            party_field: party_name,
            "company": company_name,
            "items": sanitized_items,
            "docstatus": doc.docstatus,
        },
    }


@frappe.whitelist()
def answer_erp_query(question: str) -> dict[str, Any]:
    try:
        return answer_erp_query_internal(question)
    except frappe.PermissionError:
        return _error(_("You do not have permission to read that data."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Answer ERP Query Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


def create_sales_order_internal(customer: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    customer_name = str(customer or "").strip()
    return _create_transaction_document(
        "Sales Order",
        "Customer",
        "customer",
        customer_name,
        items,
        company=company,
    )


def create_quotation_internal(customer: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    return _create_transaction_document("Quotation", "Customer", "party_name", str(customer or "").strip(), items, company=company)


def create_purchase_order_internal(supplier: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    return _create_transaction_document("Purchase Order", "Supplier", "supplier", str(supplier or "").strip(), items, company=company)


def create_transaction_document_internal(doctype: str, party_name: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    transaction_config = _transaction_config_for_doctype(doctype)
    if not transaction_config:
        return {"ok": False, "type": "document", "message": _("Transaction creation is not enabled for {0}.").format(doctype)}
    return _create_transaction_document(
        transaction_config["doctype"],
        transaction_config["party_doctype"],
        transaction_config["party_field"],
        str(party_name or "").strip(),
        items,
        company=company,
    )


@frappe.whitelist()
def create_erp_document(doctype: str, values: dict[str, Any] | str) -> dict[str, Any]:
    try:
        return create_erp_document_internal(doctype=doctype, values=values)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to create that document.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Create ERP Document Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def create_transaction_document(doctype: str, party_name: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    try:
        return create_transaction_document_internal(doctype=doctype, party_name=party_name, items=items, company=company)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to create that document.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Create Transaction Document Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


def update_erp_document_internal(doctype: str, record: str, field: str, value: Any) -> dict[str, Any]:
    resolved = _resolve_safe_doctype(doctype)
    if not resolved:
        resolved_doctype = resolve_doctype_name_internal(doctype)
        if resolved_doctype and _is_general_mutation_allowed(resolved_doctype):
            resolved = (resolved_doctype, {"doctype": resolved_doctype, "search_fields": ["name"]})
    if not resolved:
        return {"ok": False, "type": "document", "message": _("Unsupported ERP document type.")}
    resolved_doctype, config = resolved
    if not _is_general_mutation_allowed(resolved_doctype):
        return {"ok": False, "type": "document", "message": _("Update is not enabled for {0}.").format(resolved_doctype)}
    _require_doctype_permission(resolved_doctype, "write")

    resolved_record = _resolve_record_name_detailed(resolved_doctype, record, config)
    if resolved_record.get("status") == "ambiguous":
        return {
            "ok": False,
            "type": "document",
            "message": _build_ambiguous_message(resolved_doctype, record, resolved_record.get("candidates") or []),
            "error_type": "ambiguous_record",
            "pending_action": _build_pending_update_action(
                resolved_doctype,
                record,
                field,
                value,
                resolved_record.get("candidates") or [],
            ),
            "candidates": _candidate_choice_rows(resolved_record.get("candidates") or []),
        }
    resolved_name = resolved_record.get("name")
    if not resolved_name:
        return {"ok": False, "type": "document", "message": _("{0} {1} was not found.").format(resolved_doctype, record)}

    fieldname, meta_field = _resolve_fieldname(resolved_doctype, field)
    if not fieldname or not meta_field:
        writable_candidates = [row.get("label") or row.get("fieldname") for row in _metadata_field_rows(resolved_doctype, writable_only=True)[:8]]
        detail = f" Supported writable fields include: {', '.join(writable_candidates)}" if writable_candidates else ""
        return {"ok": False, "type": "document", "message": _("Field {0} is not supported for updates on {1}.").format(field, resolved_doctype) + detail}

    doc = frappe.get_doc(resolved_doctype, resolved_name)
    doc.check_permission("write")
    try:
        coerced_value = _coerce_field_value(meta_field, value)
    except Exception:
        return {"ok": False, "type": "document", "message": _("Value {0} is not valid for field {1}.").format(value, field)}

    if str(getattr(meta_field, "fieldtype", "") or "").strip() == "Link":
        options = str(getattr(meta_field, "options", "") or "").strip()
        link_result = _resolve_link_value_detailed(options, coerced_value) if options else {"status": "resolved", "name": coerced_value, "candidates": []}
        if options and link_result.get("status") == "ambiguous":
            return {
                "ok": False,
                "type": "document",
                "message": _build_ambiguous_message(options, str(value), link_result.get("candidates") or []),
                "error_type": "ambiguous_link",
                "pending_action": _build_pending_update_action(
                    resolved_doctype,
                    resolved_name,
                    field,
                    value,
                    link_result.get("candidates") or [],
                ),
            }
        resolved_link = link_result.get("name") if options else coerced_value
        if options and not resolved_link:
            return {
                "ok": False,
                "type": "document",
                "message": _("I could not find a matching {0} record for value {1}.").format(options, value),
                "error_type": "link_not_found",
            }
        coerced_value = resolved_link or coerced_value

    doc.set(fieldname, coerced_value)
    try:
        doc.save()
    except frappe.MandatoryError:
        missing_fields = _missing_required_fields(resolved_doctype, doc.as_dict())
        return {
            "ok": False,
            "type": "document",
            "message": _build_missing_fields_message(resolved_doctype, missing_fields, action="update"),
            "error_type": "missing_fields",
            "missing_fields": missing_fields,
        }
    return {
        "ok": True,
        "type": "document",
        "doctype": resolved_doctype,
        "name": doc.name,
        "url": get_url_to_form(resolved_doctype, doc.name),
        "message": f"{resolved_doctype} updated successfully",
        "data": {
            "fieldname": fieldname,
            "value": coerced_value,
        },
    }


def _resolve_generic_record_for_action(doctype: str, record: str, action_key: str, extra: dict[str, Any] | None = None) -> tuple[str | None, dict[str, Any] | None]:
    config = {"doctype": doctype, "search_fields": ["name"]}
    resolution = _resolve_record_name_detailed(doctype, record, config)
    if resolution.get("status") == "ambiguous":
        pending = {"action": action_key, "doctype": doctype, "record_hint": record, "candidates": _candidate_choice_rows(resolution.get("candidates") or [])}
        if extra:
            pending.update(extra)
        return None, {
            "ok": False,
            "type": "document",
            "message": _build_ambiguous_message(doctype, record, resolution.get("candidates") or []),
            "error_type": "ambiguous_record",
            "pending_action": pending,
        }
    resolved_name = str(resolution.get("name") or "").strip()
    if not resolved_name:
        return None, {"ok": False, "type": "document", "message": _("{0} {1} was not found.").format(doctype, record)}
    return resolved_name, None


def submit_erp_document_internal(doctype: str, record: str) -> dict[str, Any]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {"ok": False, "type": "document", "message": _("Unsupported ERP document type.")}
    resolved_name, error = _resolve_generic_record_for_action(resolved_doctype, record, "submit_erp_document")
    if error:
        return error
    doc = frappe.get_doc(resolved_doctype, resolved_name)
    doc.check_permission("submit")
    if not hasattr(doc, "submit"):
        return {"ok": False, "type": "document", "message": _("Submit is not supported for {0}.").format(resolved_doctype)}
    if getattr(doc, "docstatus", 0) == 1:
        return {"ok": True, "type": "document", "doctype": resolved_doctype, "name": doc.name, "url": get_url_to_form(resolved_doctype, doc.name), "message": f"{resolved_doctype} is already submitted"}
    doc.submit()
    return {"ok": True, "type": "document", "doctype": resolved_doctype, "name": doc.name, "url": get_url_to_form(resolved_doctype, doc.name), "message": f"{resolved_doctype} submitted successfully"}


def cancel_erp_document_internal(doctype: str, record: str) -> dict[str, Any]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    if not resolved_doctype:
        return {"ok": False, "type": "document", "message": _("Unsupported ERP document type.")}
    resolved_name, error = _resolve_generic_record_for_action(resolved_doctype, record, "cancel_erp_document")
    if error:
        return error
    doc = frappe.get_doc(resolved_doctype, resolved_name)
    doc.check_permission("cancel")
    if not hasattr(doc, "cancel"):
        return {"ok": False, "type": "document", "message": _("Cancel is not supported for {0}.").format(resolved_doctype)}
    if getattr(doc, "docstatus", 0) == 2:
        return {"ok": True, "type": "document", "doctype": resolved_doctype, "name": doc.name, "url": get_url_to_form(resolved_doctype, doc.name), "message": f"{resolved_doctype} is already cancelled"}
    doc.cancel()
    return {"ok": True, "type": "document", "doctype": resolved_doctype, "name": doc.name, "url": get_url_to_form(resolved_doctype, doc.name), "message": f"{resolved_doctype} cancelled successfully"}


def run_workflow_action_internal(doctype: str, record: str, action: str) -> dict[str, Any]:
    resolved_doctype = resolve_doctype_name_internal(doctype)
    workflow_action = str(action or "").strip().title()
    if not resolved_doctype or not workflow_action:
        return {"ok": False, "type": "document", "message": _("Doctype and workflow action are required.")}
    resolved_name, error = _resolve_generic_record_for_action(resolved_doctype, record, "run_workflow_action", {"workflow_action": workflow_action})
    if error:
        return error
    doc = frappe.get_doc(resolved_doctype, resolved_name)
    doc.check_permission("write")
    updated = frappe.get_attr("frappe.model.workflow.apply_workflow")(doc, workflow_action)
    return {"ok": True, "type": "document", "doctype": resolved_doctype, "name": updated.name, "url": get_url_to_form(resolved_doctype, updated.name), "message": f"{workflow_action} applied to {resolved_doctype} successfully"}


@frappe.whitelist()
def create_sales_order(customer: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    try:
        return create_sales_order_internal(customer=customer, items=items, company=company)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to create Sales Orders.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Create Sales Order Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def list_erp_documents(doctype: str, filters: dict[str, Any] | str | None = None, limit: int = 20) -> dict[str, Any]:
    try:
        return list_erp_documents_internal(doctype, filters=filters, limit=limit)
    except frappe.PermissionError:
        return _error(_("You do not have permission to read that data."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant List ERP Documents Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


@frappe.whitelist()
def get_erp_document(doctype: str, name: str) -> dict[str, Any]:
    try:
        return get_erp_document_internal(doctype, name)
    except frappe.PermissionError:
        return _error(_("You do not have permission to read that document."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Get ERP Document Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


@frappe.whitelist()
def search_erp_documents(query: str, doctype: str | None = None, limit: int = 10) -> dict[str, Any]:
    try:
        return search_erp_documents_internal(query, doctype=doctype, limit=limit)
    except frappe.PermissionError:
        return _error(_("You do not have permission to read that data."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Search ERP Documents Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


@frappe.whitelist()
def create_quotation(customer: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    try:
        return create_quotation_internal(customer=customer, items=items, company=company)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to create Quotations.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Create Quotation Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def create_purchase_order(supplier: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    try:
        return create_purchase_order_internal(supplier=supplier, items=items, company=company)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to create Purchase Orders.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Create Purchase Order Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def update_erp_document(doctype: str, record: str, field: str, value: Any) -> dict[str, Any]:
    try:
        return update_erp_document_internal(doctype=doctype, record=record, field=field, value=value)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to update that document.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Update ERP Document Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def submit_erp_document(doctype: str, record: str) -> dict[str, Any]:
    try:
        return submit_erp_document_internal(doctype=doctype, record=record)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to submit that document.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Submit ERP Document Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def cancel_erp_document(doctype: str, record: str) -> dict[str, Any]:
    try:
        return cancel_erp_document_internal(doctype=doctype, record=record)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to cancel that document.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Cancel ERP Document Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def run_workflow_action(doctype: str, record: str, action: str) -> dict[str, Any]:
    try:
        return run_workflow_action_internal(doctype=doctype, record=record, action=action)
    except frappe.PermissionError:
        return {"ok": False, "type": "document", "message": _("You do not have permission to run that workflow action.")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Run Workflow Action Error")
        return {"ok": False, "type": "document", "message": str(exc) or _("Unknown error")}


@frappe.whitelist()
def list_erp_doctypes(search: str | None = None, module: str | None = None, limit: int = 100) -> dict[str, Any]:
    try:
        return list_erp_doctypes_internal(search=search, module=module, limit=limit)
    except frappe.PermissionError:
        return _error(_("You do not have permission to read DocType metadata."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant List ERP Doctypes Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


@frappe.whitelist()
def get_doctype_fields(doctype: str, writable_only: int | str = 0) -> dict[str, Any]:
    try:
        writable_flag = int(str(writable_only or 0).strip() or 0)
        return get_doctype_fields_internal(doctype=doctype, writable_only=bool(writable_flag))
    except frappe.PermissionError:
        return _error(_("You do not have permission to read DocType metadata."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Get Doctype Fields Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


@frappe.whitelist()
def describe_erp_schema(doctype: str) -> dict[str, Any]:
    try:
        return describe_erp_schema_internal(doctype=doctype)
    except frappe.PermissionError:
        return _error(_("You do not have permission to read DocType metadata."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Describe ERP Schema Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")
