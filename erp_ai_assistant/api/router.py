import json
import re
from typing import Any

import frappe
from frappe import _

from .catalog import SAFE_ERP_ALIASES, resolve_safe_doctype_from_text
from .context_resolver import normalize_context_payload
from .entity_extractor import extract_natural_filters as _extract_natural_filters_core
from .erp_tools import (
    answer_erp_query_internal,
    cancel_erp_document_internal,
    count_erp_documents_internal,
    create_erp_document_internal,
    create_transaction_document_internal,
    create_purchase_order_internal,
    create_quotation_internal,
    create_sales_order_internal,
    describe_erp_schema_internal,
    extract_child_table_rows_internal,
    extract_field_value_pairs_internal,
    extract_update_instruction_internal,
    get_required_doctype_fields_internal,
    get_doctype_fields_internal,
    get_erp_document_internal,
    get_person_details_internal,
    list_erp_doctypes_internal,
    list_erp_documents_internal,
    resolve_doctype_name_internal,
    run_workflow_action_internal,
    search_erp_documents_internal,
    submit_erp_document_internal,
    update_erp_document_internal,
)
from .file_tools import export_doctype_list_excel_internal, generate_document_pdf_internal
from .intent_detector import normalize_prompt as _normalize_prompt_core
from .parser import extract_requested_fields, parse_prompt
from .resource_registry import list_resource_specs
from .tool_registry import list_tool_specs


def _parse_json_arg(value: Any, default: Any) -> Any:
    if value in (None, "", []):
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _normalize_prompt(prompt: str) -> str:
    return _normalize_prompt_core(prompt)


def _match_count_prompt(prompt: str) -> bool:
    text = _normalize_prompt(prompt).lower()
    return any(term in text for term in ("how many ", "count ", "count of "))


def _resolve_safe_target(text: str) -> tuple[str, dict[str, Any]] | None:
    return resolve_safe_doctype_from_text(text)


def _parse_sales_order_items(prompt: str) -> list[dict[str, Any]]:
    text = _normalize_prompt(prompt)
    json_match = re.search(r"(\[[\s\S]+\])", text)
    if json_match:
        try:
            parsed = json.loads(json_match.group(1))
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

    items_clause = ""
    for marker in (" with items ", " items ", " with item "):
        if marker in text.lower():
            lower_text = text.lower()
            index = lower_text.find(marker)
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
            r"(?P<item_code>[\w./-]+)\s*(?:x|qty|quantity)?\s*(?P<qty>\d+(?:\.\d+)?)?(?:\s*@\s*(?P<rate>\d+(?:\.\d+)?))?$",
            part,
            re.IGNORECASE,
        )
        if not match:
            continue
        item_code = str(match.group("item_code") or "").strip()
        qty = float(match.group("qty") or 1)
        row = {"item_code": item_code, "qty": qty}
        if match.group("rate"):
            row["rate"] = float(match.group("rate"))
        rows.append(row)
    return rows


def _parse_sales_order_request(prompt: str, context: dict[str, Any]) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    if "sales order" not in lowered:
        return None

    customer = ""
    customer_match = re.search(r"(?:for|customer)\s+(?:customer\s+)?(?P<customer>.+?)(?:\s+with\s+items|\s+items|$)", text, re.IGNORECASE)
    if customer_match:
        customer = str(customer_match.group("customer") or "").strip(" .")
    elif str(context.get("doctype") or "").strip() == "Customer":
        customer = str(context.get("docname") or "").strip()

    items = _parse_sales_order_items(text)
    company_match = re.search(r"\bcompany\s+(?P<company>.+?)(?:\s+with\s+items|$)", text, re.IGNORECASE)
    company = str(company_match.group("company") or "").strip(" .") if company_match else None

    return {
        "doctype": "Sales Order",
        "customer": customer,
        "items": items,
        "company": company,
    }


def _parse_transaction_request(prompt: str, context: dict[str, Any], transaction_label: str, party_field: str) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    if transaction_label not in lowered:
        return None

    party = ""
    party_match = re.search(rf"(?:for|{re.escape(party_field)})\s+(?:{re.escape(party_field)}\s+)?(?P<party>.+?)(?:\s+with\s+items|\s+items|$)", text, re.IGNORECASE)
    if party_match:
        party = str(party_match.group("party") or "").strip(" .")
    elif str(context.get("doctype") or "").strip() == party_field.title():
        party = str(context.get("docname") or "").strip()

    items = _parse_sales_order_items(text)
    company_match = re.search(r"\bcompany\s+(?P<company>.+?)(?:\s+with\s+items|$)", text, re.IGNORECASE)
    company = str(company_match.group("company") or "").strip(" .") if company_match else None
    return {"party": party, "items": items, "company": company}


def _parse_generic_transaction_request(prompt: str, context: dict[str, Any]) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    target = _resolve_safe_target(text)
    if not target:
        return None
    _key, config = target
    doctype = str(config.get("doctype") or "").strip()
    party_field = str(config.get("party_field") or "").strip()
    if not doctype or not party_field or "item" not in lowered:
        return None
    if doctype in {"Sales Order", "Quotation", "Purchase Order"}:
        return None
    party = ""
    party_label = "supplier" if party_field == "supplier" else "customer"
    party_match = re.search(rf"(?:for|{re.escape(party_label)}|party)\s+(?:{re.escape(party_label)}\s+)?(?P<party>.+?)(?:\s+with\s+items|\s+items|$)", text, re.IGNORECASE)
    if party_match:
        party = str(party_match.group("party") or "").strip(" .")
    elif str(context.get("doctype") or "").strip() in {"Customer", "Supplier"}:
        party = str(context.get("docname") or "").strip()
    items = _parse_sales_order_items(text)
    company_match = re.search(r"\bcompany\s+(?P<company>.+?)(?:\s+with\s+items|$)", text, re.IGNORECASE)
    company = str(company_match.group("company") or "").strip(" .") if company_match else None
    return {"doctype": doctype, "party": party, "items": items, "company": company, "party_label": party_label}


def _parse_workflow_request(prompt: str, context: dict[str, Any]) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    match = re.match(r"^(?P<action>submit|cancel|approve|reject|reopen)\s+(?:(?:this|current)\s+(?:record|document)|(?P<target>.+))$", text, re.IGNORECASE)
    if not match:
        return None
    action = str(match.group("action") or "").strip().lower()
    target_text = str(match.group("target") or "").strip()
    if not target_text and context.get("doctype") and context.get("docname"):
        return {"action": action, "doctype": str(context["doctype"]), "record": str(context["docname"])}
    target = _resolve_safe_target(target_text) if target_text else None
    if target:
        _key, config = target
        doctype = str(config.get("doctype") or "").strip()
        stripped = re.sub(rf"^{re.escape(doctype)}\s+", "", target_text, flags=re.IGNORECASE).strip()
        if stripped == target_text:
            alias_candidates = [alias for alias, mapped in SAFE_ERP_ALIASES.items() if mapped == _key]
            for alias in sorted(alias_candidates, key=len, reverse=True):
                updated = re.sub(rf"^{re.escape(alias)}\s+", "", target_text, flags=re.IGNORECASE).strip()
                if updated != target_text:
                    stripped = updated
                    break
        return {"action": action, "doctype": doctype, "record": stripped or target_text}
    return None


def _parse_pdf_request(prompt: str) -> tuple[str, str] | None:
    text = _normalize_prompt(prompt)
    target = _resolve_safe_target(text)
    if not target:
        return None
    _key, config = target
    doctype = str(config.get("doctype") or "").strip()
    alias_candidates = [alias for alias, key in SAFE_ERP_ALIASES.items() if key == _key]
    for alias in sorted(alias_candidates, key=len, reverse=True):
        patterns = [
            rf"(?:generate|create|export|download)\s+pdf\s+(?:for|of)?\s*{re.escape(alias)}\s+(?P<docname>[A-Za-z0-9./_-]+)",
            rf"(?:generate|create|export|download)\s+{re.escape(alias)}\s+(?P<docname>[A-Za-z0-9./_-]+)\s+pdf",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            docname = str(match.group("docname") or "").strip()
            if doctype and docname:
                return doctype, docname
    return None


def _extract_natural_filters(text: str, config: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    return _extract_natural_filters_core(text, config, context=context)


def _parse_excel_request(prompt: str, context: dict[str, Any] | None = None) -> tuple[str, dict[str, Any] | None, list[str] | None] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    export_terms = (
        "excel",
        "xlsx",
        "spreadsheet",
        "csv",
        "sheet",
        "download file",
        "make sheet",
        "save this as excel",
        "save as excel",
        "save to excel",
        "export file",
        "download excel",
        "create excel file",
    )
    if not any(term in lowered for term in export_terms):
        return None
    target = _resolve_safe_target(text)
    if not target:
        context_doctype = str((context or {}).get("doctype") or "").strip()
        if context_doctype:
            resolved_context = resolve_doctype_name_internal(context_doctype)
            if resolved_context:
                target = (resolved_context.lower(), {"doctype": resolved_context, "filter_fields": set()})
    if not target:
        return None
    _key, config = target
    filters = _extract_natural_filters(text, config, context)
    fields = extract_requested_fields(text) or None
    return str(config.get("doctype") or "").strip(), filters or None, fields


def _parse_related_list_request(prompt: str, context: dict[str, Any]) -> tuple[str, dict[str, Any], str] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    target = _resolve_safe_target(text)
    if not target:
        return None
    _key, config = target
    doctype = str(config.get("doctype") or "").strip()
    filter_fields = set(config.get("filter_fields") or set())
    if not doctype:
        return None
    filters = _extract_natural_filters(text, config, context)
    if not filters:
        return None
    if "customer" in filters and any(term in lowered for term in ("customer", "invoice", "order", "quotation", "delivery")):
        return doctype, filters, f"{doctype} for customer {filters['customer']}"
    if "supplier" in filters and any(term in lowered for term in ("supplier", "purchase", "receipt", "invoice", "order")):
        return doctype, filters, f"{doctype} for supplier {filters['supplier']}"
    if any(field in filters for field in ("posting_date", "transaction_date", "status")):
        return doctype, filters, f"{doctype} list"

    return None


def _parse_update_request(prompt: str, context: dict[str, Any]) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    if not any(lowered.startswith(term) for term in ("update ", "set ", "change ")):
        return None

    value_match = re.search(r"\s+(?:to|as)\s+(?P<value>.+)$", text, re.IGNORECASE)
    if not value_match:
        return None
    value = str(value_match.group("value") or "").strip()
    body = text[: value_match.start()].strip()
    target = _resolve_safe_target(body)

    if not target:
        context_doctype = str(context.get("doctype") or "").strip()
        if context_doctype:
            target = _resolve_safe_target(context_doctype)
            leading = re.sub(r"^(?:update|set|change)\s+", "", body, flags=re.IGNORECASE).strip()
            body = leading
    if not target:
        body_without_verb = re.sub(r"^(?:update|set|change)\s+", "", body, flags=re.IGNORECASE).strip()
        any_doctype = resolve_doctype_name_internal(body_without_verb.split(" ", 1)[0])
        if not any_doctype:
            any_doctype = resolve_doctype_name_internal(body_without_verb)
        if any_doctype:
            target = (any_doctype.lower(), {"doctype": any_doctype})
        else:
            return None

    _key, config = target
    doctype = str(config.get("doctype") or "").strip()
    body_without_verb = re.sub(r"^(?:update|set|change)\s+", "", body, flags=re.IGNORECASE).strip()
    stripped_doctype = re.sub(rf"^{re.escape(doctype)}\s+", "", body_without_verb, flags=re.IGNORECASE).strip()
    if stripped_doctype != body_without_verb:
        body_without_verb = stripped_doctype
    alias_candidates = [alias for alias, mapped in SAFE_ERP_ALIASES.items() if mapped == _key]
    for alias in sorted(alias_candidates, key=len, reverse=True):
        updated_body = re.sub(rf"^{re.escape(alias)}\s+", "", body_without_verb, flags=re.IGNORECASE).strip()
        if updated_body != body_without_verb:
            body_without_verb = updated_body
            break

    instruction = extract_update_instruction_internal(doctype, body_without_verb, value)
    if instruction:
        return {
            "doctype": doctype,
            "record": instruction["record"] or str(context.get("docname") or "").strip(),
            "field": instruction["field"],
            "value": instruction["value"],
        }
    return {
        "doctype": doctype,
        "record": str(context.get("docname") or "").strip(),
        "field": "",
        "value": value,
    }


def _parse_generic_create_request(prompt: str, context: dict[str, Any]) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    match = re.search(r"^(?:create|new|add)\s+(?P<doctype>.+?)(?:\s+with\s+(?P<body>.+))?$", text, re.IGNORECASE)
    if not match:
        return None
    body = str(match.group("body") or "").strip()
    doctype_hint = str(match.group("doctype") or "").strip(" .")
    if not doctype_hint:
        return None

    resolved_doctype = None
    candidate_hint = doctype_hint
    hint_parts = doctype_hint.split()
    while hint_parts:
        candidate_hint = " ".join(hint_parts).strip()
        resolved_doctype = resolve_doctype_name_internal(candidate_hint)
        if resolved_doctype:
            remainder = doctype_hint[len(candidate_hint):].strip(" ,.")
            if remainder and not body:
                body = remainder
            break
        hint_parts.pop()
    if not resolved_doctype:
        return None
    if not body and str(context.get("doctype") or "").strip() == resolved_doctype:
        body = ""
    values = extract_field_value_pairs_internal(resolved_doctype, body)
    child_rows = extract_child_table_rows_internal(resolved_doctype, body)
    if child_rows:
        values.update(child_rows)
    return {"doctype": resolved_doctype, "values": values, "raw_body": body}


def _render_lookup_result(result: dict[str, Any], label: str) -> dict[str, Any]:
    data = result.get("data") or {}
    lines = [label, ""]
    shown = 0
    for key, value in data.items():
        if value in (None, "", [], {}):
            continue
        lines.append(f"{key}: {value}")
        shown += 1
        if shown >= 8:
            break
    return {
        "ok": True,
        "type": "answer",
        "answer": "\n".join(lines).strip(),
        "data": result,
    }


def _render_list_result(result: dict[str, Any], label: str) -> dict[str, Any]:
    rows = result.get("data") or []
    lines = [label, ""]
    if not rows:
        lines.append("No records found.")
    for index, row in enumerate(rows[:20], start=1):
        if not isinstance(row, dict):
            lines.append(f"{index}. {row}")
            continue
        primary = row.get("name") or f"Row {index}"
        extras = [f"{key}: {value}" for key, value in row.items() if key != "name" and value not in (None, "", [], {})][:3]
        suffix = f" ({', '.join(extras)})" if extras else ""
        lines.append(f"{index}. {primary}{suffix}")
    return {
        "ok": True,
        "type": "answer",
        "answer": "\n".join(lines).strip(),
        "data": result,
    }


def _parse_named_document_request(prompt: str) -> tuple[str, str] | None:
    text = _normalize_prompt(prompt)
    target = _resolve_safe_target(text)
    if not target:
        return None
    key, config = target
    alias_candidates = [alias for alias, mapped in SAFE_ERP_ALIASES.items() if mapped == key]
    for alias in sorted(alias_candidates, key=len, reverse=True):
        patterns = [
            rf"(?:show|open|get|view|find)\s+(?:me\s+)?{re.escape(alias)}\s+(?P<docname>[A-Za-z0-9./_-]+)",
            rf"{re.escape(alias)}\s+(?P<docname>[A-Za-z0-9./_-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                docname = str(match.group("docname") or "").strip()
                if docname.lower() in {
                    "of",
                    "for",
                    "list",
                    "details",
                    "detail",
                    "profile",
                    "information",
                    "info",
                    "customer",
                    "supplier",
                    "employee",
                }:
                    continue
                return str(config.get("doctype") or "").strip(), docname
    return None


def _parse_list_request(prompt: str) -> str | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    if not any(term in lowered for term in ("list ", "show ", "display ", "get ")):
        return None
    target = _resolve_safe_target(text)
    if not target:
        return None
    _key, config = target
    return str(config.get("doctype") or "").strip()


def _parse_doctype_list_request(prompt: str) -> dict[str, Any] | None:
    text = _normalize_prompt(prompt)
    lowered = text.lower()
    if not any(term in lowered for term in ("list doctypes", "show doctypes", "all doctypes", "available doctypes", "list all doctypes")):
        return None
    module_match = re.search(r"\bmodule\s+(?P<module>.+)$", text, re.IGNORECASE)
    search_match = re.search(r"\b(?:search|matching)\s+(?P<search>.+)$", text, re.IGNORECASE)
    return {
        "module": str(module_match.group("module") or "").strip() if module_match else None,
        "search": str(search_match.group("search") or "").strip() if search_match else None,
    }


def _parse_doctype_fields_request(prompt: str) -> tuple[str, bool] | None:
    text = _normalize_prompt(prompt)
    patterns = (
        r"^(?:show|list|get|display)\s+(?P<writable>writable\s+)?fields\s+(?:for|of)\s+(?P<doctype>.+)$",
        r"^(?:what are the|which are the)\s+(?P<writable>writable\s+)?fields\s+(?:for|of)\s+(?P<doctype>.+)$",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return str(match.group("doctype") or "").strip(" ."), bool(match.group("writable"))
    return None


def _parse_schema_request(prompt: str) -> str | None:
    text = _normalize_prompt(prompt)
    patterns = (
        r"^(?:describe|show)\s+schema\s+(?:for|of)\s+(?P<doctype>.+)$",
        r"^(?:describe|show)\s+(?P<doctype>.+)\s+schema$",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return str(match.group("doctype") or "").strip(" .")
    return None


def _parse_how_to_create_request(prompt: str) -> str | None:
    text = _normalize_prompt(prompt)
    match = re.match(r"^(?:how to|how do i|how can i)\s+create\s+(?P<doctype>.+?)(?:\?|$)", text, re.IGNORECASE)
    if not match:
        return None
    return str(match.group("doctype") or "").strip(" ?.") or None


def _parse_tool_catalog_request(prompt: str) -> bool:
    text = _normalize_prompt(prompt).lower()
    return any(
        phrase in text
        for phrase in (
            "list available tools",
            "show available tools",
            "what tools can you use",
            "what tools do you have",
            "list tools",
            "show tools",
            "available tools",
        )
    )


def _parse_resource_catalog_request(prompt: str) -> bool:
    text = _normalize_prompt(prompt).lower()
    return any(
        phrase in text
        for phrase in (
            "list available resources",
            "show available resources",
            "what resources can you use",
            "what resources do you have",
            "list resources",
            "show resources",
            "available resources",
        )
    )


def _render_catalog_answer(title: str, rows: list[dict[str, Any]], *, key_name: str = "name") -> dict[str, Any]:
    lines = [title, ""]
    for index, row in enumerate(rows, start=1):
        name = str(row.get(key_name) or "").strip() or f"Item {index}"
        description = str(row.get("description") or row.get("title") or "").strip()
        suffix = f": {description}" if description else ""
        lines.append(f"{index}. {name}{suffix}")
    if len(lines) == 2:
        lines.append("No entries found.")
    return {
        "ok": True,
        "type": "answer",
        "answer": "\n".join(lines).strip(),
        "data": rows,
    }


def _unmatched_router_result(message: str | None = None) -> dict[str, Any]:
    return {
        "ok": False,
        "matched": False,
        "type": "router",
        "action": None,
        "message": message or "Sorry, I do not support that action yet.",
    }


def _planner_priority_route(
    prompt: str,
    context: dict[str, Any],
    planner_result: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(planner_result, dict) or not planner_result:
        return None

    if str(planner_result.get("route_target") or "").strip() == "provider_chat":
        return _unmatched_router_result()

    recommended_tools = {str(name or "").strip() for name in planner_result.get("tool_names") or [] if str(name or "").strip()}
    if not recommended_tools:
        return None

    text = _normalize_prompt(prompt)
    lowered = text.lower()

    if _parse_tool_catalog_request(text):
        result = _render_catalog_answer("Available tools", list_tool_specs())
        result["matched"] = True
        result["action"] = "list_available_tools"
        return result

    if _parse_resource_catalog_request(text):
        result = _render_catalog_answer("Available resources", list_resource_specs())
        result["matched"] = True
        result["action"] = "list_available_resources"
        return result

    if recommended_tools.intersection({"answer_erp_query"}) and _match_count_prompt(text):
        target = _resolve_safe_target(text)
        if target:
            _key, config = target
            filters = {"status": "Active"} if "active employee" in lowered and str(config.get("doctype")) == "Employee" else None
            result = count_erp_documents_internal(str(config.get("doctype") or "").strip(), filters=filters)
        else:
            result = answer_erp_query_internal(text)
        result["matched"] = True
        result["action"] = "answer_erp_query"
        return result

    if recommended_tools.intersection({"describe_erp_schema", "get_doctype_fields", "list_erp_doctypes"}):
        doctype_list_request = _parse_doctype_list_request(text)
        if doctype_list_request is not None:
            result = list_erp_doctypes_internal(
                search=doctype_list_request.get("search"),
                module=doctype_list_request.get("module"),
            )
            result["matched"] = True
            result["action"] = "list_erp_doctypes"
            return result
        doctype_fields_request = _parse_doctype_fields_request(text)
        if doctype_fields_request:
            doctype, writable_only = doctype_fields_request
            result = get_doctype_fields_internal(doctype, writable_only=writable_only)
            result["matched"] = True
            result["action"] = "get_doctype_fields"
            return result
        schema_request = _parse_schema_request(text)
        if schema_request:
            result = describe_erp_schema_internal(schema_request)
            result["matched"] = True
            result["action"] = "describe_erp_schema"
            return result

    if recommended_tools.intersection({"export_doctype_list_excel", "export_employee_list_excel", "generate_document_pdf", "generate_report"}):
        excel_request = _parse_excel_request(text, context)
        if excel_request:
            doctype, filters, fields = excel_request
            result = export_doctype_list_excel_internal(doctype, filters, fields=fields)
            result["matched"] = True
            result["action"] = "export_doctype_list_excel"
            return result
        pdf_request = _parse_pdf_request(text)
        if pdf_request:
            doctype, docname = pdf_request
            result = generate_document_pdf_internal(doctype, docname)
            result["matched"] = True
            result["action"] = "generate_document_pdf"
            return result

    if recommended_tools.intersection({"submit_erp_document", "cancel_erp_document", "run_workflow_action"}):
        workflow_request = _parse_workflow_request(text, context)
        if workflow_request:
            action = workflow_request["action"]
            doctype = workflow_request["doctype"]
            record = workflow_request["record"]
            if action == "submit":
                result = submit_erp_document_internal(doctype, record)
                result["matched"] = True
                result["action"] = "submit_erp_document"
                return result
            if action == "cancel":
                result = cancel_erp_document_internal(doctype, record)
                result["matched"] = True
                result["action"] = "cancel_erp_document"
                return result
            result = run_workflow_action_internal(doctype, record, action)
            result["matched"] = True
            result["action"] = "run_workflow_action"
            return result

    if recommended_tools.intersection({"update_erp_document", "update_document"}):
        update_request = _parse_update_request(text, context)
        if update_request and update_request.get("record") and update_request.get("field"):
            result = update_erp_document_internal(
                doctype=update_request["doctype"],
                record=update_request["record"],
                field=update_request["field"],
                value=update_request["value"],
            )
            result["matched"] = True
            result["action"] = "update_erp_document"
            return result

    if recommended_tools.intersection({"create_sales_order", "create_quotation", "create_purchase_order", "create_transaction_document", "create_erp_document", "create_document"}):
        sales_order_request = _parse_sales_order_request(text, context)
        if sales_order_request and sales_order_request.get("customer") and sales_order_request.get("items"):
            result = create_sales_order_internal(**sales_order_request)
            result["matched"] = True
            result["action"] = "create_sales_order"
            return result
        quotation_request = _parse_transaction_request(text, context, "quotation", "customer")
        if quotation_request and quotation_request.get("party") and quotation_request.get("items"):
            result = create_quotation_internal(customer=quotation_request["party"], items=quotation_request["items"], company=quotation_request["company"])
            result["matched"] = True
            result["action"] = "create_quotation"
            return result
        purchase_order_request = _parse_transaction_request(text, context, "purchase order", "supplier")
        if purchase_order_request and purchase_order_request.get("party") and purchase_order_request.get("items"):
            result = create_purchase_order_internal(supplier=purchase_order_request["party"], items=purchase_order_request["items"], company=purchase_order_request["company"])
            result["matched"] = True
            result["action"] = "create_purchase_order"
            return result
        generic_transaction_request = _parse_generic_transaction_request(text, context)
        if generic_transaction_request and generic_transaction_request.get("party") and generic_transaction_request.get("items"):
            result = create_transaction_document_internal(
                doctype=generic_transaction_request["doctype"],
                party_name=generic_transaction_request["party"],
                items=generic_transaction_request["items"],
                company=generic_transaction_request["company"],
            )
            result["matched"] = True
            result["action"] = "create_transaction_document"
            return result
        generic_create_request = _parse_generic_create_request(text, context)
        if generic_create_request and generic_create_request.get("values"):
            result = create_erp_document_internal(
                doctype=generic_create_request["doctype"],
                values=generic_create_request["values"],
            )
            result["matched"] = True
            result["action"] = "create_erp_document"
            return result

    if recommended_tools.intersection({"list_erp_documents", "get_erp_document", "search_erp_documents"}):
        details_request = _parse_details_request(text)
        if details_request:
            target = _resolve_safe_target(details_request)
            if target:
                _key, config = target
                doctype = str(config.get("doctype") or "").strip()
                stripped = re.sub(rf"^{re.escape(doctype)}\s+", "", details_request, flags=re.IGNORECASE).strip()
                if stripped:
                    result = _render_lookup_result(get_erp_document_internal(doctype, stripped), f"{doctype} {stripped}")
                else:
                    result = _render_list_result(list_erp_documents_internal(doctype), f"{doctype} list")
                result["matched"] = True
                result["action"] = "get_erp_document"
                return result
        named_document_request = _parse_named_document_request(text)
        if named_document_request:
            doctype, docname = named_document_request
            result = _render_lookup_result(get_erp_document_internal(doctype, docname), f"{doctype} {docname}")
            result["matched"] = True
            result["action"] = "get_erp_document"
            return result
        related_list_request = _parse_related_list_request(text, context)
        if related_list_request:
            doctype, filters, heading = related_list_request
            result = _render_list_result(list_erp_documents_internal(doctype, filters=filters), heading)
            result["matched"] = True
            result["action"] = "list_erp_documents"
            return result
        list_request = _parse_list_request(text)
        if list_request:
            result = _render_list_result(list_erp_documents_internal(list_request), f"{list_request} list")
            result["matched"] = True
            result["action"] = "list_erp_documents"
            return result
        if any(term in lowered for term in ("search ", "find ")) and not any(term in lowered for term in ("pdf", "excel", "xlsx", "spreadsheet", "sales order")):
            target = _resolve_safe_target(text)
            search_result = search_erp_documents_internal(text, doctype=str(target[1].get("doctype")) if target else None)
            if search_result.get("ok"):
                result = _render_list_result(search_result, "Search results")
                result["matched"] = True
                result["action"] = "search_erp_documents"
                return result

    return None


def _parse_details_request(prompt: str) -> str | None:
    text = _normalize_prompt(prompt)
    patterns = (
        r"^(?:pull out|show|get|find|retrieve|open|display)\s+(?P<subject>.+?)\s+(?:details|detail|profile|information|info)$",
        r"^(?:show|get|find|retrieve|open|display)\s+(?:details|detail|profile|information|info)\s+(?:for|of)\s+(?P<subject>.+)$",
        r"^(?P<subject>.+?)\s+(?:details|detail|profile|information|info)$",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return str(match.group("subject") or "").strip(" .?")
    return None


def route_prompt_internal(
    prompt: str,
    context: dict[str, Any] | None = None,
    planner_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_context = context or {}
    text = _normalize_prompt(prompt)
    if not text:
        return {"ok": False, "matched": False, "type": "router", "action": None, "message": _("Prompt is required")}

    lowered = text.lower()

    prioritized = _planner_priority_route(text, current_context, planner_result)
    if isinstance(prioritized, dict):
        if prioritized.get("matched"):
            return prioritized
        if str((planner_result or {}).get("route_target") or "").strip() == "provider_chat":
            return prioritized

    if _match_count_prompt(text):
        target = _resolve_safe_target(text)
        if target:
            _key, config = target
            filters = {"status": "Active"} if "active employee" in lowered and str(config.get("doctype")) == "Employee" else None
            result = count_erp_documents_internal(str(config.get("doctype") or "").strip(), filters=filters)
        else:
            result = answer_erp_query_internal(text)
        result["matched"] = True
        result["action"] = "answer_erp_query"
        return result

    doctype_list_request = _parse_doctype_list_request(text)
    if doctype_list_request is not None:
        result = list_erp_doctypes_internal(
            search=doctype_list_request.get("search"),
            module=doctype_list_request.get("module"),
        )
        result["matched"] = True
        result["action"] = "list_erp_doctypes"
        return result

    doctype_fields_request = _parse_doctype_fields_request(text)
    if doctype_fields_request:
        doctype, writable_only = doctype_fields_request
        result = get_doctype_fields_internal(doctype, writable_only=writable_only)
        result["matched"] = True
        result["action"] = "get_doctype_fields"
        return result

    schema_request = _parse_schema_request(text)
    if schema_request:
        result = describe_erp_schema_internal(schema_request)
        result["matched"] = True
        result["action"] = "describe_erp_schema"
        return result

    how_to_create_request = _parse_how_to_create_request(text)
    if how_to_create_request:
        resolved_doctype = resolve_doctype_name_internal(how_to_create_request)
        if resolved_doctype:
            missing_fields = get_required_doctype_fields_internal(resolved_doctype)
            field_labels = ", ".join(str(row.get("label") or row.get("fieldname") or "").strip() for row in missing_fields[:6])
            guidance = f"To create {resolved_doctype}, provide the fields in a prompt like: create {resolved_doctype} with field name value."
            if field_labels:
                guidance += f" Common required fields: {field_labels}."
            return {
                "ok": True,
                "matched": True,
                "type": "answer",
                "action": "how_to_create",
                "answer": guidance,
                "data": {"doctype": resolved_doctype, "required_fields": missing_fields},
            }

    if _parse_tool_catalog_request(text):
        result = _render_catalog_answer("Available tools", list_tool_specs())
        result["matched"] = True
        result["action"] = "list_available_tools"
        return result

    if _parse_resource_catalog_request(text):
        result = _render_catalog_answer("Available resources", list_resource_specs())
        result["matched"] = True
        result["action"] = "list_available_resources"
        return result

    excel_request = _parse_excel_request(text, current_context)
    if excel_request:
        doctype, filters, fields = excel_request
        result = export_doctype_list_excel_internal(doctype, filters, fields=fields)
        result["matched"] = True
        result["action"] = "export_doctype_list_excel"
        return result
    if any(
        term in lowered
        for term in (
            "excel",
            "xlsx",
            "spreadsheet",
            "csv",
            "sheet",
            "download file",
            "make sheet",
            "save this as excel",
            "save as excel",
            "save to excel",
            "export file",
            "download excel",
            "create excel file",
        )
    ):
        return {
            "ok": False,
            "matched": True,
            "type": "router",
            "action": "export_doctype_list_excel",
            "message": "Please specify what to export, for example: export employee list to excel. If you are on a document page, you can also say: create excel file.",
        }

    details_request = _parse_details_request(text)
    if details_request:
        target = _resolve_safe_target(details_request)
        if target:
            _key, config = target
            doctype = str(config.get("doctype") or "").strip()
            stripped = re.sub(rf"^{re.escape(doctype)}\s+", "", details_request, flags=re.IGNORECASE).strip()
            if stripped:
                result = _render_lookup_result(get_erp_document_internal(doctype, stripped), f"{doctype} {stripped}")
            else:
                result = _render_list_result(list_erp_documents_internal(doctype), f"{doctype} list")
            result["matched"] = True
            result["action"] = "get_erp_document"
            return result
        result = get_person_details_internal(details_request)
        if result.get("ok") and result.get("doctype") and result.get("name"):
            rendered = _render_lookup_result(result, f"{result.get('doctype')} {result.get('name')}")
            rendered["matched"] = True
            rendered["action"] = "get_erp_document"
            return rendered
        result["matched"] = True
        result["action"] = "get_person_details"
        return result

    workflow_request = _parse_workflow_request(text, current_context)
    if workflow_request:
        action = workflow_request["action"]
        doctype = workflow_request["doctype"]
        record = workflow_request["record"]
        if action == "submit":
            result = submit_erp_document_internal(doctype, record)
            result["matched"] = True
            result["action"] = "submit_erp_document"
            return result
        if action == "cancel":
            result = cancel_erp_document_internal(doctype, record)
            result["matched"] = True
            result["action"] = "cancel_erp_document"
            return result
        result = run_workflow_action_internal(doctype, record, action)
        result["matched"] = True
        result["action"] = "run_workflow_action"
        return result

    update_request = _parse_update_request(text, current_context)
    if update_request:
        if not update_request["record"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "update_erp_document",
                "message": "Please specify which record to update, for example: update employee EMP-0001 birthday to April 30, 1993",
            }
        if not update_request["field"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "update_erp_document",
                "message": "Please specify which field to update, for example: update employee Macdenver Magbojos birthday to April 30, 1993",
            }
        result = update_erp_document_internal(
            doctype=update_request["doctype"],
            record=update_request["record"],
            field=update_request["field"],
            value=update_request["value"],
        )
        result["matched"] = True
        result["action"] = "update_erp_document"
        return result

    named_document_request = _parse_named_document_request(text)
    if named_document_request:
        doctype, docname = named_document_request
        result = _render_lookup_result(get_erp_document_internal(doctype, docname), f"{doctype} {docname}")
        result["matched"] = True
        result["action"] = "get_erp_document"
        return result

    related_list_request = _parse_related_list_request(text, current_context)
    if related_list_request:
        doctype, filters, heading = related_list_request
        result = _render_list_result(list_erp_documents_internal(doctype, filters=filters), heading)
        result["matched"] = True
        result["action"] = "list_erp_documents"
        return result

    list_request = _parse_list_request(text)
    if list_request:
        result = _render_list_result(list_erp_documents_internal(list_request), f"{list_request} list")
        result["matched"] = True
        result["action"] = "list_erp_documents"
        return result

    if any(term in lowered for term in ("search ", "find ")) and not any(term in lowered for term in ("pdf", "excel", "xlsx", "spreadsheet", "sales order")):
        target = _resolve_safe_target(text)
        search_result = search_erp_documents_internal(text, doctype=str(target[1].get("doctype")) if target else None)
        if search_result.get("ok"):
            result = _render_list_result(search_result, "Search results")
            result["matched"] = True
            result["action"] = "search_erp_documents"
            return result

    if "pdf" in lowered:
        pdf_request = _parse_pdf_request(text)
        if pdf_request:
            doctype, docname = pdf_request
            result = generate_document_pdf_internal(doctype, docname)
            result["matched"] = True
            result["action"] = "generate_document_pdf"
            return result

    sales_order_request = _parse_sales_order_request(text, current_context)
    if sales_order_request:
        if not sales_order_request["customer"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "create_sales_order",
                "message": "Please specify the customer, for example: create sales order for customer ABC with items ITEM-001 x 2",
            }
        if not sales_order_request["items"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "create_sales_order",
                "message": "Please specify items, for example: create sales order for customer ABC with items ITEM-001 x 2, ITEM-002 x 1",
            }
        result = create_sales_order_internal(**sales_order_request)
        result["matched"] = True
        result["action"] = "create_sales_order"
        return result

    quotation_request = _parse_transaction_request(text, current_context, "quotation", "customer")
    if quotation_request:
        if not quotation_request["party"] or not quotation_request["items"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "create_quotation",
                "message": "Please specify the customer and items, for example: create quotation for customer ABC with items ITEM-001 x 2",
            }
        result = create_quotation_internal(customer=quotation_request["party"], items=quotation_request["items"], company=quotation_request["company"])
        result["matched"] = True
        result["action"] = "create_quotation"
        return result

    purchase_order_request = _parse_transaction_request(text, current_context, "purchase order", "supplier")
    if purchase_order_request:
        if not purchase_order_request["party"] or not purchase_order_request["items"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "create_purchase_order",
                "message": "Please specify the supplier and items, for example: create purchase order for supplier ABC with items ITEM-001 x 2",
            }
        result = create_purchase_order_internal(supplier=purchase_order_request["party"], items=purchase_order_request["items"], company=purchase_order_request["company"])
        result["matched"] = True
        result["action"] = "create_purchase_order"
        return result

    generic_transaction_request = _parse_generic_transaction_request(text, current_context)
    if generic_transaction_request:
        if not generic_transaction_request["party"] or not generic_transaction_request["items"]:
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "create_transaction_document",
                "message": f"Please specify the {generic_transaction_request['party_label']} and items, for example: create {generic_transaction_request['doctype']} for {generic_transaction_request['party_label']} ABC with items ITEM-001 qty 2",
            }
        result = create_transaction_document_internal(
            doctype=generic_transaction_request["doctype"],
            party_name=generic_transaction_request["party"],
            items=generic_transaction_request["items"],
            company=generic_transaction_request["company"],
        )
        result["matched"] = True
        result["action"] = "create_transaction_document"
        return result

    generic_create_request = _parse_generic_create_request(text, current_context)
    if generic_create_request:
        if not generic_create_request["values"]:
            missing_fields = get_required_doctype_fields_internal(generic_create_request["doctype"])
            missing_labels = ", ".join(str(row.get("label") or row.get("fieldname") or "").strip() for row in missing_fields[:5])
            hint = f" Required fields include: {missing_labels}." if missing_labels else ""
            return {
                "ok": False,
                "matched": True,
                "type": "router",
                "action": "create_erp_document",
                "message": f"Please provide field values, for example: create {generic_create_request['doctype']} with field name value, another field value.{hint}",
            }
        result = create_erp_document_internal(
            doctype=generic_create_request["doctype"],
            values=generic_create_request["values"],
        )
        result["matched"] = True
        result["action"] = "create_erp_document"
        return result

    return {
        "ok": False,
        "matched": False,
        "type": "router",
        "action": None,
        "message": "Sorry, I do not support that action yet.",
    }


@frappe.whitelist()
def route_prompt(
    prompt: str,
    context: dict[str, Any] | str | None = None,
    planner_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        parsed_context = normalize_context_payload(context)
        if isinstance(planner_result, str):
            try:
                planner_result = json.loads(planner_result)
            except Exception:
                planner_result = None
        parsed = parse_prompt(prompt, parsed_context)
        if not parsed.get("ok"):
            legacy = route_prompt_internal(prompt, context=parsed_context, planner_result=planner_result)
            if legacy.get("matched"):
                return legacy
            return {
                "ok": False,
                "type": "router",
                "action": "unknown",
                "message": str(parsed.get("message") or "Sorry, I do not support that action yet."),
                "parsed": parsed,
            }
        if parsed.get("needs_clarification"):
            return {
                "ok": False,
                "type": "clarification",
                "action": str(parsed.get("intent") or "unknown"),
                "message": str(parsed.get("clarification_question") or "Please clarify your request."),
                "parsed": parsed,
            }

        intent = str(parsed.get("intent") or "").strip()
        if intent == "count_records":
            question = f"how many {parsed.get('count_target') or ''}".strip()
            result = answer_erp_query_internal(question)
        elif _parse_tool_catalog_request(prompt):
            result = {
                "ok": True,
                "type": "tools",
                "tools": list_tool_specs(),
                "message": "Available tools",
            }
        elif _parse_resource_catalog_request(prompt):
            result = {
                "ok": True,
                "type": "resources",
                "resources": list_resource_specs(),
                "message": "Available resources",
            }
        elif intent == "create_sales_order":
            result = create_sales_order_internal(
                customer=str(parsed.get("customer") or "").strip(),
                items=parsed.get("items") or [],
                company=str(parsed.get("filters", {}).get("company") or "").strip() or None,
            )
        elif intent == "export_employee_excel":
            result = export_doctype_list_excel_internal(
                "Employee",
                parsed.get("filters") or None,
                fields=parsed.get("fields") or None,
            )
        elif intent == "export_doctype_excel":
            result = export_doctype_list_excel_internal(
                str(parsed.get("doctype") or "").strip(),
                parsed.get("filters") or None,
                fields=parsed.get("fields") or None,
            )
        elif intent == "generate_pdf":
            result = generate_document_pdf_internal(
                str(parsed.get("doctype") or "").strip(),
                str(parsed.get("docname") or "").strip(),
            )
        else:
            return {
                "ok": False,
                "type": "router",
                "action": "unknown",
                "message": "Sorry, I do not support that action yet.",
                "parsed": parsed,
            }

        if not isinstance(result, dict):
            result = {"ok": False, "type": "router", "message": "Router returned an invalid result."}
        result["action"] = intent
        result["parsed"] = parsed
        return result
    except frappe.PermissionError:
        return {"ok": False, "type": "router", "action": "unknown", "message": _("Permission denied"), "parsed": None}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Route Prompt Error")
        return {"ok": False, "type": "router", "action": "unknown", "message": str(exc) or _("Unknown error"), "parsed": None}
