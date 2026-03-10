import json
import os
import re
from urllib.parse import urlencode
from typing import Any, Optional

import frappe
import requests
from frappe import _

from .chat import add_message, create_conversation
from .fac_proxy import TOOL_DEFINITIONS, _dispatch_tool


TOOL_NAME_MAP = {
    "get_list": "list_documents",
    "get_report": "generate_report",
}


def _cfg(key: str, default: Any = None) -> Any:
    candidates = [key, key.lower(), key.upper()]
    for candidate in candidates:
        value = frappe.conf.get(candidate)
        if value not in (None, ""):
            return value
    for candidate in candidates:
        value = os.getenv(candidate)
        if value not in (None, ""):
            return value
    return default


@frappe.whitelist()
def send_prompt(
    prompt: str,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
):
    """Process a user prompt for the ERP-native assistant drawer."""
    if not prompt or not prompt.strip():
        raise frappe.ValidationError(_("Prompt is required"))

    conversation_name = conversation or create_conversation(title=_summarize_title(prompt))["name"]
    add_message(conversation_name, "user", prompt)
    _set_conversation_title_from_prompt(conversation_name, prompt)

    context = {
        "doctype": doctype,
        "docname": docname,
        "route": route,
        "user": frappe.session.user,
    }

    history = _conversation_history_for_llm(conversation_name)
    response = _generate_response(prompt.strip(), context, history=history)
    add_message(
        conversation_name,
        "assistant",
        response["text"],
        tool_events=json.dumps(response.get("tool_events", [])),
    )

    return {
        "conversation": conversation_name,
        "reply": response["text"],
        "tool_events": response.get("tool_events", []),
        "payload": response.get("payload"),
        "context": context,
    }


def _generate_response(
    prompt: str, context: dict[str, Any], history: Optional[list[dict[str, Any]]] = None
) -> dict[str, Any]:
    if _llm_chat_configured():
        try:
            return _anthropic_chat(prompt, context, history=history)
        except Exception as exc:
            frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Chat Error")
            return {
                "text": (
                    "I could not complete that request because the assistant backend call failed.\n\n"
                    f"Error: {str(exc) or 'Unknown error'}\n\n"
                    "Please retry, and if this persists check the AI provider/API configuration in site config."
                ),
                "tool_events": [],
                "payload": None,
            }

    direct_plan = _plan_prompt(prompt, context)
    if direct_plan:
        result = _run_tool(direct_plan["tool"], direct_plan["arguments"])
        return {
            "text": _render_tool_output(direct_plan["tool"], result, direct_plan.get("heading")),
            "tool_events": [f"{direct_plan['tool']} {direct_plan['arguments']}"],
            "payload": result,
        }

    guidance = "AI provider is not configured for open-ended chat yet. Try direct prompts like 'Show me list of customers' or 'Summarize this record'."
    if context.get("doctype") and context.get("docname"):
        guidance += f" Current context: {context['doctype']} / {context['docname']}."
    return {"text": guidance, "tool_events": [], "payload": None}


def _llm_chat_configured() -> bool:
    api_key = _cfg("ANTHROPIC_API_KEY")
    auth_token = _cfg("ANTHROPIC_AUTH_TOKEN")
    base_url = _cfg("ANTHROPIC_BASE_URL")
    messages_path = _cfg("ANTHROPIC_MESSAGES_PATH")
    if api_key or auth_token:
        return True
    if base_url and base_url.rstrip("/") != "https://api.anthropic.com":
        return True
    return bool(messages_path)


def _set_conversation_title_from_prompt(conversation_name: str, prompt: str) -> None:
    title = _summarize_title(prompt)
    if not title:
        return

    doc = frappe.get_doc("AI Conversation", conversation_name)
    existing_title = (doc.title or "").strip()
    if existing_title and existing_title != _("New chat"):
        return

    doc.title = title
    doc.save(ignore_permissions=True)


def _conversation_history_for_llm(conversation_name: str, limit: int = 24) -> list[dict[str, Any]]:
    messages = frappe.get_all(
        "AI Message",
        filters={"conversation": conversation_name},
        fields=["role", "content"],
        order_by="creation desc",
        limit_page_length=max(1, limit),
    )
    history: list[dict[str, Any]] = []
    for row in reversed(messages):
        role = (row.get("role") or "").strip().lower()
        content = (row.get("content") or "").strip()
        if not content:
            continue
        if role == "user":
            history.append({"role": "user", "content": content})
        elif role == "assistant":
            history.append({"role": "assistant", "content": content})
    return history


def _plan_prompt(prompt: str, context: dict[str, Any]) -> Optional[dict[str, Any]]:
    raw_text = _normalize_name(prompt)
    text = raw_text.lower()

    sales_report_match = re.match(
        r"^(create|show|get|run|generate)( me)?( a)? sales order report( for)? (?P<year>\d{4})$",
        text,
        re.IGNORECASE,
    ) or re.match(r"^sales( orders?)?( report)? (?P<year>\d{4})$", text, re.IGNORECASE)
    if sales_report_match:
        year = sales_report_match.group("year")
        return {
            "tool": "get_list",
            "arguments": {
                "doctype": "Sales Order",
                "fields": ["name", "customer", "transaction_date", "status", "grand_total"],
                "filters": {
                    "transaction_date": ["between", [f"{year}-01-01", f"{year}-12-31"]],
                },
                "limit_page_length": 20,
                "order_by": "transaction_date desc",
            },
            "heading": f"Sales Order report for {year}",
        }

    employee_match = re.match(
        r"^(show|get|find|list)( me)? employees?( named)? (?P<name>.+)$",
        text,
        re.IGNORECASE,
    )
    if employee_match:
        employee_name = _normalize_name(employee_match.group("name"))
        return {
            "tool": "get_list",
            "arguments": {
                "doctype": "Employee",
                "fields": ["name", "employee_name", "designation", "cell_number"],
                "filters": {"employee_name": ["like", f"%{employee_name}%"]},
                "limit_page_length": 20,
                "order_by": "modified desc",
            },
            "heading": f"Employees matching {employee_name}",
        }

    customer_match = re.match(
        r"^(show|get|find|list)( me)? customers?( from| in)? (?P<territory>.+)$",
        text,
        re.IGNORECASE,
    )
    if customer_match:
        territory = _normalize_name(customer_match.group("territory"))
        return {
            "tool": "get_list",
            "arguments": {
                "doctype": "Customer",
                "fields": ["name", "customer_name", "territory", "customer_group"],
                "filters": {"territory": ["like", f"%{territory}%"]},
                "limit_page_length": 20,
                "order_by": "modified desc",
            },
            "heading": f"Customers in {territory}",
        }

    item_match = re.match(
        r"^(show|get|find|list)( me)? items?( matching| named)? (?P<name>.+)$",
        text,
        re.IGNORECASE,
    )
    if item_match:
        item_name = _normalize_name(item_match.group("name"))
        return {
            "tool": "get_list",
            "arguments": {
                "doctype": "Item",
                "fields": ["name", "item_name", "item_code", "stock_uom"],
                "filters": {"item_name": ["like", f"%{item_name}%"]},
                "limit_page_length": 20,
                "order_by": "modified desc",
            },
            "heading": f"Items matching {item_name}",
        }

    if context.get("doctype") and context.get("docname"):
        if re.match(r"^(summarize|show|explain)( this| current)?( record| document)?$", text):
            return {
                "tool": "get_document",
                "arguments": {"doctype": context["doctype"], "name": context["docname"]},
                "heading": f"{context['doctype']} {context['docname']}",
            }

        if text.startswith("update this ") and " set " in text:
            updates = _parse_field_assignments(raw_text.split(" set ", 1)[1])
            if updates:
                return {
                    "tool": "update_document",
                    "arguments": {
                        "doctype": context["doctype"],
                        "name": context["docname"],
                        "document": updates,
                    },
                    "heading": f"Updated {context['doctype']} {context['docname']}",
                }

    list_patterns = [
        (r"^(show|list)( me)?( the)?( list of)? customers$", "Customer", ["name", "customer_name", "territory"]),
        (r"^(show|list)( me)?( the)?( list of)? employees$", "Employee", ["name", "employee_name", "designation"]),
        (r"^(show|list)( me)?( the)?( list of)? items$", "Item", ["name", "item_name", "item_code"]),
        (r"^(show|list)( me)?( the)?( list of)? sales orders$", "Sales Order", ["name", "customer", "transaction_date", "status"]),
    ]
    for pattern, doctype, fields in list_patterns:
        if re.match(pattern, text, re.IGNORECASE):
            return {
                "tool": "get_list",
                "arguments": {
                    "doctype": doctype,
                    "fields": fields,
                    "limit_page_length": 20,
                    "order_by": "modified desc",
                },
                "heading": f"{doctype} list",
            }

    report_match = re.match(r"^(run|show|get|generate)( me)?( the)? report (.+)$", raw_text, re.IGNORECASE)
    if report_match:
        report_name = _normalize_name(report_match.group(4))
        return {
            "tool": "get_report",
            "arguments": {"report_name": report_name},
            "heading": report_name,
        }

    named_doc = [
        (r"^(show|get)( me)? customer (.+)$", "Customer"),
        (r"^(show|get)( me)? employee (.+)$", "Employee"),
        (r"^(show|get)( me)? item (.+)$", "Item"),
        (r"^(show|get)( me)? sales order (.+)$", "Sales Order"),
    ]
    for pattern, doctype in named_doc:
        match = re.match(pattern, raw_text, re.IGNORECASE)
        if match:
            return {
                "tool": "get_document",
                "arguments": {"doctype": doctype, "name": _normalize_name(match.group(3))},
                "heading": f"{doctype} {_normalize_name(match.group(3))}",
            }

    return None


def _anthropic_chat(
    prompt: str, context: dict[str, Any], history: Optional[list[dict[str, Any]]] = None
) -> dict[str, Any]:
    api_key = _cfg("ANTHROPIC_API_KEY")
    auth_token = _cfg("ANTHROPIC_AUTH_TOKEN")
    model = _cfg("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
    base_url = str(_cfg("ANTHROPIC_BASE_URL", "https://api.anthropic.com")).rstrip("/")
    messages_path = str(_cfg("ANTHROPIC_MESSAGES_PATH", "/v1/messages"))
    if not messages_path.startswith("/"):
        messages_path = f"/{messages_path}"

    query_params = {}
    anthropic_beta = _cfg("ANTHROPIC_BETA")
    beta_param = _normalize_beta_param(anthropic_beta)
    if beta_param is not None:
        query_params["beta"] = beta_param

    endpoint = f"{base_url}{messages_path}"
    if query_params:
        endpoint = f"{endpoint}?{urlencode(query_params)}"

    headers = {"content-type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    elif auth_token:
        headers["authorization"] = f"Bearer {auth_token}"
    anthropic_version = _cfg("ANTHROPIC_VERSION", "2023-06-01")
    if anthropic_version:
        headers["anthropic-version"] = anthropic_version
    tool_specs = [
        {
            "name": name,
            "description": spec["description"],
            "input_schema": spec["inputSchema"],
        }
        for name, spec in TOOL_DEFINITIONS.items()
    ]

    system = (
        "You are a helpful Frappe and ERPNext assistant. "
        "Use available tools whenever live data is needed or when creating/updating records. "
        "Prefer tool calls over guessing. Keep responses concise, factual, and action-oriented. "
        "Never invent connectivity/authentication/server issues. If a tool call fails, report the exact returned error text only. "
        f"Current context: doctype={context.get('doctype')}, docname={context.get('docname')}, route={context.get('route')}."
    )
    messages: list[dict[str, Any]] = history[:] if history else [{"role": "user", "content": prompt}]
    tool_events: list[str] = []
    rendered_payload = None

    while True:
        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json={
                    "model": model,
                    "max_tokens": 1200,
                    "system": system,
                    "tools": tool_specs,
                    "messages": messages,
                },
                timeout=60,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc
        response.raise_for_status()
        body = _parse_backend_json(response, endpoint)
        content_blocks = body.get("content", [])
        messages.append({"role": "assistant", "content": content_blocks})

        text_chunks = [block.get("text", "") for block in content_blocks if block.get("type") == "text" and block.get("text")]
        tool_uses = [block for block in content_blocks if block.get("type") == "tool_use"]

        if not tool_uses:
            return {
                "text": "\n\n".join(chunk for chunk in text_chunks if chunk).strip() or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
            }

        tool_results = []
        for tool_use in tool_uses:
            tool_name = tool_use["name"]
            tool_input = tool_use.get("input", {})
            try:
                tool_result = _run_tool(tool_name, tool_input)
                tool_events.append(f"{tool_name} {tool_input}")
                rendered_payload = tool_result
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "content": json.dumps(tool_result, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_events.append(f"{tool_name} {tool_input} (error)")
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "is_error": True,
                        "content": json.dumps(error_payload, default=str),
                    }
                )

        messages.append({"role": "user", "content": tool_results})


def _normalize_beta_param(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return "true" if value else None
    text = str(value).strip().lower()
    if text in {"", "0", "false", "none", "null", "off", "no"}:
        return None
    return text


def _parse_backend_json(response: requests.Response, endpoint: str) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        preview = (response.text or "").strip().replace("\n", " ")[:300]
        content_type = response.headers.get("content-type", "")
        status = response.status_code
        detail = (
            f"AI endpoint returned non-JSON response (status={status}, content_type='{content_type}') "
            f"from {endpoint}. Body preview: {preview or '<empty>'}"
        )
        raise RuntimeError(detail) from exc
    if not isinstance(body, dict):
        raise RuntimeError(
            f"AI endpoint returned unexpected JSON payload type ({type(body).__name__}) from {endpoint}; expected object."
        )
    return body


def _run_tool(tool_name: str, arguments: dict[str, Any]) -> Any:
    mapped_name = TOOL_NAME_MAP.get(tool_name, tool_name)
    if tool_name == "get_list":
        arguments = {
            "doctype": arguments["doctype"],
            "fields": arguments.get("fields") or ["name"],
            "filters": _filters_to_object(arguments.get("filters")),
            "limit": arguments.get("limit_page_length", 20),
            "order_by": arguments.get("order_by"),
        }
    elif tool_name == "get_report":
        arguments = {
            "report_name": arguments["report_name"],
            "filters": arguments.get("filters") or {},
        }
    elif tool_name in {"create_document", "update_document"} and "document" in arguments:
        arguments = dict(arguments)
        arguments["data"] = arguments.pop("document")

    return _dispatch_tool(mapped_name, arguments)


def _filters_to_object(filters: Any) -> dict[str, Any]:
    if not filters:
        return {}
    if isinstance(filters, dict):
        return filters
    converted = {}
    for item in filters:
        if isinstance(item, list) and len(item) == 3:
            field, operator, value = item
            converted[field] = value if operator == "=" else [operator, value]
    return converted


def _render_tool_output(tool_name: str, payload: Any, heading: Optional[str] = None) -> str:
    if tool_name == "get_list":
        return _format_list_result(payload.get("data", payload), heading)
    if tool_name == "get_document":
        return _format_document_result(payload.get("data", payload), heading)
    if tool_name == "get_report":
        return _format_report_result(payload, heading)
    if tool_name in {"create_document", "update_document", "delete_document"}:
        return _format_mutation_result(payload, heading)
    return _format_generic_result(payload, heading)


def _format_list_result(payload: Any, heading: Optional[str]) -> str:
    rows = payload if isinstance(payload, list) else []
    title = heading or "Results"
    if not rows:
        return f"{title}\n\nNo records found."
    lines = [title, ""]
    for index, row in enumerate(rows[:20], start=1):
        if isinstance(row, dict):
            primary = row.get("customer_name") or row.get("employee_name") or row.get("item_name") or row.get("name") or f"Row {index}"
            extras = [f"{key}: {value}" for key, value in row.items() if key not in {"name", "customer_name", "employee_name", "item_name"} and value not in (None, "", [])][:3]
            suffix = f" ({', '.join(extras)})" if extras else ""
            lines.append(f"{index}. {primary}{suffix}")
        else:
            lines.append(f"{index}. {row}")
    return "\n".join(lines)


def _format_document_result(payload: Any, heading: Optional[str]) -> str:
    if not isinstance(payload, dict):
        return str(payload)
    title = heading or payload.get("name") or "Document"
    lines = [title, ""]
    shown = 0
    for key in ["name", "customer_name", "employee_name", "designation", "status", "territory", "modified", "modified_by"]:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            lines.append(f"{key}: {value}")
            shown += 1
    if shown == 0:
        for key, value in payload.items():
            if value not in (None, "", [], {}):
                lines.append(f"{key}: {value}")
                shown += 1
            if shown >= 8:
                break
    return "\n".join(lines)


def _format_report_result(payload: Any, heading: Optional[str]) -> str:
    if isinstance(payload, dict):
        result_rows = payload.get("result") or payload.get("data")
        if isinstance(result_rows, list):
            return _format_list_result(result_rows, heading or "Report results")
    if isinstance(payload, list):
        return _format_list_result(payload, heading or "Report results")
    return _format_generic_result(payload, heading)


def _format_mutation_result(payload: Any, heading: Optional[str]) -> str:
    if not isinstance(payload, dict):
        return str(payload)
    title = heading or payload.get("message") or "Operation completed"
    lines = [title, ""]
    for key in ["message", "doctype", "name", "modified", "modified_by", "creation"]:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            lines.append(f"{key}: {value}")
    return "\n".join(lines)


def _format_generic_result(payload: Any, heading: Optional[str]) -> str:
    if isinstance(payload, dict):
        lines = [heading or payload.get("message") or "Result", ""]
        for key, value in payload.items():
            if value not in (None, "", [], {}):
                lines.append(f"{key}: {value}")
            if len(lines) >= 10:
                break
        return "\n".join(lines)
    return str(payload)


def _normalize_name(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip(" .?!")


def _summarize_title(text: str) -> str:
    summary = _normalize_name(text)
    return summary[:40] + ("..." if len(summary) > 40 else "")


def _parse_field_assignments(text: str) -> dict[str, Any]:
    assignments: dict[str, Any] = {}
    normalized = text.strip().replace(" and ", ", ")
    aliases = {
        "territory": "territory",
        "designation": "designation",
        "email": "email_id",
        "mobile": "mobile_no",
        "phone": "mobile_no",
        "department": "department",
        "description": "description",
    }
    for part in [item.strip() for item in normalized.split(",") if item.strip()]:
        if "=" in part:
            raw_key, raw_value = part.split("=", 1)
        elif ":" in part:
            raw_key, raw_value = part.split(":", 1)
        else:
            continue
        key = aliases.get(raw_key.strip().lower(), raw_key.strip().replace(" ", "_"))
        assignments[key] = _coerce_value(raw_value.strip())
    return assignments


def _coerce_value(value: str) -> Any:
    cleaned = value.strip().strip("\"'")
    if re.fullmatch(r"-?\d+", cleaned):
        return int(cleaned)
    if re.fullmatch(r"-?\d+\.\d+", cleaned):
        return float(cleaned)
    lowered = cleaned.lower()
    if lowered in {"true", "yes"}:
        return 1
    if lowered in {"false", "no"}:
        return 0
    return cleaned
