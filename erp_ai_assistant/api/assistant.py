import json
from typing import Any

import frappe
from frappe import _

from . import ai as ai_api
from .chat import add_message, clear_pending_action, create_conversation, get_pending_action, set_pending_action
from .context_resolver import build_request_context
from .erp_tools import ping_assistant as ping_assistant_tool
from .erp_tools import answer_erp_query as answer_erp_query_tool
from .erp_tools import create_sales_order as create_sales_order_tool
from .erp_tools import create_purchase_order as create_purchase_order_tool
from .erp_tools import create_quotation as create_quotation_tool
from .erp_tools import create_erp_document as create_erp_document_tool
from .erp_tools import create_transaction_document as create_transaction_document_tool
from .erp_tools import submit_erp_document as submit_erp_document_tool
from .erp_tools import cancel_erp_document as cancel_erp_document_tool
from .erp_tools import run_workflow_action as run_workflow_action_tool
from .erp_tools import continue_pending_action_internal
from .erp_tools import get_erp_document as get_erp_document_tool
from .erp_tools import list_erp_documents as list_erp_documents_tool
from .erp_tools import list_erp_doctypes as list_erp_doctypes_tool
from .erp_tools import search_erp_documents as search_erp_documents_tool
from .erp_tools import update_erp_document as update_erp_document_tool
from .erp_tools import get_doctype_fields as get_doctype_fields_tool
from .erp_tools import describe_erp_schema as describe_erp_schema_tool
from .file_tools import export_doctype_list_excel as export_doctype_list_excel_tool
from .fac_client import test_fac_connection as test_fac_connection_internal
from .resource_registry import get_resource_catalog_summary, list_resource_specs, read_resource
from .tool_registry import get_tool_catalog_summary, list_tool_specs
from .copilot_response import build_copilot_package


def _build_router_attachment_package(result: dict[str, Any]) -> dict[str, Any]:
    if str(result.get("type") or "").strip().lower() != "file":
        return {"attachments": [], "exports": {}}
    file_url = str(result.get("file_url") or "").strip()
    file_name = str(result.get("file_name") or "download").strip() or "download"
    if not file_url:
        return {"attachments": [], "exports": {}}
    return {
        "attachments": [
            {
                "id": f"router-{file_name}",
                "label": "File",
                "filename": file_name,
                "file_type": file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "file",
                "file_url": file_url,
            }
        ],
        "exports": {},
    }


def _result_to_reply_text(result: dict[str, Any]) -> str:
    result_type = str(result.get("type") or "").strip().lower()
    if result_type == "answer":
        return str(result.get("answer") or result.get("message") or "").strip()
    if result_type == "tools":
        rows = result.get("tools") or []
        lines = [str(result.get("message") or "Available tools").strip(), ""]
        for index, row in enumerate(rows, start=1):
            name = str((row or {}).get("name") or "").strip() or f"Tool {index}"
            description = str((row or {}).get("description") or "").strip()
            suffix = f": {description}" if description else ""
            lines.append(f"{index}. {name}{suffix}")
        return "\n".join(lines).strip()
    if result_type == "resources":
        rows = result.get("resources") or []
        lines = [str(result.get("message") or "Available resources").strip(), ""]
        for index, row in enumerate(rows, start=1):
            name = str((row or {}).get("name") or "").strip() or f"Resource {index}"
            description = str((row or {}).get("description") or (row or {}).get("title") or "").strip()
            suffix = f": {description}" if description else ""
            lines.append(f"{index}. {name}{suffix}")
        return "\n".join(lines).strip()
    if result_type == "document":
        doc_link = str(result.get("url") or "").strip()
        doc_label = f"{result.get('doctype')} {result.get('name')}".strip()
        text = str(result.get("message") or "Document created successfully").strip()
        if doc_link:
            text += f"\n\nOpen document: [{doc_label}]({doc_link})"
        return text
    if result_type == "file":
        text = str(result.get("message") or "File generated successfully").strip()
        file_name = str(result.get("file_name") or "").strip()
        file_url = str(result.get("file_url") or "").strip()
        if result.get("ok") and (file_name or file_url):
            text += f"\n\nFile: {file_name}"
        return text
    return str(result.get("message") or "").strip()


def _build_tool_event_payload(result: dict[str, Any]) -> list[dict[str, Any] | str]:
    events: list[dict[str, Any] | str] = [f"router {result.get('action') or 'deterministic'}"]
    error_type = str(result.get("error_type") or "").strip()
    if error_type == "missing_fields" and result.get("missing_fields"):
        events.append({"type": "missing_fields", "items": result.get("missing_fields")})
    if error_type == "missing_child_rows" and result.get("missing_child_rows"):
        events.append({"type": "missing_child_rows", "items": result.get("missing_child_rows")})
    if result.get("candidates"):
        events.append({"type": "candidates", "items": result.get("candidates")})
    pending_action = result.get("pending_action") or {}
    if isinstance(pending_action, dict):
        ambiguous_link = pending_action.get("ambiguous_link") or {}
        if isinstance(ambiguous_link, dict) and ambiguous_link.get("candidates"):
            events.append({"type": "candidates", "items": ambiguous_link.get("candidates")})
        if pending_action.get("missing_child_rows"):
            events.append({"type": "missing_child_rows", "items": pending_action.get("missing_child_rows")})
    if result.get("type") == "document" and result.get("doctype") and result.get("name"):
        events.append({"type": "document_ref", "doctype": result.get("doctype"), "name": result.get("name"), "url": result.get("url")})
    return events


def _should_override_pending_action(
    prompt_text: str,
    pending_action: dict[str, Any] | None,
) -> bool:
    if not isinstance(pending_action, dict) or not pending_action:
        return False
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    override_markers = (
        "i mean ",
        "actually ",
        "instead ",
        "rather ",
        "no, ",
        "no ",
    )
    if any(text.startswith(marker) for marker in override_markers):
        return True
    return False


@frappe.whitelist()
def ping_assistant() -> dict[str, Any]:
    return ping_assistant_tool()


@frappe.whitelist()
def answer_erp_query(question: str) -> dict[str, Any]:
    return answer_erp_query_tool(question)


@frappe.whitelist()
def create_sales_order(customer: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    return create_sales_order_tool(customer=customer, items=items, company=company)


@frappe.whitelist()
def create_quotation(customer: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    return create_quotation_tool(customer=customer, items=items, company=company)


@frappe.whitelist()
def create_purchase_order(supplier: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    return create_purchase_order_tool(supplier=supplier, items=items, company=company)


@frappe.whitelist()
def create_erp_document(doctype: str, values: dict[str, Any] | str) -> dict[str, Any]:
    return create_erp_document_tool(doctype=doctype, values=values)


@frappe.whitelist()
def create_transaction_document(doctype: str, party_name: str, items: list[dict[str, Any]] | str, company: str | None = None) -> dict[str, Any]:
    return create_transaction_document_tool(doctype=doctype, party_name=party_name, items=items, company=company)


@frappe.whitelist()
def submit_erp_document(doctype: str, record: str) -> dict[str, Any]:
    return submit_erp_document_tool(doctype=doctype, record=record)


@frappe.whitelist()
def cancel_erp_document(doctype: str, record: str) -> dict[str, Any]:
    return cancel_erp_document_tool(doctype=doctype, record=record)


@frappe.whitelist()
def run_workflow_action(doctype: str, record: str, action: str) -> dict[str, Any]:
    return run_workflow_action_tool(doctype=doctype, record=record, action=action)


@frappe.whitelist()
def list_erp_documents(doctype: str, filters: dict[str, Any] | str | None = None, limit: int = 500) -> dict[str, Any]:
    return list_erp_documents_tool(doctype=doctype, filters=filters, limit=limit)


@frappe.whitelist()
def list_erp_doctypes(search: str | None = None, module: str | None = None, limit: int = 100) -> dict[str, Any]:
    return list_erp_doctypes_tool(search=search, module=module, limit=limit)


@frappe.whitelist()
def get_erp_document(doctype: str, name: str) -> dict[str, Any]:
    return get_erp_document_tool(doctype=doctype, name=name)


@frappe.whitelist()
def get_doctype_fields(doctype: str, writable_only: int | str = 0) -> dict[str, Any]:
    return get_doctype_fields_tool(doctype=doctype, writable_only=writable_only)


@frappe.whitelist()
def describe_erp_schema(doctype: str) -> dict[str, Any]:
    return describe_erp_schema_tool(doctype=doctype)


@frappe.whitelist()
def search_erp_documents(query: str, doctype: str | None = None, limit: int = 500) -> dict[str, Any]:
    return search_erp_documents_tool(query=query, doctype=doctype, limit=limit)


@frappe.whitelist()
def update_erp_document(doctype: str, record: str, field: str, value: Any) -> dict[str, Any]:
    return update_erp_document_tool(doctype=doctype, record=record, field=field, value=value)


@frappe.whitelist()
def export_doctype_list_excel(
    doctype: str,
    filters: dict[str, Any] | str | None = None,
    fields: list[str] | str | None = None,
) -> dict[str, Any]:
    return export_doctype_list_excel_tool(doctype=doctype, filters=filters, fields=fields)


@frappe.whitelist()
def test_fac_mcp_connection() -> dict[str, Any]:
    return test_fac_connection_internal()


@frappe.whitelist()
def test_ai_provider_connection(model: str | None = None) -> dict[str, Any]:
    provider = ai_api._provider_name()
    selected_model = ai_api._resolve_model(model)

    if provider in {"openai", "openai_compatible"}:
        api_key = str(ai_api._cfg("OPENAI_API_KEY", "") or "").strip()
        base_url = str(
            ai_api._cfg(
                "OPENAI_BASE_URL",
                "https://api.openai.com" if provider == "openai" else "https://integrate.api.nvidia.com",
            )
            or ""
        ).rstrip("/")
        path = str(
            ai_api._cfg(
                "OPENAI_RESPONSES_PATH",
                ai_api.DEFAULT_OPENAI_RESPONSES_PATH if provider == "openai" else "/v1/chat/completions",
            )
            or ""
        )
        if not path.startswith("/"):
            path = f"/{path}"
        endpoint = f"{base_url}{path}"
        compat_profile = ai_api._provider_compatibility_profile(provider, base_url, path, model=selected_model)
        if not api_key:
            return {
                "ok": False,
                "provider": provider,
                "profile": compat_profile.get("profile"),
                "model": selected_model,
                "endpoint": endpoint,
                "has_api_key": False,
                "message": "OPENAI_API_KEY is not configured.",
            }

        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {api_key}",
        }
        payload = {
            "model": selected_model,
            "messages": [{"role": "user", "content": "Reply with OK only."}],
            "stream": False,
        }
        if provider == "openai":
            payload = {
                "model": selected_model,
                "instructions": "Reply with OK only.",
                "input": [{"role": "user", "content": [{"type": "input_text", "text": "Ping"}]}],
            }
        try:
            response = ai_api.requests.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=ai_api._llm_request_timeout_seconds(),
            )
            detail = ai_api._extract_error_detail(response) if response.status_code >= 400 else ""
            response.raise_for_status()
            body = ai_api._parse_backend_json(response, endpoint)
            text = ai_api._openai_output_text(body) if provider == "openai" else str((((body.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()
            return {
                "ok": True,
                "provider": provider,
                "profile": compat_profile.get("profile"),
                "model": selected_model,
                "endpoint": endpoint,
                "has_api_key": True,
                "message": "AI provider connection succeeded.",
                "preview": text[:200],
            }
        except Exception as exc:
            return {
                "ok": False,
                "provider": provider,
                "profile": compat_profile.get("profile"),
                "model": selected_model,
                "endpoint": endpoint,
                "has_api_key": True,
                "message": str(exc) or "AI provider connection failed.",
                "detail": detail if 'detail' in locals() else "",
            }

    api_key = str(ai_api._cfg("ANTHROPIC_API_KEY", "") or "").strip()
    auth_token = str(ai_api._cfg("ANTHROPIC_AUTH_TOKEN", "") or "").strip()
    base_url = str(ai_api._cfg("ANTHROPIC_BASE_URL", "https://api.anthropic.com") or "").rstrip("/")
    path = str(ai_api._cfg("ANTHROPIC_MESSAGES_PATH", "/v1/messages") or "/v1/messages")
    if not path.startswith("/"):
        path = f"/{path}"
    endpoint = f"{base_url}{path}"
    compat_profile = ai_api._provider_compatibility_profile(provider, base_url, path, model=selected_model)
    if not api_key and not auth_token:
        return {
            "ok": False,
            "provider": provider,
            "profile": compat_profile.get("profile"),
            "model": selected_model,
            "endpoint": endpoint,
            "has_api_key": False,
            "message": "Anthropic authentication is not configured.",
        }

    headers = {"content-type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    else:
        headers["authorization"] = f"Bearer {auth_token}"
    anthropic_version = ai_api._cfg("ANTHROPIC_VERSION", "2023-06-01")
    if anthropic_version:
        headers["anthropic-version"] = anthropic_version
    beta_param = ai_api._normalize_beta_param(ai_api._cfg("ANTHROPIC_BETA"))
    if beta_param:
        headers["anthropic-beta"] = beta_param

    payload = {
        "model": selected_model,
        "max_tokens": 32,
        "stream": False,
        "system": "Reply with OK only.",
        "messages": [{"role": "user", "content": "Ping"}],
    }
    try:
        response = ai_api.requests.post(
            endpoint,
            headers=headers,
            json=payload,
            timeout=ai_api._llm_request_timeout_seconds(),
        )
        detail = ai_api._extract_error_detail(response) if response.status_code >= 400 else ""
        response.raise_for_status()
        body = ai_api._parse_backend_json(response, endpoint)
        text_chunks = [
            block.get("text", "")
            for block in (body.get("content") or [])
            if isinstance(block, dict) and block.get("type") == "text" and block.get("text")
        ]
        return {
            "ok": True,
            "provider": provider,
            "profile": compat_profile.get("profile"),
            "model": selected_model,
            "endpoint": endpoint,
            "has_api_key": True,
            "message": "AI provider connection succeeded.",
            "preview": "\n".join(text_chunks).strip()[:200],
        }
    except Exception as exc:
        return {
            "ok": False,
            "provider": provider,
            "profile": compat_profile.get("profile"),
            "model": selected_model,
            "endpoint": endpoint,
            "has_api_key": True,
            "message": str(exc) or "AI provider connection failed.",
            "detail": detail if 'detail' in locals() else "",
        }


@frappe.whitelist()
def list_available_tools(category: str | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "type": "tools",
        "tools": list_tool_specs(category=category),
    }


@frappe.whitelist()
def get_tool_catalog() -> dict[str, Any]:
    return {
        "ok": True,
        "type": "tool_catalog",
        "catalog": get_tool_catalog_summary(),
    }


@frappe.whitelist()
def list_available_resources() -> dict[str, Any]:
    return {
        "ok": True,
        "type": "resources",
        "resources": list_resource_specs(),
    }


@frappe.whitelist()
def get_resource_catalog() -> dict[str, Any]:
    return {
        "ok": True,
        "type": "resource_catalog",
        "catalog": get_resource_catalog_summary(),
    }


@frappe.whitelist()
def read_available_resource(
    resource_name: str,
    context: dict[str, Any] | str | None = None,
    conversation: str | None = None,
    arguments: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    parsed_args = arguments
    if isinstance(arguments, str):
        try:
            parsed_args = json.loads(arguments)
        except Exception:
            parsed_args = {}
    if not isinstance(parsed_args, dict):
        parsed_args = {}
    if conversation:
        parsed_args.setdefault("conversation", conversation)
    return read_resource(resource_name, context=context, arguments=parsed_args)


@frappe.whitelist()
def handle_prompt(
    prompt: str | None = None,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    prompt_text = str(prompt or "").strip()
    parsed_images = ai_api._parse_prompt_images(images)

    if parsed_images:
        return ai_api.enqueue_prompt(
            prompt=prompt,
            conversation=conversation,
            doctype=doctype,
            docname=docname,
            route=route,
            model=model,
            images=images,
        )

    conversation_name = conversation or None
    context = build_request_context(doctype=doctype, docname=docname, route=route, user=frappe.session.user)
    resumed = None
    pending_action = get_pending_action(conversation_name) if conversation_name and prompt_text else None
    if conversation_name and prompt_text:
        lowered = prompt_text.lower()
        if lowered in {"cancel", "cancel that", "never mind", "forget it", "stop"}:
            clear_pending_action(conversation_name)
            pending_action = None
        elif _should_override_pending_action(prompt_text, pending_action):
            clear_pending_action(conversation_name)
            pending_action = None
        else:
            resumed = continue_pending_action_internal(pending_action or {}, prompt_text)
            if isinstance(resumed, dict):
                resumed["matched"] = True
                resumed["action"] = str(resumed.get("action") or "continue_pending_action").strip()

    routed = resumed or {"matched": False}
    if not routed.get("matched"):
        return ai_api.enqueue_prompt(
            prompt=prompt,
            conversation=conversation,
            doctype=doctype,
            docname=docname,
            route=route,
            model=model,
            images=images,
        )

    conversation_name = conversation_name or create_conversation(title=ai_api._summarize_title(prompt_text or _("New chat")))["name"]
    user_content = prompt_text or ai_api._format_image_only_user_content(len(parsed_images))
    user_attachments = ai_api._build_prompt_image_attachments(parsed_images)
    add_message(
        conversation_name,
        "user",
        user_content,
        attachments_json=json.dumps(user_attachments, default=str) if user_attachments.get("attachments") else None,
    )
    ai_api._set_conversation_title_from_prompt(conversation_name, prompt_text or user_content)

    reply_text = _result_to_reply_text(routed)
    attachments = _build_router_attachment_package(routed)
    attachments["copilot"] = build_copilot_package(prompt=prompt_text, context=context, payload=routed, reply_text=reply_text)
    assistant_message = add_message(
        conversation_name,
        "assistant",
        reply_text,
        tool_events=json.dumps(_build_tool_event_payload(routed), default=str),
        attachments_json=json.dumps(attachments, default=str),
    )
    if routed.get("pending_action"):
        set_pending_action(conversation_name, routed.get("pending_action"))
    else:
        clear_pending_action(conversation_name)
    return {
        "conversation": conversation_name,
        "reply": reply_text,
        "tool_events": _build_tool_event_payload(routed),
        "payload": routed,
        "attachments": attachments,
        "context": context,
        "message_name": assistant_message.get("name"),
    }
