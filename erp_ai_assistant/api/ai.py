import json
import os
import re
from typing import Any, Optional

import frappe
import requests
from frappe import _

from .chat import add_message, create_conversation
from .export import add_message_attachment_urls, create_message_artifacts
from .fac_client import dispatch_tool, get_tool_definitions
from .provider_settings import get_active_provider, get_provider_setting, get_remote_mcp_servers


TOOL_NAME_MAP = {
    "get_list": "list_documents",
    "get_report": "generate_report",
}

DEFAULT_ANTHROPIC_MAX_TOKENS = 4000
DEFAULT_ANTHROPIC_REQUEST_TIMEOUT = 120.0
DEFAULT_ANTHROPIC_STREAM = True
DEFAULT_ANTHROPIC_MAX_TOOL_ROUNDS = 6
DEFAULT_ANTHROPIC_TEMPERATURE = 0.2
DEFAULT_ANTHROPIC_TOP_P = 0.9
DEFAULT_FORCE_TOOL_USE = True
DEFAULT_VERIFY_PASS = True
DEFAULT_ANTHROPIC_VISION_MODEL = ""
DEFAULT_OPENAI_MODEL = "gpt-5"
DEFAULT_OPENAI_RESPONSES_PATH = "/v1/responses"
READ_TOOL_NAMES = {
    "get_document",
    "list_documents",
    "get_doctype_info",
    "search_documents",
    "generate_report",
}
WRITE_TOOL_NAMES = {
    "create_document",
    "update_document",
}
DELETE_TOOL_NAMES = {"delete_document"}


def _cfg(key: str, default: Any = None) -> Any:
    candidates = [key, key.lower(), key.upper()]
    for candidate in candidates:
        value = get_provider_setting(candidate)
        if value not in (None, ""):
            return value
    for candidate in candidates:
        value = frappe.conf.get(candidate)
        if value not in (None, ""):
            return value
    for candidate in candidates:
        value = os.getenv(candidate)
        if value not in (None, ""):
            return value
    return default


def _cfg_int(key: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    value = _cfg(key, default)
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    if minimum is not None and parsed < minimum:
        return default
    if maximum is not None and parsed > maximum:
        return default
    return parsed


def _cfg_float(
    key: str, default: float, *, minimum: float | None = None, maximum: float | None = None
) -> float:
    value = _cfg(key, default)
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    if minimum is not None and parsed < minimum:
        return default
    if maximum is not None and parsed > maximum:
        return default
    return parsed


def _cfg_bool(key: str, default: bool) -> bool:
    value = _cfg(key, default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _llm_request_max_tokens() -> int:
    """Get max tokens for chat calls from config/env with safe bounds."""
    key = (
        "ERP_AI_ANTHROPIC_MAX_TOKENS"
        if _cfg("ERP_AI_ANTHROPIC_MAX_TOKENS") not in (None, "")
        else "ANTHROPIC_MAX_TOKENS"
    )
    return _cfg_int(
        key,
        DEFAULT_ANTHROPIC_MAX_TOKENS,
        minimum=256,
        maximum=200000,
    )


def _llm_request_timeout_seconds() -> float:
    """Get backend request timeout from config/env with safe bounds."""
    key = (
        "ERP_AI_ANTHROPIC_TIMEOUT_SECONDS"
        if _cfg("ERP_AI_ANTHROPIC_TIMEOUT_SECONDS") not in (None, "")
        else "ANTHROPIC_TIMEOUT_SECONDS"
    )
    return _cfg_float(
        key,
        DEFAULT_ANTHROPIC_REQUEST_TIMEOUT,
        minimum=5.0,
        maximum=600.0,
    )


def _llm_request_stream_enabled() -> bool:
    """Get stream mode from config/env."""
    key = (
        "ERP_AI_ANTHROPIC_STREAM"
        if _cfg("ERP_AI_ANTHROPIC_STREAM") not in (None, "")
        else "ANTHROPIC_STREAM"
    )
    return _cfg_bool(key, DEFAULT_ANTHROPIC_STREAM)


def _llm_max_tool_rounds() -> int:
    """Get max assistant tool-calling rounds to prevent runaway loops."""
    key = (
        "ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS"
        if _cfg("ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS") not in (None, "")
        else "ANTHROPIC_MAX_TOOL_ROUNDS"
    )
    return _cfg_int(
        key,
        DEFAULT_ANTHROPIC_MAX_TOOL_ROUNDS,
        minimum=1,
        maximum=20,
    )


def _llm_temperature() -> float:
    key = (
        "ERP_AI_ANTHROPIC_TEMPERATURE"
        if _cfg("ERP_AI_ANTHROPIC_TEMPERATURE") not in (None, "")
        else "ANTHROPIC_TEMPERATURE"
    )
    return _cfg_float(key, DEFAULT_ANTHROPIC_TEMPERATURE, minimum=0.0, maximum=2.0)


def _llm_top_p() -> float:
    key = (
        "ERP_AI_ANTHROPIC_TOP_P"
        if _cfg("ERP_AI_ANTHROPIC_TOP_P") not in (None, "")
        else "ANTHROPIC_TOP_P"
    )
    return _cfg_float(key, DEFAULT_ANTHROPIC_TOP_P, minimum=0.0, maximum=1.0)


def _llm_force_tool_use_enabled() -> bool:
    key = (
        "ERP_AI_FORCE_TOOL_USE"
        if _cfg("ERP_AI_FORCE_TOOL_USE") not in (None, "")
        else "ANTHROPIC_FORCE_TOOL_USE"
    )
    return _cfg_bool(key, DEFAULT_FORCE_TOOL_USE)


def _llm_verify_pass_enabled() -> bool:
    key = (
        "ERP_AI_VERIFY_PASS"
        if _cfg("ERP_AI_VERIFY_PASS") not in (None, "")
        else "ANTHROPIC_VERIFY_PASS"
    )
    return _cfg_bool(key, DEFAULT_VERIFY_PASS)


def _provider_name() -> str:
    configured = str(_cfg("ERP_AI_PROVIDER", "")).strip().lower()
    if configured in {"openai", "openai compatible", "openai_compatible", "anthropic"}:
        if configured in {"openai compatible", "openai_compatible"}:
            return "openai_compatible"
        return configured
    return get_active_provider()


def _tool_choice_mode(base_url: str, messages_path: str) -> str:
    override = str(_cfg("ERP_AI_TOOL_CHOICE_MODE", "")).strip().lower()
    if override in {"anthropic", "openai"}:
        return override

    path = str(messages_path or "").strip().lower()
    endpoint = f"{str(base_url or '').strip().lower().rstrip('/')}/{path.lstrip('/')}"
    openai_markers = (
        "/v1/chat/completions",
        "/chat/completions",
        "/v1/responses",
        "/responses",
    )
    if any(marker in endpoint for marker in openai_markers):
        return "openai"
    return "anthropic"


def _tool_choice_payload(
    force_tool_use: bool,
    prompt: str,
    context: dict[str, Any],
    *,
    mode: str,
) -> Optional[str | dict[str, str]]:
    should_force = force_tool_use and _should_force_tool_use(prompt, context)
    if mode == "openai":
        return "required" if should_force else "auto"
    # Let Anthropic-compatible backends default to auto when not forcing.
    return {"type": "any"} if should_force else None


def _should_force_tool_use(prompt: str, context: dict[str, Any]) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False

    # If we're in a document context, factual actions should be tool-grounded.
    if context.get("doctype") and context.get("docname"):
        return True

    data_intent_terms = (
        "show",
        "list",
        "find",
        "get",
        "report",
        "sales",
        "customer",
        "employee",
        "item",
        "invoice",
        "purchase",
        "stock",
        "balance",
        "aging",
        "payment",
        "order",
        "doctype",
    )
    return any(term in text for term in data_intent_terms)


def _has_destructive_intent(prompt: str) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False
    destructive_terms = (
        "delete",
        "remove",
        "erase",
        "trash",
        "purge",
    )
    return any(term in text for term in destructive_terms)


def _has_write_intent(prompt: str) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False
    write_terms = (
        "create",
        "add",
        "insert",
        "new ",
        "update",
        "edit",
        "change",
        "modify",
        "set ",
        "rename",
        "submit",
        "cancel",
    )
    return any(term in text for term in write_terms)


def _allowed_tool_names(prompt: str, available_names: Optional[set[str]] = None) -> set[str]:
    universe = set(available_names or set()) or set(READ_TOOL_NAMES | WRITE_TOOL_NAMES | DELETE_TOOL_NAMES)
    allowed = {name for name in universe if name not in WRITE_TOOL_NAMES and name not in DELETE_TOOL_NAMES}
    if _has_destructive_intent(prompt):
        allowed.update(name for name in universe if name in WRITE_TOOL_NAMES or name in DELETE_TOOL_NAMES)
        return allowed
    if _has_write_intent(prompt):
        allowed.update(name for name in universe if name in WRITE_TOOL_NAMES)
    return allowed


def _tool_access_summary(prompt: str) -> str:
    if _has_destructive_intent(prompt):
        return "You may use read, create, update, and delete ERP tools when the request explicitly requires it."
    if _has_write_intent(prompt):
        return "You may use read, create, and update ERP tools when the request explicitly requires it. Delete tools are unavailable."
    return "You may use read-only ERP tools for this request. Create, update, and delete tools are unavailable unless the user explicitly asks for a record change."


def _verification_prompt(tool_events: list[str]) -> str:
    recent = tool_events[-5:]
    recent_lines = "\n".join(f"- {event}" for event in recent) or "- none"
    return (
        "Verification pass required before final answer.\n"
        "1) Re-check whether your current conclusion is fully supported by tool results.\n"
        "2) If evidence is missing or inconsistent, call additional tools now.\n"
        "3) If evidence is sufficient, return the final answer with concrete values and avoid guesses.\n"
        "Recent tool activity:\n"
        f"{recent_lines}"
    )


def _progress_cache_key(conversation: str, user: str) -> str:
    return f"erp_ai_assistant:progress:{user}:{conversation}"


def _progress_update(
    progress: Optional[dict[str, Any]],
    stage: str,
    step: str | None = None,
    done: bool = False,
    error: str | None = None,
) -> None:
    if not progress:
        return
    conversation = (progress.get("conversation") or "").strip()
    user = (progress.get("user") or "").strip()
    if not conversation or not user:
        return

    steps = progress.setdefault("steps", [])
    if step:
        normalized = str(step).strip()
        if normalized and (not steps or steps[-1] != normalized):
            steps.append(normalized)
    if len(steps) > 12:
        del steps[:-12]

    payload = {
        "stage": stage,
        "steps": steps,
        "done": bool(done),
        "error": (error or "").strip() or None,
        "model": progress.get("model"),
        "updated_at": frappe.utils.now(),
    }
    expires_in_sec = 300 if done else 900
    frappe.cache().set_value(
        _progress_cache_key(conversation, user),
        json.dumps(payload, default=str),
        expires_in_sec=expires_in_sec,
    )


@frappe.whitelist()
def get_prompt_progress(conversation: str) -> dict[str, Any]:
    if not conversation:
        return {"stage": "idle", "steps": [], "done": True}

    key = _progress_cache_key(conversation, frappe.session.user)
    raw_payload = frappe.cache().get_value(key)
    if not raw_payload:
        return {"stage": "idle", "steps": [], "done": True}

    try:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
    except Exception:
        return {"stage": "idle", "steps": [], "done": True}

    if not isinstance(payload, dict):
        return {"stage": "idle", "steps": [], "done": True}
    return payload


@frappe.whitelist()
def send_prompt(
    prompt: str | None = None,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
):
    """Process a user prompt for the ERP-native assistant drawer."""
    prompt_text = (prompt or "").strip()
    had_image_payload = images not in (None, "", "[]", [])
    parsed_images = _parse_prompt_images(images)
    if had_image_payload and not parsed_images:
        raise frappe.ValidationError(
            _("Image payload was received but could not be parsed. Please reattach or paste the image again.")
        )
    if not prompt_text and not parsed_images:
        raise frappe.ValidationError(_("Prompt or image is required"))

    conversation_name = conversation or create_conversation(title=_summarize_title(prompt_text or _("Image prompt")))["name"]
    user_content = prompt_text or _format_image_only_user_content(len(parsed_images))
    user_attachments = _build_prompt_image_attachments(parsed_images)
    add_message(
        conversation_name,
        "user",
        user_content,
        attachments_json=json.dumps(user_attachments, default=str) if user_attachments.get("attachments") else None,
    )
    _set_conversation_title_from_prompt(conversation_name, prompt_text or user_content)

    context = {
        "doctype": doctype,
        "docname": docname,
        "route": route,
        "user": frappe.session.user,
    }

    selected_model = _resolve_model_for_request(model, has_images=bool(parsed_images))
    progress = {"conversation": conversation_name, "user": frappe.session.user, "model": selected_model, "steps": []}
    _progress_update(progress, stage="working", step="Preparing request")
    if parsed_images:
        _progress_update(progress, stage="working", step=f"Attached images: {len(parsed_images)}")

    history = _conversation_history_for_llm(conversation_name)
    try:
        response = _generate_response(
            prompt_text,
            context,
            history=history,
            model=selected_model,
            progress=progress,
            images=parsed_images,
        )
    except Exception as exc:
        _progress_update(progress, stage="failed", done=True, error=str(exc) or "Unknown error")
        raise

    attachments = _build_message_attachments(
        response.get("payload"),
        title=f"{_summarize_title(prompt_text or user_content)} export",
        conversation=conversation_name,
        prompt=prompt_text,
    )
    message = add_message(
        conversation_name,
        "assistant",
        response["text"],
        tool_events=json.dumps(response.get("tool_events", [])),
        attachments_json=json.dumps({"attachments": [], "exports": {}}),
    )
    attachments = add_message_attachment_urls(message["name"], attachments)
    if attachments.get("attachments"):
        assistant_message = frappe.get_doc("AI Message", message["name"])
        assistant_message.db_set("attachments_json", json.dumps(attachments, default=str), update_modified=False)
    _progress_update(progress, stage="completed", done=True, step="Response ready")

    return {
        "conversation": conversation_name,
        "reply": response["text"],
        "tool_events": response.get("tool_events", []),
        "payload": response.get("payload"),
        "attachments": attachments,
        "context": context,
    }


def _build_message_attachments(payload: Any, title: str, conversation: str, prompt: str) -> dict[str, Any]:
    if payload in (None, "", [], {}):
        return {"attachments": [], "exports": {}}
    try:
        formats = _requested_export_formats(prompt)
        if not formats:
            return {"attachments": [], "exports": {}}
        return create_message_artifacts(
            payload=payload,
            title=title,
            formats=formats,
        )
    except Exception:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Artifact Generation Error")
        return {"attachments": [], "exports": {}}


def _requested_export_formats(prompt: str) -> list[str]:
    text = (prompt or "").lower()
    if not any(
        term in text
        for term in [
            "export",
            "download",
            "file",
            "excel",
            "pdf",
            "word",
            "docx",
            "xlsx",
            "csv",
            "spreadsheet",
            "report file",
            "save as",
            "save it as",
            "create excel",
            "create xlsx",
            "create spreadsheet",
            "on excel",
            "into excel",
        ]
    ):
        return []

    formats: list[str] = []
    if any(term in text for term in ["excel", ".xlsx", "xlsx", "spreadsheet", "csv", "on excel", "into excel"]):
        formats.append("xlsx")
    if any(term in text for term in ["pdf", ".pdf"]):
        formats.append("pdf")
    if any(term in text for term in ["word", "docx", ".docx", "document file"]):
        formats.append("docx")
    if not formats and any(term in text for term in ["export", "download", "file"]):
        formats.append("xlsx")
    return formats


def _should_execute_plan_before_llm(prompt: str, plan: dict[str, Any]) -> bool:
    tool = str(plan.get("tool") or "").strip()
    text = (prompt or "").strip().lower()
    if tool in {"get_document", "get_list", "get_report", "report_list", "report_requirements"}:
        return True
    if tool in {"update_document", "create_document", "delete_document"}:
        return True
    strong_terms = (
        "show",
        "list",
        "get",
        "find",
        "export",
        "download",
        "create excel",
        "create pdf",
        "create word",
        "report",
        "requirements",
        "filters",
    )
    return any(term in text for term in strong_terms)


def _generate_response(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    direct_plan = _plan_prompt(prompt, context)
    requested_formats = _requested_export_formats(prompt)

    if direct_plan and (requested_formats or _should_execute_plan_before_llm(prompt, direct_plan)):
        _progress_update(progress, stage="working", step=f"Running tool: {direct_plan['tool']}")
        result = _run_tool(direct_plan["tool"], direct_plan["arguments"])
        _progress_update(progress, stage="working", step="Preparing response")
        return {
            "text": _render_tool_output(direct_plan["tool"], result, direct_plan.get("heading")),
            "tool_events": [f"{direct_plan['tool']} {direct_plan['arguments']}"],
            "payload": result,
        }

    if _llm_chat_configured():
        try:
            response = _provider_chat(prompt, context, history=history, model=model, progress=progress, images=images)
            if images and _response_rejects_images(response.get("text")):
                retry_prompt = (
                    f"{prompt}\n\n"
                    "An image is already attached in this same user message as multimodal input. "
                    "Analyze the visual content directly and answer only from what is visible in the image."
                ).strip()
                response = _provider_chat(
                    retry_prompt,
                    context,
                    history=history,
                    model=model,
                    progress=progress,
                    images=images,
                )
                if _response_rejects_images(response.get("text")):
                    return {
                        "text": (
                            "Image was attached, but the current AI endpoint/model did not process the image content.\n\n"
                            "Please verify vision support for the configured provider endpoint and selected model, "
                            "then retry."
                        ),
                        "tool_events": response.get("tool_events", []),
                        "payload": response.get("payload"),
                    }
            return response
        except Exception as exc:
            _progress_update(progress, stage="failed", done=True, error=str(exc) or "Unknown error")
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

    if direct_plan:
        _progress_update(progress, stage="working", step=f"Running tool: {direct_plan['tool']}")
        result = _run_tool(direct_plan["tool"], direct_plan["arguments"])
        _progress_update(progress, stage="working", step="Preparing response")
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
    provider = _provider_name()
    if provider in {"openai", "openai_compatible"}:
        api_key = _cfg("OPENAI_API_KEY")
        base_url = _cfg("OPENAI_BASE_URL")
        responses_path = _cfg("OPENAI_RESPONSES_PATH")
        if api_key:
            return True
        if base_url and str(base_url).rstrip("/") != "https://api.openai.com":
            return True
        return bool(responses_path)

    api_key = _cfg("ANTHROPIC_API_KEY")
    auth_token = _cfg("ANTHROPIC_AUTH_TOKEN")
    base_url = _cfg("ANTHROPIC_BASE_URL")
    messages_path = _cfg("ANTHROPIC_MESSAGES_PATH")
    if api_key or auth_token:
        return True
    if base_url and str(base_url).rstrip("/") != "https://api.anthropic.com":
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
    export_terms = ("create", "export", "download", "save")
    export_format_terms = ("excel", "xlsx", "spreadsheet", "csv", "pdf", "word", "docx", "file", "document")
    is_export_request = any(term in text for term in export_terms) and any(term in text for term in export_format_terms)
    list_doctypes = _known_doctype_catalog()

    if context.get("doctype") and context.get("docname"):
        if re.match(r"^(summarize|show|explain)( this| current)?( record| document)?$", text):
            return {
                "tool": "get_document",
                "arguments": {"doctype": context["doctype"], "name": context["docname"]},
                "heading": f"{context['doctype']} {context['docname']}",
            }

        if is_export_request and re.match(
            r"^(create|export|download|save)( me)?( this| current)?( record| document)?( as| to| in)?( excel| xlsx| spreadsheet| csv| pdf| word| docx| file| document)?$",
            text,
            re.IGNORECASE,
        ):
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

    export_list_match = re.match(
        r"^(create|export|download|save)( me)?( the)?( list of)? (?P<label>employees?|customers?|items?|sales orders?|sales invoices?|purchase orders?|purchase invoices?|suppliers?|quotations?|leads?|opportunities?)( .+)?$",
        text,
        re.IGNORECASE,
    )
    export_list_alt_match = re.match(
        r"^(create|export|download|save)( me)?( an?|the)?( excel|xlsx|spreadsheet|csv|pdf|word|docx)?( file| document)?( of| for)? (?P<label>employees?|customers?|items?|sales orders?|sales invoices?|purchase orders?|purchase invoices?|suppliers?|quotations?|leads?|opportunities?)$",
        text,
        re.IGNORECASE,
    )
    export_list_target = export_list_match or export_list_alt_match
    if is_export_request and export_list_target:
        doctype_key = str(export_list_target.group("label") or "").strip().lower().rstrip("s")
        if doctype_key in list_doctypes:
            doctype, fields = list_doctypes[doctype_key]
            return {
                "tool": "get_list",
                "arguments": {
                    "doctype": doctype,
                    "fields": fields,
                    "limit_page_length": 200,
                    "order_by": "modified desc",
                },
                "heading": f"{doctype} list",
            }

    generic_report_list_match = re.match(
        r"^(show|list|get|find)( me)?( the)? (?P<module>accounts|selling|stock|hr|crm|buying)? ?reports?$",
        text,
        re.IGNORECASE,
    )
    if generic_report_list_match:
        module = _normalize_report_module(generic_report_list_match.group("module"))
        return {
            "tool": "report_list",
            "arguments": {"module": module} if module else {},
            "heading": "Available reports",
        }

    report_requirements_match = re.match(
        r"^(show|get|list|what are)( me)?( the)?( filters| requirements| filter requirements| report requirements)( for)? (?P<name>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if report_requirements_match:
        report_name = _normalize_name(report_requirements_match.group("name"))
        if report_name:
            return {
                "tool": "report_requirements",
                "arguments": {"report_name": report_name},
                "heading": report_name,
            }

    report_match = re.match(
        r"^(create|export|download|save)( me)?( the)?( report)? (?P<name>.+?)( (as|to|in) (excel|xlsx|spreadsheet|csv|pdf|word|docx|file|document))$",
        raw_text,
        re.IGNORECASE,
    )
    if report_match:
        report_name = _normalize_name(report_match.group("name"))
        report_name = re.sub(r"^(report)\s+", "", report_name, flags=re.IGNORECASE).strip()
        if report_name:
            return {
                "tool": "get_report",
                "arguments": {"report_name": report_name, "filters": {}},
                "heading": report_name,
            }

    generic_list_match = re.match(
        r"^(show|list|get)( me)?( the)?( list of)? (?P<label>customers?|employees?|items?|sales orders?|sales invoices?|purchase orders?|purchase invoices?|suppliers?|quotations?|leads?|opportunities?)$",
        text,
        re.IGNORECASE,
    )
    if generic_list_match:
        doctype_key = str(generic_list_match.group("label") or "").strip().lower().rstrip("s")
        if doctype_key in list_doctypes:
            doctype, fields = list_doctypes[doctype_key]
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

    named_doc_patterns = {
        key: value[0] for key, value in list_doctypes.items()
    }
    named_doc_match = re.match(
        r"^(show|get)( me)? (?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity) (?P<name>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if named_doc_match:
        doctype_key = str(named_doc_match.group("label") or "").strip().lower()
        doctype = named_doc_patterns.get(doctype_key)
        if doctype:
            return {
                "tool": "get_document",
                "arguments": {"doctype": doctype, "name": _normalize_name(named_doc_match.group("name"))},
                "heading": f"{doctype} {_normalize_name(named_doc_match.group('name'))}",
            }

    export_named_doc_match = re.match(
        r"^(create|export|download|save)( me)?( the)? (?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity) (?P<name>.+?)( (as|to|in) (excel|xlsx|spreadsheet|csv|pdf|word|docx|file|document))$",
        raw_text,
        re.IGNORECASE,
    )
    export_named_doc_alt_match = re.match(
        r"^(create|export|download|save)( me)?( an?|the)?( excel|xlsx|spreadsheet|csv|pdf|word|docx)?( file| document)?( of| for)? (?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity) (?P<name>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    export_named_doc_target = export_named_doc_match or export_named_doc_alt_match
    if export_named_doc_target:
        doctype_key = str(export_named_doc_target.group("label") or "").strip().lower()
        doctype = named_doc_patterns.get(doctype_key)
        if doctype:
            return {
                "tool": "get_document",
                "arguments": {"doctype": doctype, "name": _normalize_name(export_named_doc_target.group("name"))},
                "heading": f"{doctype} {_normalize_name(export_named_doc_target.group('name'))}",
            }

    return None


def _known_doctype_catalog() -> dict[str, tuple[str, list[str]]]:
    return {
        "employee": ("Employee", ["name", "employee_name", "designation", "department", "company_email", "status"]),
        "customer": ("Customer", ["name", "customer_name", "territory", "customer_group"]),
        "item": ("Item", ["name", "item_name", "item_code", "stock_uom"]),
        "sales order": ("Sales Order", ["name", "customer", "transaction_date", "status", "grand_total"]),
        "sales invoice": ("Sales Invoice", ["name", "customer", "posting_date", "status", "grand_total"]),
        "purchase order": ("Purchase Order", ["name", "supplier", "transaction_date", "status", "grand_total"]),
        "purchase invoice": ("Purchase Invoice", ["name", "supplier", "posting_date", "status", "grand_total"]),
        "supplier": ("Supplier", ["name", "supplier_name", "supplier_group", "supplier_type"]),
        "quotation": ("Quotation", ["name", "party_name", "transaction_date", "status", "grand_total"]),
        "lead": ("Lead", ["name", "lead_name", "company_name", "status", "email_id"]),
        "opportunity": ("Opportunity", ["name", "party_name", "opportunity_type", "status", "opportunity_from"]),
    }


def _normalize_report_module(value: Any) -> str | None:
    module = str(value or "").strip().lower()
    if not module:
        return None
    module_map = {
        "accounts": "Accounts",
        "selling": "Selling",
        "stock": "Stock",
        "hr": "HR",
        "crm": "CRM",
        "buying": "Buying",
    }
    return module_map.get(module)


def _provider_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    if _provider_name() == "openai":
        return _openai_chat(prompt, context, history=history, model=model, progress=progress, images=images)
    if _provider_name() == "openai_compatible":
        return _openai_compatible_chat(prompt, context, history=history, model=model, progress=progress, images=images)
    return _anthropic_chat(prompt, context, history=history, model=model, progress=progress, images=images)


def _openai_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    api_key = _cfg("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OpenAI provider is selected but OPENAI_API_KEY is not configured.")

    model = _resolve_model(model)
    max_tool_rounds = _llm_max_tool_rounds()
    timeout_seconds = _llm_request_timeout_seconds()
    temperature = _llm_temperature()
    top_p = _llm_top_p()
    force_tool_use = _llm_force_tool_use_enabled()
    verify_pass_enabled = _llm_verify_pass_enabled()
    base_url = str(_cfg("OPENAI_BASE_URL", "https://api.openai.com")).rstrip("/")
    responses_path = str(_cfg("OPENAI_RESPONSES_PATH", DEFAULT_OPENAI_RESPONSES_PATH) or DEFAULT_OPENAI_RESPONSES_PATH)
    if not responses_path.startswith("/"):
        responses_path = f"/{responses_path}"
    endpoint = f"{base_url}{responses_path}"

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    system = (
        "You are a helpful Frappe and ERPNext assistant. "
        "Use available tools whenever live data is needed or when creating/updating records. "
        "For complex tasks, follow this process: plan briefly, execute tools, verify results, then answer. "
        "Prefer tool calls over guessing. Keep responses concise, factual, and action-oriented. "
        f"{_tool_access_summary(prompt)} "
        "If a user asks to export/download files (Excel/PDF/DOCX), call tools to produce the data payload; file artifacts are generated by the host app. "
        "If image blocks are present in a user message, analyze those images directly and do not claim you cannot view images. "
        "Never invent connectivity/authentication/server issues. If a tool call fails, report the exact returned error text only. "
        f"Current context: doctype={context.get('doctype')}, docname={context.get('docname')}, route={context.get('route')}."
    )

    tool_specs = _openai_tool_specs(prompt)
    previous_response_id: str | None = None
    pending_input: Any = _build_openai_input(history, prompt, images)
    tool_events: list[str] = []
    rendered_payload = None
    had_tool_calls = False
    verification_requested = False

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload: dict[str, Any] = {
            "model": model,
            "instructions": system,
            "input": pending_input,
            "tools": tool_specs,
        }
        if _openai_supports_sampling_controls(model):
            request_payload["temperature"] = temperature
            request_payload["top_p"] = top_p
        if previous_response_id:
            request_payload["previous_response_id"] = previous_response_id
        tool_choice = _tool_choice_payload(force_tool_use, prompt, context, mode="openai")
        if tool_choice is not None:
            request_payload["tool_choice"] = tool_choice

        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc

        response.raise_for_status()
        body = _parse_backend_json(response, endpoint)
        previous_response_id = str(body.get("id") or previous_response_id or "").strip() or None

        output_items = body.get("output") or []
        function_calls = [item for item in output_items if item.get("type") == "function_call"]
        approval_requests = [item for item in output_items if str(item.get("type") or "").startswith("mcp_approval")]
        mcp_calls = [item for item in output_items if item.get("type") == "mcp_call"]

        for mcp_call in mcp_calls:
            label = mcp_call.get("server_label") or mcp_call.get("name") or "mcp"
            status = mcp_call.get("status") or "completed"
            tool_events.append(f"mcp:{label} ({status})")

        if approval_requests:
            raise RuntimeError(
                "OpenAI MCP server requested approval. Set require_approval to 'never' in MCP server config or complete approval externally."
            )

        if not function_calls:
            if had_tool_calls and verify_pass_enabled and not verification_requested and previous_response_id:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                pending_input = _verification_prompt(tool_events)
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": _openai_output_text(body) or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
            }

        had_tool_calls = True
        pending_results = []
        for tool_call in function_calls:
            tool_name = str(tool_call.get("name") or "").strip()
            tool_input = _parse_openai_tool_arguments(tool_call.get("arguments"))
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name)}")
            try:
                tool_result = _run_tool(tool_name, tool_input)
                rendered_payload = tool_result
                tool_events.append(f"{tool_name} {tool_input}")
                pending_results.append(
                    {
                        "type": "function_call_output",
                        "call_id": tool_call.get("call_id"),
                        "output": json.dumps(tool_result, default=str),
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
                _progress_update(progress, stage="working", step=f"Tool failed: {_humanize_tool_name(tool_name)}")
                pending_results.append(
                    {
                        "type": "function_call_output",
                        "call_id": tool_call.get("call_id"),
                        "output": json.dumps(error_payload, default=str),
                    }
                )

        pending_input = pending_results

    raise RuntimeError(
        f"AI assistant exceeded configured tool-call rounds ({max_tool_rounds}). "
        "Try a narrower prompt or increase ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS."
    )


def _openai_compatible_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    api_key = _cfg("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OpenAI-compatible provider is selected but OPENAI_API_KEY is not configured.")

    model = _resolve_model(model)
    max_tool_rounds = _llm_max_tool_rounds()
    timeout_seconds = _llm_request_timeout_seconds()
    temperature = _llm_temperature()
    top_p = _llm_top_p()
    force_tool_use = _llm_force_tool_use_enabled()
    verify_pass_enabled = _llm_verify_pass_enabled()
    base_url = str(_cfg("OPENAI_BASE_URL", "https://integrate.api.nvidia.com")).rstrip("/")
    path = str(_cfg("OPENAI_RESPONSES_PATH", "/v1/chat/completions") or "/v1/chat/completions")
    if not path.startswith("/"):
        path = f"/{path}"
    endpoint = f"{base_url}{path}"

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    system = (
        "You are a helpful Frappe and ERPNext assistant. "
        "Use available tools whenever live data is needed or when creating/updating records. "
        "For complex tasks, follow this process: plan briefly, execute tools, verify results, then answer. "
        "Prefer tool calls over guessing. Keep responses concise, factual, and action-oriented. "
        f"{_tool_access_summary(prompt)} "
        "If image blocks are present in a user message, analyze those images directly and do not claim you cannot view images. "
        "Never invent connectivity/authentication/server issues. If a tool call fails, report the exact returned error text only. "
        f"Current context: doctype={context.get('doctype')}, docname={context.get('docname')}, route={context.get('route')}."
    )

    messages = _build_openai_compatible_messages(history, prompt, images)
    tool_specs = _openai_compatible_tool_specs(prompt)
    tool_events: list[str] = []
    rendered_payload = None
    had_tool_calls = False
    verification_requested = False
    disable_tool_choice = False
    tools_enabled = True

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "system", "content": system}] + messages,
            "stream": False,
        }
        if tools_enabled and tool_specs:
            request_payload["tools"] = tool_specs
            tool_choice = None if disable_tool_choice else _tool_choice_payload(force_tool_use, prompt, context, mode="openai")
            if tool_choice is not None:
                request_payload["tool_choice"] = tool_choice
        if _openai_compatible_supports_sampling_controls(model):
            request_payload["temperature"] = temperature
            request_payload["top_p"] = top_p

        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc

        if response.status_code >= 400:
            error_detail = _extract_error_detail(response)
            if tools_enabled and _is_tool_choice_function_none_error(error_detail):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected forced tool_choice; retrying without tool_choice",
                )
                continue
            if tools_enabled and _is_tool_choice_schema_error(error_detail):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected tool_choice schema; retrying with safer tool_choice",
                )
                continue
            if tools_enabled and _is_degraded_function_error(error_detail):
                tools_enabled = False
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected tool calls; retrying text-only",
                )
                continue
            raise RuntimeError(f"AI endpoint rejected request ({endpoint}): {error_detail}")
        response.raise_for_status()
        body = _parse_backend_json(response, endpoint)
        choice = ((body.get("choices") or [{}])[0]) if isinstance(body.get("choices"), list) else {}
        message = choice.get("message") or {}
        tool_calls = message.get("tool_calls") or []
        text_body = str(message.get("content") or "").strip()

        if not tool_calls:
            if had_tool_calls and verify_pass_enabled and not verification_requested:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                messages.append({"role": "user", "content": _verification_prompt(tool_events)})
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": text_body or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
            }

        had_tool_calls = True
        messages.append({"role": "assistant", "content": text_body or "", "tool_calls": tool_calls})
        for tool_call in tool_calls:
            function_payload = tool_call.get("function") or {}
            tool_name = str(function_payload.get("name") or "").strip()
            tool_input = _parse_openai_tool_arguments(function_payload.get("arguments"))
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name)}")
            try:
                tool_result = _run_tool(tool_name, tool_input)
                rendered_payload = tool_result
                tool_events.append(f"{tool_name} {tool_input}")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
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
                _progress_update(progress, stage="working", step=f"Tool failed: {_humanize_tool_name(tool_name)}")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": json.dumps(error_payload, default=str),
                    }
                )

    raise RuntimeError(
        f"AI assistant exceeded configured tool-call rounds ({max_tool_rounds}). "
        "Try a narrower prompt or increase ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS."
    )


def _anthropic_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    api_key = _cfg("ANTHROPIC_API_KEY")
    auth_token = _cfg("ANTHROPIC_AUTH_TOKEN")
    model = _resolve_model(model)
    max_tokens = _llm_request_max_tokens()
    timeout_seconds = _llm_request_timeout_seconds()
    stream_enabled = _llm_request_stream_enabled()
    max_tool_rounds = _llm_max_tool_rounds()
    temperature = _llm_temperature()
    top_p = _llm_top_p()
    force_tool_use = _llm_force_tool_use_enabled()
    verify_pass_enabled = _llm_verify_pass_enabled()
    base_url = str(_cfg("ANTHROPIC_BASE_URL", "https://api.anthropic.com")).rstrip("/")
    messages_path = str(_cfg("ANTHROPIC_MESSAGES_PATH", "/v1/messages"))
    if not messages_path.startswith("/"):
        messages_path = f"/{messages_path}"
    tool_choice_mode = _tool_choice_mode(base_url, messages_path)

    beta_param = _normalize_beta_param(_cfg("ANTHROPIC_BETA"))

    endpoint = f"{base_url}{messages_path}"

    headers = {"content-type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    elif auth_token:
        headers["authorization"] = f"Bearer {auth_token}"
    anthropic_version = _cfg("ANTHROPIC_VERSION", "2023-06-01")
    if anthropic_version and tool_choice_mode == "anthropic":
        headers["anthropic-version"] = anthropic_version
    if beta_param and tool_choice_mode == "anthropic":
        headers["anthropic-beta"] = beta_param
    tool_definitions = get_tool_definitions()
    allowed_names = _allowed_tool_names(prompt, set(tool_definitions))
    tool_specs = [
        {
            "name": name,
            "description": spec["description"],
            "input_schema": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
        if name in allowed_names
    ]
    if images:
        frappe.logger("erp_ai_assistant").info(
            "vision_request model=%s base_url=%s image_count=%s",
            model,
            base_url,
            len(images),
        )

    system = (
        "You are a helpful Frappe and ERPNext assistant. "
        "Use available tools whenever live data is needed or when creating/updating records. "
        "For complex tasks, follow this process: plan briefly, execute tools, verify results, then answer. "
        "Prefer tool calls over guessing. Keep responses concise, factual, and action-oriented. "
        f"{_tool_access_summary(prompt)} "
        "If a user asks to export/download files (Excel/PDF/DOCX), call tools to produce the data payload; file artifacts are generated by the host app. "
        "If image blocks are present in a user message, analyze those images directly and do not claim you cannot view images. "
        "Never invent connectivity/authentication/server issues. If a tool call fails, report the exact returned error text only. "
        f"Current context: doctype={context.get('doctype')}, docname={context.get('docname')}, route={context.get('route')}."
    )
    messages: list[dict[str, Any]] = _build_messages_with_images(history, prompt, images)
    tool_events: list[str] = []
    rendered_payload = None
    had_tool_calls = False
    verification_requested = False
    tools_enabled = True
    tool_choice_fallback_applied = False
    disable_tool_choice = False

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload = {
            "model": model,
            "max_tokens": max_tokens,
            "stream": stream_enabled,
            "temperature": temperature,
            "top_p": top_p,
            "system": system,
            "messages": messages,
        }
        if tools_enabled:
            request_payload["tools"] = tool_specs
            if not disable_tool_choice:
                tool_choice = _tool_choice_payload(
                    force_tool_use,
                    prompt,
                    context,
                    mode=tool_choice_mode,
                )
                if tool_choice is not None:
                    request_payload["tool_choice"] = tool_choice

        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc

        if response.status_code >= 400:
            error_detail = _extract_error_detail(response)
            if tools_enabled and _is_tool_choice_function_none_error(error_detail):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider rejected forced tool_choice; retrying without tool_choice",
                )
                continue
            if (
                tools_enabled
                and not tool_choice_fallback_applied
                and tool_choice_mode != "openai"
                and _is_tool_choice_schema_error(error_detail)
            ):
                tool_choice_mode = "openai"
                tool_choice_fallback_applied = True
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider rejected Anthropic tool_choice; retrying OpenAI tool_choice format",
                )
                continue
            if tools_enabled and _is_degraded_function_error(error_detail):
                tools_enabled = False
                force_tool_use = False
                _progress_update(progress, stage="working", step="Provider rejected tool calls; retrying text-only")
                continue
        response.raise_for_status()
        body = _parse_backend_json(response, endpoint)
        content_blocks = body.get("content", [])
        messages.append({"role": "assistant", "content": content_blocks})

        text_chunks = [block.get("text", "") for block in content_blocks if block.get("type") == "text" and block.get("text")]
        tool_uses = [block for block in content_blocks if block.get("type") == "tool_use"]

        if not tool_uses:
            text_body = "\n".join(text_chunks)
            if tools_enabled and not disable_tool_choice and _is_tool_choice_function_none_error(text_body):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider returned tool_choice error in body; retrying without tool_choice",
                )
                continue
            if tools_enabled and not disable_tool_choice and _is_tool_choice_schema_error(text_body):
                disable_tool_choice = True
                force_tool_use = False
                if tool_choice_mode != "openai":
                    tool_choice_mode = "openai"
                    tool_choice_fallback_applied = True
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider returned tool_choice schema error in body; retrying with safer tool_choice",
                )
                continue
            if had_tool_calls and verify_pass_enabled and not verification_requested:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                messages.append(
                    {
                        "role": "user",
                        "content": _verification_prompt(tool_events),
                    }
                )
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": "\n\n".join(chunk for chunk in text_chunks if chunk).strip() or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
            }

        had_tool_calls = True
        tool_results = []
        for tool_use in tool_uses:
            tool_name = tool_use["name"]
            tool_input = tool_use.get("input", {})
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name)}")
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
                _progress_update(progress, stage="working", step=f"Tool failed: {_humanize_tool_name(tool_name)}")
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "is_error": True,
                        "content": json.dumps(error_payload, default=str),
                    }
                )

        messages.append({"role": "user", "content": tool_results})

    raise RuntimeError(
        f"AI assistant exceeded configured tool-call rounds ({max_tool_rounds}). "
        "Try a narrower prompt or increase ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS."
    )


def _extract_error_detail(response: requests.Response) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            return json.dumps(payload, default=str)
        return str(payload)
    except Exception:
        return (response.text or "").strip()


def _is_degraded_function_error(detail: str) -> bool:
    text = str(detail or "").lower()
    return "degraded function" in text or "cannot be invoked" in text


def _is_tool_choice_schema_error(detail: str) -> bool:
    text = str(detail or "").lower()
    required_fragments = (
        "tool_choice",
        "input should be 'auto', 'required' or 'none'",
        "input should be 'function'",
    )
    return all(fragment in text for fragment in required_fragments)


def _is_tool_choice_function_none_error(detail: str) -> bool:
    text = str(detail or "").lower()
    return (
        "tool_choice" in text
        and "invalid value for `function`" in text
        and "`none`" in text
    )


def _openai_tool_specs(prompt: str) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions()
    allowed_names = _allowed_tool_names(prompt, set(tool_definitions))
    specs = [
        {
            "type": "function",
            "name": name,
            "description": spec["description"],
            "parameters": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
        if name in allowed_names
    ]
    if _cfg_bool("ERP_AI_OPENAI_MCP_ENABLED", False):
        specs.extend(get_remote_mcp_servers())
    return specs


def _openai_compatible_tool_specs(prompt: str) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions()
    allowed_names = _allowed_tool_names(prompt, set(tool_definitions))
    return [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": spec["description"],
                "parameters": spec["inputSchema"],
            },
        }
        for name, spec in tool_definitions.items()
        if name in allowed_names
    ]


def _build_openai_input(
    history: Optional[list[dict[str, Any]]], prompt: str, images: Optional[list[dict[str, str]]] = None
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in (history or [])]
    normalized: list[dict[str, Any]] = []
    last_user_index = -1

    for row in rows:
        role = str(row.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _build_openai_input_content(str(row.get("content") or "").strip(), None)
        if not content:
            continue
        normalized.append({"role": role, "content": content})
        if role == "user":
            last_user_index = len(normalized) - 1

    current_content = _build_openai_input_content(prompt, images)
    if current_content:
        if last_user_index >= 0 and images:
            normalized[last_user_index]["content"] = current_content
        elif not normalized or normalized[-1].get("role") != "user" or images:
            normalized.append({"role": "user", "content": current_content})
        else:
            normalized[-1]["content"] = current_content

    return normalized or [{"role": "user", "content": _build_openai_input_content(prompt, images)}]


def _build_openai_compatible_messages(
    history: Optional[list[dict[str, Any]]], prompt: str, images: Optional[list[dict[str, str]]] = None
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in (history or [])]
    normalized: list[dict[str, Any]] = []
    for row in rows:
        role = str(row.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _build_openai_compatible_content(str(row.get("content") or "").strip(), None)
        if content in ("", []):
            continue
        normalized.append({"role": role, "content": content})

    current_content = _build_openai_compatible_content(prompt, images)
    normalized.append({"role": "user", "content": current_content})
    return normalized


def _build_openai_compatible_content(text: str, images: Optional[list[dict[str, str]]]) -> Any:
    blocks: list[dict[str, Any]] = []
    if str(text or "").strip():
        blocks.append({"type": "text", "text": text.strip()})
    for image in images or []:
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        blocks.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:{media_type};base64,{data}"},
            }
        )
    if str(text or "").strip() and not images:
        return text.strip()
    return blocks if blocks else (text or "").strip()


def _build_openai_input_content(text: str, images: Optional[list[dict[str, str]]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if str(text or "").strip():
        blocks.append({"type": "input_text", "text": text.strip()})
    for image in images or []:
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        blocks.append(
            {
                "type": "input_image",
                "image_url": f"data:{media_type};base64,{data}",
            }
        )
    return blocks


def _openai_output_text(body: dict[str, Any]) -> str:
    top_level = str(body.get("output_text") or "").strip()
    if top_level:
        return top_level

    chunks: list[str] = []
    for item in body.get("output") or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            text = content.get("text") or content.get("output_text")
            if text:
                chunks.append(str(text).strip())
    return "\n\n".join(chunk for chunk in chunks if chunk).strip()


def _parse_openai_tool_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str):
        try:
            parsed = json.loads(arguments)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _openai_supports_sampling_controls(model: str | None) -> bool:
    name = str(model or "").strip().lower()
    if not name:
        return True
    # OpenAI's GPT-5 family can reject temperature/top_p on the Responses API.
    return not name.startswith("gpt-5")


def _openai_compatible_supports_sampling_controls(model: str | None) -> bool:
    return True


def _build_messages_with_images(
    history: Optional[list[dict[str, Any]]], prompt: str, images: Optional[list[dict[str, str]]] = None
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in (history or [])]
    image_blocks = _build_image_blocks(images)

    if not image_blocks:
        return rows or [{"role": "user", "content": prompt}]

    merged_content = _build_user_multimodal_content(prompt, image_blocks)
    for index in range(len(rows) - 1, -1, -1):
        if str(rows[index].get("role") or "").strip().lower() == "user":
            rows[index]["content"] = merged_content
            return rows

    rows.append({"role": "user", "content": merged_content})
    return rows


def _build_user_multimodal_content(prompt: str, image_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    text = (prompt or "").strip()
    if text:
        blocks.append({"type": "text", "text": text})
    blocks.extend(image_blocks)
    if not blocks:
        blocks.append({"type": "text", "text": "Please analyze the attached image."})
    return blocks


def _build_image_blocks(images: Optional[list[dict[str, str]]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for image in images or []:
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        blocks.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": data,
                },
            }
        )
    return blocks


def _parse_prompt_images(images: str | list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if images in (None, "", []):
        return []

    payload: Any = images
    if isinstance(images, str):
        try:
            payload = json.loads(images)
        except Exception:
            return []

    if not isinstance(payload, list):
        return []

    parsed: list[dict[str, str]] = []
    for row in payload[:4]:
        if not isinstance(row, dict):
            continue
        data_url = str(row.get("data_url") or "").strip()
        media_type, data = _extract_base64_image(data_url)
        if not media_type or not data:
            continue
        if len(data) > 8_000_000:
            continue
        parsed.append({"media_type": media_type, "data": data})
    return parsed


def _extract_base64_image(data_url: str) -> tuple[str, str]:
    if not data_url.startswith("data:image/"):
        return "", ""
    marker = ";base64,"
    if marker not in data_url:
        return "", ""
    header, data = data_url.split(marker, 1)
    media_type = header.replace("data:", "", 1).strip().lower()
    if not media_type.startswith("image/"):
        return "", ""
    cleaned = re.sub(r"\s+", "", data or "")
    if not cleaned:
        return "", ""
    return media_type, cleaned


def _format_image_only_user_content(count: int) -> str:
    size = max(1, int(count or 0))
    return f"[Attached {size} image{'s' if size != 1 else ''}]"


def _build_prompt_image_attachments(images: Optional[list[dict[str, str]]]) -> dict[str, Any]:
    attachments: list[dict[str, str]] = []
    for index, image in enumerate(images or [], start=1):
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        extension = media_type.split("/", 1)[1].split("+", 1)[0] or "png"
        attachments.append(
            {
                "label": "Image",
                "filename": f"image-{index}.{extension}",
                "file_type": extension,
                "file_url": f"data:{media_type};base64,{data}",
            }
        )
    return {"attachments": attachments}


def _response_rejects_images(text: Any) -> bool:
    body = str(text or "").lower()
    if not body:
        return False
    blocked_terms = [
        "i can't see",
        "i cannot see",
        "i don't see any image",
        "i do not see any image",
        "i don't see an image",
        "i do not see an image",
        "no image in your message",
        "can't view images",
        "cannot view images",
        "don't have the ability to view images",
        "do not have the ability to view images",
        "please upload the image",
    ]
    return any(term in body for term in blocked_terms)


def _humanize_tool_name(name: str) -> str:
    value = str(name or "").strip().replace("_", " ")
    return value.capitalize() if value else "Tool"


@frappe.whitelist()
def get_available_models() -> dict[str, Any]:
    models = _available_llm_models()
    return {"models": models, "default_model": models[0] if models else None}


def _resolve_model(model: str | None) -> str:
    provider = _provider_name()
    is_openai_family = provider in {"openai", "openai_compatible"}
    default_key = "OPENAI_MODEL" if is_openai_family else "ANTHROPIC_MODEL"
    default_model = str(_cfg(default_key, DEFAULT_OPENAI_MODEL if is_openai_family else "claude-sonnet-4-6")).strip()
    available = _available_llm_models()
    requested = str(model or "").strip()
    if requested and requested in available:
        return requested
    return default_model


def _resolve_model_for_request(model: str | None, *, has_images: bool) -> str:
    """Resolve model, optionally routing image prompts to a vision-capable alias."""
    if _provider_name() not in {"openai", "openai_compatible"} and has_images:
        vision_model = str(
            _cfg(
                "ERP_AI_ANTHROPIC_VISION_MODEL",
                _cfg("ANTHROPIC_VISION_MODEL", DEFAULT_ANTHROPIC_VISION_MODEL),
            )
        ).strip()
        if vision_model:
            available = _available_llm_models()
            if not available or vision_model in available:
                return vision_model
    return _resolve_model(model)


def _available_llm_models() -> list[str]:
    provider = _provider_name()
    is_openai_family = provider in {"openai", "openai_compatible"}
    models_key = "OPENAI_MODELS" if is_openai_family else "ANTHROPIC_MODELS"
    default_key = "OPENAI_MODEL" if is_openai_family else "ANTHROPIC_MODEL"
    default_value = DEFAULT_OPENAI_MODEL if is_openai_family else "claude-sonnet-4-6"
    raw = _cfg(models_key)
    models: list[str] = []

    if isinstance(raw, (list, tuple)):
        models = [str(item).strip() for item in raw if str(item or "").strip()]
    elif isinstance(raw, str):
        text = raw.strip()
        if text:
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        models = [str(item).strip() for item in parsed if str(item or "").strip()]
                except Exception:
                    models = []
            if not models:
                normalized = text.replace("\n", ",")
                models = [chunk.strip() for chunk in normalized.split(",") if chunk.strip()]

    default_model = str(_cfg(default_key, default_value)).strip()
    if default_model:
        models.append(default_model)

    unique: list[str] = []
    seen = set()
    for item in models:
        if item and item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


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
        content_type = (response.headers.get("content-type") or "").lower()
        if "text/event-stream" in content_type:
            return _parse_sse_response(response.text, endpoint)
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


def _parse_sse_response(text: str, endpoint: str) -> dict[str, Any]:
    blocks_by_index: dict[int, dict[str, Any]] = {}
    message_payload: dict[str, Any] = {}
    stop_reason = None

    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        raw_data = line[len("data:") :].strip()
        if not raw_data or raw_data == "[DONE]":
            continue
        try:
            payload = json.loads(raw_data)
        except Exception:
            continue

        event_type = payload.get("type")
        if event_type == "message_start":
            message_payload = payload.get("message") or {}
            continue
        if event_type == "message_delta":
            if payload.get("delta", {}).get("stop_reason"):
                stop_reason = payload["delta"]["stop_reason"]
            continue
        if event_type == "content_block_start":
            index = int(payload.get("index", 0))
            block = payload.get("content_block") or {}
            blocks_by_index[index] = block
            continue
        if event_type == "content_block_delta":
            index = int(payload.get("index", 0))
            block = blocks_by_index.setdefault(index, {})
            delta = payload.get("delta") or {}
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                block["type"] = block.get("type") or "text"
                block["text"] = (block.get("text") or "") + (delta.get("text") or "")
            elif delta_type == "input_json_delta":
                block["_input_json"] = (block.get("_input_json") or "") + (delta.get("partial_json") or "")
            continue
        if event_type == "content_block_stop":
            index = int(payload.get("index", 0))
            block = blocks_by_index.get(index) or {}
            if block.get("type") == "tool_use" and block.get("_input_json"):
                try:
                    block["input"] = json.loads(block["_input_json"])
                except Exception:
                    block["input"] = {}
            block.pop("_input_json", None)
            blocks_by_index[index] = block

    content = [blocks_by_index[i] for i in sorted(blocks_by_index.keys()) if blocks_by_index[i]]
    if not content and message_payload.get("content"):
        content = message_payload.get("content") or []

    if not content:
        preview = (text or "").strip().replace("\n", " ")[:300]
        raise RuntimeError(
            f"AI endpoint returned SSE but no parsable content from {endpoint}. Body preview: {preview or '<empty>'}"
        )

    return {
        "id": message_payload.get("id"),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": message_payload.get("model"),
        "stop_reason": stop_reason or message_payload.get("stop_reason"),
    }


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

    return dispatch_tool(mapped_name, arguments)


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
    if tool_name == "report_list":
        return _format_report_list_result(payload, heading)
    if tool_name == "report_requirements":
        return _format_report_requirements_result(payload, heading)
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


def _format_report_list_result(payload: Any, heading: Optional[str]) -> str:
    if not isinstance(payload, dict):
        return _format_generic_result(payload, heading)
    reports = payload.get("reports") or payload.get("data") or payload.get("result")
    if not isinstance(reports, list):
        return _format_generic_result(payload, heading or "Available reports")
    lines = [heading or "Available reports", ""]
    for index, row in enumerate(reports[:20], start=1):
        if not isinstance(row, dict):
            lines.append(f"{index}. {row}")
            continue
        name = row.get("report_name") or row.get("name") or f"Report {index}"
        module = row.get("module")
        report_type = row.get("report_type")
        parts = [part for part in [module, report_type] if part]
        suffix = f" ({', '.join(parts)})" if parts else ""
        lines.append(f"{index}. {name}{suffix}")
    return "\n".join(lines)


def _format_report_requirements_result(payload: Any, heading: Optional[str]) -> str:
    if not isinstance(payload, dict):
        return _format_generic_result(payload, heading)
    title = heading or payload.get("report_name") or "Report requirements"
    lines = [title, ""]
    if payload.get("report_type"):
        lines.append(f"report_type: {payload.get('report_type')}")
    requirements = payload.get("filter_requirements") or {}
    if isinstance(requirements, dict):
        required = requirements.get("common_required_filters") or []
        optional = requirements.get("common_optional_filters") or []
        if required:
            lines.append(f"required_filters: {', '.join(str(item) for item in required[:6])}")
        if optional:
            lines.append(f"optional_filters: {', '.join(str(item) for item in optional[:6])}")
    required_names = payload.get("required_filter_names") or []
    if required_names:
        lines.append(f"required_filter_names: {', '.join(str(item) for item in required_names[:8])}")
    guidance = requirements.get("guidance") if isinstance(requirements, dict) else None
    if isinstance(guidance, list) and guidance:
        lines.append(f"guidance: {guidance[0]}")
    return "\n".join(lines)


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
