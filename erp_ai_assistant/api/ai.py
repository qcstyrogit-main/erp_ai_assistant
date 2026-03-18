import json
import os
import re
import ast
import html
from urllib.parse import quote
from typing import Any, Optional

import frappe
import requests
from frappe import _

from .chat import add_message, create_conversation
from .export import add_message_attachment_urls, create_message_artifacts
from .fac_client import dispatch_tool, get_tool_definitions
from .provider_settings import get_active_provider, get_provider_setting, get_remote_mcp_servers
from .resource_registry import list_resource_specs


TOOL_NAME_MAP = {
    "get_list": "list_documents",
    "get_report": "generate_report",
    "frappe.get_list": "get_list",
    "frappe.client.get_list": "get_list",
    "frappe.get_doc": "get_document",
    "frappe.client.get": "get_document",
    "frappe.get_report": "get_report",
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
CORE_DOC_TOOL_NAMES = {
    "get_document",
    "list_documents",
    "create_document",
    "update_document",
    "delete_document",
    "submit_document",
    "search_documents",
    "search_doctype",
    "search_link",
    "get_doctype_info",
    "run_workflow",
}
REPORT_TOOL_NAMES = {
    "generate_report",
    "report_list",
    "report_requirements",
}
REPORT_DISCOVERY_TOOL_NAMES = {
    "generate_report",
    "report_list",
    "report_requirements",
}
ANALYSIS_TOOL_NAMES = {
    "run_python_code",
    "analyze_business_data",
    "run_database_query",
    "create_visualization",
    "create_dashboard",
    "create_dashboard_chart",
    "list_user_dashboards",
    "chatgpt_search",
    "chatgpt_fetch",
}


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


def _endpoint_host(base_url: str) -> str:
    text = str(base_url or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text)
    return text.split("/", 1)[0]


def _provider_compatibility_profile(
    provider: str,
    base_url: str,
    path: str = "",
    *,
    model: str | None = None,
) -> dict[str, Any]:
    host = _endpoint_host(base_url)
    normalized_model = str(model or "").strip().lower()

    profile = {
        "provider": provider,
        "profile": provider,
        "host": host,
        "path": str(path or "").strip(),
        "disable_tool_choice_by_default": False,
        "disable_sampling_by_default": False,
        "allow_textual_tool_fallback": True,
    }

    if provider == "openai":
        profile["profile"] = "openai"
        profile["allow_textual_tool_fallback"] = False
        return profile

    if provider == "anthropic":
        profile["profile"] = "anthropic"
        profile["allow_textual_tool_fallback"] = False
        return profile

    if "openrouter.ai" in host or normalized_model.startswith("openrouter/"):
        profile["profile"] = "openrouter"
        # OpenRouter routes across providers with varying tool_choice support.
        profile["disable_tool_choice_by_default"] = True
        return profile

    if "integrate.api.nvidia.com" in host or "build.nvidia.com" in host or normalized_model.startswith("nvidia/"):
        profile["profile"] = "nvidia"
        return profile

    profile["profile"] = "generic_openai_compatible"

    return profile


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

    read_verbs = (
        "show",
        "list",
        "find",
        "get",
        "fetch",
        "search",
        "open",
        "lookup",
        "display",
        "view",
    )
    mutate_verbs = (
        "create",
        "add",
        "insert",
        "update",
        "edit",
        "change",
        "modify",
        "set",
        "rename",
        "submit",
        "cancel",
        "delete",
        "remove",
        "approve",
        "reject",
    )
    export_verbs = (
        "export",
        "download",
        "save as",
        "generate excel",
        "generate pdf",
    )
    erp_nouns = (
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
        "report",
        "workflow",
        "dashboard",
    )
    has_erp_noun = any(term in text for term in erp_nouns)
    has_action = any(re.search(rf"\b{re.escape(term)}\b", text) for term in read_verbs + mutate_verbs) or any(
        term in text for term in export_verbs
    )
    return has_erp_noun and has_action


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


def _is_sample_data_request(prompt: str, context: Optional[dict[str, Any]] = None) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False

    strong_indicators = (
        "sample data",
        "sample records",
        "dummy data",
        "mock data",
        "for testing",
        "test data",
        "fictional data",
        "return the result in table format",
        "return in table format",
    )
    if any(term in text for term in strong_indicators):
        return True

    if "realistic" in text and "create" in text and ("records" in text or "rows" in text):
        return True

    return False


def _tokenize_match_text(value: Any) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    return {token for token in re.split(r"[^a-z0-9_]+", text.replace("-", "_")) if len(token) >= 2}


def _tool_match_terms(name: str, spec: dict[str, Any]) -> set[str]:
    terms = set()
    terms.update(_tokenize_match_text(name.replace("_", " ")))
    terms.update(_tokenize_match_text(spec.get("description")))

    annotations = spec.get("annotations")
    if isinstance(annotations, dict):
        for value in annotations.values():
            terms.update(_tokenize_match_text(value))

    input_schema = spec.get("inputSchema")
    if isinstance(input_schema, dict):
        properties = input_schema.get("properties")
        if isinstance(properties, dict):
            for field_name, field_spec in properties.items():
                terms.update(_tokenize_match_text(field_name))
                if isinstance(field_spec, dict):
                    terms.update(_tokenize_match_text(field_spec.get("title")))
                    terms.update(_tokenize_match_text(field_spec.get("description")))

    return terms


def _tool_match_score(prompt: str, context: dict[str, Any], name: str, spec: dict[str, Any]) -> int:
    prompt_text = str(prompt or "").strip().lower()
    normalized_name = str(name or "").strip().lower().replace("_", " ")
    prompt_terms = _tokenize_match_text(prompt_text)
    tool_terms = _tool_match_terms(name, spec)
    overlap = prompt_terms.intersection(tool_terms)

    score = len(overlap) * 3
    if normalized_name and normalized_name in prompt_text:
        score += 12
    if str(name or "").strip().lower() in prompt_text:
        score += 8

    if _has_active_document_context(context) and any(term in tool_terms for term in {"document", "doctype", "workflow"}):
        score += 2

    if _has_write_intent(prompt_text) and any(term in tool_terms for term in {"create", "update", "delete", "workflow", "submit", "cancel"}):
        score += 4
    if _has_destructive_intent(prompt_text) and any(term in tool_terms for term in {"delete", "remove", "purge", "cancel"}):
        score += 4
    if any(term in prompt_text for term in {"export", "excel", "xlsx", "csv", "pdf", "docx", "word", "download"}):
        if any(term in tool_terms for term in {"list", "document", "report", "fetch", "export", "search"}):
            score += 2

    return score


def _selected_tool_names_for_prompt(
    prompt: str,
    context: dict[str, Any],
    tool_definitions: dict[str, dict[str, Any]],
) -> set[str]:
    available_names = set(tool_definitions)
    if not available_names:
        return set()
    if not _is_erp_intent(prompt, context):
        return set()
    if _is_sample_data_request(prompt, context):
        return set()

    scored: list[tuple[int, str]] = []
    for name, spec in tool_definitions.items():
        if name not in available_names or not isinstance(spec, dict):
            continue
        score = _tool_match_score(prompt, context, name, spec)
        scored.append((score, name))

    scored.sort(key=lambda item: (-item[0], _tool_priority_key(item[1])))
    positive = [(score, name) for score, name in scored if score > 0]
    if not positive:
        return available_names

    top_score = positive[0][0]
    cutoff = max(top_score - 5, 1)
    selected = {name for score, name in positive if score >= cutoff}
    if len(available_names) <= 18:
        return available_names
    if len(selected) < 8:
        selected.update(name for _score, name in positive[:12])
    specific_business_tools = {
        "find_one_document",
        "set_document_fields",
        "create_report",
        "get_report_definition",
        "update_report",
        "run_report",
        "export_report",
        "export_doctype_records",
    }
    selected.update(name for name in available_names if name in specific_business_tools)
    return selected or available_names


def _has_active_document_context(context: Optional[dict[str, Any]] = None) -> bool:
    current = context or {}
    return bool(current.get("doctype") and current.get("docname"))


def _has_doctype_context(context: Optional[dict[str, Any]] = None) -> bool:
    current = context or {}
    return bool(current.get("doctype"))


def _context_summary(context: Optional[dict[str, Any]] = None) -> str:
    current = context or {}
    if _has_active_document_context(current):
        return (
            f"Current context: doctype={current.get('doctype')}, "
            f"docname={current.get('docname')}, route={current.get('route')}."
        )
    if _has_doctype_context(current):
        return (
            f"Current context: doctype={current.get('doctype')}, "
            f"no active document, route={current.get('route')}."
        )
    route = str(current.get("route") or "").strip()
    if route:
        return f"Current context: no active document. Current route={route}."
    return "Current context: no active document. Treat this as a general workspace chat unless the user asks for ERP data."


def _tool_catalog_summary(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    try:
        tool_definitions = get_tool_definitions()
    except Exception:
        tool_definitions = {}

    available_names = set(tool_definitions)
    selected_names = sorted(_selected_tool_names_for_prompt(prompt, context or {}, tool_definitions))
    if not selected_names:
        selected_names = sorted(available_names)
    if not selected_names:
        return "No FAC tools are currently available. Answer directly unless tool access becomes available."

    preview = ", ".join(selected_names[:24])
    suffix = " ..." if len(selected_names) > 24 else ""
    return (
        "Use only the FAC tools that are actually exposed in this session. "
        "Select tools from the live FAC catalog based on the user's request, tool descriptions, and input schema. "
        "Do not invent or assume tools that are not present. "
        f"Best-matching FAC tools for this request: {preview}{suffix}."
    )


def _erp_tool_system_prompt(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    current = context or {}
    sample_data_mode = _is_sample_data_request(prompt, current)
    return (
        "You are an ERPNext AI assistant connected to a Frappe server. "
        "You are connected to Frappe Assistant Core (FAC), which exposes the tool catalog dynamically for this session. "
        "Your final reply must read like a polished desktop assistant response: natural, direct, and user-facing. "
        "Never expose raw tool calls, XML/tool markers, JSON payloads, function names, internal schemas, or chain-of-thought. "
        "Do not narrate internal tool usage unless the user explicitly asks for technical details. "
        "If the user asks for live ERP data or a real ERP mutation, call a tool. "
        "If the user asks for sample data, dummy data, mock data, or testing data, do not call ERP tools. "
        "For sample/testing data requests, generate fictional but realistic business data directly and clearly present it as sample data. "
        "When the user requests table format for sample data, return a markdown table. "
        "Never guess ERP data. Never invent document names, fields, item codes, workflow actions, or server problems. "
        "Prefer tool calls over text explanations whenever live ERP data or mutations are involved. "
        "Choose FAC tools from the live catalog by matching the user's request to the tool name, description, annotations, and input schema. "
        "Prefer the most specific FAC tool that fits the request; only use broader tools when no more specific FAC tool is available. "
        "If the user asks to create a report, prefer create_report when available. "
        "Only fall back to create_document with doctype='Report' if create_report is not exposed by FAC. "
        "When the user does not specify a report type, default the created report to Query Report and use the referenced doctype from the prompt. "
        "Try to complete the request directly from the user's prompt, current context, prior conversation, and tool results before asking for more input. "
        "Only ask for clarification when the action is blocked after you have already used the available context and tools. "
        "If FAC does not expose a needed mutation tool, do not pretend the action succeeded; explain the limitation briefly. "
        "If the current context has a doctype but no active document, and the user asks to create a new record without naming a doctype, treat the current context doctype as the intended target. "
        "Example: on Material Request List, 'create new record' means create a Material Request. "
        "When creating documents, extract customer, supplier, items, quantities, dates, and any explicit field values from the prompt. "
        "For exports and PDFs, call tools to produce the structured data or artifact payload; the host app handles the final file download UX. "
        "If image blocks are present in a user message, analyze those images directly and do not claim you cannot view images. "
        "If a tool call fails, report the exact returned error text only. "
        "Use ERP concepts precisely: Sales Order, Quotation, Customer, Supplier, Employee, Item, Purchase Order, Sales Invoice, Delivery Note. "
        f"{_tool_catalog_summary(prompt, current)} "
        f"{_tool_access_summary(prompt, current)} "
        f"{'Sample data mode is active for this request. Do not use FAC tools.' if sample_data_mode else ''} "
        f"Current context: doctype={current.get('doctype')}, docname={current.get('docname')}, route={current.get('route')}."
    )


def _llm_user_prompt(prompt: str) -> str:
    cleaned = str(prompt or "").strip()
    return (
        "User request:\n"
        f"{cleaned}\n\n"
        "Respond by calling a tool when appropriate. "
        "Make a best-effort attempt to complete the request directly. "
        "Use the live FAC catalog to choose the best-matching tool for this request. "
        "Return the final answer in plain natural language for the end user. "
        "Do not show raw tool output, JSON, tool names, or internal markers. "
        "For sample/testing data requests, generate fictional realistic sample output directly. "
        "Ask a concise clarification question only if the task is truly blocked."
    ).strip()


def _tool_priority_key(name: str) -> tuple[int, str]:
    lowered = str(name or "").strip().lower()
    if lowered in CORE_DOC_TOOL_NAMES:
        return (0, lowered)
    if lowered in REPORT_TOOL_NAMES:
        return (1, lowered)
    if "erp" in lowered:
        return (2, lowered)
    return (3, lowered)


def _prioritize_tool_specs(tool_specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        tool_specs,
        key=lambda spec: _tool_priority_key(
            spec.get("name")
            or ((spec.get("function") or {}).get("name") if isinstance(spec.get("function"), dict) else "")
        ),
    )


def _tool_access_summary(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    if _is_sample_data_request(prompt, context or {}):
        return (
            "This request is for sample or testing data, not live ERP facts. "
            "Answer directly without tools and do not search ERP records."
        )
    if not _is_erp_intent(prompt, context or {}):
        return (
            "If the request is general and does not need ERP/Frappe data, answer directly without tools. "
            "Only use ERP tools when the user asks about live ERP data, reports, workflows, or record changes."
        )
    return (
        "For ERP requests, rely on the FAC tools exposed in this session. "
        "Use the live tool catalog rather than assuming any read, write, delete, or workflow capability exists. "
        "Let the model see and choose among the relevant live FAC tools instead of forcing a narrow static subset."
    )


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


def _result_cache_key(conversation: str, user: str) -> str:
    return f"erp_ai_assistant:result:{user}:{conversation}"


def _progress_update(
    progress: Optional[dict[str, Any]],
    stage: str,
    step: str | None = None,
    done: bool = False,
    error: str | None = None,
    partial_text: str | None = None,
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
        "partial_text": partial_text if partial_text is not None else progress.get("partial_text"),
        "updated_at": frappe.utils.now(),
    }
    progress["partial_text"] = payload.get("partial_text")
    expires_in_sec = 300 if done else 900
    frappe.cache().set_value(
        _progress_cache_key(conversation, user),
        json.dumps(payload, default=str),
        expires_in_sec=expires_in_sec,
    )


def _set_prompt_result(
    conversation: str,
    user: str,
    payload: dict[str, Any],
    *,
    expires_in_sec: int = 900,
) -> None:
    if not conversation or not user:
        return
    frappe.cache().set_value(
        _result_cache_key(conversation, user),
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
def get_prompt_result(conversation: str) -> dict[str, Any]:
    if not conversation:
        return {"status": "missing", "done": True}

    raw_payload = frappe.cache().get_value(_result_cache_key(conversation, frappe.session.user))
    if not raw_payload:
        return {"status": "pending", "done": False}

    try:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
    except Exception:
        return {"status": "pending", "done": False}

    if not isinstance(payload, dict):
        return {"status": "pending", "done": False}
    return payload


def _execute_prompt(
    *,
    prompt: str | None = None,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
    user: str | None = None,
) -> dict[str, Any]:
    effective_user = str(user or frappe.session.user or "").strip()
    if effective_user:
        frappe.set_user(effective_user)

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
            conversation=conversation_name,
            history=history,
            model=selected_model,
            progress=progress,
            images=parsed_images,
        )
    except Exception as exc:
        error_text = str(exc) or "Unknown error"
        _progress_update(progress, stage="failed", done=True, error=error_text)
        _set_prompt_result(
            conversation_name,
            frappe.session.user,
            {"status": "failed", "done": True, "error": error_text, "conversation": conversation_name},
        )
        raise

    attachments = _build_message_attachments(
        response.get("payload"),
        title=f"{_summarize_title(prompt_text or user_content)} export",
        conversation=conversation_name,
        prompt=prompt_text,
    )
    attachments = _merge_attachment_packages(attachments, response.get("attachments"))
    reply_text = _finalize_reply_text(
        response["text"],
        prompt_text,
        attachments,
        payload=response.get("payload"),
    )
    message = add_message(
        conversation_name,
        "assistant",
        reply_text,
        tool_events=json.dumps(response.get("tool_events", [])),
        attachments_json=json.dumps({"attachments": [], "exports": {}}),
    )
    attachments = add_message_attachment_urls(message["name"], attachments)
    if attachments.get("attachments"):
        assistant_message = frappe.get_doc("AI Message", message["name"])
        assistant_message.db_set("attachments_json", json.dumps(attachments, default=str), update_modified=False)
    _progress_update(progress, stage="completed", done=True, step="Response ready", partial_text="")

    result = {
        "conversation": conversation_name,
        "reply": reply_text,
        "tool_events": response.get("tool_events", []),
        "payload": response.get("payload"),
        "attachments": attachments,
        "context": context,
    }
    _set_prompt_result(
        conversation_name,
        frappe.session.user,
        {"status": "completed", "done": True, "conversation": conversation_name, "reply": reply_text},
    )
    return result


def _run_enqueued_prompt(
    prompt: str | None = None,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
    user: str | None = None,
) -> None:
    _execute_prompt(
        prompt=prompt,
        conversation=conversation,
        doctype=doctype,
        docname=docname,
        route=route,
        model=model,
        images=images,
        user=user,
    )


@frappe.whitelist()
def enqueue_prompt(
    prompt: str | None = None,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    prompt_text = (prompt or "").strip()
    parsed_images = _parse_prompt_images(images)
    if not prompt_text and not parsed_images:
        raise frappe.ValidationError(_("Prompt or image is required"))

    conversation_name = conversation or create_conversation(title=_summarize_title(prompt_text or _("Image prompt")))["name"]
    progress = {"conversation": conversation_name, "user": frappe.session.user, "model": _resolve_model_for_request(model, has_images=bool(parsed_images)), "steps": []}
    _progress_update(progress, stage="queued", step="Queued request")
    _set_prompt_result(
        conversation_name,
        frappe.session.user,
        {"status": "pending", "done": False, "conversation": conversation_name},
    )

    frappe.enqueue(
        "erp_ai_assistant.api.ai._run_enqueued_prompt",
        queue="short",
        timeout=int(max(_llm_request_timeout_seconds() * 3, 300)),
        prompt=prompt,
        conversation=conversation_name,
        doctype=doctype,
        docname=docname,
        route=route,
        model=model,
        images=images,
        user=frappe.session.user,
    )
    return {
        "queued": True,
        "conversation": conversation_name,
    }


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
    return _execute_prompt(
        prompt=prompt,
        conversation=conversation,
        doctype=doctype,
        docname=docname,
        route=route,
        model=model,
        images=images,
    )


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


def _merge_attachment_packages(*packages: Any) -> dict[str, Any]:
    merged = {"attachments": [], "exports": {}}
    for package in packages:
        if not isinstance(package, dict):
            continue
        attachments = package.get("attachments") or []
        exports = package.get("exports") or {}
        if isinstance(attachments, list):
            merged["attachments"].extend(item for item in attachments if isinstance(item, dict))
        if isinstance(exports, dict):
            merged["exports"].update(exports)
    return merged


def _finalize_reply_text(text: str, prompt: str, attachments: dict[str, Any], payload: Any = None) -> str:
    attachment_rows = attachments.get("attachments") or []
    if attachment_rows and _requested_export_formats(prompt) and any(item.get("export_id") for item in attachment_rows):
        labels = ", ".join(str(item.get("label") or item.get("file_type") or "file").strip() for item in attachment_rows[:3])
        base_text = f"Prepared your export. Use the downloadable attachment below{f' ({labels})' if labels else ''}."
    else:
        base_text = text
    return _append_related_links(base_text, prompt, payload)


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


def _desk_form_link(doctype: Any, name: Any) -> str:
    doctype_text = str(doctype or "").strip()
    name_text = str(name or "").strip()
    if not doctype_text or not name_text:
        return ""
    return f"/app/Form/{quote(doctype_text, safe='')}/{quote(name_text, safe='')}"


def _desk_report_link(report_name: Any) -> str:
    report_text = str(report_name or "").strip()
    if not report_text:
        return ""
    return f"/app/query-report/{quote(report_text, safe='')}"


def _infer_prompt_doctype(prompt: str) -> str | None:
    alias = _match_known_doctype_alias(prompt)
    if not alias:
        return None
    catalog = _known_doctype_catalog()
    resolved = catalog.get(alias)
    return resolved[0] if resolved else None


def _extract_link_targets_from_payload(payload: Any, prompt: str) -> list[tuple[str, str, str]]:
    targets: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_target(label: str, url: str, kind: str) -> None:
        entry = (str(label or "").strip(), str(url or "").strip(), str(kind or "").strip())
        if not entry[0] or not entry[1] or entry in seen:
            return
        seen.add(entry)
        targets.append(entry)

    def visit(value: Any, inferred_doctype: str | None = None) -> None:
        if isinstance(value, dict):
            report_name = str(value.get("report_name") or "").strip()
            if report_name:
                add_target(report_name, _desk_report_link(report_name), "report")

            doctype = str(value.get("doctype") or inferred_doctype or "").strip()
            docname = str(value.get("name") or value.get("docname") or "").strip()
            if doctype and docname:
                if doctype == "Report":
                    add_target(f"Report {docname}", _desk_report_link(docname), "report")
                else:
                    add_target(f"{doctype} {docname}", _desk_form_link(doctype, docname), "document")

            for key in ("data", "result", "reports", "message"):
                nested = value.get(key)
                if isinstance(nested, (dict, list)):
                    visit(nested, inferred_doctype=doctype or inferred_doctype)
            return

        if isinstance(value, list):
            effective_doctype = inferred_doctype or _infer_prompt_doctype(prompt)
            for row in value[:12]:
                if isinstance(row, dict):
                    row_name = str(row.get("name") or row.get("docname") or "").strip()
                    row_doctype = str(row.get("doctype") or effective_doctype or "").strip()
                    if row_doctype and row_name:
                        if row_doctype == "Report":
                            add_target(f"Report {row_name}", _desk_report_link(row_name), "report")
                        else:
                            add_target(f"{row_doctype} {row_name}", _desk_form_link(row_doctype, row_name), "document")
                    report_name = str(row.get("report_name") or "").strip()
                    if report_name:
                        add_target(report_name, _desk_report_link(report_name), "report")
                elif effective_doctype and str(row or "").strip():
                    row_name = str(row).strip()
                    add_target(f"{effective_doctype} {row_name}", _desk_form_link(effective_doctype, row_name), "document")

    visit(payload, inferred_doctype=_infer_prompt_doctype(prompt))
    return targets[:8]


def _append_related_links(text: str, prompt: str, payload: Any) -> str:
    base = str(text or "").strip()
    link_targets = _extract_link_targets_from_payload(payload, prompt)
    if not link_targets:
        return base

    lines = [base] if base else []
    lines.extend(["", "Open links:"])
    for label, url, _kind in link_targets:
        lines.append(f"- [{label}]({url})")
    return "\n".join(lines).strip()


def _is_erp_intent(prompt: str, context: dict[str, Any]) -> bool:
    text = (prompt or "").strip().lower()
    if context.get("doctype"):
        return True
    if not text:
        return False
    erp_markers = (
        "customer",
        "supplier",
        "employee",
        "item",
        "invoice",
        "order",
        "quotation",
        "lead",
        "opportunity",
        "report",
        "dashboard",
        "chart",
        "doctype",
        "workflow",
        "erp",
        "sales",
        "purchase",
        "stock",
        "accounts",
        "hr",
    )
    return any(marker in text for marker in erp_markers)


def _fallback_model_candidates(selected_model: str | None = None) -> list[str]:
    candidates: list[str] = []
    if selected_model:
        normalized = str(selected_model).strip()
        if normalized:
            candidates.append(normalized)

    provider = _provider_name()
    if provider in {"openai", "openai_compatible"}:
        raw = _cfg("OPENAI_MODELS", "")
    elif provider == "anthropic":
        raw = _cfg("ANTHROPIC_MODELS", "")
    else:
        raw = ""

    if isinstance(raw, str) and raw.strip():
        parsed = [row.strip() for row in raw.split(",") if row.strip()]
        candidates.extend(parsed)

    deduped: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _is_transient_provider_error(message: str) -> bool:
    text = str(message or "").strip().lower()
    if not text:
        return False
    markers = (
        "timeout",
        "timed out",
        "connection reset",
        "temporary",
        "rate limit",
        "too many requests",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
    )
    return any(marker in text for marker in markers)


def _provider_chat_with_resilience(
    prompt: str,
    context: dict[str, Any],
    *,
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    errors: list[str] = []
    model_candidates = _fallback_model_candidates(model)
    if not model_candidates:
        model_candidates = [model] if model else [None]

    max_attempts = max(1, min(3, len(model_candidates)))
    for index in range(max_attempts):
        attempt_model = model_candidates[index]
        try:
            return _provider_chat(
                prompt,
                context,
                history=history,
                model=attempt_model,
                progress=progress,
                images=images,
            )
        except Exception as exc:
            message = str(exc) or "Unknown provider error"
            errors.append(message)
            if index + 1 < max_attempts and _is_transient_provider_error(message):
                continue
            if index + 1 < max_attempts and "model" in message.lower():
                continue
            break

    raise RuntimeError(" | ".join(errors[-2:]) if errors else "Unknown provider error")


def _generate_response(
    prompt: str,
    context: dict[str, Any],
    conversation: str | None = None,
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    requested_formats = _requested_export_formats(prompt)

    if requested_formats and conversation:
        follow_up_payload = _rerun_last_exportable_tool(conversation, progress=progress)
        if follow_up_payload is not None:
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": "Prepared export from the previous result in this conversation.",
                "tool_events": [],
                "payload": follow_up_payload,
            }

    if _llm_chat_configured():
        try:
            response = _provider_chat_with_resilience(
                prompt,
                context,
                history=history,
                model=model,
                progress=progress,
                images=images,
            )
            if images and _response_rejects_images(response.get("text")):
                retry_prompt = (
                    f"{prompt}\n\n"
                    "An image is already attached in this same user message as multimodal input. "
                    "Analyze the visual content directly and answer only from what is visible in the image."
                ).strip()
                response = _provider_chat_with_resilience(
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

    guidance = (
        "AI provider is not configured for FAC-native chat yet. "
        "Configure the provider so the assistant can call FAC tools dynamically."
    )
    if _has_active_document_context(context):
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
        fields=["role", "content", "attachments_json"],
        order_by="creation desc",
        limit_page_length=max(1, limit),
    )
    history: list[dict[str, Any]] = []
    for row in reversed(messages):
        role = (row.get("role") or "").strip().lower()
        content = (row.get("content") or "").strip()
        attachments = _parse_message_attachments(row.get("attachments_json"))
        attachment_notes = _describe_message_attachments(attachments)
        if not content and not attachments:
            continue
        history_text = _merge_history_content_and_attachment_notes(content, attachment_notes)
        if role == "user":
            history.append(
                {
                    "role": "user",
                    "content": history_text,
                }
            )
        elif role == "assistant":
            history.append({"role": "assistant", "content": history_text})
    return history


def _rerun_last_exportable_tool(
    conversation_name: str,
    *,
    progress: Optional[dict[str, Any]] = None,
) -> Any:
    rows = frappe.get_all(
        "AI Message",
        filters={"conversation": conversation_name, "role": "assistant"},
        fields=["tool_events"],
        order_by="creation desc",
        limit_page_length=6,
    )
    for row in rows:
        event = _last_exportable_tool_event(row.get("tool_events"))
        if not event:
            continue
        tool_name, arguments = event
        _progress_update(progress, stage="working", step=f"Reusing previous result: {tool_name}")
        return _run_tool(tool_name, arguments)
    return None


def _last_exportable_tool_event(raw: Any) -> tuple[str, dict[str, Any]] | None:
    eligible_tools = {
        "get_list",
        "get_document",
        "get_report",
        "list_documents",
        "generate_report",
    }
    if not raw:
        return None
    try:
        events = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None
    if not isinstance(events, list):
        return None
    for item in reversed(events):
        text = str(item or "").strip()
        if not text or text.endswith("(error)") or " (error)" in text:
            continue
        tool_name, arguments = _parse_tool_event(text)
        if tool_name in eligible_tools:
            return tool_name, arguments
    return None


def _parse_tool_event(text: str) -> tuple[str, dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return "", {}
    if " " not in raw:
        return raw, {}
    tool_name, raw_args = raw.split(" ", 1)
    try:
        parsed = ast.literal_eval(raw_args.strip())
        if isinstance(parsed, dict):
            return tool_name.strip(), parsed
    except Exception:
        pass
    return tool_name.strip(), {}


def _plan_prompt(prompt: str, context: dict[str, Any]) -> Optional[dict[str, Any]]:
    steps = _plan_prompt_steps(prompt, context)
    return steps[0] if steps else None


def _plan_prompt_steps(prompt: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    raw_text = _normalize_planner_prompt(prompt)
    text = raw_text.lower()
    export_terms = ("create", "export", "download", "save", "generate")
    export_format_terms = ("excel", "xlsx", "spreadsheet", "csv", "pdf", "word", "docx", "file", "document")
    is_export_request = any(term in text for term in export_terms) and any(term in text for term in export_format_terms)
    list_doctypes = _known_doctype_catalog()

    if context.get("doctype") and context.get("docname"):
        if re.match(r"^(summarize|show|explain)( this| current)?( record| document)?$", text):
            return [{
                "tool": "get_document",
                "arguments": {"doctype": context["doctype"], "name": context["docname"]},
                "heading": f"{context['doctype']} {context['docname']}",
            }]

        if is_export_request and re.match(
            r"^(create|export|download|save|generate)( me)?( this| current)?( record| document)?( as| to| in)?( excel| xlsx| spreadsheet| csv| pdf| word| docx| file| document)?$",
            text,
            re.IGNORECASE,
        ):
            return [{
                "tool": "get_document",
                "arguments": {"doctype": context["doctype"], "name": context["docname"]},
                "heading": f"{context['doctype']} {context['docname']}",
            }]

        if text.startswith("update this ") and " set " in text:
            updates = _parse_field_assignments(raw_text.split(" set ", 1)[1])
            if updates:
                return [{
                    "tool": "update_document",
                    "arguments": {
                        "doctype": context["doctype"],
                        "name": context["docname"],
                        "document": updates,
                    },
                "heading": f"Updated {context['doctype']} {context['docname']}",
                }]
        context_workflow_plan = _plan_context_workflow_action(raw_text, context)
        if context_workflow_plan:
            return context_workflow_plan

    export_list_match = re.match(
        r"^(create|export|download|save|generate)( me)?( the)?( list of)? (?P<label>employees?|customers?|items?|sales orders?|sales invoices?|purchase orders?|purchase invoices?|suppliers?|quotations?|leads?|opportunities?)( .+)?$",
        text,
        re.IGNORECASE,
    )
    export_list_alt_match = re.match(
        r"^(create|export|download|save|generate)( me)?( an?|the)?( excel|xlsx|spreadsheet|csv|pdf|word|docx)?( file| document)?( of| for)? (?P<label>employees?|customers?|items?|sales orders?|sales invoices?|purchase orders?|purchase invoices?|suppliers?|quotations?|leads?|opportunities?)$",
        text,
        re.IGNORECASE,
    )
    export_list_target = export_list_match or export_list_alt_match
    if is_export_request:
        doctype_key = None
        if export_list_target:
            doctype_key = str(export_list_target.group("label") or "").strip().lower().rstrip("s")
        if not doctype_key:
            doctype_key = _match_known_doctype_alias(text, plural_only=True)
        if doctype_key in list_doctypes:
            doctype, fields = list_doctypes[doctype_key]
            filters = _doctype_year_filters(doctype, raw_text)
            return [{
                "tool": "get_list",
                "arguments": {
                    "doctype": doctype,
                    "fields": fields,
                    "filters": filters,
                    "limit_page_length": 200,
                    "order_by": "modified desc",
                },
                "heading": f"{doctype} list",
            }]

    generic_report_list_match = re.match(
        r"^(show|list|get|find|fetch|display|retrieve|pull)( me)?( the)? (?P<module>accounts|selling|stock|hr|crm|buying)? ?reports?$",
        text,
        re.IGNORECASE,
    )
    if generic_report_list_match:
        module = _normalize_report_module(generic_report_list_match.group("module"))
        return [{
            "tool": "report_list",
            "arguments": {"module": module} if module else {},
            "heading": "Available reports",
        }]

    report_requirements_match = re.match(
        r"^(show|get|list|find|fetch|what are)( me)?( the)?( filters| requirements| filter requirements| report requirements)( for)? (?P<name>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if report_requirements_match:
        report_name = _normalize_name(report_requirements_match.group("name"))
        if report_name:
            return [
                {
                    "tool": "report_list",
                    "arguments": {},
                    "heading": "Available reports",
                },
                {
                    "tool": "report_requirements",
                    "arguments": {"report_name": report_name},
                    "heading": report_name,
                },
            ]

    report_match = re.match(
        r"^(create|export|download|save)( me)?( the)?( report)? (?P<name>.+?)( (as|to|in) (excel|xlsx|spreadsheet|csv|pdf|word|docx|file|document))$",
        raw_text,
        re.IGNORECASE,
    )
    if report_match:
        report_name = _normalize_name(report_match.group("name"))
        report_name = re.sub(r"^(report)\s+", "", report_name, flags=re.IGNORECASE).strip()
        if report_name:
            return [
                {
                    "tool": "report_list",
                    "arguments": {},
                    "heading": "Available reports",
                },
                {
                    "tool": "report_requirements",
                    "arguments": {"report_name": report_name},
                    "heading": report_name,
                },
                {
                    "tool": "get_report",
                    "arguments": {"report_name": report_name, "filters": _report_prompt_filters(raw_text)},
                    "heading": report_name,
                },
            ]

    report_run_match = re.match(
        r"^(show|run|get|generate|open|display)( me)?( the)? (?P<name>.+?) report( for .+)?$",
        raw_text,
        re.IGNORECASE,
    )
    if report_run_match:
        report_name = _normalize_name(report_run_match.group("name"))
        if report_name:
            return [
                {
                    "tool": "report_list",
                    "arguments": {},
                    "heading": "Available reports",
                },
                {
                    "tool": "report_requirements",
                    "arguments": {"report_name": report_name},
                    "heading": report_name,
                },
                {
                    "tool": "get_report",
                    "arguments": {"report_name": report_name, "filters": _report_prompt_filters(raw_text)},
                    "heading": report_name,
                },
            ]

    generic_list_match = re.match(
        r"^(show|list|get|find|fetch|display|retrieve|pull)( me)?( the)?( list of)? (?P<label>customers?|employees?|items?|sales orders?|sales invoices?|purchase orders?|purchase invoices?|suppliers?|quotations?|leads?|opportunities?)$",
        text,
        re.IGNORECASE,
    )
    if generic_list_match:
        doctype_key = str(generic_list_match.group("label") or "").strip().lower().rstrip("s")
        if doctype_key in list_doctypes:
            doctype, fields = list_doctypes[doctype_key]
            return [{
                "tool": "get_list",
                "arguments": {
                    "doctype": doctype,
                    "fields": fields,
                    "limit_page_length": 20,
                    "order_by": "modified desc",
                },
                "heading": f"{doctype} list",
            }]

    named_doc_patterns = {
        key: value[0] for key, value in list_doctypes.items()
    }
    create_resolution_plan = _plan_named_create(raw_text, named_doc_patterns)
    if create_resolution_plan:
        return create_resolution_plan

    update_resolution_plan = _plan_named_update(raw_text, named_doc_patterns)
    if update_resolution_plan:
        return update_resolution_plan

    delete_resolution_plan = _plan_named_delete(raw_text, named_doc_patterns)
    if delete_resolution_plan:
        return delete_resolution_plan

    named_workflow_plan = _plan_named_workflow_action(raw_text, named_doc_patterns)
    if named_workflow_plan:
        return named_workflow_plan

    named_doc_match = re.match(
        r"^(show|get|find|fetch|display|retrieve|pull)( me)? (?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity) (?P<name>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if named_doc_match:
        doctype_key = str(named_doc_match.group("label") or "").strip().lower()
        doctype = named_doc_patterns.get(doctype_key)
        if doctype:
            return [{
                "tool": "get_document",
                "arguments": {"doctype": doctype, "name": _normalize_name(named_doc_match.group("name"))},
                "heading": f"{doctype} {_normalize_name(named_doc_match.group('name'))}",
            }]

    export_named_doc_match = re.match(
        r"^(create|export|download|save|generate)( me)?( the)? (?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity) (?P<name>.+?)( (as|to|in) (excel|xlsx|spreadsheet|csv|pdf|word|docx|file|document))$",
        raw_text,
        re.IGNORECASE,
    )
    export_named_doc_alt_match = re.match(
        r"^(create|export|download|save|generate)( me)?( an?|the)?( excel|xlsx|spreadsheet|csv|pdf|word|docx)?( file| document)?( of| for)? (?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity) (?P<name>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    export_named_doc_target = export_named_doc_match or export_named_doc_alt_match
    if export_named_doc_target:
        doctype_key = str(export_named_doc_target.group("label") or "").strip().lower()
        doctype = named_doc_patterns.get(doctype_key)
        if doctype:
            return [{
                "tool": "get_document",
                "arguments": {"doctype": doctype, "name": _normalize_name(export_named_doc_target.group("name"))},
                "heading": f"{doctype} {_normalize_name(export_named_doc_target.group('name'))}",
            }]

    if is_export_request:
        for alias, doctype_key in _known_doctype_aliases().items():
            if alias == doctype_key:
                pattern = rf"\b{re.escape(alias)}\b\s+(?P<name>.+)$"
                match = re.search(pattern, raw_text, re.IGNORECASE)
                if match and doctype_key in named_doc_patterns:
                    doctype = named_doc_patterns[doctype_key]
                    return [{
                        "tool": "get_document",
                        "arguments": {"doctype": doctype, "name": _normalize_name(match.group('name'))},
                        "heading": f"{doctype} {_normalize_name(match.group('name'))}",
                    }]

    return []


def _plan_named_update(raw_text: str, named_doc_patterns: dict[str, str]) -> list[dict[str, Any]]:
    match = re.match(
        r"^(update|change|modify|set)\s+(?:(?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity)\s+)?(?P<target>.+?)\s+(?P<field>[a-zA-Z][\w\s]*?)\s+(?:to|as)\s+(?P<value>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if not match:
        return []

    label = str(match.group("label") or "").strip().lower()
    field_label = _normalize_name(match.group("field"))
    target = _normalize_name(match.group("target"))
    if not label:
        split = _split_named_update_target_and_field(_normalize_name(f"{target} {field_label}"))
        if split:
            target, field_label = split
    value = _coerce_value(match.group("value"))
    doctype_key = label or _infer_update_doctype(field_label)
    doctype = named_doc_patterns.get(doctype_key or "")
    display_field = _doctype_display_field(doctype)
    if not doctype or not display_field or not target:
        return []

    updates = _parse_field_assignments(f"{field_label}: {match.group('value')}")
    if not updates:
        updates = {_normalize_field_key(field_label): value}

    preferred_field = next(iter(updates.keys()), field_label)
    preferred_value = updates.get(preferred_field, value)

    return [
        {
            "tool": "update_erp_document",
            "arguments": {
                "doctype": doctype,
                "record": target,
                "field": preferred_field,
                "value": preferred_value,
            },
            "heading": f"Updated {doctype} {target}",
        }
    ]


def _plan_named_create(raw_text: str, named_doc_patterns: dict[str, str]) -> list[dict[str, Any]]:
    lowered = str(raw_text or "").strip().lower()
    if lowered.startswith("how to ") or lowered.startswith("how do i ") or lowered.startswith("how can i "):
        return []
    match = re.match(
        r"^(create|new|add)\s+(?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity)(?:\s+(?:with|set))?\s+(?P<data>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if not match:
        return []
    doctype = named_doc_patterns.get(str(match.group("label") or "").strip().lower())
    payload = _parse_create_fields(match.group("data"))
    if not doctype or not payload:
        return []
    return [
        {
            "tool": "create_document",
            "arguments": {
                "doctype": doctype,
                "document": payload,
            },
            "heading": f"Created {doctype}",
        }
    ]


def _plan_named_delete(raw_text: str, named_doc_patterns: dict[str, str]) -> list[dict[str, Any]]:
    match = re.match(
        r"^(delete|remove|erase|trash|purge)\s+(?:(?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity)\s+)?(?P<target>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if not match:
        return []

    label = str(match.group("label") or "").strip().lower()
    target = _normalize_name(match.group("target"))
    doctype = named_doc_patterns.get(label or "")
    display_field = _doctype_display_field(doctype)
    if not doctype or not target:
        return []

    if display_field == "name":
        return [
            {
                "tool": "delete_document",
                "arguments": {"doctype": doctype, "name": target},
                "heading": f"Deleted {doctype} {target}",
            }
        ]

    return [
        {
            "tool": "get_list",
            "arguments": {
                "doctype": doctype,
                "fields": ["name", display_field],
                "filters": [[display_field, "=", target]],
                "limit_page_length": 1,
                "order_by": "modified desc",
            },
            "heading": f"Lookup {doctype}",
        },
        {
            "tool": "delete_document",
            "arguments": {"doctype": doctype, "name_from_previous_result": True},
            "heading": f"Deleted {doctype} {target}",
        },
    ]


def _plan_named_workflow_action(raw_text: str, named_doc_patterns: dict[str, str]) -> list[dict[str, Any]]:
    match = re.match(
        r"^(?P<action>submit|approve|reject|cancel|reopen)\s+(?:(?P<label>customer|employee|item|sales order|sales invoice|purchase order|purchase invoice|supplier|quotation|lead|opportunity)\s+)?(?P<target>.+)$",
        raw_text,
        re.IGNORECASE,
    )
    if not match:
        return []
    action = str(match.group("action") or "").strip().lower()
    doctype = named_doc_patterns.get(str(match.group("label") or "").strip().lower())
    target = _normalize_name(match.group("target"))
    if not doctype or not target:
        return []
    display_field = _doctype_display_field(doctype)
    if action == "submit":
        tool_name = "submit_document"
    else:
        tool_name = "run_workflow"
    if display_field == "name":
        arguments = {"doctype": doctype, "name": target}
        if tool_name == "run_workflow":
            arguments["action"] = action.title()
        return [{"tool": tool_name, "arguments": arguments, "heading": f"{action.title()} {doctype} {target}"}]
    arguments = {
        "doctype": doctype,
        "fields": ["name", display_field],
        "filters": [[display_field, "=", target]],
        "limit_page_length": 1,
        "order_by": "modified desc",
    }
    final_arguments = {"doctype": doctype, "name_from_previous_result": True}
    if tool_name == "run_workflow":
        final_arguments["action"] = action.title()
    return [
        {"tool": "get_list", "arguments": arguments, "heading": f"Lookup {doctype}"},
        {"tool": tool_name, "arguments": final_arguments, "heading": f"{action.title()} {doctype} {target}"},
    ]


def _plan_context_workflow_action(raw_text: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    match = re.match(r"^(submit|approve|reject|cancel|reopen)( this| current)?( record| document)?$", raw_text, re.IGNORECASE)
    if not match or not context.get("doctype") or not context.get("docname"):
        return []
    action = str(match.group(1) or "").strip().lower()
    if action == "submit":
        return [{
            "tool": "submit_document",
            "arguments": {"doctype": context["doctype"], "name": context["docname"]},
            "heading": f"Submitted {context['doctype']} {context['docname']}",
        }]
    return [{
        "tool": "run_workflow",
        "arguments": {"doctype": context["doctype"], "name": context["docname"], "action": action.title()},
        "heading": f"{action.title()} {context['doctype']} {context['docname']}",
    }]


def _normalize_planner_prompt(prompt: str) -> str:
    text = _normalize_name(prompt)
    if not text:
        return text
    filler_patterns = [
        r"^(can you|could you|would you|will you)\s+",
        r"^(please|kindly)\s+",
        r"^(can you help me\??|help me)\s*",
        r"^(i want you to|i want to)\s+",
    ]
    normalized = text
    changed = True
    while changed:
        changed = False
        for pattern in filler_patterns:
            updated = re.sub(pattern, "", normalized, flags=re.IGNORECASE).strip()
            if updated != normalized:
                normalized = updated
                changed = True
    normalized = re.sub(r"^[,:;\-]\s*", "", normalized).strip()
    return normalized or text


def _parse_create_fields(text: str) -> dict[str, Any]:
    parsed = _parse_field_assignments(text)
    if parsed:
        return parsed
    chunks = [item.strip() for item in re.split(r",|\band\b", str(text or ""), flags=re.IGNORECASE) if item.strip()]
    values: dict[str, Any] = {}
    for chunk in chunks:
        token_match = re.match(r"^(?P<key>[a-zA-Z][\w\s]+?)\s+(?P<value>.+)$", chunk)
        if not token_match:
            continue
        key = _normalize_field_key(token_match.group("key"))
        values[key] = _coerce_value(token_match.group("value"))
    return values


def _report_prompt_filters(prompt: str) -> dict[str, Any]:
    text = str(prompt or "")
    year_match = re.search(r"\b(20\d{2})\b", text)
    if not year_match:
        return {}
    year = int(year_match.group(1))
    return {
        "from_date": f"{year}-01-01",
        "to_date": f"{year}-12-31",
    }


def _infer_update_doctype(field_label: str) -> str:
    normalized = _normalize_field_key(field_label)
    if normalized in {
        "salary",
        "basic_salary",
        "designation",
        "department",
        "date_of_birth",
        "birthday",
        "birth_date",
        "dob",
        "company_email",
        "email_id",
        "mobile_no",
    }:
        return "employee"
    return ""


def _split_named_update_target_and_field(text: str) -> tuple[str, str] | None:
    phrase = _normalize_name(text)
    lowered = phrase.lower()
    alias_map = {
        "date of birth": "date_of_birth",
        "birth date": "date_of_birth",
        "birthday": "date_of_birth",
        "dob": "date_of_birth",
        "designation": "designation",
        "department": "department",
        "salary": "salary",
        "basic salary": "basic_salary",
        "company email": "company_email",
        "email": "email_id",
        "mobile": "mobile_no",
        "phone": "phone",
        "status": "status",
        "territory": "territory",
    }
    for alias in sorted(alias_map.keys(), key=len, reverse=True):
        if lowered == alias:
            return "", alias_map[alias]
        if lowered.endswith(f" {alias}"):
            target = phrase[: len(phrase) - len(alias)].strip(" ,-")
            if target:
                return target, alias_map[alias]
    return None


def _doctype_display_field(doctype: str | None) -> str:
    mapping = {
        "Employee": "employee_name",
        "Customer": "customer_name",
        "Item": "item_name",
        "Supplier": "supplier_name",
        "Lead": "lead_name",
    }
    return mapping.get(str(doctype or "").strip(), "name")


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


def _known_doctype_aliases() -> dict[str, str]:
    return {
        "employee": "employee",
        "employees": "employee",
        "customer": "customer",
        "customers": "customer",
        "item": "item",
        "items": "item",
        "sales order": "sales order",
        "sales orders": "sales order",
        "sales invoice": "sales invoice",
        "sales invoices": "sales invoice",
        "purchase order": "purchase order",
        "purchase orders": "purchase order",
        "purchase invoice": "purchase invoice",
        "purchase invoices": "purchase invoice",
        "supplier": "supplier",
        "suppliers": "supplier",
        "quotation": "quotation",
        "quotations": "quotation",
        "lead": "lead",
        "leads": "lead",
        "opportunity": "opportunity",
        "opportunities": "opportunity",
    }


def _match_known_doctype_alias(text: str, *, plural_only: bool = False) -> str | None:
    normalized = str(text or "").strip().lower()
    aliases = _known_doctype_aliases()
    for alias in sorted(aliases.keys(), key=len, reverse=True):
        if plural_only and alias == aliases[alias]:
            continue
        if re.search(rf"\b{re.escape(alias)}\b", normalized):
            return aliases[alias]
    return None


def _doctype_year_filters(doctype: str, prompt: str) -> list[list[Any]]:
    year_match = re.search(r"\b(20\d{2})\b", str(prompt or ""))
    if not year_match:
        return []
    date_field_map = {
        "Sales Order": "transaction_date",
        "Sales Invoice": "posting_date",
        "Purchase Order": "transaction_date",
        "Purchase Invoice": "posting_date",
        "Quotation": "transaction_date",
    }
    date_field = date_field_map.get(str(doctype or "").strip())
    if not date_field:
        return []
    year = int(year_match.group(1))
    return [
        [date_field, ">=", f"{year}-01-01"],
        [date_field, "<=", f"{year}-12-31"],
    ]


def _execute_plan_steps(
    steps: list[dict[str, Any]],
    *,
    progress: Optional[dict[str, Any]] = None,
) -> tuple[Any, list[str]]:
    tool_events: list[str] = []
    latest_result: Any = None
    for step in steps:
        tool_name = str(step.get("tool") or "").strip()
        arguments = dict(step.get("arguments") or {})
        heading = str(step.get("heading") or tool_name or "tool").strip()
        if not tool_name:
            continue
        if arguments.pop("name_from_previous_result", False):
            resolved_name = _extract_name_from_tool_result(latest_result)
            if not resolved_name:
                raise RuntimeError("Could not resolve the target document from the previous lookup result.")
            arguments["name"] = resolved_name
        _progress_update(progress, stage="working", step=f"Running tool: {tool_name}")
        latest_result = _run_tool(tool_name, arguments)
        tool_events.append(f"{tool_name} {arguments}")
        if tool_name == "report_list":
            report_name = str((steps[-1].get("arguments") or {}).get("report_name") or "").strip()
            if report_name and not _report_exists_in_payload(latest_result, report_name):
                raise RuntimeError(f"Report not found: {report_name}")
        if tool_name == "report_requirements" and steps[-1].get("tool") == "get_report":
            final_filters = _default_report_filters(latest_result)
            if final_filters:
                steps[-1].setdefault("arguments", {})
                steps[-1]["arguments"]["filters"] = final_filters
        _progress_update(progress, stage="working", step=f"Prepared {heading}")
    return latest_result, tool_events


def _extract_name_from_tool_result(payload: Any) -> str:
    rows = _unwrap_tool_payload(payload)
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, dict):
            return str(first.get("name") or "").strip()
    if isinstance(rows, dict):
        return str(rows.get("name") or "").strip()
    return ""


def _unwrap_tool_payload(payload: Any) -> Any:
    current = payload
    for _ in range(6):
        if not isinstance(current, dict):
            return current
        if isinstance(current.get("data"), list):
            return current.get("data")
        if isinstance(current.get("result"), list):
            return current.get("result")
        if isinstance(current.get("data"), dict):
            current = current.get("data")
            continue
        if isinstance(current.get("result"), dict):
            current = current.get("result")
            continue
        return current
    return current


def _report_exists_in_payload(payload: Any, report_name: str) -> bool:
    if not report_name:
        return True
    rows = []
    if isinstance(payload, dict):
        rows = payload.get("reports") or payload.get("data") or payload.get("result") or []
    elif isinstance(payload, list):
        rows = payload
    normalized = _normalize_name(report_name).lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        current = _normalize_name(row.get("report_name") or row.get("name") or "").lower()
        if current == normalized:
            return True
    return False


def _default_report_filters(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    requirements = payload.get("requirements") or payload.get("filters") or payload.get("data") or []
    filters: dict[str, Any] = {}
    if isinstance(requirements, dict):
        requirements = requirements.get("filters") or []
    if not isinstance(requirements, list):
        return filters
    today = frappe.utils.nowdate()
    month_start = frappe.utils.get_first_day(today)
    for row in requirements:
        if not isinstance(row, dict):
            continue
        key = str(row.get("fieldname") or row.get("field") or row.get("name") or "").strip()
        label = str(row.get("label") or key).strip().lower()
        if not key:
            continue
        if "company" in label:
            default_company = frappe.defaults.get_user_default("Company")
            if default_company:
                filters[key] = default_company
        elif any(marker in label for marker in ("from date", "start date", "date from")):
            filters[key] = month_start
        elif any(marker in label for marker in ("to date", "end date", "date to")):
            filters[key] = today
    return filters


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


def _parse_json_object_text(raw_text: Any) -> dict[str, Any]:
    if isinstance(raw_text, dict):
        return raw_text
    text = str(raw_text or "").strip()
    if not text:
        raise RuntimeError("Planner returned empty output.")
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError(f"Planner returned invalid JSON: {text[:240]}")


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
    compat_profile = _provider_compatibility_profile("openai", base_url, responses_path, model=model)

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    system = _erp_tool_system_prompt(prompt, context)

    tool_specs = _prioritize_tool_specs(_openai_tool_specs(prompt, context))
    previous_response_id: str | None = None
    pending_input: Any = _build_openai_input(history, prompt, images)
    tool_events: list[str] = []
    rendered_payload = None
    had_tool_calls = False
    verification_requested = False
    tools_enabled = bool(tool_specs)
    plain_text_retry_requested = False
    seen_tool_signatures: set[str] = set()
    last_tool_name: str | None = None

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload: dict[str, Any] = {
            "model": model,
            "instructions": system,
            "input": pending_input,
        }
        if tools_enabled:
            request_payload["tools"] = tool_specs
        if not compat_profile.get("disable_sampling_by_default") and _openai_supports_sampling_controls(model):
            request_payload["temperature"] = temperature
            request_payload["top_p"] = top_p
        if previous_response_id:
            request_payload["previous_response_id"] = previous_response_id
        tool_choice = _tool_choice_payload(force_tool_use, prompt, context, mode="openai")
        if tools_enabled and tool_choice is not None:
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

        if function_calls and not tools_enabled:
            fallback_text = _openai_output_text(body)
            if fallback_text:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": fallback_text,
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider returned stray tool calls; retrying text-only")
                pending_input = "Answer the user's last message directly in plain text. Do not call tools."
                continue
            raise RuntimeError("Provider returned tool calls even though no tools were offered.")

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

        current_signatures = [
            _tool_call_signature(str(tool_call.get("name") or "").strip(), _parse_openai_tool_arguments(tool_call.get("arguments")))
            for tool_call in function_calls
        ]
        if current_signatures and all(signature in seen_tool_signatures for signature in current_signatures):
            if rendered_payload is not None:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": _render_provider_tool_output(last_tool_name, rendered_payload),
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider repeated the same tool calls; forcing final answer")
                pending_input = "You already have the tool results. Answer the user directly in plain text without any more tool calls."
                tools_enabled = False
                continue

        had_tool_calls = True
        pending_results = []
        for tool_call in function_calls:
            tool_name = str(tool_call.get("name") or "").strip()
            tool_input = _parse_openai_tool_arguments(tool_call.get("arguments"))
            seen_tool_signatures.add(_tool_call_signature(tool_name, tool_input))
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name)}")
            try:
                tool_result = _run_tool(tool_name, tool_input)
                _validate_tool_result(tool_name, tool_result)
                last_tool_name = tool_name
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
    compat_profile = _provider_compatibility_profile("openai_compatible", base_url, path, model=model)

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    system = _erp_tool_system_prompt(prompt, context)

    messages = _build_openai_compatible_messages(history, prompt, images)
    tool_specs = _prioritize_tool_specs(_openai_compatible_tool_specs(prompt, context))
    tool_events: list[str] = []
    rendered_payload = None
    had_tool_calls = False
    verification_requested = False
    disable_tool_choice = bool(compat_profile.get("disable_tool_choice_by_default"))
    tools_enabled = bool(tool_specs)
    plain_text_retry_requested = False
    seen_tool_signatures: set[str] = set()
    last_tool_name: str | None = None
    disable_sampling_controls = bool(compat_profile.get("disable_sampling_by_default"))

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
        if not disable_sampling_controls and _openai_compatible_supports_sampling_controls(model):
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
            if _is_sampling_parameter_error(error_detail):
                disable_sampling_controls = True
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected sampling controls; retrying without temperature/top_p",
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
        if not tool_calls and text_body and compat_profile.get("allow_textual_tool_fallback"):
            textual_tool_call = _extract_textual_tool_call(text_body)
            if textual_tool_call is not None:
                tool_calls = [textual_tool_call]
                text_body = ""

        if tool_calls and not tools_enabled:
            if text_body:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": text_body,
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider returned stray tool calls; retrying text-only")
                messages.append({"role": "user", "content": "Answer directly in plain text. Do not call tools."})
                continue
            raise RuntimeError("Provider returned tool calls even though no tools were offered.")

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

        current_signatures = []
        for tool_call in tool_calls:
            function_payload = tool_call.get("function") or {}
            current_signatures.append(
                _tool_call_signature(
                    str(function_payload.get("name") or "").strip(),
                    _parse_openai_tool_arguments(function_payload.get("arguments")),
                )
            )
        if current_signatures and all(signature in seen_tool_signatures for signature in current_signatures):
            if rendered_payload is not None:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": _render_provider_tool_output(last_tool_name, rendered_payload),
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider repeated the same tool calls; forcing final answer")
                tools_enabled = False
                messages.append({"role": "user", "content": "You already have the tool results. Answer directly in plain text without any more tool calls."})
                continue

        had_tool_calls = True
        messages.append({"role": "assistant", "content": text_body or "", "tool_calls": tool_calls})
        for tool_call in tool_calls:
            function_payload = tool_call.get("function") or {}
            tool_name = str(function_payload.get("name") or "").strip()
            tool_input = _parse_openai_tool_arguments(function_payload.get("arguments"))
            seen_tool_signatures.add(_tool_call_signature(tool_name, tool_input))
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name)}")
            try:
                tool_result = _run_tool(tool_name, tool_input)
                _validate_tool_result(tool_name, tool_result)
                last_tool_name = tool_name
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
    compat_profile = _provider_compatibility_profile("anthropic", base_url, messages_path, model=model)
    tool_choice_mode = _tool_choice_mode(base_url, messages_path)

    beta_param = _normalize_beta_param(_cfg("ANTHROPIC_BETA"))

    endpoint = f"{base_url}{messages_path}"

    headers = {"content-type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    elif auth_token:
        headers["authorization"] = f"Bearer {auth_token}"
    anthropic_version = _cfg("ANTHROPIC_VERSION", "2023-06-01")
    if anthropic_version and compat_profile.get("profile") == "anthropic" and tool_choice_mode == "anthropic":
        headers["anthropic-version"] = anthropic_version
    if beta_param and compat_profile.get("profile") == "anthropic" and tool_choice_mode == "anthropic":
        headers["anthropic-beta"] = beta_param
    tool_definitions = get_tool_definitions()
    allowed_names = _selected_tool_names_for_prompt(prompt, context, tool_definitions)
    tool_specs = [
        {
            "name": name,
            "description": spec["description"],
            "input_schema": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
        if name in allowed_names
    ]
    tool_specs = _prioritize_tool_specs(tool_specs)
    if images:
        frappe.logger("erp_ai_assistant").info(
            "vision_request model=%s base_url=%s image_count=%s",
            model,
            base_url,
            len(images),
        )

    system = _erp_tool_system_prompt(prompt, context)
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
        content_type = (response.headers.get("content-type") or "").lower()
        if stream_enabled and "text/event-stream" in content_type:
            body = _parse_sse_stream(response, endpoint, progress=progress)
        else:
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
                _validate_tool_result(tool_name, tool_result)
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
        _progress_update(progress, stage="working", partial_text="")

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
    if (
        "tool_choice" in text
        and "no endpoints found that support the provided 'tool_choice' value" in text
    ):
        return True
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


def _is_sampling_parameter_error(detail: str) -> bool:
    text = str(detail or "").lower()
    if not text:
        return False
    unsupported_markers = (
        "temperature",
        "top_p",
        "unsupported parameter",
        "extra inputs are not permitted",
        "unknown field",
        "unknown parameter",
    )
    return any(marker in text for marker in unsupported_markers) and (
        "temperature" in text or "top_p" in text
    )


def _openai_tool_specs(prompt: str, context: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions()
    allowed_names = _selected_tool_names_for_prompt(prompt, context or {}, tool_definitions)
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


def _openai_compatible_tool_specs(prompt: str, context: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions()
    allowed_names = _selected_tool_names_for_prompt(prompt, context or {}, tool_definitions)
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
        content = _build_openai_input_content(str(row.get("content") or "").strip(), row.get("images"))
        if not content:
            continue
        normalized.append({"role": role, "content": content})
        if role == "user":
            last_user_index = len(normalized) - 1

    current_content = _build_openai_input_content(_llm_user_prompt(prompt), images)
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
        content = _build_openai_compatible_content(str(row.get("content") or "").strip(), row.get("images"))
        if content in ("", []):
            continue
        normalized.append({"role": role, "content": content})

    current_content = _build_openai_compatible_content(_llm_user_prompt(prompt), images)
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


def _extract_textual_tool_call(raw_text: Any) -> dict[str, Any] | None:
    text = str(raw_text or "").strip()
    if not text:
        return None

    unescaped_text = html.unescape(text)

    xml_tool_match = re.search(
        r"<tool_call>\s*<function=(?P<name>[\w\.\-]+)>\s*(?P<body>[\s\S]*?)\s*</function>\s*</tool_call>",
        unescaped_text,
        re.IGNORECASE,
    )
    if xml_tool_match:
        raw_name = str(xml_tool_match.group("name") or "").strip()
        tool_name = raw_name.split(".")[-1] if raw_name else ""
        normalized_name = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name, tool_name))
        arguments: dict[str, Any] = {}
        body = str(xml_tool_match.group("body") or "")
        for param_match in re.finditer(
            r"<parameter=(?P<key>[\w\.\-]+)>\s*(?P<value>[\s\S]*?)\s*</parameter>",
            body,
            re.IGNORECASE,
        ):
            key = str(param_match.group("key") or "").strip()
            raw_value = str(param_match.group("value") or "").strip()
            if not key:
                continue
            parsed_value: Any = raw_value
            if raw_value.startswith("{") or raw_value.startswith("["):
                try:
                    parsed_value = json.loads(raw_value)
                except Exception:
                    parsed_value = raw_value
            arguments[key] = parsed_value
        if normalized_name:
            return {
                "id": f"textual-{normalized_name}",
                "type": "function",
                "function": {
                    "name": normalized_name,
                    "arguments": json.dumps(arguments, default=str),
                },
            }

    inline_tool_match = re.search(
        r"<\|tool_call_begin\|>\s*(?P<name>[\w\.\-]+)(?::\d+)?\s*<\|tool_call_argument_begin\|>\s*(?P<args>\{[\s\S]*?\})\s*<\|tool_call_end\|>",
        unescaped_text,
        re.IGNORECASE,
    )
    if inline_tool_match:
        raw_name = str(inline_tool_match.group("name") or "").strip()
        tool_name = raw_name.split(".")[-1] if raw_name else ""
        normalized_name = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name, tool_name))
        arguments = _parse_openai_tool_arguments(inline_tool_match.group("args"))
        if normalized_name:
            return {
                "id": f"textual-{normalized_name}",
                "type": "function",
                "function": {
                    "name": normalized_name,
                    "arguments": json.dumps(arguments, default=str),
                },
            }

    if "{" not in unescaped_text:
        return None

    try:
        payload = _parse_json_object_text(unescaped_text)
    except Exception:
        return None

    tool_name = str(payload.get("tool") or payload.get("name") or "").strip()
    args_payload: Any = None
    if tool_name:
        args_payload = (
            payload.get("args")
            if "args" in payload
            else payload.get("arguments")
            if "arguments" in payload
            else payload.get("input")
            if "input" in payload
            else payload.get("parameters")
        )
    else:
        function_payload = payload.get("function")
        if isinstance(function_payload, dict):
            tool_name = str(function_payload.get("name") or "").strip()
            args_payload = function_payload.get("arguments")

    if not tool_name:
        return None

    normalized_name = TOOL_NAME_MAP.get(tool_name, tool_name)
    arguments = _parse_openai_tool_arguments(args_payload)
    if not arguments and isinstance(args_payload, dict):
        arguments = args_payload

    return {
        "id": f"textual-{normalized_name}",
        "type": "function",
        "function": {
            "name": normalized_name,
            "arguments": json.dumps(arguments, default=str),
        },
    }


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
    rows = []
    for item in history or []:
        row = dict(item)
        role = str(row.get("role") or "").strip().lower()
        if role == "user":
            row["content"] = _build_user_multimodal_content(str(row.get("content") or "").strip(), _build_image_blocks(row.get("images")))
        rows.append(row)
    image_blocks = _build_image_blocks(images)

    if not image_blocks:
        return rows or [{"role": "user", "content": _llm_user_prompt(prompt)}]

    merged_content = _build_user_multimodal_content(_llm_user_prompt(prompt), image_blocks)
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


def _parse_message_attachments(raw: Any) -> list[dict[str, Any]]:
    if not raw:
        return []
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        attachments = payload.get("attachments") or []
        return [item for item in attachments if isinstance(item, dict)]
    return []


def _history_image_attachments(attachments: list[dict[str, Any]]) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    for attachment in attachments:
        file_url = str(attachment.get("file_url") or "").strip()
        media_type, data = _extract_base64_image(file_url)
        if not media_type or not data:
            continue
        images.append({"media_type": media_type, "data": data})
    return images


def _describe_message_attachments(attachments: list[dict[str, Any]]) -> list[str]:
    notes: list[str] = []
    for attachment in attachments[:4]:
        filename = str(attachment.get("filename") or "attachment").strip()
        label = str(attachment.get("label") or attachment.get("file_type") or "file").strip()
        if str(attachment.get("file_url") or "").startswith("data:image/"):
            notes.append(f"Attached image: {filename}")
        else:
            notes.append(f"Attached file: {filename} ({label})")
    return notes


def _merge_history_content_and_attachment_notes(content: str, notes: list[str]) -> str:
    body = str(content or "").strip()
    if not notes:
        return body
    note_text = "\n".join(notes)
    if not body:
        return note_text
    if note_text in body:
        return body
    return f"{body}\n\n{note_text}"


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
    if has_images and _provider_name() in {"openai", "openai_compatible"}:
        vision_model = str(
            _cfg(
                "ERP_AI_OPENAI_VISION_MODEL",
                _cfg("OPENAI_VISION_MODEL", ""),
            )
        ).strip()
        if vision_model:
            available = _available_llm_models()
            if not available or vision_model in available:
                return vision_model
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
    vision_key = "OPENAI_VISION_MODEL" if is_openai_family else "ANTHROPIC_VISION_MODEL"
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
    vision_model = str(_cfg(vision_key, "")).strip()
    if vision_model:
        models.append(vision_model)

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
    return _parse_sse_events((text or "").splitlines(), endpoint)


def _parse_sse_stream(
    response: requests.Response,
    endpoint: str,
    progress: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    decoded_lines = []
    partial_text = ""
    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = str(raw_line)
        decoded_lines.append(line)
        stripped = line.strip()
        if not stripped.startswith("data:"):
            continue
        raw_data = stripped[len("data:") :].strip()
        if not raw_data or raw_data == "[DONE]":
            continue
        try:
            payload = json.loads(raw_data)
        except Exception:
            continue
        if payload.get("type") == "content_block_delta":
            delta = payload.get("delta") or {}
            if delta.get("type") == "text_delta":
                partial_text += str(delta.get("text") or "")
                _progress_update(progress, stage="thinking", partial_text=partial_text)
    return _parse_sse_events(decoded_lines, endpoint)


def _parse_sse_events(lines: list[str], endpoint: str) -> dict[str, Any]:
    blocks_by_index: dict[int, dict[str, Any]] = {}
    message_payload: dict[str, Any] = {}
    stop_reason = None

    for raw_line in lines:
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
        preview = "\n".join(lines).strip().replace("\n", " ")[:300]
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


def _tool_call_signature(tool_name: str, arguments: dict[str, Any]) -> str:
    return f"{str(tool_name or '').strip()}::{json.dumps(arguments or {}, sort_keys=True, default=str)}"


def _document_name_from_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    name = str(payload.get("name") or payload.get("docname") or "").strip()
    if name:
        return name
    data = payload.get("data")
    if isinstance(data, dict):
        return str(data.get("name") or data.get("docname") or "").strip()
    result = payload.get("result")
    if isinstance(result, dict):
        return str(result.get("name") or result.get("docname") or "").strip()
    return ""


def _validate_tool_result(tool_name: str, payload: Any) -> None:
    if not isinstance(payload, dict):
        return

    if payload.get("success") is False:
        message = str(payload.get("error") or payload.get("message") or "Tool returned success=false.").strip()
        raise RuntimeError(message)

    normalized = str(tool_name or "").strip().lower()
    if normalized == "create_document":
        if not _document_name_from_payload(payload):
            raise RuntimeError("FAC did not confirm the created document name.")
    elif normalized == "create_report":
        report_name = str(payload.get("name") or "").strip()
        report_type = str(payload.get("report_type") or "").strip()
        if not report_name:
            raise RuntimeError("FAC did not confirm the created report name.")
        if not report_type:
            raise RuntimeError("FAC did not confirm the created report type.")
        if report_type == "Query Report" and not bool(payload.get("has_query")):
            raise RuntimeError("FAC created the report record but did not confirm a query definition.")
        if report_type == "Script Report" and not bool(payload.get("has_script")):
            raise RuntimeError("FAC created the report record but did not confirm a script definition.")
    elif normalized == "update_report":
        report_name = str(payload.get("name") or "").strip()
        updated_parts = payload.get("updated_parts")
        if not report_name:
            raise RuntimeError("FAC did not confirm the updated report name.")
        if not isinstance(updated_parts, list) or not updated_parts:
            raise RuntimeError("FAC did not confirm which report parts were updated.")
    elif normalized in {"export_report", "export_doctype_records"}:
        file_url = str(payload.get("file_url") or "").strip()
        file_name = str(payload.get("file_name") or "").strip()
        if not file_url or not file_name:
            raise RuntimeError("FAC did not return a downloadable export file.")


def _render_provider_tool_output(tool_name: str | None, payload: Any) -> str:
    reverse_map = {
        "list_documents": "get_list",
        "generate_report": "get_report",
    }
    normalized = reverse_map.get(str(tool_name or "").strip(), str(tool_name or "").strip())
    if normalized in {
        "get_list",
        "get_document",
        "get_report",
        "report_list",
        "report_requirements",
        "create_document",
        "update_document",
        "delete_document",
    }:
        return _render_tool_output(normalized, payload)
    return _format_generic_result(payload, "Result")


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
    for part in [item.strip() for item in normalized.split(",") if item.strip()]:
        if "=" in part:
            raw_key, raw_value = part.split("=", 1)
        elif ":" in part:
            raw_key, raw_value = part.split(":", 1)
        else:
            continue
        key = _normalize_field_key(raw_key)
        assignments[key] = _coerce_value(raw_value.strip())
    return assignments


def _normalize_field_key(raw_key: str) -> str:
    aliases = {
        "territory": "territory",
        "designation": "designation",
        "email": "email_id",
        "mobile": "mobile_no",
        "phone": "mobile_no",
        "department": "department",
        "description": "description",
        "salary": "salary",
        "basic salary": "basic_salary",
        "company email": "company_email",
        "status": "status",
    }
    normalized = str(raw_key or "").strip().lower()
    return aliases.get(normalized, normalized.replace(" ", "_"))


def _coerce_value(value: str) -> Any:
    cleaned = value.strip().strip("\"'")
    compact_match = re.fullmatch(r"(-?\d+(?:\.\d+)?)\s*([kKmM])", cleaned)
    if compact_match:
        number = float(compact_match.group(1))
        suffix = compact_match.group(2).lower()
        multiplier = 1000 if suffix == "k" else 1_000_000
        expanded = number * multiplier
        return int(expanded) if float(expanded).is_integer() else expanded
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
