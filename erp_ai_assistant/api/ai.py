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

import time as _time_module

from .chat import add_message, create_conversation
from .export import add_message_attachment_urls, create_message_artifacts
from .copilot_response import build_copilot_package
from .fac_client import dispatch_tool, get_tool_definitions, read_fac_resource
from .provider_settings import get_active_provider, get_provider_setting, get_remote_mcp_servers
from .resource_registry import list_resource_specs
from .audit import write_audit_log, PromptTimer
from .rate_limit import check_rate_limit


from .tool_aliases import resolve_tool_name as _resolve_tool_name

# ── Config module (PRODUCTION_REFACTOR_NOTES.md Step 1) ─────────────────────
# All configuration helpers have been extracted to ai_config.py.
# The definitions below (lines ~40–283) remain as local copies for now so that
# existing code inside this file continues to work without changes.
# In the next cleanup pass, delete the in-file copies and replace with:
#   from .ai_config import _cfg, _cfg_int, _cfg_float, _cfg_bool, _llm_*, DEFAULT_*
from .ai_config import (  # noqa: F401 — re-exported for external callers
    DEFAULT_ANTHROPIC_MAX_TOKENS,
    DEFAULT_ANTHROPIC_MAX_TOOL_ROUNDS,
    DEFAULT_ANTHROPIC_REQUEST_TIMEOUT,
    DEFAULT_ANTHROPIC_STREAM,
    DEFAULT_ANTHROPIC_TEMPERATURE,
    DEFAULT_ANTHROPIC_TOP_P,
    DEFAULT_ANTHROPIC_VISION_MODEL,
    DEFAULT_CONVERSATION_HISTORY_LIMIT,
    DEFAULT_FORCE_TOOL_USE,
    DEFAULT_OPENAI_MODEL,
    DEFAULT_OPENAI_RESPONSES_PATH,
    DEFAULT_VERIFY_PASS,
)


# Kept for backward compatibility — callers that do TOOL_NAME_MAP.get(name, name)
# will now get the registry-resolved value.
class _ToolNameMap:
    """Proxy that delegates to the registry-driven alias resolver."""
    def get(self, key: str, default: str | None = None) -> str:
        resolved = _resolve_tool_name(key)
        # If unchanged and no explicit default, return the key itself
        if resolved == key and default is not None:
            return default
        return resolved

TOOL_NAME_MAP = _ToolNameMap()

DEFAULT_ANTHROPIC_MAX_TOKENS = 4000
DEFAULT_ANTHROPIC_REQUEST_TIMEOUT = 120.0
DEFAULT_ANTHROPIC_STREAM = True
DEFAULT_ANTHROPIC_MAX_TOOL_ROUNDS = 20
DEFAULT_ANTHROPIC_TEMPERATURE = 0.2
DEFAULT_ANTHROPIC_TOP_P = 0.9
DEFAULT_FORCE_TOOL_USE = True
DEFAULT_VERIFY_PASS = True
DEFAULT_ANTHROPIC_VISION_MODEL = ""
DEFAULT_OPENAI_MODEL = "gpt-5"
DEFAULT_OPENAI_RESPONSES_PATH = "/v1/responses"
DEFAULT_CONVERSATION_HISTORY_LIMIT = 12
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
FAC_PREFERRED_TOOL_NAMES = {
    "get_document",
    "list_documents",
    "create_document",
    "update_document",
    "delete_document",
    "get_doctype_info",
    "search_documents",
    "search_doctype",
    "search_link",
    "submit_document",
    "run_workflow",
    "generate_report",
    "report_list",
    "report_requirements",
}
LEGACY_INTERNAL_TOOL_NAMES = {
    "get_erp_document",
    "list_erp_documents",
    "create_erp_document",
    "update_erp_document",
    "get_doctype_fields",
    "describe_erp_schema",
    "search_erp_documents",
    "submit_erp_document",
    "cancel_erp_document",
    "run_workflow_action",
    "answer_erp_query",
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


def _conversation_history_limit() -> int:
    return _cfg_int(
        "ERP_AI_CONVERSATION_HISTORY_LIMIT",
        DEFAULT_CONVERSATION_HISTORY_LIMIT,
        minimum=4,
        maximum=40,
    )


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
    if _is_instructional_request(prompt) or _is_sample_data_request(prompt, context):
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
    if _is_instructional_request(text):
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


def _is_instructional_request(prompt: str) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False
    instructional_prefixes = (
        "how to ",
        "how do i ",
        "how can i ",
        "what is ",
        "what are ",
        "explain ",
        "show me how ",
        "guide me ",
        "teach me ",
        "help me understand ",
    )
    if any(text.startswith(prefix) for prefix in instructional_prefixes):
        return True
    return bool(re.search(r"\b(steps|procedure|process|workflow|guide)\b", text) and "?" in text)


def _is_sample_data_request(prompt: str, context: Optional[dict[str, Any]] = None) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False

    # If the prompt contains a clear ERP creation/action intent, never treat it
    # as a sample-data request — the user wants real documents via FAC tools.
    erp_creation_signals = (
        "material request", "sales order", "purchase order", "sales invoice",
        "purchase invoice", "stock entry", "journal entry", "payment entry",
        "delivery note", "purchase receipt", "quotation", "invoice",
        "entries", "records", "documents", "dashboard", "report", "chart",
        "customer", "supplier", "item", "employee",
    )
    creation_verbs = (
        "create", "make", "add", "generate", "insert", "build",
        "update", "edit", "submit", "approve", "list", "show", "find",
        "export", "download",
    )
    has_erp_creation_intent = (
        any(v in text for v in creation_verbs)
        and any(s in text for s in erp_creation_signals)
    )
    if has_erp_creation_intent:
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

    if "realistic" in text and "create" in text and (
        any(qualifier in text for qualifier in ("sample", "dummy", "mock", "fake", "test", "fictional", "placeholder"))
        and ("records" in text or "rows" in text or "data" in text)
    ):
        return True


    return False


def _is_bulk_operation_request(prompt: str, context: Optional[dict[str, Any]] = None) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False

    explicit_bulk_terms = (
        "bulk ",
        "mass ",
        "batch ",
        "all records",
        "all documents",
        "update all",
        "import ",
    )
    if any(term in text for term in explicit_bulk_terms):
        return True

    # Catch any explicit number >= 2 with a create/update/generate/export verb
    quantity_match = re.search(r"\b(create|update|insert|modify|add|generate|export)\s+(\d+)\b", text)
    if quantity_match:
        qty = int(quantity_match.group(2))
        if qty >= 2:
            return True

    if any(term in text for term in {"all ", "every "}) and _has_write_intent(text):
        return True

    return False


def _needs_multi_step_plan(prompt: str, context: Optional[dict[str, Any]] = None) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False
    if _is_bulk_operation_request(prompt, context):
        return True
    multi_step_terms = (
        "check",
        "compare",
        "confirm",
        "then",
        "before",
        "after",
        "if available",
        "availability",
        "inventory",
        "balance",
        "validate",
        "verify",
        "analyze",
        "summary",
    )
    if sum(1 for term in multi_step_terms if term in text) >= 2:
        return True
    if _has_write_intent(prompt) and any(term in text for term in ("check", "confirm", "verify", "availability")):
        return True
    return False


def _resolved_prompt_doctype(prompt: str, context: Optional[dict[str, Any]] = None) -> str | None:
    current = context or {}
    explicit = str(current.get("doctype") or "").strip()
    if explicit:
        return explicit

    inferred = _infer_prompt_doctype(prompt)
    if inferred:
        return inferred

    text = str(prompt or "").strip()
    if not text:
        return None

    patterns = (
        r"\bdoctype\s+([A-Z][A-Za-z0-9_/& -]{2,60})",
        r"\b(?:create|new|add|insert|update|edit|modify|show|open|list|find|search|submit|cancel|approve|reject)\s+(?:a\s+|an\s+|the\s+)?([A-Z][A-Za-z0-9_/& -]{2,60})",
    )
    stop_terms = (
        " with ",
        " for ",
        " from ",
        " in ",
        " on ",
        " by ",
        " where ",
        " that ",
        " which ",
        " using ",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = str(match.group(1) or "").strip(" .,:;!?")
        lowered = candidate.lower()
        for stop_term in stop_terms:
            if stop_term in lowered:
                candidate = candidate[: lowered.index(stop_term)].strip(" .,:;!?")
                lowered = candidate.lower()
        if candidate and len(candidate.split()) <= 6:
            return candidate
    return None


def _resolved_erp_intent(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    text = str(prompt or "").strip().lower()
    current = context or {}
    if _is_sample_data_request(prompt, current):
        return "sample"
    if _is_instructional_request(prompt):
        return "read"
    if _is_bulk_operation_request(prompt, current):
        # Distinguish bulk-create from bulk-read/update
        _bulk_text = str(prompt or "").strip().lower()
        _create_verbs = ("create", "add", "insert", "generate", "make", "build")
        if any(v in _bulk_text for v in _create_verbs):
            return "bulk_create"
        return "bulk"
    if any(term in text for term in {"submit", "approve", "reject", "cancel", "reopen", "workflow"}):
        return "workflow"
    if any(term in text for term in {"export", "download", "xlsx", "csv", "pdf", "docx", "word"}):
        return "export"
    if any(term in text for term in {"report", "dashboard", "chart"}):
        return "report"
    if any(re.search(rf"\b{re.escape(term)}\b", text) for term in ("create", "add", "insert", "new")):
        return "create"
    if any(re.search(rf"\b{re.escape(term)}\b", text) for term in ("update", "edit", "change", "modify", "set", "rename")):
        return "update"
    if _has_destructive_intent(prompt):
        return "delete"
    if _has_active_document_context(current):
        return "read"
    return "read"


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
    intent = _resolved_erp_intent(prompt, context)
    target_doctype = str(_resolved_prompt_doctype(prompt, context) or "").strip().lower()
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

    if target_doctype and any(term in tool_terms for term in _tokenize_match_text(target_doctype)):
        score += 4

    if _has_write_intent(prompt_text) and any(term in tool_terms for term in {"create", "update", "delete", "workflow", "submit", "cancel"}):
        score += 4
    if _has_write_intent(prompt_text) and _has_doctype_context(context):
        if name in {"create_document", "get_doctype_info"}:
            score += 8
    if intent == "create" and name in {"get_doctype_info", "create_document", "search_link"}:
        score += 8
    if intent == "update" and name in {"get_document", "list_documents", "update_document"}:
        score += 8
    if intent == "update" and _has_active_document_context(context) and name == "get_doctype_info":
        score -= 2
    if intent == "workflow" and name in {"get_document", "list_documents", "submit_document", "run_workflow"}:
        score += 8
    if intent == "report" and name in {"report_list", "report_requirements", "generate_report"}:
        score += 8
    if intent == "export" and name in {"export_report", "export_doctype_records", "generate_report", "list_documents", "get_document"}:
        score += 8
    if intent == "bulk" and name in {"get_doctype_info", "list_documents", "search_documents", "export_doctype_records"}:
        score += 6
    if intent == "bulk_create" and name in {"get_doctype_info", "list_documents", "search_link"}:
        score += 8  # discovery tools needed before bulk create
    if intent == "bulk_create" and name in {"run_python_code", "create_document"}:
        score += 12  # primary execution tools for bulk create
    if "create" in prompt_text and name == "get_doctype_info":
        score += 3
    if "create" in prompt_text and name == "search_link":
        score += 2
    if _has_destructive_intent(prompt_text) and any(term in tool_terms for term in {"delete", "remove", "purge", "cancel"}):
        score += 4
    if any(term in prompt_text for term in {"export", "excel", "xlsx", "csv", "pdf", "docx", "word", "download"}):
        if any(term in tool_terms for term in {"list", "document", "report", "fetch", "export", "search"}):
            score += 2

    return score


def _has_active_document_context(context: Optional[dict[str, Any]] = None) -> bool:
    current = context or {}
    return bool(current.get("doctype") and current.get("docname"))


def _has_doctype_context(context: Optional[dict[str, Any]] = None) -> bool:
    current = context or {}
    return bool(current.get("doctype"))


def _context_summary(context: Optional[dict[str, Any]] = None) -> str:
    current = context or {}
    target_doctype = _resolved_prompt_doctype("", current)
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
        if target_doctype:
            return f"Current context: target doctype={target_doctype}. Current route={route}."
        return f"Current context: no active document. Current route={route}."
    return "Current context: no active document. Treat this as a general workspace chat unless the user asks for ERP data."


def _tool_catalog_summary(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    try:
        tool_definitions = get_tool_definitions(user=(context or {}).get("user"))
    except Exception:
        tool_definitions = {}

    available_names = set(tool_definitions)
    if not available_names:
        return "No FAC tools are currently available. Answer directly unless tool access becomes available."

    selected_names = [
        name
        for name, _score in sorted(
            ((name, _tool_match_score(prompt, context or {}, name, spec)) for name, spec in tool_definitions.items()),
            key=lambda item: (-item[1], _tool_priority_key(item[0])),
        )
    ]
    preview = []
    for name in selected_names[:12]:
        description = str((tool_definitions.get(name) or {}).get("description") or "").strip()
        if description:
            preview.append(f"{name}: {description}")
        else:
            preview.append(name)
    suffix = " ..." if len(selected_names) > 8 else ""
    intent = _resolved_erp_intent(prompt, context or {})
    target_doctype = _resolved_prompt_doctype(prompt, context or {})
    return (
        "Use only the FAC tools that are actually exposed in this session. "
        "Route the request by target doctype first, then by user intent. "
        "Select tools from the live FAC catalog based on the user's request, tool descriptions, and input schema. "
        "Do not invent or assume tools that are not present. "
        f"{f'Target doctype for this request: {target_doctype}. ' if target_doctype else ''}"
        f"Resolved intent for this request: {intent}. "
        f"Best-matching FAC tools for this request: {'; '.join(preview)}{suffix}."
    )


def _system_identity_rules() -> str:
    return (
        # ── CORE IDENTITY ─────────────────────────────────────────────────────
        "You are an advanced AI Operations Copilot inside ERPNext, functioning like Claude with MCP capabilities. "
        "You can reason, plan, and execute actions using tools. "
        "You are an intelligent desktop agent that interacts with ERPNext data, workflows, and external systems. "
        "You are proactive, precise, and action-oriented. "
        "You combine reasoning + tool execution. "
        "You help users complete real work, not just answer questions. "
        "You think step-by-step internally, but present clean results. "

        # ── OPERATING LOOP ────────────────────────────────────────────────────
        "For every request follow this loop: "
        "(1) UNDERSTAND — identify user intent, identify entities (doctype, document, report), "
        "classify as query / action / analysis / automation. "
        "(2) PLAN — decide steps needed, decide which tools to call, prefer minimal efficient steps. "
        "(3) ACT — call tools when needed, never guess data if tools are available, chain multiple tools if required. "
        "(4) VERIFY — check results, ensure correctness, handle errors gracefully. "
        "(5) RESPOND — provide a clean structured answer, summarise results, suggest next actions. "
        "Do NOT expose raw chain-of-thought. Keep the plan internal unless the user asks to see it. "

        # ── AUTONOMOUS BEHAVIOUR ──────────────────────────────────────────────
        "When appropriate: take initiative, suggest next steps, offer automation, "
        "detect inefficiencies, highlight risks. "
        "Example: if asked to show overdue invoices, fetch data, summarise totals, identify top debtors, "
        "and suggest follow-up actions — do not just list records. "

        # ── ERP INTELLIGENCE ──────────────────────────────────────────────────
        "You understand the full ERPNext business cycle. "
        "Sales cycle: Lead → Opportunity → Quotation → Sales Order → Delivery Note → Sales Invoice → Payment. "
        "Purchase cycle: Material Request → RFQ → Supplier Quotation → Purchase Order → Receipt → Invoice. "
        "Accounting: GL Entry, Journal Entry, Payment Entry, Cost Centers, Fiscal Year. "
        "Stock: Bin, Stock Ledger, Warehouses, Reorder Levels. "
        "HR: Employees, Attendance, Leave, Payroll. "
        "Projects: Tasks, Timesheets, Progress tracking. "
        "Manufacturing: BOM, Work Order, Production. "
        "Common abbreviations — SO: Sales Order, PO: Purchase Order, SI/SINV: Sales Invoice, "
        "PI/PINV: Purchase Invoice, DN: Delivery Note, PR: Purchase Receipt, MR: Material Request, "
        "SE: Stock Entry, JV/JE: Journal Entry, PE: Payment Entry, WO: Work Order, GRN: Purchase Receipt. "
        "Resolve abbreviations silently without asking the user to clarify. "

        # ── META-BEHAVIOUR ────────────────────────────────────────────────────
        "If a task can be automated, suggest automation. "
        "If repetitive behaviour is detected, suggest workflow optimisation. "
        "If data shows anomalies, highlight them proactively. "

        # ── RESPONSE FORMAT ───────────────────────────────────────────────────
        "You are precise, grounded, and honest — you never invent ERP data, document names, field values, or tool results. "
        "You render responses in clean Markdown: use headings, bullet lists, numbered lists, bold/italic, tables, and fenced code blocks as appropriate. "
        "Always use a fenced code block with a language tag (```python, ```json, ```sql, etc.) for any code or structured data. "
        "Use tables when displaying multiple records or comparison data. "
        "For queries: provide Summary → Key data → Optional insights. "
        "For actions: state what you did / will do → target records → status. "
        "For analysis: provide Findings → Insights → Recommendations. "
        "Stay tightly aligned to the latest user request. "
        "Treat standard and custom DocTypes uniformly — never assume a DocType is unavailable just because it is custom or unfamiliar. "
        "If image blocks are present, analyze them directly and use the visual content to inform your ERP response. "

        # ── MODULE COVERAGE ───────────────────────────────────────────────────
        "ERPNext module coverage you are aware of: "
        "Accounts (GL Entries, Journal Entry, Payment Entry, Sales Invoice, Purchase Invoice, Cost Center, Account); "
        "Selling (Quotation, Sales Order, Delivery Note, Sales Invoice, Customer); "
        "Buying (Purchase Order, Purchase Receipt, Purchase Invoice, Supplier, Request for Quotation); "
        "Stock (Item, Warehouse, Stock Entry, Material Request, Stock Ledger Entry, Batch, Serial No); "
        "Manufacturing (Work Order, BOM, Job Card, Production Plan, Workstation); "
        "HR & Payroll (Employee, Leave Application, Attendance, Salary Slip, Payroll Entry, Appraisal, Expense Claim); "
        "Projects (Project, Task, Timesheet); "
        "CRM (Lead, Opportunity, Campaign, Contact, Address); "
        "Support (Issue, Warranty Claim); "
        "Assets (Asset, Asset Movement, Asset Maintenance, Asset Repair); "
        "Quality (Quality Inspection, Quality Goal, Non-Conformance). "
        "Never limit yourself to only these DocTypes — custom DocTypes are equally supported via FAC tools. "
    )


def _system_tool_use_rules() -> str:
    return (
        # ── ACTION SAFETY LAYER ───────────────────────────────────────────────
        "Action Safety Layer — classify every action before executing it. "
        "LOW RISK (execute immediately): read data, list records, summarise, run reports. "
        "MEDIUM RISK (create draft, confirm before submit): create draft documents, update non-critical fields. "
        "HIGH RISK (always require explicit user confirmation before executing): "
        "submit documents, cancel documents, delete records, "
        "actions with financial impact (GL, invoices, payments), "
        "actions with stock impact, actions with payroll impact, workflow approvals. "
        "For HIGH RISK actions: first respond with what will happen, which records are affected, "
        "what the impact is, and ask for confirmation. DO NOT execute until the user confirms. "

        # ── TOOL USAGE MODEL ──────────────────────────────────────────────────
        "Tool-use rules (mandatory): "
        "1. If the user's request requires live ERP data, document lookup, workflow actions, reports, or record changes, call the appropriate FAC tool. "
        "2. NEVER guess, invent, or hallucinate ERP data, document names, field values, workflow states, or tool results. "
        "   If you do not know a value and cannot discover it via tools, say 'I don't know' or ask the user. "
        "3. Always prefer FAC tool results over your own training knowledge for ERP-related requests. "
        "4. For ERP creation or mutation requests, always discover required field values via FAC tools first — call get_doctype_info, list_documents, and search_link to retrieve real companies, warehouses, items, and other linked values from the live system before acting. "
        "   Never stop to ask the user for values that can be discovered via FAC tools. "
        "   Only ask the user if a required value genuinely cannot be found via any tool lookup. "
        "   When list_documents returns multiple records during a discovery step, this is EXPECTED — use those records as valid values to pick from, never stop or ask for clarification. "
        "   Treat each user request as a completely fresh independent task. "
        "   Never skip or reduce the number of documents to create based on conversation history. "
        "5. After receiving tool output, summarize the result clearly in well-formatted Markdown for the user. "
        "   Use tables for lists of records, fenced code blocks for code/data, and bold for document names. "
        "6. Do not expose internal tool logic, raw payloads, schemas, JSON, XML, hidden reasoning, or tool names unless the user asks for technical details. "
        "7. If the request is instructional only, answer directly without calling tools unless live ERP verification is necessary. "
        "8. If no suitable FAC tool is available, say so briefly and do not pretend the action succeeded. "
        "9. Stop calling tools as soon as the request is completed or the available tool results are sufficient to answer. "
        "10. For bulk actions prefer run_python_code when the requested count is greater than 5; otherwise use create_document or update_document individually. "
        "For complex or multi-step ERP requests, first form a short internal plan, then execute the required FAC tools, then provide the final answer. "
        "Keep that plan internal unless the user explicitly asks to see it. "
        "11. For date filters, interpret relative terms correctly: "
        "   'today' = current date, 'this month' = first to last day of the current month, "
        "   'last month' = first to last day of the previous month, "
        "   'this quarter' = current financial quarter, 'this year' = Jan 1 to Dec 31 of the current year, "
        "   'last year' = Jan 1 to Dec 31 of the previous year. Always convert to YYYY-MM-DD for filters. "
        "12. When amounts or totals are involved, always include the currency symbol and present grand totals clearly. "
        "   If multiple currencies exist in results, group or flag them. "
        "13. For ambiguous doctype names, apply this precedence: "
        "   'invoice' → Sales Invoice (unless buying context → Purchase Invoice); "
        "   'order' → Sales Order (unless buying context → Purchase Order); "
        "   'receipt' → Purchase Receipt; 'note' → Delivery Note; 'entry' → Journal Entry. "
        "   Use the current route/module context to resolve ambiguity before asking the user. "
        "14. If a tool returns a permissions error, state it plainly and do not retry with different arguments or try to bypass it. "
        "   Offer the nearest read-only or draft-safe alternative if one exists. "
        "15. Before executing delete, cancel, submit, or amend on any document, "
        "   always confirm the exact document name and action with the user unless the request was already explicit and unambiguous. "

        # ── MULTI-STEP TASK HANDLING ──────────────────────────────────────────
        "Multi-step task handling: if a task is complex, break it into steps, execute sequentially, "
        "and keep the user informed briefly at each step. "
        "Example — 'Create invoice from sales order and email customer': "
        "Step 1: Fetch Sales Order. Step 2: Create Invoice (draft). "
        "Step 3: Submit only after user confirms. Step 4: Send Email. "

        # ── CONTEXT AWARENESS ─────────────────────────────────────────────────
        "Context awareness: remember within the session — company, customer, supplier, project, "
        "date range, warehouse. Reuse context intelligently. Ask only if genuinely needed. "
        "Map natural language to correct DocTypes automatically. "
    )


def _system_erp_fac_rules() -> str:
    return (
        "ERP/FAC rules: "
        "Use the exact live FAC tool names and argument shapes for this session. "
        "When the current context has a doctype but no active document, and the user asks to create a new record without naming a doctype, treat the current context doctype as the target. "
        "For DocType creation, prefer get_doctype_info before create_document when metadata is needed. "
        "For updates, identify the exact target document before update_document. "
        "Common DocType abbreviations the user may use: "
        "SO → Sales Order, PO → Purchase Order, SI/SINV → Sales Invoice, PI/PINV → Purchase Invoice, "
        "DN → Delivery Note, PR → Purchase Receipt, MR → Material Request, SE → Stock Entry, "
        "JV/JE → Journal Entry, PE → Payment Entry, WO → Work Order, GRN → Purchase Receipt. "
        "Resolve these abbreviations silently without asking the user to clarify. "
        "For submit_document, the document must be in Saved (docstatus=0) state first; "
        "for amend_document, it must be in Submitted (docstatus=1) state. "
        "Check docstatus before attempting submit or amend and report the current state if the action is not possible. "
    )


def _system_tool_trigger_rules() -> str:
    return (
        "ERP tool-trigger rules are mandatory. "
        "Any request involving ERP data, documents, lists, reports, creation, updates, workflow actions, or system-backed values must use live FAC tools rather than plain-text fabrication. "
        "Never answer ERP action requests with fictional, sample, placeholder, or invented data when FAC tools are available. "
        "Treat verbs such as create, make, add, generate, list, show, find, get, fetch, open, lookup, search, update, change, edit, set, amend, submit, cancel, approve, reject, "
        "report, summarize, export, download, print, compare, analyze, analytics, dashboard, random, sample, and test entries as strong tool-use triggers when the request concerns ERP records or ERP outputs. "
        "For requests like create random test entries or create 3 Material Requests, use live FAC discovery to fetch real companies, warehouses, items, customers, and other linked values first, then create real draft ERP documents with verified values. "
        "Do not treat ERP requests as general knowledge or creative writing tasks when a live FAC tool path exists. "
        "Do not stop to ask the user for field values such as company, warehouse, item code, request type, or section when those values can be discovered by calling list_documents or search_link on the relevant DocType with empty filters. "
        "Discover first using FAC tools, act second, report third. "
        "Each user request must be treated as a FRESH independent task regardless of conversation history. "
        "If the user repeats a request like create 5 Material Requests, always execute it fully again "
        "from scratch — never assume it was already completed because history shows similar actions. "
    )


def _system_data_validation_rules() -> str:
    return (
        "Data validation rules for ERP mutations are mandatory. "
        "Never invent or assume field values, linked records, placeholder names, warehouses, companies, request types, sections, or any other ERP value. "
        "Before creating, updating, or referencing ERP documents, always discover real metadata and valid linked values first. "
        "For create or update flows, call get_doctype_info to inspect required fields, field types, and link structure before mutating when that information is not already verified in-session. "
        "For each linked DocType such as Warehouse, Company, Item, Customer, Supplier, Employee, User, or custom links, call list_documents, search_link, search_doctype, or another appropriate FAC discovery tool to retrieve real existing values from the system before using them. "
        "When calling list_documents to discover Company, Warehouse, Item, PR Type, PRSection, or any other linked DocType, always use an empty filter {} to fetch all available records. "
        "Never filter list_documents by the user's prompt text, conversation keywords, or assumed names such as Test, Default, Main, Standard, or Sample. "
        "The goal of linked-record discovery is to retrieve whatever actually exists in the system, then pick from those real returned values. "
        "When discovering linked records, do not start with guessed name filters such as Default, Main Warehouse, Standard, or similar assumptions. "
        "Prefer broad discovery first, such as list_documents without a name filter or search_link with the user's actual text, then choose from the real returned values. "
        "Use exact database values for link fields, including exact capitalization and spacing. "
        "Do not use placeholders such as Sample, Test Warehouse, Central Warehouse, Central Storage, Default Company, or similar guessed values. "
        "For explicit test-data or sample-data creation requests, if a required linked master DocType exists but has zero usable records, "
        "prefer creating the prerequisite linked master records first instead of stopping, then resume the requested document creation. "
        "Apply this only when the missing values are prerequisite ERP records that can be created safely with available FAC tools. "
        "If company-specific records are involved, verify cross-document constraints such as warehouse-company compatibility before create_document or update_document. "
        "Preferred mutation order is: get_doctype_info, then linked-record discovery, then create_document or update_document. "
        "If the required live value cannot be verified from tool results, ask the user for the minimum missing information instead of guessing. "
        "For transaction_date and posting_date fields, always use today's actual date in YYYY-MM-DD format unless the user specified a different date. "
        "For due_date, default to today + 30 days unless payment terms dictate otherwise. "
        "For schedule_date on item rows, default to today + 7 days. "
        "When creating documents that belong to a specific company, always ensure that all linked warehouses, cost centres, and accounts belong to the same company. "
        "Cross-company mismatches in these fields will cause ERPNext server validation errors — verify company consistency before calling create_document. "
    )


def _system_bulk_execution_rules() -> str:
    return (
        "Bulk execution rules are mandatory for multi-record ERP mutations. "
        "When the user says create N <DocType> or generate N <DocType>, the intent is CREATION not search or listing. "
        "Never respond to a create N request by searching or listing existing records. "
        "When the user requests a specific number of records to CREATE, read that number from the request and treat it as dynamic rather than fixed. "
        "If the requested count is 5 or fewer, use create_document individually for each record after discovering real linked values. "
        "If the requested count is greater than 5, prefer run_python_code when that tool is available, using a loop and live FAC data discovery inside the code path. "
        "Outside run_python_code, use normal FAC tools such as list_documents, get_document, get_doctype_info, create_document, and update_document. "
        "Inside run_python_code, use the tool API exposed in that environment for live ERP reads instead of inventing data. "
        "For bulk creation, always discover real companies, warehouses, items, and other linked records first using list_documents with empty filters, then create records in a loop. "
        "When creating multiple documents, vary the data across each one — different items, qty (random 1-20), remarks (Test Entry 1, Test Entry 2...) per document. "
        "Each create N request must create exactly N NEW documents regardless of conversation history. "
        "Treat every request as completely fresh — never skip or reduce the count. "
        "For partial failures in bulk operations, continue where safe, track successes and failures separately, and report both counts clearly. "
    )


def _system_response_format_rules() -> str:
    return (
        "Response formatting rules are mandatory. "
        "Format all responses using clean Markdown that renders well in a chat UI. "
        "Use fenced code blocks (```language) for ALL code, scripts, JSON, SQL, shell commands, and structured data. "
        "Use tables for comparing records, listing multiple documents, or showing report data — never use plain text lists for tabular data. "
        "Use numbered lists for step-by-step procedures; use bullet lists for features, options, or unordered items. "
        "Use **bold** for key terms, document names, and important values. "
        "Use headings (##, ###) to organize long responses. "
        "Keep responses concise and actionable — avoid unnecessary prose and padding. "
        "After completing create, update, or bulk ERP actions, always summarize results in a markdown table with columns like: "
        "Document Name, Type, Status/Company, and any errors. "
        "Include final counts (Created: N, Failed: N, Total: N) after the table when applicable. "
        "For error responses, clearly state what failed, why, and what the user can do next. "
        "Never dump raw JSON, internal tool payloads, or Python tracebacks in the response unless the user explicitly asks for technical details. "
        "When displaying monetary amounts, always prefix with the currency symbol (e.g. ₱, $, €) if available from the tool result. "
        "When referencing a specific ERP document in a response, include a Frappe Desk link in the format: "
        "/app/Form/<DocType>/<DocumentName> — formatted as a Markdown link so the user can click directly to the record. "
        "For responses that span multiple doctypes or modules, use ## headings to separate sections clearly. "
        "When a request results in zero records, say 'No records found' and suggest possible reasons or alternative filters. "
    )


def _system_doctype_specific_rules() -> str:
    return (
        "DocType-specific ERP rules are mandatory when applicable. "
        "For Material Request, distinguish carefully between select fields and link fields. "
        "material_request_type is a Select field and valid fixed values include Purchase, Material Transfer, Material Issue, Manufacture, and Customer Provided. "
        "custom_request_type is not the same field; if present as a Link field, it must be discovered from PR Type records before use and must never be guessed as Purchase, Sample, Standard, or any other assumed value. "
        "For Material Request creation, always use these EXACT DocType names: "
        "company → Company, set_warehouse → Warehouse, "
        "custom_request_type → PR Type (with space), "
        "custom_pr_section → PRSection (NO space — not PR Section), "
        "items[].item_code → Item. "
        "If Material Request metadata shows that a required linked DocType such as PRSection or PR Type has zero records, "
        "and the user asked for test entries, sample data, or random entries, do not stop at the missing-link warning. "
        "First create the minimum required linked master records in that exact linked DocType, then continue creating the Material Requests. "
        "Only stop and report a blocker if the linked DocType itself does not exist, cannot be read, or cannot be created with the available FAC tools. "
        "When explaining a blocker, use the exact linked DocType name returned by metadata. "
        "Do not rename PRSection to PR Section based on the field label. "
        "NEVER guess DocType names from field labels. "
        "Required fields for every Material Request create call: "
        "naming_series — ALWAYS use EXACTLY this literal string: "
        ".{custom_wh_abbr}..{custom_pr_series_abbrv}.PR-.YY.-.##### "
        "This is the ONLY valid naming_series. "
        "NEVER invent one like PROJ-YYYY-MM-DD, MR-2025, MCY.MTP, or any other format. "
        "NEVER call list_documents for naming_series — it is not a queryable DocType. "
        "material_request_type (Purchase), custom_request_type (from PR Type), "
        "custom_pr_section (from PRSection), company (from Company), "
        "transaction_date (today YYYY-MM-DD), set_warehouse (Warehouse matching company). "
        "Each item row: item_code (from Item), qty (random 1-10), uom (Nos), "
        "warehouse (same as set_warehouse), schedule_date (today + 7 days YYYY-MM-DD). "
        "Never omit schedule_date from item rows — required by ERPNext server validation. "
        "Do not reuse a valid value for one field as if it were valid for a different field. "
        "For export requests such as generate Sales Orders as Excel, PDF, Word, or CSV: "
        "generate_report supports format values of json, csv, and excel only — use this for Excel and CSV exports from standard reports. "
        "For PDF export, use run_python_code with frappe.get_print() to render HTML and frappe.utils.pdf.get_pdf() to convert it, then save to /files/ and return the download URL. "
        "For Word or DOCX export, use run_python_code with the python-docx library (imported as docx) to build a .docx file from fetched ERP data, save to the Frappe /files/ directory, and return the download URL. "
        "For any file export, always fetch the real ERP data first using tools.get_documents() or tools.generate_report() inside run_python_code, then build the file from that real data. "
        "After generating any file, always return the Frappe file URL so the user can download it directly. "
        "If a requested export format is not achievable with available tools, clearly state which formats are supported and suggest the closest alternative. "
        "For Sales Invoice: required fields are customer, posting_date, items[].item_code, items[].qty, items[].rate. "
        "Always discover the customer and item via list_documents before creating. "
        "debit_to is auto-set by ERPNext — do not include it unless explicitly overriding. "
        "For Purchase Invoice: required fields are supplier, bill_no, bill_date, items[].item_code, items[].qty, items[].rate. "
        "credit_to is auto-set — do not include it unless explicitly overriding. "
        "For Sales Order: required fields are customer, transaction_date, delivery_date, items[].item_code, items[].qty, items[].rate. "
        "For Purchase Order: required fields are supplier, transaction_date, items[].item_code, items[].qty, items[].rate. "
        "For Stock Entry: required fields are stock_entry_type, posting_date, items[].item_code, items[].qty, and either s_warehouse or t_warehouse depending on the type. "
        "Valid stock_entry_type values: Material Issue, Material Receipt, Material Transfer, Manufacture, Repack, Send to Subcontractor. "
        "For Payment Entry: required fields are payment_type (Receive/Pay/Internal Transfer), party_type, party, paid_amount, paid_from, paid_to, reference_date. "
        "For Journal Entry: required fields are posting_date, voucher_type, accounts[].account, accounts[].debit_in_account_currency or credit_in_account_currency. "
        "Debits must equal credits — validate totals before calling create_document. "
        "For Employee: required fields are first_name, company, date_of_joining, gender. "
        "For Leave Application: required fields are employee, leave_type, from_date, to_date. "
        "Always verify the leave type exists via list_documents on Leave Type before creating. "
        "For Salary Slip: prefer using run_python_code with frappe.get_doc().save() pattern; do not create salary slips manually field-by-field as ERPNext calculates components automatically. "
        "For Work Order: required fields are production_item, bom_no, qty, company, planned_start_date, wip_warehouse, fg_warehouse. "
        "Always discover a valid BOM for the item first via list_documents on BOM with item filter. "
        "For Asset: required fields are asset_name, asset_category, company, purchase_date, gross_purchase_amount, location. "
    )


def _safe_get_roles(user: str | None = None) -> list[str]:
    target_user = str(user or frappe.session.user or "").strip()
    if not target_user:
        return []
    try:
        roles = frappe.get_roles(target_user) or []
    except Exception:
        return []
    cleaned = sorted({str(role).strip() for role in roles if str(role or "").strip()})
    return cleaned


_DEF_ROUTE_MODULE_MAP = {
    "selling": "Selling",
    "crm": "CRM",
    "buying": "Buying",
    "stock": "Stock",
    "manufacturing": "Manufacturing",
    "accounts": "Accounts",
    "accounting": "Accounts",
    "hr": "HR",
    "payroll": "HR",
    "support": "Support",
    "projects": "Projects",
    "quality": "Quality",
    "assets": "Assets",
}


def _infer_route_module(route: Any) -> str | None:
    route_text = str(route or "").strip().lower()
    if not route_text:
        return None
    for token, module in _DEF_ROUTE_MODULE_MAP.items():
        if f"/{token}" in route_text or route_text.startswith(token):
            return module
    return None


_DEF_DOCTYPE_MODULE_HINTS = {
    "Quotation": "Selling",
    "Sales Order": "Selling",
    "Delivery Note": "Stock",
    "Sales Invoice": "Accounts",
    "Customer": "CRM",
    "Lead": "CRM",
    "Opportunity": "CRM",
    "Purchase Order": "Buying",
    "Purchase Receipt": "Buying",
    "Purchase Invoice": "Accounts",
    "Supplier": "Buying",
    "Material Request": "Stock",
    "Stock Entry": "Stock",
    "Item": "Stock",
    "BOM": "Manufacturing",
    "Work Order": "Manufacturing",
    "Job Card": "Manufacturing",
    "Employee": "HR",
    "Leave Application": "HR",
    "Attendance": "HR",
    "Expense Claim": "HR",
    "Payment Entry": "Accounts",
    "Journal Entry": "Accounts",
    "Project": "Projects",
    "Issue": "Support",
    "Asset": "Assets",
    "Asset Movement": "Assets",
    "Asset Maintenance": "Assets",
    "Asset Repair": "Assets",
    "Salary Slip": "Payroll",
    "Payroll Entry": "Payroll",
    "Appraisal": "HR",
    "Training Event": "HR",
    "Task": "Projects",
    "Timesheet": "Projects",
    "Issue": "Support",
    "Warranty Claim": "Support",
    "Quality Inspection": "Quality",
    "Non-Conformance": "Quality",
    "Serial No": "Stock",
    "Batch": "Stock",
    "Stock Ledger Entry": "Stock",
    "Stock Reconciliation": "Stock",
    "Request for Quotation": "Buying",
    "Supplier Quotation": "Buying",
    "Contact": "CRM",
    "Address": "CRM",
    "Campaign": "CRM",
    "Newsletter": "CRM",
    "Cost Center": "Accounts",
    "Account": "Accounts",
    "Sales Taxes and Charges Template": "Accounts",
    "Purchase Taxes and Charges Template": "Accounts",
    "Price List": "Stock",
    "Item Price": "Stock",
    "UOM": "Stock",
    "Brand": "Stock",
    "Item Group": "Stock",
    "Customer Group": "CRM",
    "Supplier Group": "Buying",
    "Territory": "CRM",
    "Sales Person": "CRM",
    "Department": "HR",
    "Designation": "HR",
    "Leave Type": "HR",
    "Holiday List": "HR",
    "Employee Grade": "HR",
    "Workstation": "Manufacturing",
    "Routing": "Manufacturing",
    "Production Plan": "Manufacturing",
    "Subcontracting Order": "Manufacturing",
}


def _infer_doctype_module(doctype: Any) -> str | None:
    doctype_text = str(doctype or "").strip()
    if not doctype_text:
        return None
    if doctype_text in _DEF_DOCTYPE_MODULE_HINTS:
        return _DEF_DOCTYPE_MODULE_HINTS[doctype_text]
    try:
        module = frappe.db.get_value("DocType", doctype_text, "module")
    except Exception:
        module = None
    module_text = str(module or "").strip()
    return module_text or None


DAILY_TASK_READ_INTENTS = {"read", "report", "export"}
DAILY_TASK_MUTATION_INTENTS = {"create", "update", "workflow", "delete", "bulk", "bulk_create"}


def _assistant_operating_mode(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    intent = _resolved_erp_intent(prompt, context)
    if intent in DAILY_TASK_READ_INTENTS:
        return "ask"
    if intent in DAILY_TASK_MUTATION_INTENTS:
        return "do"
    return "explain"


def _target_doctype_permission_summary(target_doctype: str | None, user: str | None = None) -> dict[str, bool]:
    doctype_text = str(target_doctype or "").strip()
    target_user = str(user or frappe.session.user or "").strip()
    summary = {
        "read": False,
        "create": False,
        "write": False,
        "submit": False,
        "cancel": False,
        "delete": False,
    }
    if not doctype_text or not target_user:
        return summary
    for ptype in tuple(summary.keys()):
        try:
            summary[ptype] = bool(frappe.has_permission(doctype=doctype_text, ptype=ptype, user=target_user))
        except Exception:
            summary[ptype] = False
    return summary


def _build_user_execution_context(prompt: str, context: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    current = dict(context or {})
    user = str(current.get("user") or frappe.session.user or "").strip()
    target_doctype = str(_resolved_prompt_doctype(prompt, current) or "").strip() or None
    route_module = _infer_route_module(current.get("route"))
    doctype_module = _infer_doctype_module(target_doctype or current.get("doctype"))
    roles = _safe_get_roles(user)
    role_names = [role for role in roles if role not in {"All", "Guest"}]
    operational_roles = [
        role for role in role_names
        if role not in {"Desk User", "Employee", "System User"}
    ]
    return {
        "user": user,
        "roles": role_names[:20],
        "operational_roles": operational_roles[:12],
        "target_doctype": target_doctype,
        "target_module": doctype_module or route_module,
        "route_module": route_module,
        "assistant_mode": _assistant_operating_mode(prompt, current),
        "permission_summary": _target_doctype_permission_summary(target_doctype, user=user),
    }


def _system_role_and_scope_rules(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    user_ctx = _build_user_execution_context(prompt, context)
    roles_text = ", ".join(user_ctx.get("operational_roles") or user_ctx.get("roles") or []) or "none detected"
    perms = user_ctx.get("permission_summary") or {}
    return (
        "Whole-ERP daily-use rules are mandatory. "
        "Treat this assistant as a role-aware ERP copilot for day-to-day work across Sales, Buying, Stock, Accounts, HR, Projects, Support, Manufacturing, and related modules. "
        "Use the current user, roles, active route, and current document context to decide how much power to use. "
        f"Current user roles: {roles_text}. "
        f"Resolved working module: {user_ctx.get('target_module') or user_ctx.get('route_module') or 'unknown'}. "
        f"Assistant mode for this request: {user_ctx.get('assistant_mode')}. "
        "Prefer the smallest safe action that still completes the user task. "
        "For create/update/workflow requests, default to draft-safe behavior first, then summarize what was prepared or changed. "
        "Never perform delete, cancel, or irreversible actions unless the request is explicit and the FAC tools confirm success. "
        "If the user lacks permission for the requested DocType action, do not attempt to bypass security; explain the limitation and offer the nearest read-only or draft-safe alternative. "
        f"Resolved DocType permission summary: read={perms.get('read')}, create={perms.get('create')}, write={perms.get('write')}, submit={perms.get('submit')}, cancel={perms.get('cancel')}, delete={perms.get('delete')}. "
        "When a current document is open, use it as the default task context before asking the user to repeat information. "
        "For daily-task requests, prefer concise operational answers: what is pending, what was done, what needs approval next, and any direct links or document names available. "
        "If the user's roles include Accounts Manager, Sales Manager, Purchase Manager, or HR Manager, "
        "they are likely approvers — offer to show pending approvals relevant to their role when context suggests it. "
        "When showing records that need attention (overdue invoices, pending leave, open orders), always include document name, "
        "party name, date, and amount/value so the user can act immediately without a follow-up query. "
        "For audit or compliance requests (who changed X, history of Y), guide the user to the Version DocType or frappe.get_doc().get_doc_before_save() approach via run_python_code. "
    )


def _erp_tool_system_prompt(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    current = context or {}
    sample_data_mode = _is_sample_data_request(prompt, current)
    bulk_mode = _is_bulk_operation_request(prompt, current)
    multi_step_plan_mode = _needs_multi_step_plan(prompt, current)
    target_doctype = _resolved_prompt_doctype(prompt, current)
    intent = _resolved_erp_intent(prompt, current)
    user_execution_context = _build_user_execution_context(prompt, current)

    # ── Base sections — always included ─────────────────────────────────────
    lines = [
        _system_identity_rules(),
        _system_tool_use_rules(),
        _system_erp_fac_rules(),
        _system_resource_rules(),
        _system_tool_trigger_rules(),
        _system_response_format_rules(),
        _system_role_and_scope_rules(prompt, current),
        _tool_catalog_summary(prompt, current),
        _resource_catalog_summary(prompt, current),
        _tool_access_summary(prompt, current),
    ]

    # ── Conditional sections — only when relevant ────────────────────────────
    # Data-validation rules: only needed for write/create/update/workflow/bulk
    # intents where the risk of bad field values is real.  Skipping for reads
    # and reports saves ~1 KB on the majority of queries.
    _write_intents = {"create", "update", "workflow", "bulk", "bulk_create", "delete"}
    if intent in _write_intents or _has_write_intent(prompt):
        lines.append(_system_data_validation_rules())

    # Bulk-execution rules: only when the prompt is actually a bulk operation.
    # (They are also dynamically appended below with a document count, so the
    # static copy in the base list was redundant on non-bulk requests.)
    if bulk_mode:
        lines.append(_system_bulk_execution_rules())

    # Doctype-specific rules: only when a doctype is already resolved from
    # context or the prompt.  On General/home views this block is ~2 KB of
    # irrelevant Material-Request / Sales-Invoice guidance.
    if target_doctype or current.get("doctype"):
        lines.append(_system_doctype_specific_rules())

    # ── Dynamic context appends ──────────────────────────────────────────────
    if bulk_mode:
        _bulk_prompt_text = str(prompt or "").strip().lower()
        _bulk_create_verbs = ("create", "add", "insert", "generate", "make", "build")
        if any(v in _bulk_prompt_text for v in _bulk_create_verbs):
            _n_docs = 0
            _m_bulk = re.search(r"\b(\d+)\b", str(prompt or ""))
            if _m_bulk:
                _n_docs = int(_m_bulk.group(1))
            lines.append(
                f"Bulk-CREATE mode: Create exactly {_n_docs if _n_docs else 'the requested number of'} "
                "NEW ERP documents RIGHT NOW — this is a FRESH independent request. "
                "Ignore any previously created documents in conversation history — those are already done. "
                "Step 1: discover real linked values via list_documents with empty filters. "
                "Step 2: use create_document for each document one by one, varying item/qty/remarks. "
                "Step 3: ONLY after ALL documents are created, report results in a summary table. "
                "Do NOT stop after creating 1 document. Create ALL of them before responding. "
                "Each request is completely fresh — never skip based on conversation history."
            )
        else:
            lines.append("Bulk-operation mode is active for this request. Prefer bulk-safe FAC workflows and avoid many repetitive mutation calls.")
    if multi_step_plan_mode:
        lines.append("Multi-step planning mode is active for this request. Internally follow PLAN -> TOOLS -> ANSWER.")
    if target_doctype:
        lines.append(f"Resolved target doctype: {target_doctype}.")
    lines.append(f"Resolved intent: {intent}.")
    lines.append(
        "Execution context: "
        f"user={user_execution_context.get('user')}, "
        f"assistant_mode={user_execution_context.get('assistant_mode')}, "
        f"target_module={user_execution_context.get('target_module')}, "
        f"roles={user_execution_context.get('roles')}"
        "."
    )
    lines.append(f"Current context: doctype={current.get('doctype')}, docname={current.get('docname')}, route={current.get('route')}.")
    return " ".join(line.strip() for line in lines if str(line or "").strip())



def _request_focus_summary(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    current = context or {}
    lines = ["Resolve the user's latest request exactly as written."]
    target_doctype = _resolved_prompt_doctype(prompt, current)
    if target_doctype:
        lines.append(f"Target doctype: {target_doctype}.")
    if _has_active_document_context(current):
        lines.append(
            f"Active ERP record: {current.get('doctype')} {current.get('docname')}."
        )
    elif _has_doctype_context(current):
        lines.append(
            f"Active ERP list or doctype context: {current.get('doctype')}."
        )
    route = str(current.get("route") or "").strip()
    if route:
        lines.append(f"Current route: {route}.")
    if _has_write_intent(prompt):
        lines.append("This is a write/mutation request. Prefer completing the action in ERP over giving advice.")
    elif _is_erp_intent(prompt, current):
        lines.append("This is a live ERP request. Ground the answer in FAC tools and their results.")
    lines.append(f"Resolved intent: {_resolved_erp_intent(prompt, current)}.")
    if _is_bulk_operation_request(prompt, current):
        lines.append("This is a bulk operation request. Prefer batched execution or clearly state the bulk-tool limitation.")
    if _needs_multi_step_plan(prompt, current):
        lines.append("This request is multi-step. Internally do PLAN -> TOOLS -> ANSWER and keep the plan hidden unless the user asks for it.")
    return " ".join(lines)


def _llm_user_prompt(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    cleaned = str(prompt or "").strip()
    resource_manifest = _resource_runtime_manifest(cleaned, context)
    manifest_block = f"{resource_manifest}\n\n" if resource_manifest else ""
    return (
        f"{_request_focus_summary(cleaned, context)}\n\n"
        f"{manifest_block}"
        "User request:\n"
        f"{cleaned}\n\n"
        "Format your response using clean Markdown: use fenced code blocks for code/JSON/SQL/scripts, "
        "tables for lists of records or comparisons, bullet/numbered lists for steps and options, "
        "and **bold** for document names and key values. "
        "Use the live FAC tool catalog when the request needs live ERP data or actions. "
        "For complex requests, first make a short hidden plan, then call tools, then answer. "
        "If the request is instructional only, answer directly unless live verification is necessary. "
        "Ask only for the minimum missing information if the request is blocked. "
        "Summarize tool results clearly using Markdown formatting. "
        "Do not reveal internal reasoning, raw tool output, JSON, XML, or hidden tags such as <think>. "
        "Never generate fictional, placeholder, or invented ERP data — always use live FAC tools to fetch and create real values. "
        # ── Accuracy rules (mirrors verify-pass) ──────────────────────────────
        "ACCURACY RULES: "
        "(1) State which FAC tool produced each fact you report (e.g. 'list_documents returned 5 invoices'). "
        "(2) Quote all numbers, totals, amounts, and dates VERBATIM from tool output — never round or estimate. "
        "(3) If a value was not present in any tool result, say so explicitly rather than inferring it. "
        "Prior [Model-only] history messages should be treated as unverified context, not as authoritative ERP data."
    ).strip()



def _tool_priority_key(name: str) -> tuple[int, str]:
    lowered = str(name or "").strip().lower()
    if lowered in FAC_PREFERRED_TOOL_NAMES:
        return (-1, lowered)
    if lowered in CORE_DOC_TOOL_NAMES:
        return (0, lowered)
    if lowered in REPORT_TOOL_NAMES:
        return (1, lowered)
    if lowered in LEGACY_INTERNAL_TOOL_NAMES:
        return (4, lowered)
    if "erp" in lowered:
        return (5, lowered)
    return (3, lowered)


def _prefer_fac_core_tools(names: set[str]) -> set[str]:
    available = {str(name or "").strip() for name in names if str(name or "").strip()}
    if not available:
        return set()

    preferred_pairs = (
        ("get_document", "get_erp_document"),
        ("list_documents", "list_erp_documents"),
        ("create_document", "create_erp_document"),
        ("update_document", "update_erp_document"),
        ("get_doctype_info", "get_doctype_fields"),
        ("get_doctype_info", "describe_erp_schema"),
        ("search_documents", "search_erp_documents"),
        ("submit_document", "submit_erp_document"),
        ("run_workflow", "run_workflow_action"),
    )
    for preferred, legacy in preferred_pairs:
        if preferred in available and legacy in available:
            available.discard(legacy)
    return available


def _prioritize_tool_specs(tool_specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        tool_specs,
        key=lambda spec: _tool_priority_key(
            spec.get("name")
            or ((spec.get("function") or {}).get("name") if isinstance(spec.get("function"), dict) else "")
        ),
    )


def _tool_access_summary(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
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


def _system_resource_rules() -> str:
    return (
        "Resource-use rules are mandatory. "
        "Treat FAC resources like Claude Desktop workspace context: read them to ground the session before guessing. "
        "Prefer current_page_context for route and user scope, current_document for the active ERP record, "
        "doctype_schema for field structure and required values, and pending_assistant_action for resumable clarifications. "
        "Use resource contents as read-only grounding, then call FAC tools for mutations, reports, searches, and workflow actions. "
        "Do not ask the user to restate information that is already available in session resources. "
        "current_page_context: use to identify which module/doctype/docname the user is currently viewing. "
        "current_document: use to read field values of the active ERP record without calling get_document again. "
        "doctype_schema: use to inspect required fields, field types, and link targets before create_document or update_document. "
        "pending_assistant_action: use to resume a multi-step task the user previously started and did not finish. "
        "available_doctypes: use to verify that a DocType exists in this system before referencing it in tool calls. "
        "If a resource is listed as available but its content is empty or stale, fall back to the equivalent FAC tool call. "
    )


def _resource_catalog_summary(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    specs = list_resource_specs()
    if not specs:
        return "No FAC resources are available in this session."

    current = context or {}
    available = {str(spec.get('name') or '').strip(): spec for spec in specs if str(spec.get('name') or '').strip()}
    preferred: list[str] = []
    for name in ("current_page_context", "current_document", "doctype_schema", "pending_assistant_action", "available_doctypes"):
        if name not in available:
            continue
        if name == "current_document" and not _has_active_document_context(current):
            continue
        if name == "doctype_schema" and not (_resolved_prompt_doctype(prompt, current) or current.get("doctype")):
            continue
        if name == "pending_assistant_action" and not current.get("conversation"):
            continue
        preferred.append(name)

    preview: list[str] = []
    for spec in specs[:8]:
        name = str(spec.get("name") or "").strip()
        title = str(spec.get("title") or name).strip()
        if name:
            preview.append(f"{name} ({title})")

    lines = [
        f"Live FAC resources available in this session: {len(specs)}.",
        f"Resource catalog preview: {'; '.join(preview)}.",
    ]
    if preferred:
        lines.append(f"Best-fit resources for this request: {', '.join(preferred)}.")
    return " ".join(lines)


def _schema_field_hints(fields_payload: Any) -> str:
    rows = fields_payload if isinstance(fields_payload, list) else []
    if not rows:
        return "No field metadata was returned"

    required: list[str] = []
    linked: list[str] = []
    child_tables: list[str] = []
    preview: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        fieldname = str(row.get("fieldname") or "").strip()
        fieldtype = str(row.get("fieldtype") or "").strip()
        options = str(row.get("options") or "").strip()
        if not fieldname:
            continue
        if len(preview) < 8:
            preview.append(f"{fieldname}:{fieldtype or 'Data'}")
        if row.get("reqd") and len(required) < 8:
            required.append(fieldname)
        if fieldtype == "Link" and len(linked) < 8:
            linked.append(f"{fieldname}->{options or '?'}")
        if fieldtype == "Table" and len(child_tables) < 5:
            child_tables.append(f"{fieldname}->{options or '?'}")

    parts: list[str] = []
    if required:
        parts.append(f"required fields: {', '.join(required)}")
    if linked:
        parts.append(f"link fields: {', '.join(linked)}")
    if child_tables:
        parts.append(f"child tables: {', '.join(child_tables)}")
    if preview:
        parts.append(f"field preview: {', '.join(preview)}")
    return "; ".join(parts) if parts else "Field metadata was present but could not be summarized"


def _compact_resource_snapshot(name: str, payload: Any) -> str:
    if not isinstance(payload, dict):
        return f"{name}: unavailable."

    data = payload.get("data")
    if name == "current_page_context" and isinstance(data, dict):
        return (
            "current_page_context: "
            f"doctype={data.get('doctype')}, docname={data.get('docname')}, route={data.get('route')}, "
            f"user={data.get('user')}, target_doctype={data.get('target_doctype')}."
        )

    if name == "current_document" and isinstance(data, dict):
        document = data.get("document") if isinstance(data.get("document"), dict) else {}
        highlights: list[str] = []
        for key in ("name", "docstatus", "status", "workflow_state", "company", "customer", "supplier", "posting_date", "transaction_date"):
            value = document.get(key)
            if value not in (None, "", []):
                highlights.append(f"{key}={value}")
        summary = ", ".join(highlights[:8]) if highlights else "document loaded"
        return f"current_document: {summary}."

    if name == "doctype_schema" and isinstance(data, dict):
        schema = data.get("schema") if isinstance(data.get("schema"), dict) else {}
        fields = data.get("fields")
        schema_doctype = str(schema.get("doctype") or schema.get("name") or "").strip()
        prefix = f"doctype_schema: {schema_doctype}. " if schema_doctype else "doctype_schema: "
        return prefix + _schema_field_hints(fields) + "."

    if name == "pending_assistant_action" and isinstance(data, dict):
        pending = data.get("pending_action")
        if isinstance(pending, dict) and pending:
            action = str(pending.get("action") or pending.get("type") or "pending").strip()
            missing = pending.get("missing_fields") if isinstance(pending.get("missing_fields"), list) else []
            suffix = f" missing_fields={missing[:6]}" if missing else ""
            return f"pending_assistant_action: {action}.{suffix}"
        return "pending_assistant_action: none."

    return f"{name}: available."


def _resource_runtime_manifest(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    current = dict(context or {})
    user = str(current.get("user") or frappe.session.user or "").strip() or None
    names: list[str] = ["current_page_context"]
    target_doctype = _resolved_prompt_doctype(prompt, current) or str(current.get("doctype") or "").strip() or None
    if _has_active_document_context(current):
        names.append("current_document")
    if target_doctype:
        names.append("doctype_schema")
    if current.get("conversation"):
        names.append("pending_assistant_action")

    snapshots: list[str] = []
    for name in names:
        arguments = {"doctype": target_doctype} if name == "doctype_schema" and target_doctype else {}
        if name == "pending_assistant_action" and current.get("conversation"):
            arguments["conversation"] = current.get("conversation")
        try:
            payload = read_fac_resource(name, context=current, arguments=arguments, user=user)
        except Exception as exc:
            snapshots.append(f"{name}: unavailable ({str(exc)[:120]}).")
            continue
        snapshots.append(_compact_resource_snapshot(name, payload))

    if not snapshots:
        return ""
    return "Workspace manifest:\n" + "\n".join(f"- {line}" for line in snapshots)


def _verification_prompt(tool_events: list[str]) -> str:
    recent = tool_events[-8:]
    recent_lines = "\n".join(f"- {event}" for event in recent) or "- none"
    return (
        "Verification pass — review your answer against the tool evidence before replying.\n\n"
        "Rules you MUST follow:\n"
        "1. **Numeric precision**: Every number, total, count, amount, quantity, or date in your response "
        "must be quoted VERBATIM from the tool output above. Do NOT round, estimate, or paraphrase numeric values.\n"
        "2. **Cite your source**: For every factual claim, state which tool produced it "
        '(e.g. "list_documents returned 12 open orders" not just "there are 12 open orders").\n'
        "3. **No inference on absent data**: If a field, value, or record was NOT present in any tool result, "
        "say so explicitly. Do NOT invent, extrapolate, or assume values that were not returned.\n"
        "4. **Correct prior contradictions**: If your earlier response in this conversation conflicts with the "
        "tool results you now have, the tool results take priority. Correct the discrepancy explicitly.\n\n"
        "Action:\n"
        "- If evidence is insufficient or inconsistent → call the missing tools now.\n"
        "- If evidence is complete → return the final verified answer, following all 4 rules above.\n\n"
        "Recent tool results:\n"
        f"{recent_lines}"
    )



def _no_tools_available_response(prompt: str, context: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    current = context or {}
    target = str(current.get("doctype") or "").strip()
    if _has_active_document_context(current):
        target = f"{current.get('doctype')} {current.get('docname')}".strip()
    elif target:
        target = f"{target} record"
    request_hint = f" for {target}" if target else ""
    return {
        "text": (
            "I could not complete that ERP request because no FAC tools are available in this session"
            f"{request_hint}. Configure or expose the required ERP create/read/update tools, then retry."
        ),
        "tool_events": [],
        "payload": None,
    }


def _stray_tool_call_response() -> dict[str, Any]:
    return {
        "text": (
            "I could not complete that ERP request because the current AI provider kept returning tool-call output "
            "even when no callable tools were available for this session. No ERP record was created."
        ),
        "tool_events": [],
        "payload": None,
    }


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
        "conversation": conversation,
    }
    progress["partial_text"] = payload.get("partial_text")
    expires_in_sec = 300 if done else 900
    frappe.cache().set_value(
        _progress_cache_key(conversation, user),
        json.dumps(payload, default=str),
        expires_in_sec=expires_in_sec,
    )
    # ── Realtime push (Socket.IO) ────────────────────────────────────────────
    # Publish to the specific user so the frontend can react instantly without
    # waiting for the next poll tick.  The Redis cache write above is the
    # fallback for clients that are polling but not yet subscribed.
    try:
        frappe.publish_realtime(
            event="erp_ai_progress",
            message=payload,
            user=user,
            after_commit=False,
        )
    except Exception:
        pass  # realtime is best-effort; polling fallback remains active




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
    _audit_timer = PromptTimer()
    _audit_timer.__enter__()
    _audit_error: str = ""

    try:
        result = _execute_prompt_inner(
            prompt=prompt_text,
            conversation=conversation,
            doctype=doctype,
            docname=docname,
            route=route,
            model=model,
            images=images,
            user=effective_user,
        )
        return result
    except Exception as exc:
        _audit_error = str(exc)[:1000]
        raise
    finally:
        _audit_timer.__exit__(None, None, None)
        try:
            _audit_result = locals().get("result") or {}
            _tools_used = [
                ev.get("tool") for ev in (_audit_result.get("tool_events") or [])
                if isinstance(ev, dict) and ev.get("tool")
            ]
            _affected = [
                {"doctype": ev.get("doctype"), "name": ev.get("name")}
                for ev in (_audit_result.get("tool_events") or [])
                if isinstance(ev, dict) and ev.get("name")
            ]
            write_audit_log(
                user=effective_user or frappe.session.user,
                conversation=conversation,
                prompt=prompt_text,
                tools_used=_tools_used,
                affected_records=_affected,
                tokens_in=0,
                tokens_out=0,
                duration_ms=_audit_timer.elapsed_ms,
                provider=get_active_provider(),
                model=str(model or ""),
                route=str(route or ""),
                doctype_context=str(doctype or ""),
                docname_context=str(docname or ""),
                error_message=_audit_error,
                confirmed_destructive=False,
            )
        except Exception:
            pass


def _execute_prompt_inner(
    *,
    prompt: str = "",
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
    user: str | None = None,
) -> dict[str, Any]:
    prompt_text = prompt
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
    frappe.db.commit()

    target_doctype = doctype or _resolved_prompt_doctype(prompt_text, {"doctype": doctype, "docname": docname, "route": route, "user": frappe.session.user})
    context = {
        "doctype": doctype,
        "docname": docname,
        "route": route,
        "conversation": conversation_name,
        "user": frappe.session.user,
        "target_doctype": target_doctype,
        "route_module": _infer_route_module(route),
        "target_module": _infer_doctype_module(target_doctype or doctype),
        "user_roles": _safe_get_roles(frappe.session.user),
        "permission_summary": _target_doctype_permission_summary(target_doctype, user=frappe.session.user),
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
    attachments["copilot"] = build_copilot_package(prompt=prompt_text, context=context, payload=response.get("payload"), reply_text=response.get("text"))
    reply_text = _finalize_reply_text(
        response["text"],
        prompt_text,
        attachments,
        payload=response.get("payload"),
        all_payloads=response.get("all_payloads"),
    )
    # Guard: if finalization produced an empty string (e.g. LLM returned only
    # a filler opener that got stripped, or verify-pass returned nothing),
    # synthesize a safe fallback so the frontend never shows a blank bubble.
    if not reply_text or not reply_text.strip():
        _intent = _resolved_erp_intent(prompt_text or "", context)
        if _intent in {"update", "create", "workflow", "delete"}:
            reply_text = "Done. The record has been updated successfully."
        else:
            reply_text = "The request was completed. No additional details were returned."
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
    frappe.db.commit()
    _progress_update(progress, stage="completed", done=True, step="Response ready", partial_text=reply_text)

    result = {
        "conversation": conversation_name,
        "reply": reply_text,
        "tool_events": response.get("tool_events", []),
        "payload": response.get("payload"),
        "attachments": attachments,
        "context": context,
        "debug": _build_debug_payload(),
    }
    _set_prompt_result(
        conversation_name,
        frappe.session.user,
        {
            "status": "completed",
            "done": True,
            "conversation": conversation_name,
            "reply": reply_text,
            "debug": result.get("debug"),
        },
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

    # ── Rate limit check (per-user hourly quota) ──────────────────────────────
    check_rate_limit(frappe.session.user)

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
    # ── Mandatory context injection warning ───────────────────────────────────
    # If the frontend sends no route and no doctype, log a warning so developers
    # know to pass page context. The request still proceeds but with degraded
    # context awareness.
    if not route and not doctype:
        frappe.log_error(
            "erp_ai_assistant: send_prompt called without route or doctype context. "
            "Update the frontend to pass doctype, docname, and route on every request.",
            "ERP AI Assistant: Missing Context"
        )
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
    formats = _requested_export_formats(prompt)
    # Do not generate any file exports unless the user explicitly asked for one.
    # An empty formats list means no export keywords were found — do not fall
    # through to create_message_artifacts which defaults to generating all formats
    # (causing unwanted xlsx/csv/pdf/docx files on simple show/find/list prompts).
    if not formats:
        return {"attachments": [], "exports": {}}
    # Skip export generation for single-record lookups even if the prompt
    # accidentally contains a trigger word (e.g. "show me employee named ... file").
    if _is_single_record_payload(payload):
        return {"attachments": [], "exports": {}}
    try:
        return create_message_artifacts(
            payload=payload,
            title=title,
            formats=formats,
        )
    except Exception:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Artifact Generation Error")
        return {"attachments": [], "exports": {}}


def _is_single_record_payload(payload: Any) -> bool:
    """Return True when the payload looks like a single ERP document rather than a list or report."""
    if isinstance(payload, dict):
        data = payload.get("data")
        result = payload.get("result")
        # Plain document dict from get_document — no 'data' or 'result' list wrapper
        if data is None and result is None:
            return True
        # Wrapped single document: {"data": {...}}
        if isinstance(data, dict):
            return True
    return False


def _merge_attachment_packages(*packages: Any) -> dict[str, Any]:
    merged = {"attachments": [], "exports": {}, "copilot": {}}
    for package in packages:
        if not isinstance(package, dict):
            continue
        attachments = package.get("attachments") or []
        exports = package.get("exports") or {}
        copilot = package.get("copilot") or {}
        if isinstance(attachments, list):
            merged["attachments"].extend(item for item in attachments if isinstance(item, dict))
        if isinstance(exports, dict):
            merged["exports"].update(exports)
        if isinstance(copilot, dict):
            merged["copilot"].update(copilot)
    return merged


def _finalize_reply_text(text: str, prompt: str, attachments: dict[str, Any], payload: Any = None, all_payloads: list[Any] | None = None) -> str:
    attachment_rows = attachments.get("attachments") or []
    if attachment_rows and _requested_export_formats(prompt) and any(item.get("export_id") for item in attachment_rows):
        labels = ", ".join(str(item.get("label") or item.get("file_type") or "file").strip() for item in attachment_rows[:3])
        base_text = f"Prepared your export. Use the downloadable attachment below{f' ({labels})' if labels else ''}."
    else:
        base_text = _sanitize_assistant_reply(text)
    return _append_related_links(base_text, prompt, payload, all_payloads=all_payloads)


def _sanitize_assistant_reply(text: Any) -> str:
    cleaned = html.unescape(str(text or "").strip())
    if not cleaned:
        return ""

    # Remove leaked reasoning or planning tags from providers that expose hidden traces.
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<analysis>[\s\S]*?</analysis>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<reasoning>[\s\S]*?</reasoning>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<tool_call>[\s\S]*?</tool_call>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<TOOLCALL>[\s\S]*?</TOOLCALL>", "", cleaned)
    cleaned = re.sub(r"<next_steps>[\s\S]*?</next_steps>", "", cleaned, flags=re.IGNORECASE)
    # Only strip the filler opener word/phrase itself (up to the first comma, period, or newline),
    # NOT the entire paragraph — the original greedy regex could wipe the whole response
    # if the LLM gave a short single-paragraph answer like "Alright, I updated the record."
    cleaned = re.sub(r"^\s*(okay[,!.]?|alright[,!.]?|sure[,!.]?|of course[,!.]?)\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip()

    if not cleaned:
        return "I'm ready to continue. Please resend the request in one line."

    return cleaned


def _requested_export_formats(prompt: str) -> list[str]:
    text = (prompt or "").lower()
    export_terms = [
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
        "generate file",
        "generate excel",
        "generate pdf",
        "generate report",
        "attach file",
        "send as file",
        "tabular",
        "printable",
        "print report",
    ]
    if not any(term in text for term in export_terms):
        return []

    formats: list[str] = []
    format_hints = {
        "xlsx": ["excel", ".xlsx", "xlsx", "spreadsheet", "on excel", "into excel"],
        "csv": ["csv", ".csv"],
        "pdf": ["pdf", ".pdf"],
        "docx": ["word", "docx", ".docx", "document file"],
    }
    for export_format, hints in format_hints.items():
        if any(term in text for term in hints) and export_format not in formats:
            formats.append(export_format)

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
    text = str(prompt or "").strip()
    if not text:
        return None

    patterns = (
        r"\bdoctype\s+([A-Z][A-Za-z0-9_/& -]{2,60})",
        r"\b(?:create|new|add|insert|update|edit|modify|show|open|list|find|search|submit|cancel|approve|reject)\s+(?:a\s+|an\s+|the\s+)?([A-Z][A-Za-z0-9_/& -]{2,60})",
    )
    stop_terms = (
        " with ",
        " for ",
        " from ",
        " in ",
        " on ",
        " by ",
        " where ",
        " that ",
        " which ",
        " using ",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = str(match.group(1) or "").strip(" .,:;!?")
        lowered = candidate.lower()
        for stop_term in stop_terms:
            if stop_term in lowered:
                candidate = candidate[: lowered.index(stop_term)].strip(" .,:;!?")
                lowered = candidate.lower()
        if candidate and len(candidate.split()) <= 6:
            return candidate
    return None


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


def _extract_discovery_doctype_names(payload: Any) -> set[str]:
    names: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key in ("data", "result", "contents", "message"):
                nested = value.get(key)
                if isinstance(nested, (dict, list)):
                    visit(nested)
            name = str(value.get("name") or value.get("doctype") or "").strip()
            if name:
                names.add(name)
            return

        if isinstance(value, list):
            for row in value:
                visit(row)

    visit(payload)
    return names


def _get_discovery_doctypes() -> set[str]:
    cached = getattr(frappe.local, "_erp_ai_discovery_doctypes", None)
    if isinstance(cached, set) and cached:
        return cached

    user = str(getattr(frappe.session, "user", "") or "").strip() or None

    try:
        resource_payload = read_fac_resource(
            "available_doctypes",
            context={"user": user} if user else {},
            arguments={"limit": 500},
            user=user,
        )
        discovered = _extract_discovery_doctype_names(resource_payload)
        if discovered:
            frappe.local._erp_ai_discovery_doctypes = discovered
            return discovered
    except Exception:
        pass

    try:
        tool_definitions = get_tool_definitions(user=user)
        if "list_erp_doctypes" in tool_definitions:
            tool_payload = dispatch_tool("list_erp_doctypes", {"limit": 500}, user=user)
            discovered = _extract_discovery_doctype_names(tool_payload)
            if discovered:
                frappe.local._erp_ai_discovery_doctypes = discovered
                return discovered
    except Exception:
        pass

    fallback = {
        "Company", "Warehouse", "Item", "PR Type", "PRSection",
        "Customer", "Supplier", "Employee", "User", "Item Group",
    }
    frappe.local._erp_ai_discovery_doctypes = fallback
    return fallback


def _build_debug_payload() -> dict[str, Any]:
    return {
        "discovery_doctypes": sorted(_get_discovery_doctypes()),
    }


def _append_related_links(text: str, prompt: str, payload: Any, all_payloads: list[Any] | None = None) -> str:
    base = str(text or "").strip()
    combined_targets: list[tuple[str, str, str]] = []
    seen_labels: set[str] = set()
    _discovery_doctypes = _get_discovery_doctypes()

    def _is_mutation_payload(p: Any) -> bool:
        if not isinstance(p, dict):
            return False
        result = p.get("result") or p
        if isinstance(result, dict):
            doctype = str(result.get("doctype") or "").strip()
            name = str(result.get("name") or "").strip()
            if doctype and name and doctype not in _discovery_doctypes:
                return True
            doc = result.get("document") or {}
            if isinstance(doc, dict):
                dt = str(doc.get("doctype") or "").strip()
                nm = str(doc.get("name") or "").strip()
                if dt and nm and dt not in _discovery_doctypes:
                    return True
        return False

    def _merge(targets: list[tuple[str, str, str]]) -> None:
        for entry in targets:
            if entry[0] not in seen_labels:
                seen_labels.add(entry[0])
                combined_targets.append(entry)

    if all_payloads:
        mutation_payloads = [p for p in all_payloads if _is_mutation_payload(p)]
        if mutation_payloads:
            for p in mutation_payloads:
                _merge(_extract_link_targets_from_payload(p, prompt))
        else:
            _merge(_extract_link_targets_from_payload(payload, prompt))
    else:
        _merge(_extract_link_targets_from_payload(payload, prompt))

    if not combined_targets:
        return base

    # Only show "Open links" for read/lookup responses (show, get, find).
    # For update, create, delete, and workflow responses the LLM already
    # mentions the document name in its reply — the extra link block is
    # redundant noise and confuses users into thinking it is a separate result.
    _prompt_lower = str(prompt or "").lower()
    _write_verbs = ("update", "edit", "change", "modify", "set ", "create", "add ", "delete",
                    "remove", "submit", "cancel", "approve", "reject", "amend")
    if any(v in _prompt_lower for v in _write_verbs):
        return base

    return base


def _is_erp_intent(prompt: str, context: dict[str, Any]) -> bool:
    text = (prompt or "").strip().lower()
    if _is_instructional_request(prompt) or _is_sample_data_request(prompt, context):
        return False

    # ── Fast paths ────────────────────────────────────────────────────────────
    # Active document context → always ERP regardless of wording
    if _has_active_document_context(context):
        return True
    if _resolved_prompt_doctype(prompt, context):
        return True

    if not text:
        return False

    # ── Document reference codes (PO-XXXX, SI-XXXX, etc.) ────────────────────
    # Users type these directly without any ERP noun context.
    import re as _re_intent
    doc_code_pattern = (
        r"\b(?:po|so|si|pi|pr|dn|qtn|mr|mtn|hr|lr|jv|pe|acc|inv|crm|ser|bat)"
        r"[-/]\d{4,}"
    )
    if _re_intent.search(doc_code_pattern, text):
        return True

    # ── Indirect / conversational ERP phrasing ───────────────────────────────
    indirect_phrases = (
        "pull up", "can you check", "can you get", "can you find", "can you show",
        "look up", "look for", "tell me about", "what is the", "what are the",
        "how many", "how much", "what's the status", "what's the balance",
        "give me", "fetch", "retrieve", "display", "open the", "show the",
        "outstanding", "balance", "overdue", "aging", "pending", "due date",
        "total", "amount", "quantity", "available", "on hand",
    )
    if any(phrase in text for phrase in indirect_phrases):
        # Only treat as ERP if there's also a data noun nearby
        erp_nouns_short = (
            "customer", "supplier", "employee", "item", "invoice", "order",
            "quotation", "payment", "balance", "stock", "warehouse", "report",
            "sales", "purchase", "account", "record",
        )
        if any(noun in text for noun in erp_nouns_short):
            return True

    # ── Follow-up pronoun queries (short completions after context is set) ────
    # e.g. "And that invoice?", "What about it?", "Show me those records"
    tokens = text.split()
    if len(tokens) <= 8:
        follow_up_pronouns = ("it", "that", "this", "those", "these", "the document",
                              "the record", "the invoice", "the order", "the entry")
        if any(phrase in text for phrase in follow_up_pronouns):
            if context.get("doctype") or context.get("route"):
                return True

    # ── Expanded ERP keyword list ─────────────────────────────────────────────
    erp_markers = (
        "customer", "supplier", "employee", "item", "invoice", "order",
        "quotation", "lead", "opportunity", "report", "dashboard", "chart",
        "doctype", "workflow", "erp", "sales", "purchase", "stock",
        "accounts", "hr",
        "material request", "material transfer", "material issue",
        "stock entry", "journal entry", "payment entry",
        "delivery note", "purchase receipt", "purchase order", "sales order",
        "warehouse", "company", "entries", "records", "documents",
        "create", "update", "submit", "approve", "reject",
        "export", "excel", "pdf", "word", "csv", "download",
        # Additional high-confidence ERP terms
        "ledger", "trial balance", "profit and loss", "balance sheet",
        "payroll", "attendance", "leave", "expense", "asset", "depreciation",
        "batch", "serial no", "bom", "production", "work order",
        "cost center", "project", "task", "timesheet",
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


def _tool_round_limit_response(
    last_tool_name: str | None,
    rendered_payload: Any,
    rendered_feedback: Optional[dict[str, Any]],
    tool_events: list[str],
    max_tool_rounds: int,
) -> dict[str, Any]:
    if rendered_feedback is not None:
        return {
            "text": _render_provider_tool_output(last_tool_name, rendered_feedback),
            "tool_events": tool_events,
            "payload": rendered_payload,
        }
    if rendered_payload is not None:
        return {
            "text": _render_provider_tool_output(last_tool_name, rendered_payload),
            "tool_events": tool_events,
            "payload": rendered_payload,
        }
    # ── Build a state summary from completed tool events ───────────────────
    completed_steps = []
    for raw_event in tool_events:
        try:
            ev = json.loads(raw_event) if isinstance(raw_event, str) else raw_event
            tool_label = str(ev.get("tool") or ev.get("name") or "").strip()
            status = str(ev.get("status") or "ok").strip().lower()
            if tool_label and status == "ok":
                completed_steps.append(tool_label)
        except Exception:
            pass

    if completed_steps:
        step_list = "\n".join(f"  • {s}" for s in completed_steps[-10:])
        summary = (
            f"The request could not be fully completed — the AI reached the maximum "
            f"tool-call limit ({max_tool_rounds} rounds).\n\n"
            f"**Completed steps ({len(completed_steps)}):**\n{step_list}\n\n"
            "The remaining steps were not executed. No further changes were made after the limit was reached.\n\n"
            "**Suggestion:** Break the request into smaller parts, or increase the tool-call limit in AI Provider Settings."
        )
    else:
        summary = (
            f"The request could not be completed — the AI loop reached the configured "
            f"tool-call limit ({max_tool_rounds} rounds) before producing a final answer.\n\n"
            "No ERP records were changed after this point.\n\n"
            "**Suggestion:** Try a more specific prompt, or increase the tool-call limit in AI Provider Settings."
        )
    return {
        "text": summary,
        "tool_events": tool_events,
        "payload": None,
    }



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


def _conversation_history_for_llm(conversation_name: str, limit: int | None = None) -> list[dict[str, Any]]:
    effective_limit = limit if isinstance(limit, int) and limit > 0 else _conversation_history_limit()
    messages = frappe.get_all(
        "AI Message",
        filters={"conversation": conversation_name},
        fields=["role", "content", "attachments_json", "tool_events"],
        order_by="creation desc",
        limit_page_length=max(1, effective_limit),
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
            history.append({"role": "user", "content": history_text})
        elif role == "assistant":
            # ── Grounding tag ─────────────────────────────────────────────────
            # Mark whether this response was backed by live ERP tool results.
            # The LLM uses this to distinguish authoritative data from its own
            # prior inferences, preventing cascading hallucination across turns.
            tool_events_raw = row.get("tool_events") or ""
            try:
                tool_events_list = json.loads(tool_events_raw) if tool_events_raw else []
            except Exception:
                tool_events_list = []
            is_tool_grounded = bool(tool_events_list)
            grounding_prefix = "[Tool-grounded] " if is_tool_grounded else "[Model-only] "
            history.append({"role": "assistant", "content": grounding_prefix + history_text})
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
        arguments = _prepare_export_tool_arguments(tool_name, arguments)
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


def _is_minimal_export_field_request(fields: Any) -> bool:
    if not isinstance(fields, list):
        return True
    normalized = [str(item or "").strip() for item in fields if str(item or "").strip()]
    if not normalized:
        return True
    return set(normalized).issubset({"name", "creation", "modified"})


def _default_export_fields_for_doctype(doctype: str, *, max_fields: int = 24) -> list[str]:
    doctype_name = str(doctype or "").strip()
    if not doctype_name:
        return ["name", "creation", "modified"]
    try:
        meta = frappe.get_meta(doctype_name)
    except Exception:
        return ["name", "creation", "modified"]

    excluded_fieldtypes = {
        "Section Break",
        "Column Break",
        "Tab Break",
        "Fold",
        "HTML",
        "Button",
        "Table",
        "Table MultiSelect",
        "Attach Image",
        "Signature",
        "Image",
        "Geolocation",
        "Code",
        "Text Editor",
    }
    preferred = ["name"]
    title_field = str(getattr(meta, "title_field", "") or "").strip()
    if title_field:
        preferred.append(title_field)
    search_fields = str(getattr(meta, "search_fields", "") or "").strip()
    if search_fields:
        preferred.extend([item.strip() for item in search_fields.split(",") if item.strip()])
    try:
        actual_fieldnames = {
            str(getattr(f, "fieldname", "") or "").strip()
            for f in (getattr(meta, "fields", []) or [])
        }
    except Exception:
        actual_fieldnames = set()
    common_fields = ["status", "docstatus", "company", "customer", "supplier", "posting_date", "transaction_date"]
    for cf in common_fields:
        if cf == "docstatus" or cf in actual_fieldnames:
            preferred.append(cf)

    selected: list[str] = []
    seen: set[str] = set()

    def _push(fieldname: str) -> None:
        value = str(fieldname or "").strip()
        if not value or value in seen:
            return
        selected.append(value)
        seen.add(value)

    for fieldname in preferred:
        _push(fieldname)

    for field in getattr(meta, "fields", []) or []:
        fieldname = str(getattr(field, "fieldname", "") or "").strip()
        fieldtype = str(getattr(field, "fieldtype", "") or "").strip()
        hidden = int(bool(getattr(field, "hidden", 0)))
        if not fieldname or hidden or fieldtype in excluded_fieldtypes:
            continue
        _push(fieldname)
        if len(selected) >= max_fields:
            break

    for fallback in ("creation", "modified"):
        _push(fallback)

    return selected[:max_fields] or ["name", "creation", "modified"]


def _prepare_export_tool_arguments(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(arguments, dict):
        return arguments
    normalized_tool = str(TOOL_NAME_MAP.get(tool_name, tool_name) or "").strip()
    if normalized_tool not in {"get_list", "list_documents", "export_doctype_records"}:
        return arguments

    prepared = dict(arguments)
    doctype = str(prepared.get("doctype") or "").strip()
    if not doctype:
        return prepared

    fields = prepared.get("fields")
    if fields is None and normalized_tool == "export_doctype_records":
        fields = prepared.get("columns")
    if _is_minimal_export_field_request(fields):
        prepared["fields"] = _default_export_fields_for_doctype(doctype)
    if normalized_tool == "export_doctype_records" and not prepared.get("columns"):
        prepared["columns"] = prepared.get("fields")
    return prepared


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
    pending_input: Any = _build_openai_input(history, prompt, context, images)
    tool_events: list[str] = []
    rendered_payload = None
    rendered_feedback = None
    all_payloads: list[Any] = []
    had_tool_calls = False
    verification_requested = False
    tools_enabled = bool(tool_specs)
    if _is_erp_intent(prompt, context) and not tool_specs:
        return _no_tools_available_response(prompt, context)
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
            _progress_update(progress, stage="failed", done=True, error="No callable tools were available for this provider response.")
            return _stray_tool_call_response()

        if not function_calls:
            # Skip verification during bulk-create: it fires too early and cuts off
            # document creation before all N docs are created.
            _skip_verify = _is_bulk_operation_request(prompt, context)
            if had_tool_calls and verify_pass_enabled and not verification_requested and previous_response_id and not _skip_verify:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                pending_input = _verification_prompt(tool_events)
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": _openai_output_text(body) or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
                "all_payloads": all_payloads,
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
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name, tool_input)}")
            try:
                tool_result = _run_tool(tool_name, tool_input, user=context.get("user"))
                _validate_tool_result(tool_name, tool_result)
                tool_feedback = _tool_result_feedback_payload(tool_name, tool_result)
                last_tool_name = tool_name
                rendered_payload = tool_result
                all_payloads.append(tool_result)
                tool_events.append(f"{tool_name} {tool_input}")
                pending_results.append(
                    {
                        "type": "function_call_output",
                        "call_id": tool_call.get("call_id"),
                        "output": json.dumps(tool_feedback, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_feedback = _tool_result_feedback_payload(tool_name, error_payload)
                tool_events.append(f"{tool_name} {tool_input} (error)")
                _progress_update(
                    progress,
                    stage="working",
                    step=f"Tool failed: {_humanize_tool_name(tool_name, tool_input)} — {str(exc) or 'Unknown tool error'}",
                )
                pending_results.append(
                    {
                        "type": "function_call_output",
                        "call_id": tool_call.get("call_id"),
                        "output": json.dumps(tool_feedback, default=str),
                    }
                )

        pending_input = pending_results

    _progress_update(progress, stage="working", step="Preparing response")
    return _tool_round_limit_response(last_tool_name, rendered_payload, rendered_feedback, tool_events, max_tool_rounds)


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

    messages = _build_openai_compatible_messages(history, prompt, context, images)
    tool_specs = _prioritize_tool_specs(_openai_compatible_tool_specs(prompt, context))
    tool_events: list[str] = []
    rendered_payload = None
    rendered_feedback = None
    all_payloads: list[Any] = []
    had_tool_calls = False
    verification_requested = False
    disable_tool_choice = bool(compat_profile.get("disable_tool_choice_by_default"))
    tools_enabled = bool(tool_specs)
    if _is_erp_intent(prompt, context) and not tool_specs:
        return _no_tools_available_response(prompt, context)
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
            _progress_update(progress, stage="failed", done=True, error="No callable tools were available for this provider response.")
            return _stray_tool_call_response()

        if not tool_calls:
            _skip_verify_b = _is_bulk_operation_request(prompt, context)
            if had_tool_calls and verify_pass_enabled and not verification_requested and not _skip_verify_b:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                messages.append({"role": "user", "content": _verification_prompt(tool_events)})
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": text_body or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
                "all_payloads": all_payloads,
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
                    "text": _render_provider_tool_output(last_tool_name, rendered_feedback or rendered_payload),
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
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name, tool_input)}")
            try:
                tool_result = _run_tool(tool_name, tool_input, user=context.get("user"))
                _validate_tool_result(tool_name, tool_result)
                tool_feedback = _tool_result_feedback_payload(tool_name, tool_result)
                last_tool_name = tool_name
                rendered_payload = tool_result
                all_payloads.append(tool_result)
                rendered_feedback = tool_feedback
                tool_events.append(f"{tool_name} {tool_input}")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_feedback = _tool_result_feedback_payload(tool_name, error_payload)
                rendered_feedback = tool_feedback
                tool_events.append(f"{tool_name} {tool_input} (error)")
                _progress_update(
                    progress,
                    stage="working",
                    step=f"Tool failed: {_humanize_tool_name(tool_name, tool_input)} — {str(exc) or 'Unknown tool error'}",
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )

    _progress_update(progress, stage="working", step="Preparing response")
    return _tool_round_limit_response(last_tool_name, rendered_payload, rendered_feedback, tool_events, max_tool_rounds)


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
    tool_definitions = get_tool_definitions(user=context.get("user"))
    tool_specs = [
        {
            "name": name,
            "description": spec["description"],
            "input_schema": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
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
    messages: list[dict[str, Any]] = _build_messages_with_images(history, prompt, context, images)
    tool_events: list[str] = []
    rendered_payload = None
    rendered_feedback = None
    all_payloads: list[Any] = []
    had_tool_calls = False
    verification_requested = False
    tools_enabled = bool(tool_specs)
    if _is_erp_intent(prompt, context) and not tool_specs:
        return _no_tools_available_response(prompt, context)
    tool_choice_fallback_applied = False
    disable_tool_choice = False
    seen_tool_signatures: set[str] = set()
    plain_text_retry_requested = False
    last_tool_name: str | None = None

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
            _skip_verify_c = _is_bulk_operation_request(prompt, context)
            if had_tool_calls and verify_pass_enabled and not verification_requested and not _skip_verify_c:
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
                "all_payloads": all_payloads,
            }

        current_signatures = [
            _tool_call_signature(str(tool_use.get("name") or "").strip(), tool_use.get("input", {}))
            for tool_use in tool_uses
        ]
        if current_signatures and all(signature in seen_tool_signatures for signature in current_signatures):
            if rendered_payload is not None:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": _render_provider_tool_output(last_tool_name, rendered_feedback or rendered_payload),
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                tools_enabled = False
                _progress_update(progress, stage="working", step="Provider repeated the same tool calls; forcing final answer")
                messages.append(
                    {
                        "role": "user",
                        "content": "You already have the tool results. Answer directly in plain text without any more tool calls.",
                    }
                )
                continue

        had_tool_calls = True
        tool_results = []
        for tool_use in tool_uses:
            tool_name = tool_use["name"]
            tool_input = tool_use.get("input", {})
            seen_tool_signatures.add(_tool_call_signature(tool_name, tool_input))
            last_tool_name = tool_name
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name, tool_input)}")
            try:
                tool_result = _run_tool(tool_name, tool_input, user=context.get("user"))
                _validate_tool_result(tool_name, tool_result)
                tool_feedback = _tool_result_feedback_payload(tool_name, tool_result)
                tool_events.append(f"{tool_name} {tool_input}")
                rendered_payload = tool_result
                all_payloads.append(tool_result)
                rendered_feedback = tool_feedback
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_feedback = _tool_result_feedback_payload(tool_name, error_payload)
                rendered_feedback = tool_feedback
                tool_events.append(f"{tool_name} {tool_input} (error)")
                _progress_update(
                    progress,
                    stage="working",
                    step=f"Tool failed: {_humanize_tool_name(tool_name, tool_input)} — {str(exc) or 'Unknown tool error'}",
                )
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "is_error": True,
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )

        messages.append({"role": "user", "content": tool_results})
        _progress_update(progress, stage="working", partial_text="")

    _progress_update(progress, stage="working", step="Preparing response")
    return _tool_round_limit_response(last_tool_name, rendered_payload, rendered_feedback, tool_events, max_tool_rounds)


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
    tool_definitions = get_tool_definitions(user=(context or {}).get("user"))
    specs = [
        {
            "type": "function",
            "name": name,
            "description": spec["description"],
            "parameters": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
    ]
    specs = _prioritize_tool_specs(specs)
    if _cfg_bool("ERP_AI_OPENAI_MCP_ENABLED", False):
        specs.extend(get_remote_mcp_servers())
    return specs


def _openai_compatible_tool_specs(prompt: str, context: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions(user=(context or {}).get("user"))
    specs = [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": spec["description"],
                "parameters": spec["inputSchema"],
            },
        }
        for name, spec in tool_definitions.items()
    ]
    return _prioritize_tool_specs(specs)


def _build_openai_input(
    history: Optional[list[dict[str, Any]]],
    prompt: str,
    context: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
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

    current_content = _build_openai_input_content(_llm_user_prompt(prompt, context), images)
    if current_content:
        if last_user_index >= 0 and images:
            normalized[last_user_index]["content"] = current_content
        elif not normalized or normalized[-1].get("role") != "user" or images:
            normalized.append({"role": "user", "content": current_content})
        else:
            normalized[-1]["content"] = current_content

    return normalized or [{"role": "user", "content": _build_openai_input_content(_llm_user_prompt(prompt, context), images)}]


def _build_openai_compatible_messages(
    history: Optional[list[dict[str, Any]]],
    prompt: str,
    context: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
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

    current_content = _build_openai_compatible_content(_llm_user_prompt(prompt, context), images)
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

    # Handle <TOOLCALL>[{"name":...,"arguments":{...}}]</TOOLCALL> format
    toolcall_array_match = re.search(
        r"<TOOLCALL>\s*(?P<body>\[[\s\S]*?\])\s*</TOOLCALL>",
        unescaped_text,
    )
    if toolcall_array_match:
        body = str(toolcall_array_match.group("body") or "").strip()
        try:
            arr = json.loads(body)
        except Exception:
            arr = None
        if isinstance(arr, list) and arr:
            payload = arr[0] if isinstance(arr[0], dict) else {}
            raw_name = str(payload.get("name") or payload.get("tool_name") or "").strip()
            tool_name_tc = raw_name.split(".")[-1] if raw_name else ""
            normalized_tc = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name_tc, tool_name_tc))
            args_tc = payload.get("arguments") or payload.get("args") or {}
            if not isinstance(args_tc, dict):
                try:
                    args_tc = json.loads(str(args_tc))
                except Exception:
                    args_tc = {}
            if normalized_tc:
                return {
                    "id": f"textual-{normalized_tc}",
                    "type": "function",
                    "function": {
                        "name": normalized_tc,
                        "arguments": json.dumps(args_tc, default=str),
                    },
                }

    json_tool_match = re.search(r"<tool_call>\s*(?P<body>\{[\s\S]*?\})\s*</tool_call>", unescaped_text, re.IGNORECASE)
    if json_tool_match:
        body = str(json_tool_match.group("body") or "").strip()
        try:
            payload = json.loads(body)
        except Exception:
            payload = None
        if isinstance(payload, dict):
            raw_name = str(payload.get("tool_name") or payload.get("name") or "").strip()
            tool_name = raw_name.split(".")[-1] if raw_name else ""
            normalized_name = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name, tool_name))
            arguments = payload.get("args")
            if not isinstance(arguments, dict):
                arguments = payload.get("arguments")
            if not isinstance(arguments, dict):
                arguments = {}
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
    history: Optional[list[dict[str, Any]]],
    prompt: str,
    context: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
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
        return rows or [{"role": "user", "content": _llm_user_prompt(prompt, context)}]

    merged_content = _build_user_multimodal_content(_llm_user_prompt(prompt, context), image_blocks)
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


def _humanize_tool_name(name: str, tool_input: Optional[dict[str, Any]] = None) -> str:
    label_map = {
        "get_doctype_info": "Reading DocType structure",
        "list_documents": "Fetching records",
        "get_document": "Reading document",
        "create_document": "Creating document",
        "update_document": "Updating document",
        "submit_document": "Submitting document",
        "run_workflow": "Running workflow action",
        "run_workflow_action": "Running workflow action",
        "run_python_code": "Running bulk operation",
        "generate_report": "Generating report",
        "report_requirements": "Reading report structure",
        "search_link": "Searching linked records",
        "search_documents": "Searching documents",
        "search_doctype": "Searching DocType",
        "extract_file_content": "Reading file",
        "create_dashboard": "Creating dashboard",
        "create_dashboard_chart": "Creating chart",
    }
    base = str(name or "").strip()
    label = label_map.get(base, base.replace("_", " ").capitalize())
    payload = tool_input if isinstance(tool_input, dict) else {}
    target = (
        payload.get("doctype")
        or payload.get("report_name")
        or payload.get("name")
        or payload.get("operation")
        or ""
    )
    target_text = str(target or "").strip()
    if target_text:
        label = f"{label} — {target_text}"
    return label or "Tool"


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


def _run_tool(tool_name: str, arguments: dict[str, Any], user: str | None = None) -> Any:
    mapped_name = TOOL_NAME_MAP.get(tool_name, tool_name)
    if mapped_name in {"list_documents", "export_doctype_records"}:
        arguments = _prepare_export_tool_arguments(tool_name, arguments)
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

    payload = dispatch_tool(mapped_name, arguments, user=user)
    return _enrich_tool_result_with_request_context(mapped_name, arguments, payload)


def _tool_call_signature(tool_name: str, arguments: dict[str, Any]) -> str:
    _mutation_tools = {
        "create_document", "create_erp_document",
        "update_document", "update_erp_document",
        "submit_document", "submit_erp_document",
        "run_workflow", "run_workflow_action",
        "run_python_code",
    }
    normalized = str(tool_name or "").strip()
    if normalized in _mutation_tools:
        import time as _time
        return f"{normalized}::mutation::{_time.time_ns()}"
    return f"{normalized}::{json.dumps(arguments or {}, sort_keys=True, default=str)}"


def _enrich_tool_result_with_request_context(tool_name: str, arguments: dict[str, Any], payload: Any) -> Any:
    normalized_tool = _normalized_tool_name(tool_name)
    if not isinstance(payload, dict):
        return payload
    if payload.get("success") is False:
        return payload
    if normalized_tool not in (DOCUMENT_MUTATION_TOOL_NAMES | WORKFLOW_TOOL_NAMES):
        return payload

    enriched = dict(payload)
    requested_doctype = str(arguments.get("doctype") or "").strip()
    requested_name = str(arguments.get("name") or arguments.get("docname") or arguments.get("document_name") or "").strip()

    if requested_doctype and not str(enriched.get("doctype") or enriched.get("ref_doctype") or "").strip():
        enriched["doctype"] = requested_doctype
    if requested_name and not _document_name_from_payload(enriched):
        enriched["name"] = requested_name

    document = enriched.get("document")
    if isinstance(document, dict):
        document_copy = dict(document)
        if requested_doctype and not str(document_copy.get("doctype") or "").strip():
            document_copy["doctype"] = requested_doctype
        if requested_name and not str(document_copy.get("name") or document_copy.get("docname") or document_copy.get("document_name") or "").strip():
            document_copy["name"] = requested_name
        enriched["document"] = document_copy

    return enriched


def _document_name_from_payload(payload: Any) -> str:
    def _extract_name(value: Any, *, depth: int = 0) -> str:
        if depth > 4 or not isinstance(value, dict):
            return ""
        for key in ("name", "docname", "document_name"):
            current = str(value.get(key) or "").strip()
            if current:
                return current
        message = value.get("message")
        if isinstance(message, dict):
            message_name = _extract_name(message, depth=depth + 1)
            if message_name:
                return message_name
        for key in ("document", "data", "result"):
            nested = value.get(key)
            if isinstance(nested, dict):
                nested_name = _extract_name(nested, depth=depth + 1)
                if nested_name:
                    return nested_name
        return ""

    if not isinstance(payload, dict):
        return ""
    name = _extract_name(payload)
    if name:
        return name
    return ""


def _mutation_has_success_signal(payload: Any, normalized: dict[str, Any]) -> bool:
    if normalized.get("document_name"):
        return True
    if isinstance(payload, dict):
        if payload.get("success") is True:
            return True
        if isinstance(payload.get("updated_fields"), list) and payload.get("updated_fields"):
            return True
        if isinstance(payload.get("fields_validated"), list) and payload.get("fields_validated"):
            return True
        document = payload.get("document")
        if isinstance(document, dict) and any(str(document.get(key) or "").strip() for key in ("name", "docname", "document_name")):
            return True
        for key in ("data", "result"):
            nested = payload.get(key)
            if isinstance(nested, dict) and any(str(nested.get(nested_key) or "").strip() for nested_key in ("name", "docname", "document_name")):
                return True
    data = normalized.get("data")
    if isinstance(data, dict) and data:
        return True
    return False


DOCUMENT_READ_TOOL_NAMES = {"get_document", "find_one_document"}
DOCUMENT_LIST_TOOL_NAMES = {"list_documents", "get_list", "search_documents", "search_link"}
DOCUMENT_MUTATION_TOOL_NAMES = {"create_document", "update_document", "delete_document"}
WORKFLOW_TOOL_NAMES = {"submit_document", "run_workflow"}
DOCTYPE_INFO_TOOL_NAMES = {"get_doctype_info"}
REPORT_TOOL_NAMES_NORMALIZED = {"create_report", "generate_report", "get_report", "report_list", "report_requirements"}
EXPORT_TOOL_NAMES = {"export_report", "export_doctype_records"}


def _compact_tool_payload(payload: Any, *, max_items: int = 5) -> Any:
    rows = _unwrap_tool_payload(payload)
    if isinstance(rows, list):
        compact_rows: list[Any] = []
        for row in rows[:max_items]:
            if isinstance(row, dict):
                compact_rows.append(
                    {
                        key: value
                        for key, value in row.items()
                        if key
                        and key not in {"_assign", "_comments", "_liked_by", "_seen", "_user_tags"}
                        and not str(key).startswith("_")
                    }
                )
            else:
                compact_rows.append(row)
        return compact_rows
    if isinstance(rows, dict):
        return {
            key: value
            for key, value in rows.items()
            if key
            and key not in {"_assign", "_comments", "_liked_by", "_seen", "_user_tags"}
            and not str(key).startswith("_")
        }
    return rows


def _normalized_tool_name(tool_name: str) -> str:
    return str(tool_name or "").strip().lower()


def _tool_kind(normalized_name: str) -> str:
    if normalized_name in DOCUMENT_READ_TOOL_NAMES:
        return "document_read"
    if normalized_name in DOCUMENT_LIST_TOOL_NAMES:
        return "document_list"
    if normalized_name in DOCUMENT_MUTATION_TOOL_NAMES:
        return "document_mutation"
    if normalized_name in WORKFLOW_TOOL_NAMES:
        return "workflow"
    if normalized_name in DOCTYPE_INFO_TOOL_NAMES:
        return "doctype_info"
    if normalized_name in REPORT_TOOL_NAMES_NORMALIZED:
        return "report"
    if normalized_name in EXPORT_TOOL_NAMES:
        return "export"
    return "generic"


def _base_normalized_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized_name = _normalized_tool_name(tool_name)
    if not isinstance(payload, dict):
        return {
            "status": "unknown",
            "tool": normalized_name,
            "kind": _tool_kind(normalized_name),
            "summary": f"{normalized_name or 'tool'} returned a non-structured result.",
            "doctype": None,
            "document_name": None,
            "report_name": None,
            "file_url": None,
            "data": payload,
            "raw_success": None,
            "confidence": "low",
        }

    success = payload.get("success")
    status = "error" if success is False else "success"
    normalized: dict[str, Any] = {
        "status": status,
        "tool": normalized_name,
        "kind": _tool_kind(normalized_name),
        "summary": f"{normalized_name or 'tool'} completed.",
        "doctype": str(payload.get("doctype") or payload.get("ref_doctype") or "").strip() or None,
        "document_name": _document_name_from_payload(payload) or None,
        "report_name": str(payload.get("report_name") or payload.get("name") or "").strip() or None,
        "file_url": str(payload.get("file_url") or "").strip() or None,
        "data": _compact_tool_payload(payload),
        "raw_success": success if isinstance(success, bool) else None,
        "confidence": "medium",
    }
    error_text = str(payload.get("error") or "").strip()
    if error_text:
        normalized["error"] = error_text
    return normalized


def _normalize_generic_tool_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or normalized.get("summary") or "Tool execution failed.").strip()
        normalized["confidence"] = "low"
    elif normalized.get("status") == "unknown":
        normalized["confidence"] = "low"
    return normalized


def _normalize_document_list_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "Lookup failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    rows = normalized.get("data")
    row_count = len(rows) if isinstance(rows, list) else 1 if rows else 0
    if row_count > 1:
        normalized["status"] = "success"
        normalized["summary"] = f"Fetched {row_count} records for discovery."
        normalized["confidence"] = "high"
    elif row_count == 1:
        normalized["summary"] = "Found one matching record."
        normalized["confidence"] = "high"
    else:
        normalized["status"] = "partial"
        normalized["summary"] = "No matching records were returned."
        normalized["confidence"] = "medium"
    if row_count == 1 and isinstance(rows, list) and isinstance(rows[0], dict):
        normalized["document_name"] = str(rows[0].get("name") or normalized.get("document_name") or "").strip() or None
        normalized["doctype"] = str(rows[0].get("doctype") or normalized.get("doctype") or "").strip() or normalized.get("doctype")
    return normalized


def _normalize_document_read_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "Document lookup failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    if normalized.get("document_name"):
        normalized["summary"] = f"Loaded {(normalized.get('doctype') or 'document')} {normalized.get('document_name')}."
        normalized["confidence"] = "high"
    elif normalized.get("status") == "unknown":
        normalized["confidence"] = "low"
    return normalized


def _normalize_doctype_info_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "DocType metadata lookup failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    data = payload if isinstance(payload, dict) else {}
    fields = data.get("fields") or []
    if not isinstance(fields, list):
        fields = []
    normalized["summary"] = f"Loaded metadata for {normalized.get('doctype') or 'DocType'} with {len(fields)} field(s)."
    normalized["data"] = {
        "doctype": normalized.get("doctype"),
        "module": data.get("module"),
        "is_tree": bool(data.get("is_tree")),
        "is_submittable": bool(data.get("is_submittable")),
        "required_fields": [
            str(row.get("fieldname") or "").strip()
            for row in fields
            if isinstance(row, dict) and row.get("reqd")
        ][:12],
        "editable_fields": [
            str(row.get("fieldname") or "").strip()
            for row in fields
            if isinstance(row, dict) and not row.get("read_only")
        ][:20],
    }
    normalized["confidence"] = "high"
    return normalized


def _normalize_mutation_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "ERP mutation failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    action_label = {
        "create_document": "Created",
        "update_document": "Updated",
        "delete_document": "Deleted",
    }.get(_normalized_tool_name(tool_name), "Processed")
    if normalized.get("document_name"):
        normalized["summary"] = f"{action_label} {(normalized.get('doctype') or 'document')} {normalized.get('document_name')}."
        normalized["confidence"] = "high"
    else:
        normalized["status"] = "partial"
        normalized["summary"] = f"Mutation was attempted, but ERP did not confirm the resulting {(normalized.get('doctype') or 'document')} identifier."
        normalized["confidence"] = "low"
        normalized["next_action"] = "do_not_claim_mutation_success"
    return normalized


def _normalize_workflow_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "Workflow action failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    action = str(payload.get("action") or payload.get("workflow_action") or "").strip()
    document_name = normalized.get("document_name")
    doctype = normalized.get("doctype") or "document"
    if document_name and action:
        normalized["summary"] = f"Applied workflow action '{action}' to {doctype} {document_name}."
        normalized["confidence"] = "high"
    elif document_name and not action:
        normalized["status"] = "partial"
        normalized["summary"] = f"Workflow updated {doctype} {document_name}, but ERP did not explicitly confirm which action was applied."
        normalized["confidence"] = "medium"
    else:
        normalized["status"] = "partial"
        normalized["summary"] = "Workflow action was attempted, but ERP did not confirm the target document and action."
        normalized["confidence"] = "low"
        normalized["next_action"] = "do_not_claim_mutation_success"
    return normalized


def _normalize_report_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "Report operation failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    data = payload if isinstance(payload, dict) else {}
    result_rows = data.get("result") or data.get("data") or data.get("reports")
    if isinstance(result_rows, list):
        normalized["data"] = _compact_tool_payload({"data": result_rows}, max_items=10)
        normalized["summary"] = f"Prepared report result with {len(result_rows)} row(s)."
        normalized["confidence"] = "high"
    elif normalized.get("report_name"):
        normalized["summary"] = f"Prepared report result for {normalized.get('report_name')}."
        normalized["confidence"] = "medium"
    else:
        normalized["status"] = "partial"
        normalized["summary"] = "Report operation returned without rows or report identity."
        normalized["confidence"] = "low"
    return normalized


def _normalize_export_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _base_normalized_result(tool_name, payload)
    if normalized.get("status") == "error":
        normalized["summary"] = str(normalized.get("error") or "Export failed.").strip()
        normalized["confidence"] = "low"
        return normalized
    file_name = str(payload.get("file_name") or "").strip()
    file_url = normalized.get("file_url")
    if file_url:
        normalized["summary"] = f"Prepared export file {file_name or normalized.get('file_url')}."
        normalized["confidence"] = "high"
    else:
        normalized["status"] = "partial"
        normalized["summary"] = "Export was requested, but ERP did not return a downloadable file."
        normalized["confidence"] = "low"
        normalized["next_action"] = "inform_user_export_not_ready"
    return normalized


TOOL_RESULT_NORMALIZERS: dict[str, Any] = {}
for _tool_name in DOCUMENT_READ_TOOL_NAMES:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_document_read_result
for _tool_name in DOCUMENT_LIST_TOOL_NAMES:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_document_list_result
for _tool_name in DOCUMENT_MUTATION_TOOL_NAMES:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_mutation_result
for _tool_name in WORKFLOW_TOOL_NAMES:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_workflow_result
for _tool_name in DOCTYPE_INFO_TOOL_NAMES:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_doctype_info_result
for _tool_name in REPORT_TOOL_NAMES_NORMALIZED:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_report_result
for _tool_name in EXPORT_TOOL_NAMES:
    TOOL_RESULT_NORMALIZERS[_tool_name] = _normalize_export_result


def _normalize_tool_result(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized_name = str(tool_name or "").strip().lower()
    normalizer = TOOL_RESULT_NORMALIZERS.get(normalized_name, _normalize_generic_tool_result)
    return normalizer(tool_name, payload)


def _verify_generic_tool_result(tool_name: str, payload: Any, normalized: dict[str, Any]) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    checks: dict[str, Any] = {}

    structured_payload = isinstance(payload, dict)
    checks["structured_payload"] = structured_payload
    if not structured_payload:
        issues.append("Tool returned a non-structured payload.")

    status = str(normalized.get("status") or "").strip().lower()
    checks["status"] = status

    if status == "error":
        issues.append(str(normalized.get("error") or normalized.get("summary") or "Tool returned an error."))
    elif status not in {"success", "partial", "unknown"}:
        warnings.append(f"Normalized result has unexpected status '{status}'.")

    if structured_payload:
        embedded_error_fields = [
            payload.get("error"),
            payload.get("errors"),
            payload.get("exc"),
            payload.get("exception"),
            payload.get("traceback"),
            payload.get("_server_messages"),
        ]
        has_embedded_error = any(bool(item) for item in embedded_error_fields)
        checks["has_embedded_error"] = has_embedded_error
        if has_embedded_error and status != "error":
            warnings.append("Payload contains embedded error indicators.")

    if status == "unknown":
        warnings.append("Tool result could not be fully classified.")

    if status == "success" and not str(normalized.get("summary") or "").strip():
        warnings.append("Successful result is missing a summary.")

    kind = str(normalized.get("kind") or "")
    if not structured_payload and kind in {"document_mutation", "workflow", "export"}:
        issues.append("This tool class requires a structured payload to confirm success.")

    return {"ok": not issues, "issues": issues, "warnings": warnings, "checks": checks}


def _verify_document_list_result(tool_name: str, payload: Any, normalized: dict[str, Any]) -> dict[str, Any]:
    result = _verify_generic_tool_result(tool_name, payload, normalized)
    rows = normalized.get("data")
    row_count = len(rows) if isinstance(rows, list) else 1 if rows else 0
    result.setdefault("checks", {})
    result["checks"]["row_count"] = row_count
    result["checks"]["multiple_matches"] = row_count > 1
    result["checks"]["requires_disambiguation"] = False
    return result


def _verify_document_mutation_result(tool_name: str, payload: Any, normalized: dict[str, Any]) -> dict[str, Any]:
    result = _verify_generic_tool_result(tool_name, payload, normalized)
    result.setdefault("checks", {})
    result["checks"]["has_doctype"] = bool(normalized.get("doctype"))
    result["checks"]["has_document_name"] = bool(normalized.get("document_name"))
    result["checks"]["has_required_identifiers"] = bool(normalized.get("doctype") and normalized.get("document_name"))
    success_signal = _mutation_has_success_signal(payload, normalized)
    result["checks"]["has_success_signal"] = success_signal
    if normalized.get("status") != "error" and not normalized.get("document_name"):
        result["warnings"].append("ERP did not confirm the target document name.")
        if not success_signal:
            result["issues"].append("ERP did not provide enough confirmation for the mutation result.")
    result["ok"] = not result["issues"]
    return result


def _verify_workflow_result(tool_name: str, payload: Any, normalized: dict[str, Any]) -> dict[str, Any]:
    result = _verify_document_mutation_result(tool_name, payload, normalized)
    result.setdefault("checks", {})
    if normalized.get("status") != "error":
        action = str(payload.get("action") or payload.get("workflow_action") or "").strip()
        has_action = bool(action)
        result["checks"]["has_workflow_action"] = has_action
        if not has_action:
            result["warnings"].append("ERP did not explicitly confirm which workflow action was applied.")
    return result


def _verify_report_result(tool_name: str, payload: Any, normalized: dict[str, Any]) -> dict[str, Any]:
    result = _verify_generic_tool_result(tool_name, payload, normalized)
    normalized_name = _normalized_tool_name(tool_name)
    result.setdefault("checks", {})
    result["checks"]["has_report_name"] = bool(normalized.get("report_name"))
    if normalized_name == "create_report" and not normalized.get("report_name"):
        result["issues"].append("ERP did not confirm the created report name.")
    result["ok"] = not result["issues"]
    return result


def _verify_export_result(tool_name: str, payload: Any, normalized: dict[str, Any]) -> dict[str, Any]:
    result = _verify_generic_tool_result(tool_name, payload, normalized)
    result.setdefault("checks", {})
    has_file_url = bool(normalized.get("file_url"))
    result["checks"]["has_file_url"] = has_file_url
    if normalized.get("status") != "error" and not has_file_url:
        result["issues"].append("ERP did not return a downloadable file.")
    result["ok"] = not result["issues"]
    return result


TOOL_RESULT_VERIFIERS: dict[str, Any] = {}
for _tool_name in DOCUMENT_LIST_TOOL_NAMES:
    TOOL_RESULT_VERIFIERS[_tool_name] = _verify_document_list_result
for _tool_name in DOCUMENT_MUTATION_TOOL_NAMES:
    TOOL_RESULT_VERIFIERS[_tool_name] = _verify_document_mutation_result
for _tool_name in WORKFLOW_TOOL_NAMES:
    TOOL_RESULT_VERIFIERS[_tool_name] = _verify_workflow_result
for _tool_name in REPORT_TOOL_NAMES_NORMALIZED:
    TOOL_RESULT_VERIFIERS[_tool_name] = _verify_report_result
for _tool_name in EXPORT_TOOL_NAMES:
    TOOL_RESULT_VERIFIERS[_tool_name] = _verify_export_result


def _verify_tool_result(tool_name: str, payload: Any, normalized: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    normalized_payload = normalized or _normalize_tool_result(tool_name, payload)
    verifier = TOOL_RESULT_VERIFIERS.get(_normalized_tool_name(tool_name), _verify_generic_tool_result)
    result = verifier(tool_name, payload, normalized_payload)
    result.setdefault("issues", [])
    result.setdefault("warnings", [])
    result["ok"] = not bool(result.get("issues"))
    return result


def _tool_result_feedback_payload(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _normalize_tool_result(tool_name, payload)
    verification = _verify_tool_result(tool_name, payload, normalized)
    if not verification.get("ok"):
        normalized = dict(normalized)
        normalized["status"] = "error"
        if verification.get("issues"):
            normalized["error"] = "; ".join(str(item).strip() for item in verification.get("issues") if str(item).strip())
            normalized["summary"] = str(normalized["error"]).strip() or str(normalized.get("summary") or "").strip()
        normalized["confidence"] = "low"
    return {
        "normalized_result": normalized,
        "verification": verification,
    }


def _validate_tool_result(tool_name: str, payload: Any) -> None:
    if not isinstance(payload, dict):
        return

    if payload.get("success") is False:
        message = str(payload.get("error") or payload.get("message") or "Tool returned success=false.").strip()
        raise RuntimeError(message)

    normalized = str(tool_name or "").strip().lower()
    if normalized == "create_document":
        normalized_payload = _normalize_tool_result(tool_name, payload)
        if not _document_name_from_payload(payload) and not _mutation_has_success_signal(payload, normalized_payload):
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
    if isinstance(payload, dict) and isinstance(payload.get("normalized_result"), dict):
        normalized_result = payload.get("normalized_result") or {}
        verification = payload.get("verification") or {}
        summary = str(normalized_result.get("summary") or "").strip()
        issues = verification.get("issues") or []
        warnings = verification.get("warnings") or []
        lines = [summary or "Tool result prepared."]
        if issues:
            issue_text = "; ".join(str(item).strip() for item in issues if str(item).strip())
            if issue_text:
                lines.append(f"Issue: {issue_text}")
        elif warnings:
            warning_text = "; ".join(str(item).strip() for item in warnings if str(item).strip())
            if warning_text:
                lines.append(f"Note: {warning_text}")
        document_name = str(normalized_result.get("document_name") or "").strip()
        report_name = str(normalized_result.get("report_name") or "").strip()
        file_url = str(normalized_result.get("file_url") or "").strip()
        if document_name:
            lines.append(f"Document: {document_name}")
        elif report_name:
            lines.append(f"Report: {report_name}")
        elif file_url:
            lines.append(f"File: {file_url}")
        return "\n".join(lines).strip()

    reverse_map = {
        "list_documents": "get_list",
        "generate_report": "get_report",
    }
    normalized = reverse_map.get(str(tool_name or "").strip(), str(tool_name or "").strip())
    if normalized in {
        "get_list",
        "get_document",
        "get_doctype_info",
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
    if tool_name == "get_doctype_info":
        return _format_doctype_info_result(payload, heading)
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
            primary = (
                row.get("customer_name") or row.get("supplier_name") or row.get("employee_name")
                or row.get("item_name") or row.get("project_name") or row.get("lead_name")
                or row.get("opportunity_from") or row.get("subject") or row.get("asset_name")
                or row.get("report_name") or row.get("title") or row.get("name") or f"Row {index}"
            )
            _primary_keys = {"name", "customer_name", "supplier_name", "employee_name", "item_name",
                              "project_name", "lead_name", "opportunity_from", "subject", "asset_name",
                              "report_name", "title"}
            extras = [f"{key}: {value}" for key, value in row.items() if key not in _primary_keys and value not in (None, "", [])][:3]
            suffix = f" ({', '.join(extras)})" if extras else ""
            lines.append(f"{index}. {primary}{suffix}")
        else:
            lines.append(f"{index}. {row}")
    return "\n".join(lines)


def _format_document_result(payload: Any, heading: Optional[str]) -> str:
    if not isinstance(payload, dict):
        return str(payload)
    name = str(payload.get("name") or "").strip()
    status = payload.get("status")
    primary_label = (
        payload.get("title") or payload.get("customer_name") or payload.get("supplier_name")
        or payload.get("employee_name") or payload.get("item_name") or payload.get("project_name")
        or payload.get("lead_name") or payload.get("asset_name") or payload.get("subject")
    )
    title = heading or name or primary_label or "Document"
    lines = [title]
    if primary_label and str(primary_label).strip() and str(primary_label).strip() != title:
        lines.append(f"Label: {primary_label}")
    if status not in (None, "", [], {}):
        lines.append(f"Status: {status}")
    if name and name != title:
        lines.append(f"Document: {name}")
    details = []
    for key in [
        "doctype", "customer", "supplier", "employee_name", "party", "party_name",
        "posting_date", "transaction_date", "due_date", "delivery_date", "from_date", "to_date",
        "grand_total", "net_total", "outstanding_amount", "paid_amount", "net_pay", "total_debit",
        "currency", "company", "warehouse", "territory", "department", "designation",
        "status", "docstatus", "workflow_state",
        "modified", "modified_by",
    ]:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            details.append(f"{key.replace('_', ' ').title()}: {value}")
    if details:
        lines.extend(details[:6])
    if len(lines) == 1:
        for key, value in payload.items():
            if value not in (None, "", [], {}):
                lines.append(f"{key.replace('_', ' ').title()}: {value}")
            if len(lines) >= 8:
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


def _format_doctype_info_result(payload: Any, heading: Optional[str]) -> str:
    if not isinstance(payload, dict):
        return _format_generic_result(payload, heading or "DocType metadata")

    doctype = str(payload.get("doctype") or heading or "DocType").strip() or "DocType"
    module = str(payload.get("module") or "").strip()
    fields = payload.get("fields") or []
    if not isinstance(fields, list):
        fields = []

    writable_fields = []
    required_fields = []
    for row in fields:
        if not isinstance(row, dict):
            continue
        fieldname = str(row.get("fieldname") or "").strip()
        fieldtype = str(row.get("fieldtype") or "").strip()
        if not fieldname or fieldtype in {"Section Break", "Column Break", "Tab Break", "Fold", "HTML", "Button"}:
            continue
        if not int(bool(row.get("read_only"))):
            writable_fields.append(fieldname)
        if int(bool(row.get("reqd"))):
            required_fields.append(fieldname)

    lines = [f"{doctype} metadata loaded."]
    if module:
        lines.append(f"Module: {module}")
    if required_fields:
        lines.append(f"Required fields: {', '.join(required_fields[:8])}")
    if writable_fields:
        lines.append(f"Editable fields: {', '.join(writable_fields[:12])}")
    if not required_fields and not writable_fields:
        lines.append(f"Field count: {len(fields)}")
    return "\n".join(lines)


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
    message = str(payload.get("message") or "").strip()
    doctype = str(payload.get("doctype") or "").strip()
    name = _document_name_from_payload(payload)
    status = str(payload.get("status") or "").strip()
    title = heading or message or "Operation completed"
    lines = [title]
    if doctype and name:
        lines.append(f"{doctype} {name}")
    elif name:
        lines.append(name)
    elif doctype:
        lines.append(doctype)
    if status:
        lines.append(f"Status: {status}")
    detail_lines = []
    for key in ["creation", "modified", "modified_by", "owner"]:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            detail_lines.append(f"{key.replace('_', ' ').title()}: {value}")
    updated_parts = payload.get("updated_parts")
    if isinstance(updated_parts, list) and updated_parts:
        detail_lines.append(f"Updated parts: {', '.join(str(item) for item in updated_parts[:6])}")
    if message and message != title:
        detail_lines.insert(0, message)
    if detail_lines:
        lines.extend(detail_lines[:5])
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
        # ── Contact / Identity ───────────────────────────────────────────────
        "email": "email_id",
        "e-mail": "email_id",
        "mobile": "mobile_no",
        "phone": "mobile_no",
        "cell": "mobile_no",
        "fax": "fax",
        "website": "website",
        # ── Employee / HR ────────────────────────────────────────────────────
        "territory": "territory",
        "designation": "designation",
        "department": "department",
        "company": "company",
        "branch": "branch",
        "gender": "gender",
        "dob": "date_of_birth",
        "date of birth": "date_of_birth",
        "joining": "date_of_joining",
        "date of joining": "date_of_joining",
        "relieving": "relieving_date",
        "relieving date": "relieving_date",
        "salary mode": "salary_mode",
        "bank account": "bank_ac_no",
        "salary": "salary",
        "basic salary": "basic_salary",
        "company email": "company_email",
        # ── Common doc fields ────────────────────────────────────────────────
        "description": "description",
        "status": "status",
        "notes": "notes",
        "remarks": "remarks",
        "priority": "priority",
        "title": "title",
        "subject": "subject",
        # ── Customer / Supplier ──────────────────────────────────────────────
        "customer group": "customer_group",
        "supplier group": "supplier_group",
        "currency": "default_currency",
        "price list": "default_price_list",
        "payment terms": "payment_terms",
        "credit limit": "credit_limit",
        "customer type": "customer_type",
        "supplier type": "supplier_type",
        # ── Dates ────────────────────────────────────────────────────────────
        "delivery date": "delivery_date",
        "transaction date": "transaction_date",
        "due date": "due_date",
        "posting date": "posting_date",
        "schedule date": "schedule_date",
        "start date": "start_date",
        "end date": "end_date",
        "from date": "from_date",
        "to date": "to_date",
        # ── Sales / Purchase ─────────────────────────────────────────────────
        "po no": "po_no",
        "po number": "po_no",
        "so no": "sales_order",
        "rate": "rate",
        "qty": "qty",
        "quantity": "qty",
        "amount": "amount",
        "discount": "discount_percentage",
        "tax": "taxes_and_charges",
        # ── Item ─────────────────────────────────────────────────────────────
        "item group": "item_group",
        "stock uom": "stock_uom",
        "valuation rate": "valuation_rate",
        "standard rate": "standard_rate",
        "is stock item": "is_stock_item",
        "item name": "item_name",
        "item code": "item_code",
        # ── Project / Task ───────────────────────────────────────────────────
        "project": "project",
        "expected end": "exp_end_date",
        "expected start": "exp_start_date",
        "expected end date": "exp_end_date",
        "expected start date": "exp_start_date",
        "actual time": "actual_time",
        "assigned to": "assigned_to",
        # ── Asset ────────────────────────────────────────────────────────────
        "asset category": "asset_category",
        "purchase date": "purchase_date",
        "gross value": "gross_purchase_amount",
        "location": "location",
        # ── Accounts ─────────────────────────────────────────────────────────
        "cost centre": "cost_center",
        "cost center": "cost_center",
        "account": "account",
        "debit": "debit_in_account_currency",
        "credit": "credit_in_account_currency",
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
    if lowered in {"true", "yes", "1", "on", "active", "enabled"}:
        return 1
    if lowered in {"false", "no", "0", "off", "inactive", "disabled"}:
        return 0
    if lowered == "today":
        import datetime
        return datetime.date.today().strftime("%Y-%m-%d")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", cleaned):
        return cleaned  # already a valid date string
    return cleaned
