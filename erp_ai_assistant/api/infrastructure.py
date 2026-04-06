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




def _endpoint_host(base_url: str) -> str:
    text = str(base_url or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text)
    return text.split("/", 1)[0]




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

    inferred = _infer_prompt_doctype(prompt)
    if inferred:
        return inferred

    explicit = str(current.get("doctype") or "").strip()
    if explicit:
        return explicit

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
        "When the user's requested code exactly appears in returned records, that exact code is the correct choice and should be used automatically. "
        "When several valid linked records are returned for an unspecified required field, prefer a real Standard/default/active option or the strongest context match and continue; only ask the user when two or more top candidates remain equally plausible. "
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
    if not rows and isinstance(fields_payload, dict):
        nested_rows = fields_payload.get("data")
        if isinstance(nested_rows, list):
            rows = nested_rows
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
        schema_data = schema.get("data") if isinstance(schema.get("data"), dict) else {}
        schema_doctype = str(
            schema_data.get("doctype")
            or schema.get("doctype")
            or schema.get("name")
            or ""
        ).strip()
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


def _prompt_discovery_doctype_snapshot(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    current = context or {}
    discovered = sorted(_get_discovery_doctypes())
    if not discovered:
        return "available_doctypes: unavailable."

    prompt_terms = _tokenize_match_text(prompt)
    current_doctype = str(current.get("doctype") or "").strip()
    target_doctype = str(_resolved_prompt_doctype(prompt, current) or "").strip()
    matches: list[str] = []
    for name in discovered:
        lowered_name = name.lower()
        name_terms = _tokenize_match_text(name)
        if target_doctype and lowered_name == target_doctype.lower():
            matches.append(name)
            continue
        if current_doctype and lowered_name == current_doctype.lower():
            matches.append(name)
            continue
        if prompt_terms and (
            lowered_name in str(prompt or "").strip().lower()
            or prompt_terms.intersection(name_terms)
        ):
            matches.append(name)
        if len(matches) >= 8:
            break

    lines = [f"available_doctypes: {len(discovered)} discovered."]
    if target_doctype:
        lines.append(f"resolved target={target_doctype}.")
    if matches:
        lines.append(f"prompt matches: {', '.join(matches[:8])}.")
    else:
        preview = ", ".join(discovered[:12])
        if preview:
            lines.append(f"catalog preview: {preview}.")
    return " ".join(lines)


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

    snapshots: list[str] = [_prompt_discovery_doctype_snapshot(prompt, current)]
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
    retry_last_user: bool = False,
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
    if not retry_last_user:
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
            "tool_events": response.get("tool_events", []),
            "attachments": attachments,
            "payload": response.get("payload"),
            "context": context,
            "debug": result.get("debug"),
        },
    )
    _progress_update(progress, stage="completed", done=True, step="Response ready", partial_text=reply_text)
    return result










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


# --- Back-imports to resolve dependencies ---
from .orchestrator import _build_message_attachments, _finalize_reply_text, _progress_update, _conversation_history_for_llm, _generate_response, _is_bulk_operation_request, _set_conversation_title_from_prompt
from .llm_gateway import _format_image_only_user_content, _provider_name, _resolve_model_for_request, _parse_prompt_images, _build_prompt_image_attachments
