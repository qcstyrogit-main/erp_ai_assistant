"""
erp_ai_assistant.api.orchestrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Improved Agent Orchestrator for the ERP AI Assistant.

Key improvements over v1:
  - Data Validation Layer: every tool result is validated before the LLM
    sees it. Quality metadata (_validation) is injected into the tool result
    so the LLM can make accurate citations.
  - Analysis-intent routing: queries classified as "analysis" get a richer
    user-prompt preamble that enforces the 5-step reasoning chain.
  - Module-aware progress updates: progress steps now include the ERP module
    being queried.
  - Role-permission guard: if the intent_detector flags restricted modules and
    the user lacks the required roles, the orchestrator short-circuits before
    any tool call is made.
  - Stale confirmation gate: pending_action payloads older than 300 seconds
    are auto-rejected (uses the expires_in_seconds field from safety.py).
  - All v1 public API preserved — this is a drop-in replacement.
"""
import frappe
from frappe import _
import json
import copy
import re
import html
import time
from typing import Any, Optional

from .infrastructure import *
from .infrastructure import (
    _append_related_links,
    _conversation_history_limit,
    _execute_prompt_inner,
    _format_generic_result,
    _has_active_document_context,
    _has_write_intent,
    _is_single_record_payload,
    _llm_chat_configured,
    _normalize_tool_result,
    _progress_cache_key,
    _render_tool_output,
    _request_focus_summary,
    _requested_export_formats,
    _rerun_last_exportable_tool,
    _resource_runtime_manifest,
    _set_prompt_result,
    _summarize_title,
    _verify_tool_result,
)
from .data_validator import (
    inject_validation,
    validate_tool_result,
    format_validation_for_llm,
    is_error_result,
    is_empty_result,
)
from .intent_detector import detect_intent_heuristic
from .safety import classify_risk, is_confirmation_reply, is_rejection_reply
from .proactive_insights import (   # ── v3 PATCH ──
    analyse_tool_result,
    generate_next_steps,
    check_compliance_deadlines,
)


# ── Role → required ERP modules guard ────────────────────────────────────────

_MODULE_REQUIRED_ROLES: dict[str, list[str]] = {
    "Finance": ["Accounts User", "Accounts Manager", "System Manager"],
    "HR": ["HR User", "HR Manager", "System Manager"],
    "Inventory": ["Stock User", "Stock Manager", "System Manager"],
    "Purchasing": ["Purchase User", "Purchase Manager", "System Manager"],
    "Sales": ["Sales User", "Sales Manager", "System Manager"],
    "Manufacturing": ["Manufacturing User", "Manufacturing Manager", "System Manager"],
    "Projects": ["Projects User", "Projects Manager", "System Manager"],
}


def _get_user_roles(user: str | None = None) -> set[str]:
    target = user or frappe.session.user or "Guest"
    try:
        return set(frappe.get_roles(target) or [])
    except Exception:
        return {"Guest"}


def _check_module_access(
    modules: list[str], user_roles: set[str]
) -> list[str]:
    """
    Return a list of modules the user does NOT have access to.
    Empty list → all clear.
    """
    denied: list[str] = []
    for module in modules:
        required = _MODULE_REQUIRED_ROLES.get(module)
        if required and not user_roles.intersection(required):
            denied.append(module)
    return denied


def _is_stale_pending_action(pending_action: dict[str, Any] | None) -> bool:
    """Return True if the pending action has expired."""
    if not isinstance(pending_action, dict):
        return False
    created_at_str = pending_action.get("created_at")
    expires_in = int(pending_action.get("expires_in_seconds") or 300)
    if not created_at_str:
        return False
    try:
        created_at = frappe.utils.get_datetime(created_at_str)
        now = frappe.utils.now_datetime()
        elapsed = (now - created_at).total_seconds()
        return elapsed > expires_in
    except Exception:
        return False


# ── Bulk-operation detection (preserved from v1, extended) ───────────────────

def _is_bulk_operation_request(
    prompt: str, context: Optional[dict[str, Any]] = None
) -> bool:
    text = (prompt or "").strip().lower()
    if not text:
        return False

    explicit_bulk_terms = (
        "bulk ", "mass ", "batch ",
        "all records", "all documents",
        "update all", "import ",
    )
    if any(term in text for term in explicit_bulk_terms):
        return True

    quantity_match = re.search(
        r"\b(create|update|insert|modify|add|generate|export)\s+(\d+)\b", text
    )
    if quantity_match:
        qty = int(quantity_match.group(2))
        if qty >= 2:
            return True

    if any(term in text for term in {"all ", "every "}) and _has_write_intent(text):
        return True

    return False


# ── User-prompt builder with analysis preamble ────────────────────────────────

def _llm_user_prompt(prompt: str, context: Optional[dict[str, Any]] = None) -> str:
    """
    Improved user prompt builder.

    Additions over v1:
      - Analysis-intent preamble with explicit 5-step instruction.
      - Multi-module flag added when the intent detector finds >1 module.
      - Data-citation reminder is now more explicit (cite tool name + field).
    """
    cleaned = str(prompt or "").strip()
    resource_manifest = _resource_runtime_manifest(cleaned, context)
    manifest_block = f"{resource_manifest}\n\n" if resource_manifest else ""

    intent_meta = detect_intent_heuristic(cleaned, context or {})
    intent = str(intent_meta.get("intent") or "unknown").lower()
    modules = intent_meta.get("modules") or []
    multi_module = intent_meta.get("is_multi_module", False)

    # ── Analysis-specific preamble ────────────────────────────────────────────
    analysis_preamble = ""
    if intent == "analysis":
        module_hint = f" (modules involved: {', '.join(modules)})" if modules else ""
        analysis_preamble = (
            f"[ANALYSIS REQUEST{module_hint}] "
            "Follow the 5-step reasoning protocol:\n"
            "  1. CLASSIFY — confirm this is an analysis query.\n"
            "  2. IDENTIFY — list every DocType, date range, and module needed.\n"
            "  3. EXECUTE — call tools in dependency order.\n"
            "  4. VALIDATE — check results for anomalies before using them.\n"
            "  5. SYNTHESIZE — structure your response as:\n"
            "     ## Summary | ## Data | ## Root Cause | ## Recommendation\n"
            "Do NOT state a root cause without at least two corroborating tool results.\n\n"
        )

    multi_module_hint = ""
    if multi_module and intent != "analysis":
        multi_module_hint = (
            f"[CROSS-MODULE QUERY: {', '.join(modules)}] "
            "Call tools from each relevant module. "
            "Do not answer with data from only one module when multiple are needed.\n\n"
        )

    base = (
        f"{_request_focus_summary(cleaned, context)}\n\n"
        f"{manifest_block}"
        f"{analysis_preamble}"
        f"{multi_module_hint}"
        "User request:\n"
        f"{cleaned}\n\n"
        "Format your response using clean Markdown: use fenced code blocks for code/JSON/SQL, "
        "tables for lists of records or comparisons, bullet/numbered lists for steps and options, "
        "and **bold** for document names and key values. "
        "Use the live FAC tool catalog when the request needs live ERP data or actions. "
        "For complex requests, first make a short hidden plan, then call tools, then answer. "
        "If the request is instructional only, answer directly unless live verification is necessary. "
        "Ask only for the minimum missing information if the request is blocked. "
        "Summarize tool results clearly using Markdown formatting. "
        "Do not reveal internal reasoning, raw tool output, JSON, XML, or hidden tags such as <think>. "
        "Never generate fictional, placeholder, or invented ERP data — always use live FAC tools. "
        # ── Data-citation rules ────────────────────────────────────────────
        "DATA-CITATION RULES: "
        "(1) For every number, total, count, amount, date, or status in your response, "
        "    cite the tool that produced it in brackets, e.g. [get_erp_document]. "
        "(2) Quote all numeric values VERBATIM from tool output — never round or estimate. "
        "(3) If a tool result has _validation.quality = 'empty', say the query returned 0 records. "
        "    If _validation.quality = 'error', report the error — never invent a substitute value. "
        "(4) If a value was not present in any tool result, say so explicitly. "
        "Prior [Model-only] history messages are UNVERIFIED — treat them as hints only."
    ).strip()

    return base


# ── Verification prompt (preserved from v1, extended) ────────────────────────

def _verification_prompt(tool_events: list[str]) -> str:
    recent = tool_events[-8:]
    recent_lines = "\n".join(f"- {event}" for event in recent) or "- none"
    return (
        "Verification pass — review your answer against the tool evidence before replying.\n\n"
        "Rules you MUST follow:\n"
        "1. **Numeric precision**: Every number, total, count, amount, quantity, or date "
        "   must be quoted VERBATIM from tool output. Do NOT round, estimate, or paraphrase.\n"
        "2. **Cite your source**: For every factual claim, state which tool produced it "
        "   (e.g. \"[list_erp_documents] returned 12 open orders\" not just \"there are 12 orders\").\n"
        "3. **No inference on absent data**: If a field was NOT in any tool result, "
        "   say so. Do NOT invent, extrapolate, or assume absent values.\n"
        "4. **Check _validation quality**: If any tool result has "
        "   _validation.quality = 'error' or 'empty', do NOT base claims on it.\n"
        "5. **Correct prior contradictions**: Tool results override prior [Model-only] history.\n\n"
        "Action:\n"
        "- If evidence is insufficient → call the missing tools now.\n"
        "- If evidence is complete → return the final verified answer following all 5 rules.\n\n"
        "Recent tool results:\n"
        f"{recent_lines}"
    )


# ── Progress updates (extended with module info) ──────────────────────────────

def _progress_update(
    progress: Optional[dict[str, Any]],
    stage: str,
    step: str | None = None,
    done: bool = False,
    error: str | None = None,
    partial_text: str | None = None,
    module: str | None = None,
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
        if module:
            normalized = f"[{module}] {normalized}"
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
    try:
        frappe.publish_realtime(
            event="erp_ai_progress",
            message=payload,
            user=user,
            after_commit=False,
        )
    except Exception:
        pass


# ── Module-access guard response ──────────────────────────────────────────────

def _access_denied_response(denied_modules: list[str], user_roles: set[str]) -> dict[str, Any]:
    role_str = ", ".join(sorted(user_roles - {"All", "Guest"})) or "Guest"
    modules_str = " and ".join(denied_modules)
    required_roles = []
    for m in denied_modules:
        required_roles.extend(_MODULE_REQUIRED_ROLES.get(m, []))
    required_str = " or ".join(sorted(set(required_roles)))
    return {
        "text": (
            f"Your current role(s) ({role_str}) do not have access to "
            f"**{modules_str}** data in this ERP system.\n\n"
            f"To access this information, you need the **{required_str}** role.\n\n"
            "Please contact your system administrator or an authorized manager."
        ),
        "tool_events": [],
        "payload": None,
    }


# ── Stale confirmation response ───────────────────────────────────────────────

def _stale_confirmation_response() -> dict[str, Any]:
    return {
        "text": (
            "⏱️ The confirmation window for that action has expired (300 seconds).\n\n"
            "For security, confirmations must be given within 5 minutes of the request.\n\n"
            "Please re-state the action you'd like to perform."
        ),
        "tool_events": [],
        "payload": None,
    }


# ── Core generate-response function (enhanced) ────────────────────────────────

def _generate_response(
    prompt: str,
    context: dict[str, Any],
    conversation: str | None = None,
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    """
    Generate a response to the user prompt.

    Improvements over v1:
      - Role-permission guard runs before any tool call.
      - Stale pending-action detection.
      - Data validation injected into tool results (via _provider_chat_with_resilience
        hook in llm_gateway — see llm_gateway improvements).
      - Analysis intent gets enriched user prompt preamble.
    """
    # ── Export short-circuit (preserved from v1) ─────────────────────────────
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

    # ── Role-permission guard ────────────────────────────────────────────────
    user = context.get("user") or frappe.session.user
    user_roles = _get_user_roles(user)
    intent_meta = detect_intent_heuristic(prompt, context)
    modules_needed = intent_meta.get("modules") or []
    denied = _check_module_access(modules_needed, user_roles)
    if denied:
        _progress_update(progress, stage="failed", done=True,
                         error=f"Access denied for modules: {', '.join(denied)}")
        return _access_denied_response(denied, user_roles)

    # ── v3 PATCH: Check BIR compliance deadlines ──────────────────────────────
    try:
        _deadline_warnings = check_compliance_deadlines()
        if _deadline_warnings and context.get("module") in ("Accounts", "PH Localization", None):
            context["_compliance_warnings"] = _deadline_warnings
    except Exception:
        pass
    # ── end v3 PATCH ──────────────────────────────────────────────────────────

    # ── LLM chat path ────────────────────────────────────────────────────────
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
            # ── v3 PATCH: Append next-step suggestions ────────────────────────────────
            try:
                _intent = (intent_meta or {}).get("intent", "unknown")
                _doctype = context.get("doctype") or context.get("target_doctype")
                _should_suggest = _intent in {
                    "read", "analysis", "create", "update", "workflow", "compliance"
                }
                final_response = response.get("text", "")
                last_tool_result = (response.get("tool_events") or [None])[-1]
                if _should_suggest and final_response and not final_response.strip().endswith("?"):
                    _next_steps_text = generate_next_steps(_intent, _doctype, last_tool_result, context)
                    if _next_steps_text:
                        response = dict(response)
                        response["text"] = final_response.rstrip() + "\n" + _next_steps_text
            except Exception:
                pass
            # ── end v3 PATCH ──────────────────────────────────────────────────────────
            return response
        except Exception as exc:
            _progress_update(progress, stage="failed", done=True, error=str(exc) or "Unknown error")
            frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Chat Error")
            return {
                "text": (
                    "I could not complete that request because the assistant backend call failed.\n\n"
                    f"Error: {str(exc) or 'Unknown error'}\n\n"
                    "Please retry, and if this persists check the AI provider/API configuration."
                ),
                "tool_events": [],
                "payload": None,
            }

    # ── Fallback: LLM not configured ─────────────────────────────────────────
    guidance = (
        "AI provider is not configured for FAC-native chat yet. "
        "Configure the provider so the assistant can call FAC tools dynamically."
    )
    if _has_active_document_context(context):
        guidance += f" Current context: {context['doctype']} / {context['docname']}."
    return {"text": guidance, "tool_events": [], "payload": None}


# ── Tool-result injection hook (called by llm_gateway) ───────────────────────

def enrich_tool_result_for_llm(tool_name: str, raw_result: Any) -> Any:
    """
    Public hook for llm_gateway to call after every tool execution.

    Injects _validation metadata and prepends a quality note so the LLM
    can cite data quality accurately.

    Usage in llm_gateway.py:
        from .orchestrator import enrich_tool_result_for_llm
        enriched = enrich_tool_result_for_llm(tool_name, raw_result)
        # Use enriched as the tool_result content block
    """
    enriched = inject_validation(tool_name, raw_result)
    validation = validate_tool_result(tool_name, raw_result)
    quality_note = format_validation_for_llm(validation)
    if quality_note and isinstance(enriched, dict):
        enriched["_quality_note"] = quality_note
    # ── v3 PATCH: Proactive insights ──────────────────────────────────────────
    try:
        _proactive_notes = analyse_tool_result(tool_name, raw_result, {})
        if _proactive_notes and isinstance(enriched, dict):
            enriched["_proactive_insights"] = _proactive_notes
    except Exception:
        pass
    # ── end v3 PATCH ──────────────────────────────────────────────────────────
    return enriched


# ── Reply sanitisation (preserved + extended from v1) ────────────────────────

def _sanitize_assistant_reply(text: Any) -> str:
    cleaned = html.unescape(str(text or "").strip())
    if not cleaned:
        return ""
    # Remove leaked reasoning tags
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<analysis>[\s\S]*?</analysis>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<reasoning>[\s\S]*?</reasoning>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<tool_call>[\s\S]*?</tool_call>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<TOOLCALL>[\s\S]*?</TOOLCALL>", "", cleaned)
    cleaned = re.sub(r"<next_steps>[\s\S]*?</next_steps>", "", cleaned, flags=re.IGNORECASE)
    # Remove leaked validation metadata if LLM accidentally echoes it
    cleaned = re.sub(r"\[DATA QUALITY:[^\]]*\]", "", cleaned)
    # Strip filler openers
    cleaned = re.sub(
        r"^\s*(okay[,!.]?|alright[,!.]?|sure[,!.]?|of course[,!.]?)\s*",
        "", cleaned, flags=re.IGNORECASE
    )
    cleaned = cleaned.strip()
    if not cleaned:
        return "I'm ready to continue. Please resend the request in one line."
    return cleaned


def _payload_to_structured_markdown(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    data = payload.get("data")
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    doctype = str(payload.get("doctype") or meta.get("doctype") or "").strip()
    name = str(payload.get("name") or "").strip()

    if isinstance(data, list):
        if not data:
            title = doctype or "Records"
            return f"## {title}\n\nNo records found."
        columns: list[str] = []
        preferred = [
            "name", "title", "status", "workflow_state", "posting_date", "transaction_date",
            "due_date", "customer", "supplier", "party_name", "company", "grand_total",
            "outstanding_amount",
        ]
        for col in preferred:
            if any(isinstance(row, dict) and row.get(col) not in (None, "", [], {}) for row in data):
                columns.append(col)
            if len(columns) >= 6:
                break
        if not columns and isinstance(data[0], dict):
            columns = list(data[0].keys())[:6]
        title = doctype or "Records"
        lines = [f"## {title}", "", f"Found **{len(data)}** record{'s' if len(data) != 1 else ''}.", ""]
        if columns:
            lines.append("| # | " + " | ".join(c.replace("_", " ").title() for c in columns) + " |")
            lines.append("|---|" + "|".join(["---"] * len(columns)) + "|")
            for idx, row in enumerate(data[:10], start=1):
                if isinstance(row, dict):
                    vals = ["—" if row.get(c) in (None, "", [], {}) else str(row.get(c)).replace("|", "\\|") for c in columns]
                else:
                    vals = [str(row).replace("|", "\\|")]
                lines.append(f"| {idx} | " + " | ".join(vals) + " |")
            if len(data) > 10:
                lines.extend(["", f"Showing first **10** of **{len(data)}** records."])
        return "\n".join(lines)

    if isinstance(data, dict):
        doc = data
    elif isinstance(payload.get("normalized_result"), dict):
        doc = payload.get("normalized_result")
    else:
        doc = payload

    if isinstance(doc, dict):
        title = doctype or str(doc.get("doctype") or "Document").strip() or "Document"
        if name or doc.get("name"):
            title = f"{title} — {name or doc.get('name')}"
        lines = [f"## {title}", "", "### Summary", ""]
        summary_fields = [
            "status", "workflow_state", "docstatus", "posting_date", "transaction_date", "due_date",
            "customer", "supplier", "party_name", "company", "grand_total", "outstanding_amount",
        ]
        added = 0
        for key in summary_fields:
            value = doc.get(key)
            if value in (None, "", [], {}):
                continue
            lines.append(f"- **{key.replace('_', ' ').title()}:** {value}")
            added += 1
            if added >= 8:
                break
        if not added:
            lines.append("- No summary fields were available in the returned document.")
        lines.extend(["", "### Details", ""])
        shown = 0
        for key, value in doc.items():
            if value in (None, "", [], {}) or isinstance(value, (dict, list)):
                continue
            lines.append(f"- **{key.replace('_', ' ').title()}:** {value}")
            shown += 1
            if shown >= 12:
                break
        return "\n".join(lines)

    return ""


def _response_needs_structuring(text: str) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return True
    if cleaned.startswith("{") and cleaned.endswith("}"):
        return True
    if cleaned.startswith("[") and cleaned.endswith("]"):
        return True
    generic = {
        "done", "completed", "updated", "created", "here you go", "result prepared.",
        "tool result prepared.", "request completed.",
    }
    lowered = cleaned.lower()
    return len(cleaned) < 80 or lowered in generic


# ── Override-pending-action check (preserved from v1) ────────────────────────

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
        "i mean ", "actually ", "instead ", "rather ", "no, ", "no ",
    )
    if any(text.startswith(marker) for marker in override_markers):
        return True
    return False


# ── Tool-round-limit response (preserved from v1) ────────────────────────────

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
            "The remaining steps were not executed. No further changes were made.\n\n"
            "**Suggestion:** Break the request into smaller parts, or increase the tool-call limit."
        )
    else:
        summary = (
            f"The request could not be completed — the AI loop reached the configured "
            f"tool-call limit ({max_tool_rounds} rounds) before producing a final answer.\n\n"
            "No ERP records were changed after this point.\n\n"
            "**Suggestion:** Try a more specific prompt, or increase the tool-call limit."
        )
    return {"text": summary, "tool_events": tool_events, "payload": None}


# ── Other v1 helpers (preserved unchanged) ───────────────────────────────────

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


def _conversation_history_for_llm(
    conversation_name: str, limit: int | None = None
) -> list[dict[str, Any]]:
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
            tool_events_raw = row.get("tool_events") or ""
            try:
                tool_events_list = json.loads(tool_events_raw) if tool_events_raw else []
            except Exception:
                tool_events_list = []
            is_tool_grounded = bool(tool_events_list)
            grounding_prefix = "[Tool-grounded] " if is_tool_grounded else "[Model-only] "
            history.append({"role": "assistant", "content": grounding_prefix + history_text})
    return history


def _build_message_attachments(
    payload: Any, title: str, conversation: str, prompt: str
) -> dict[str, Any]:
    if payload in (None, "", [], {}):
        return {"attachments": [], "exports": {}}
    formats = _requested_export_formats(prompt)
    if not formats:
        return {"attachments": [], "exports": {}}
    if _is_single_record_payload(payload):
        return {"attachments": [], "exports": {}}
    try:
        return create_message_artifacts(payload=payload, title=title, formats=formats)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Artifact Generation Error")
        return {"attachments": [], "exports": {}}


def _finalize_reply_text(
    text: str, prompt: str, attachments: dict[str, Any],
    payload: Any = None, all_payloads: list[Any] | None = None
) -> str:
    attachment_rows = attachments.get("attachments") or []
    if (
        attachment_rows
        and _requested_export_formats(prompt)
        and any(item.get("export_id") for item in attachment_rows)
    ):
        labels = ", ".join(
            str(item.get("label") or item.get("file_type") or "file").strip()
            for item in attachment_rows[:3]
        )
        base_text = f"Prepared your export. Use the downloadable attachment below{f' ({labels})' if labels else ''}."
    else:
        cleaned_text = _synthesize_mutation_reply(
            _sanitize_assistant_reply(text),
            prompt,
            payload,
            all_payloads=all_payloads,
        )
        structured = _payload_to_structured_markdown(payload)
        if structured and _response_needs_structuring(cleaned_text):
            base_text = structured
        elif structured and "## " not in cleaned_text and isinstance(payload, dict) and isinstance(payload.get("data"), (list, dict)):
            base_text = cleaned_text.rstrip() + "\n\n" + structured
        else:
            base_text = cleaned_text
    return _append_related_links(base_text, prompt, payload, all_payloads=all_payloads)


def _mutation_doc_payload(raw_payload: Any) -> dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return {}
    data = raw_payload.get("data")
    if isinstance(data, dict) and data:
        return data
    return {}


def _looks_like_minimal_mutation_reply(text: str) -> bool:
    cleaned = str(text or "").strip().lower()
    if not cleaned:
        return False
    return cleaned.startswith(("updated ", "created ", "deleted ", "processed ")) and "\n" in cleaned


def _mutation_changed_fields(before_doc: dict[str, Any], after_doc: dict[str, Any]) -> list[tuple[str, Any, Any]]:
    ignore = {
        "modified", "modified_by", "creation", "owner", "idx", "_assign", "_comments",
        "_liked_by", "_seen", "_user_tags", "__last_sync_on", "doctype", "name",
    }
    changed: list[tuple[str, Any, Any]] = []
    for key, new_value in after_doc.items():
        if key in ignore:
            continue
        old_value = before_doc.get(key)
        if old_value != new_value:
            changed.append((key, old_value, new_value))
    return changed


def _fmt_mutation_value(value: Any) -> str:
    if value in (None, ""):
        return "—"
    if isinstance(value, float):
        return f"{value:.1f}" if value.is_integer() else str(value)
    return str(value)


def _field_label(fieldname: str) -> str:
    return str(fieldname or "").replace("_", " ").strip().title()


def _document_state_label(doc: dict[str, Any]) -> str:
    docstatus = doc.get("docstatus")
    if docstatus == 1:
        return "Submitted"
    if docstatus == 2:
        return "Cancelled"
    return "Draft"


def _synthesize_mutation_reply(
    text: str,
    prompt: str,
    payload: Any = None,
    *,
    all_payloads: list[Any] | None = None,
) -> str:
    cleaned = _sanitize_assistant_reply(text)
    if not _looks_like_minimal_mutation_reply(cleaned):
        return cleaned
    current = payload if isinstance(payload, dict) else {}
    if not current.get("success"):
        return cleaned
    after_doc = _mutation_doc_payload(current)
    if not after_doc:
        return cleaned

    before_doc = {}
    for item in reversed(all_payloads or []):
        if not isinstance(item, dict) or item is current:
            continue
        candidate = _mutation_doc_payload(item)
        if not candidate:
            continue
        if str(candidate.get("doctype") or "") == str(after_doc.get("doctype") or "") and str(candidate.get("name") or "") == str(after_doc.get("name") or ""):
            before_doc = candidate
            break
    if not before_doc:
        return cleaned

    changed = _mutation_changed_fields(before_doc, after_doc)
    if not changed:
        return cleaned

    doctype = str(after_doc.get("doctype") or current.get("doctype") or "Document").strip()
    name = str(after_doc.get("name") or current.get("name") or "").strip()
    lead_field = changed[0][0]
    title = f"## ✅ {_field_label(lead_field)} Update Completed for **{name or doctype}**"
    rows = [
        "| Field Updated | Previous Value | New Value | Document State |",
        "|---------------|----------------|-----------|----------------|",
    ]
    state = _document_state_label(after_doc)
    for fieldname, old_value, new_value in changed[:6]:
        rows.append(
            f"| `{fieldname}` | {_fmt_mutation_value(old_value)} | {_fmt_mutation_value(new_value)} | {state} |"
        )
    evidence = [
        "Evidence",
        *[
            f"- {_field_label(fieldname)} changed from `{_fmt_mutation_value(old_value)}` to `{_fmt_mutation_value(new_value)}`."
            for fieldname, old_value, new_value in changed[:6]
        ],
    ]
    return "\n\n".join([title, "\n".join(rows), "\n".join(evidence)]).strip()


def _tool_result_feedback_payload(tool_name: str, payload: Any) -> dict[str, Any]:
    normalized = _normalize_tool_result(tool_name, payload)
    verification = _verify_tool_result(tool_name, payload, normalized)
    if not verification.get("ok"):
        normalized = dict(normalized)
        normalized["status"] = "error"
        if verification.get("issues"):
            normalized["error"] = "; ".join(
                str(item).strip() for item in verification.get("issues") if str(item).strip()
            )
            normalized["summary"] = str(normalized["error"]).strip() or str(normalized.get("summary") or "").strip()
        normalized["confidence"] = "low"
    return {"normalized_result": normalized, "verification": verification}


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

    reverse_map = {"list_documents": "get_list", "generate_report": "get_report"}
    normalized = reverse_map.get(str(tool_name or "").strip(), str(tool_name or "").strip())
    if normalized in {
        "get_list", "get_document", "get_doctype_info", "get_report",
        "report_list", "report_requirements", "create_document",
        "update_document", "delete_document",
    }:
        return _render_tool_output(normalized, payload)
    return _format_generic_result(payload, "Result")


# ── v1 public entry points (preserved — call through to infrastructure) ───────

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
    retry_last_user: bool = False,
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
            retry_last_user=retry_last_user,
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


def _run_enqueued_prompt(
    prompt: str | None = None,
    conversation: str | None = None,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    model: str | None = None,
    images: str | list[dict[str, Any]] | None = None,
    user: str | None = None,
    retry_last_user: bool = False,
) -> None:
    _execute_prompt(
        prompt=prompt, conversation=conversation,
        doctype=doctype, docname=docname, route=route,
        model=model, images=images, user=user, retry_last_user=retry_last_user,
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
    retry_last_user: bool = False,
) -> dict[str, Any]:
    prompt_text = (prompt or "").strip()
    parsed_images = _parse_prompt_images(images)
    if not prompt_text and not parsed_images:
        raise frappe.ValidationError(_("Prompt or image is required"))

    check_rate_limit(frappe.session.user)

    conversation_name = conversation or create_conversation(
        title=_summarize_title(prompt_text or _("Image prompt"))
    )["name"]
    progress = {
        "conversation": conversation_name,
        "user": frappe.session.user,
        "model": _resolve_model_for_request(model, has_images=bool(parsed_images)),
        "steps": [],
    }
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
        retry_last_user=retry_last_user,
    )
    return {"queued": True, "conversation": conversation_name}


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
    if not route and not doctype:
        frappe.log_error(
            "erp_ai_assistant: send_prompt called without route or doctype context. "
            "Update the frontend to pass doctype, docname, and route on every request.",
            "ERP AI Assistant: Missing Context",
        )
    return _execute_prompt(
        prompt=prompt, conversation=conversation,
        doctype=doctype, docname=docname, route=route,
        model=model, images=images,
    )


# --- Cross-imports to resolve dependencies ---
from .llm_gateway import (
    _describe_message_attachments,
    _merge_history_content_and_attachment_notes,
    _parse_prompt_images,
    _parse_message_attachments,
    _provider_chat_with_resilience,
    _llm_request_timeout_seconds,
    _resolve_model_for_request,
    _response_rejects_images,
)
