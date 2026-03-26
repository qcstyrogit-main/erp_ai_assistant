from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from .fac_client import dispatch_tool, get_tool_definitions
from .intent_detector import detect_intent_heuristic
from .llm_client import chat_with_tools
from .prompt_builder import build_resource_snapshot, build_system_prompt, build_user_prompt
from .safety import build_confirmation_payload, classify_risk, is_confirmation_reply
from .tool_validator import normalize_tool_result, validate_tool_arguments

MAX_TOOL_ROUNDS = 8
MAX_MUTATION_REPAIRS = 2
ACTION_INTENTS = {"create", "update", "workflow", "export"}
MUTATION_CATEGORIES = {"write", "workflow", "destructive", "file", "report"}
READONLY_CATEGORIES = {"read", "resource", "system"}


@dataclass
class AgentRunResult:
    ok: bool
    reply: str
    tool_events: list[dict[str, Any] | str] = field(default_factory=list)
    pending_action: dict[str, Any] | None = None
    attachments: dict[str, Any] | None = None
    payload: dict[str, Any] | None = None
    raw_output: dict[str, Any] | None = None
    all_payloads: list[Any] = field(default_factory=list)


def run_agent_loop(
    *,
    prompt: str,
    conversation: str | None,
    context: dict[str, Any],
    model: str | None = None,
    images: list[dict[str, Any]] | None = None,
    history: list[dict[str, Any]] | None = None,
    progress: dict[str, Any] | None = None,
) -> AgentRunResult:
    intent_meta = detect_intent_heuristic(prompt, context or {}) or {}
    tools = _prioritize_tools(get_tool_definitions(user=context.get("user")) or {}, intent_meta=intent_meta, prompt=prompt)
    resource_snapshot = build_resource_snapshot(prompt=prompt, context=context, conversation=conversation)

    messages: list[dict[str, Any]] = [{"role": "system", "content": build_system_prompt(prompt=prompt, context=context, tool_definitions=tools, resource_snapshot=resource_snapshot)}]
    for item in history or []:
        if str(item.get("role") or "") in {"user", "assistant"}:
            messages.append({"role": item["role"], "content": str(item.get("content") or "")})
    messages.append({"role": "user", "content": build_user_prompt(prompt=prompt, context=context, resource_snapshot=resource_snapshot)})

    tool_events: list[dict[str, Any] | str] = []
    all_payloads: list[Any] = []
    last_payload: Any = None
    mutation_repair_count = 0
    mutation_completed = False

    for round_index in range(MAX_TOOL_ROUNDS):
        step = chat_with_tools(messages=messages, tools=tools, model=model, images=images or [])
        assistant_text = str(step.get("text") or "").strip()
        tool_calls = step.get("tool_calls") or []

        if not tool_calls:
            if _should_repair_missing_action(intent_meta, mutation_completed, last_payload, mutation_repair_count):
                mutation_repair_count += 1
                messages.append({"role": "system", "content": _mutation_repair_instruction(prompt, tools)})
                continue
            return AgentRunResult(
                ok=True,
                reply=assistant_text or _default_reply_from_payload(last_payload),
                tool_events=tool_events,
                payload=last_payload if isinstance(last_payload, dict) else None,
                raw_output=step,
                all_payloads=all_payloads,
            )

        assistant_tool_calls = []
        round_categories: list[str] = []
        for tool_call in tool_calls:
            name = str(tool_call.get("name") or "").strip()
            arguments = tool_call.get("arguments") if isinstance(tool_call.get("arguments"), dict) else {}
            tool_id = str(tool_call.get("id") or f"tool-{round_index}-{name}")
            category = _tool_category(tools.get(name))
            round_categories.append(category)

            if name not in tools:
                error = f"Tool '{name}' is not available in this session."
                tool_events.append({"tool": name, "arguments": arguments, "ok": False, "error": error})
                messages.append({"role": "tool", "tool_call_id": tool_id, "name": name, "content": json.dumps({"ok": False, "error": error})})
                continue

            validation_error = validate_tool_arguments(tools[name], arguments)
            if validation_error:
                tool_events.append({"tool": name, "arguments": arguments, "ok": False, "error": validation_error})
                messages.append({"role": "tool", "tool_call_id": tool_id, "name": name, "content": json.dumps({"ok": False, "error": validation_error})})
                continue

            risk = classify_risk(tool_name=name, arguments=arguments, context=context)
            if risk.requires_confirmation and not arguments.get("confirmed") and not arguments.get("confirmed_submit"):
                pending_action = build_confirmation_payload(
                    conversation=conversation or "",
                    tool_name=name,
                    arguments=arguments,
                    reason=risk.reason,
                    summary=risk.summary,
                )
                return AgentRunResult(
                    ok=True,
                    reply=risk.user_message,
                    tool_events=tool_events,
                    pending_action=pending_action,
                    payload={"type": "pending_action", "pending_action": pending_action, "message": risk.user_message},
                    raw_output=step,
                    all_payloads=all_payloads,
                )

            assistant_tool_calls.append({"id": tool_id, "function": {"name": name, "arguments": json.dumps(arguments)}})
            try:
                raw_result = dispatch_tool(name, arguments, user=context.get("user"))
                normalized = normalize_tool_result(name, raw_result)
                normalized.setdefault("ok", bool(normalized.get("success", True)))
                tool_events.append({"tool": name, "arguments": arguments, "ok": bool(normalized.get("ok", True)), "result": normalized})
                messages.append({"role": "tool", "tool_call_id": tool_id, "name": name, "content": json.dumps(normalized, default=str)})
                last_payload = normalized
                all_payloads.append(normalized)
                if category in MUTATION_CATEGORIES and bool(normalized.get("ok", True)):
                    mutation_completed = True
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                tool_events.append({"tool": name, "arguments": arguments, "ok": False, "error": error})
                messages.append({"role": "tool", "tool_call_id": tool_id, "name": name, "content": json.dumps({"ok": False, "error": error})})

        messages.append({"role": "assistant", "content": assistant_text, "tool_calls": assistant_tool_calls})

        if _should_repair_read_only_round(intent_meta, round_categories, mutation_completed, mutation_repair_count):
            mutation_repair_count += 1
            messages.append({"role": "system", "content": _mutation_repair_instruction(prompt, tools)})

    return AgentRunResult(
        ok=False,
        reply="I stopped because the task needed too many tool rounds. Please narrow the request or confirm the action.",
        tool_events=tool_events,
        payload=last_payload if isinstance(last_payload, dict) else None,
        all_payloads=all_payloads,
    )


def resume_pending_action(
    *,
    prompt: str,
    conversation: str,
    context: dict[str, Any],
    pending_action: dict[str, Any],
    model: str | None = None,
) -> AgentRunResult:
    if is_confirmation_reply(prompt):
        tool_name = str(pending_action.get("tool_name") or "").strip()
        arguments = dict(pending_action.get("arguments") or {})
        arguments["confirmed"] = True
        arguments["confirmed_submit"] = True
        raw_result = dispatch_tool(tool_name, arguments, user=context.get("user"))
        payload = normalize_tool_result(tool_name, raw_result)
        return AgentRunResult(
            ok=bool(payload.get("ok", payload.get("success", True))),
            reply=str(payload.get("message") or f"Completed {tool_name}."),
            tool_events=[{"tool": tool_name, "arguments": arguments, "ok": bool(payload.get("ok", payload.get("success", True))), "result": payload}],
            payload=payload,
            all_payloads=[payload],
        )
    return AgentRunResult(
        ok=True,
        reply="I still need an explicit confirmation to continue. Reply with Yes to proceed or No to cancel.",
        pending_action=pending_action,
        payload={"type": "pending_action", "pending_action": pending_action},
    )


def _prioritize_tools(tools: dict[str, dict[str, Any]], *, intent_meta: dict[str, Any], prompt: str) -> dict[str, dict[str, Any]]:
    intent = str(intent_meta.get("intent") or "unknown").strip().lower()
    target_names = _preferred_tool_names(intent=intent, prompt=prompt, tools=tools)

    def sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, int, str]:
        name, spec = item
        category = _tool_category(spec)
        target_rank = 0 if name in target_names else 1
        category_rank = 5
        if intent in {"create", "update", "workflow"}:
            if category in {"write", "workflow", "destructive"}:
                category_rank = 0
            elif category in {"file", "report"}:
                category_rank = 1
            elif category == "read":
                category_rank = 2
            elif category == "resource":
                category_rank = 3
        elif intent == "export":
            if category in {"file", "report"}:
                category_rank = 0
            elif category == "read":
                category_rank = 1
            elif category in {"write", "workflow"}:
                category_rank = 2
        elif intent == "read":
            if category == "read":
                category_rank = 0
            elif category == "resource":
                category_rank = 1
        return (target_rank, category_rank, name)

    ordered = OrderedDict()
    for name, spec in sorted(tools.items(), key=sort_key):
        ordered[name] = spec
    return dict(ordered)


def _preferred_tool_names(*, intent: str, prompt: str, tools: dict[str, dict[str, Any]]) -> set[str]:
    lowered = str(prompt or "").lower()
    preferred: set[str] = set()
    if intent == "create":
        if "sales invoice" in lowered and "create_sales_invoice" in tools:
            preferred.add("create_sales_invoice")
        if "sales order" in lowered and "create_sales_order" in tools:
            preferred.add("create_sales_order")
        if "quotation" in lowered and "create_quotation" in tools:
            preferred.add("create_quotation")
        if "purchase order" in lowered and "create_purchase_order" in tools:
            preferred.add("create_purchase_order")
        if "create_document" in tools:
            preferred.add("create_document")
        if "create_erp_document" in tools:
            preferred.add("create_erp_document")
    elif intent == "update":
        preferred.update(name for name in ("update_document", "update_erp_document") if name in tools)
    elif intent == "workflow":
        preferred.update(name for name in ("submit_erp_document", "cancel_erp_document", "run_workflow_action") if name in tools)
    elif intent == "export":
        preferred.update(name for name in tools if name.startswith("export_") or name.startswith("generate_"))
    return preferred


def _tool_category(spec: dict[str, Any] | None) -> str:
    annotations = spec.get("annotations") if isinstance(spec, dict) else None
    category = None
    if isinstance(annotations, dict):
        category = annotations.get("category")
    return str(category or "other").strip().lower()


def _should_repair_read_only_round(intent_meta: dict[str, Any], round_categories: list[str], mutation_completed: bool, repair_count: int) -> bool:
    intent = str(intent_meta.get("intent") or "").strip().lower()
    if intent not in ACTION_INTENTS or mutation_completed or repair_count >= MAX_MUTATION_REPAIRS:
        return False
    categories = {str(cat or "other").strip().lower() for cat in round_categories}
    return bool(categories) and categories.issubset(READONLY_CATEGORIES)


def _should_repair_missing_action(intent_meta: dict[str, Any], mutation_completed: bool, last_payload: Any, repair_count: int) -> bool:
    intent = str(intent_meta.get("intent") or "").strip().lower()
    if intent not in ACTION_INTENTS or mutation_completed or repair_count >= MAX_MUTATION_REPAIRS:
        return False
    if not isinstance(last_payload, dict):
        return True
    payload_type = str(last_payload.get("type") or "").strip().lower()
    if payload_type in {"document", "file", "report"}:
        return False
    if any(key in last_payload for key in ("count", "data", "filters_applied")):
        return True
    return not bool(last_payload.get("message"))


def _mutation_repair_instruction(prompt: str, tools: dict[str, dict[str, Any]]) -> str:
    preferred = ", ".join(sorted(_preferred_tool_names(intent="create" if "create" in str(prompt or "").lower() else "update", prompt=prompt, tools=tools)))
    return (
        "The user requested an action, not just a lookup. "
        "Do not stop at search or matching results. "
        "Continue with the requested mutation, export, or workflow action if you have enough data; otherwise ask only for the missing required fields. "
        f"Prefer these tools when relevant: {preferred or 'action tools before read tools'}."
    )


def _default_reply_from_payload(payload: Any) -> str:
    if isinstance(payload, dict):
        message = str(payload.get("message") or "").strip()
        if message:
            return message
        if payload.get("type") == "file" and payload.get("file_url"):
            return f"File generated: {payload['file_url']}"
        if payload.get("count") is not None:
            return f"Found {payload.get('count')} matching records."
    return "Done."
