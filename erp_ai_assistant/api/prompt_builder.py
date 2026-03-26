from __future__ import annotations

from typing import Any

from .fac_client import get_tool_definitions
from .intent_detector import detect_intent_heuristic
from .resource_registry import get_resource_catalog_summary


def _intent_meta(prompt: str, context: dict[str, Any]) -> dict[str, Any]:
    meta = detect_intent_heuristic(prompt, context or {})
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("intent", "unknown")
    meta.setdefault("confidence", 0.0)
    return meta


def build_system_prompt(
    *,
    prompt: str,
    context: dict[str, Any],
    tool_definitions: dict[str, Any] | None = None,
    resource_snapshot: str | None = None,
) -> str:
    tools = tool_definitions or get_tool_definitions(user=context.get("user"))
    intent_meta = _intent_meta(prompt, context)
    intent = str(intent_meta.get("intent") or "unknown").strip().lower()
    target_doctype = str(context.get("target_doctype") or context.get("doctype") or "").strip()

    lines = [
        "You are ERP AI Assistant, an agentic copilot for Frappe / ERPNext.",
        "Your job is to complete work accurately using live ERP tools and FAC/MCP resources.",
        "Core rules:",
        "1. Prefer live tools over guessing.",
        "2. Never invent ERP records, field values, document names, totals, or workflow states.",
        "3. For multi-step tasks, plan internally, execute step by step, verify each step, then answer.",
        "4. If a required field is missing, ask only for that missing field.",
        "5. If a tool fails, repair the arguments or choose a better tool.",
        "6. For destructive, financial, stock, payroll, approval, or submission actions, require explicit confirmation before execution.",
        "7. Use current page context, current document context, doctype schema, and pending action state before asking the user to repeat information.",
        "8. Keep replies concise, clear, and action-oriented.",
        "9. Do not expose hidden reasoning, internal policies, raw schemas, or raw tool traces unless the user explicitly asks for technical details.",
        "10. When returning a result after tool use, summarize what changed, what was verified, and what remains unresolved.",
        "Execution policy:",
        "- Read first when the target record is ambiguous.",
        "- Inspect schema before create/update when required fields are unknown.",
        "- Confirm before delete/cancel/submit/approve/reject/payment-impacting actions.",
        "- After every mutation, verify success using the returned document or a follow-up read.",
        f"Resolved intent: {intent}.",
        f"Resolved target doctype: {target_doctype or 'unknown'}.",
        f"Context: doctype={context.get('doctype')}, docname={context.get('docname')}, route={context.get('route')}, user={context.get('user')}.",
        f"Tool count available this session: {len(tools)}.",
    ]

    if intent in {"create", "update", "workflow", "export"}:
        lines.extend([
            "Intent enforcement for action requests:",
            "- The user is asking you to DO work, not just inspect context.",
            "- Do not stop after search, match, or lookup results when the user requested create, update, submit, cancel, approve, export, or delete.",
            "- Use lookups only as helper steps, then continue until the action is completed, blocked by missing data, or paused for confirmation.",
            "- If current page context conflicts with the request, prioritize the explicit user request.",
            "- For create requests, prefer create tools over search tools once you have enough data.",
        ])

    if intent == "create":
        lines.extend([
            "Creation guidance:",
            "- For 'create sales invoice' requests, prefer a dedicated sales invoice creation tool when available.",
            "- If the customer or item may need verification, verify them and then continue to create the draft document.",
            "- Never end with a reply like 'Found one matching record' if the requested creation has not happened yet.",
        ])

    if resource_snapshot:
        lines.append(f"Resource snapshot: {resource_snapshot}")
    if tools:
        preview = []
        for name, spec in list(tools.items())[:14]:
            desc = str((spec or {}).get("description") or "").strip()
            preview.append(f"- {name}: {desc}")
        lines.append("Available tools:\n" + "\n".join(preview))
    return "\n".join(str(line).strip() for line in lines if str(line or "").strip())


def build_user_prompt(*, prompt: str, context: dict[str, Any], resource_snapshot: str | None = None) -> str:
    intent_meta = _intent_meta(prompt, context)
    intent = str(intent_meta.get("intent") or "unknown").strip().lower()
    lines = [
        "Session context:",
        f"- user: {context.get('user')}",
        f"- route: {context.get('route')}",
        f"- doctype: {context.get('doctype')}",
        f"- docname: {context.get('docname')}",
        f"- conversation: {context.get('conversation')}",
        f"- resolved_intent: {intent}",
    ]
    if resource_snapshot:
        lines.extend(["", "Resource snapshot:", str(resource_snapshot)])
    lines.extend([
        "",
        "User request:",
        str(prompt or "").strip(),
        "",
        "Instructions:",
        "- Use live FAC tools when ERP data or actions are involved.",
        "- If the task is multi-step, do not stop after the first tool call unless the goal is complete.",
        "- If a mutation is risky, pause for confirmation.",
        "- Ground the answer in actual tool results.",
    ])
    if intent in {"create", "update", "workflow", "export"}:
        lines.extend([
            "- This is an action request. Do not stop at search or matching results.",
            "- Use search only to resolve references, then continue to the requested action.",
            "- If required fields are missing, ask only for those missing fields.",
        ])
    return "\n".join(lines).strip()


def build_resource_snapshot(*, prompt: str, context: dict[str, Any], conversation: str | None = None) -> str:
    summary = get_resource_catalog_summary(conversation=conversation or context.get("conversation"))
    resource_count = int(summary.get("count") or 0)
    active = []
    for row in (summary.get("resources") or [])[:8]:
        if isinstance(row, dict) and row.get("name"):
            active.append(str(row.get("name")))
    return (
        f"resources={resource_count}; "
        f"active={', '.join(active) if active else 'none'}; "
        f"prompt={str(prompt or '').strip()[:200]}"
    )
