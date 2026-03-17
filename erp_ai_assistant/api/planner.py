from typing import Any

import frappe

from .context_resolver import normalize_context_payload
from .intent_detector import detect_intent_heuristic, normalize_prompt
from .resource_registry import list_resource_specs
from .tool_registry import list_tool_specs


ALLOWED_INTENTS = {
    "answer",
    "guide",
    "read",
    "create",
    "update",
    "workflow",
    "export",
    "erp_chat",
    "general_chat",
    "unknown",
}

INTENT_TOOL_CATEGORIES = {
    "answer": {"read"},
    "guide": {"resource"},
    "read": {"read", "resource"},
    "create": {"write"},
    "update": {"write"},
    "workflow": {"workflow"},
    "export": {"file", "report"},
    "erp_chat": {"read", "resource"},
}

INTENT_PREFERRED_TOOLS = {
    "answer": ("answer_erp_query",),
    "guide": ("describe_erp_schema", "get_doctype_fields", "list_erp_doctypes"),
    "read": ("list_erp_documents", "get_erp_document", "search_erp_documents", "answer_erp_query"),
    "create": ("create_sales_order", "create_quotation", "create_purchase_order", "create_erp_document", "create_document"),
    "update": ("update_erp_document", "update_document"),
    "workflow": ("submit_erp_document", "cancel_erp_document", "run_workflow_action"),
    "export": ("export_doctype_list_excel", "export_employee_list_excel", "generate_document_pdf", "generate_report"),
    "erp_chat": ("get_erp_document", "list_erp_documents", "describe_erp_schema"),
}

INTENT_PREFERRED_RESOURCES = {
    "guide": ("doctype_schema", "available_doctypes"),
    "read": ("current_document", "current_page_context", "doctype_schema"),
    "create": ("doctype_schema", "current_page_context", "pending_assistant_action"),
    "update": ("current_document", "doctype_schema", "pending_assistant_action"),
    "workflow": ("current_document", "current_page_context", "pending_assistant_action"),
    "export": ("current_page_context", "doctype_schema", "available_doctypes"),
    "erp_chat": ("current_document", "current_page_context", "pending_assistant_action"),
}
def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def _safe_confidence(value: Any, fallback: float) -> float:
    try:
        confidence = float(value)
    except Exception:
        return fallback
    return max(0.0, min(1.0, confidence))


def _resource_lookup() -> dict[str, dict[str, Any]]:
    return {row["name"]: row for row in list_resource_specs()}


def _tool_lookup() -> dict[str, dict[str, Any]]:
    tools = {}
    for category in {value for values in INTENT_TOOL_CATEGORIES.values() for value in values} | {"system", "destructive"}:
        for row in list_tool_specs(category=category):
            tools[row["name"]] = row
    return tools


def _select_planner_tools(intent: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    allowed_categories = INTENT_TOOL_CATEGORIES.get(intent, set())
    if not allowed_categories:
        return []

    preferred = INTENT_PREFERRED_TOOLS.get(intent, ())
    tool_lookup = _tool_lookup()
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()

    for name in preferred:
        row = tool_lookup.get(name)
        if row and name not in seen:
            selected.append(row)
            seen.add(name)

    for category in allowed_categories:
        for row in list_tool_specs(category=category):
            name = str(row.get("name") or "").strip()
            if name and name not in seen:
                selected.append(row)
                seen.add(name)

    if intent in {"read", "erp_chat", "update", "workflow"} and context.get("doctype") and context.get("docname"):
        for contextual_name in ("get_erp_document", "current_document"):
            if contextual_name in seen:
                continue
            row = tool_lookup.get(contextual_name)
            if row:
                selected.insert(0, row)
                seen.add(contextual_name)
    return selected[:8]


def _select_planner_resources(intent: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    preferred = INTENT_PREFERRED_RESOURCES.get(intent, ())
    resource_lookup = _resource_lookup()
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()

    for name in preferred:
        row = resource_lookup.get(name)
        if not row or name in seen:
            continue
        if name == "current_document" and not (context.get("doctype") and context.get("docname")):
            continue
        selected.append(row)
        seen.add(name)

    if context.get("doctype") and "doctype_schema" not in seen:
        row = resource_lookup.get("doctype_schema")
        if row:
            selected.insert(0, row)
            seen.add("doctype_schema")
    return selected[:6]


def _route_target_for_plan(intent: str, should_route: bool) -> str:
    if not should_route:
        return "provider_chat"
    if intent in {"create", "update", "workflow", "export", "read", "answer", "guide", "erp_chat"}:
        return "deterministic_router"
    return "provider_chat"


def _planner_reason_with_catalog(
    reason: str,
    *,
    intent: str,
    tools: list[dict[str, Any]],
    resources: list[dict[str, Any]],
) -> str:
    base = str(reason or "").strip()
    if base:
        return base
    tool_names = ", ".join(str(row.get("name") or "") for row in tools[:2] if row.get("name"))
    resource_names = ", ".join(str(row.get("name") or "") for row in resources[:2] if row.get("name"))
    detail = []
    if tool_names:
        detail.append(f"tools: {tool_names}")
    if resource_names:
        detail.append(f"resources: {resource_names}")
    suffix = f" using {', '.join(detail)}" if detail else ""
    return f"Planned as {intent}{suffix}."


def _enrich_plan(plan: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    intent = str(plan.get("intent") or "").strip().lower()
    should_route = bool(plan.get("should_route"))
    recommended_tools = _select_planner_tools(intent, context)
    recommended_resources = _select_planner_resources(intent, context)
    plan["recommended_tools"] = recommended_tools
    plan["recommended_resources"] = recommended_resources
    plan["route_target"] = _route_target_for_plan(intent, should_route)
    plan["tool_names"] = [str(row.get("name") or "").strip() for row in recommended_tools if row.get("name")]
    plan["resource_names"] = [str(row.get("name") or "").strip() for row in recommended_resources if row.get("name")]
    plan["reason"] = _planner_reason_with_catalog(
        str(plan.get("reason") or "").strip(),
        intent=intent,
        tools=recommended_tools,
        resources=recommended_resources,
    )
    return plan


def _normalize_model_plan(raw_plan: Any, prompt: str, heuristic: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw_plan, dict):
        return None
    intent = str(raw_plan.get("intent") or "").strip().lower()
    if intent not in ALLOWED_INTENTS:
        return None
    normalized_prompt = str(raw_plan.get("normalized_prompt") or "").strip() or heuristic.get("normalized_prompt") or _normalize_prompt(prompt)
    return {
        "intent": intent,
        "confidence": _safe_confidence(raw_plan.get("confidence"), float(heuristic.get("confidence") or 0.0)),
        "should_route": _safe_bool(raw_plan.get("should_route"), bool(heuristic.get("should_route"))),
        "normalized_prompt": normalized_prompt,
        "reason": str(raw_plan.get("reason") or "").strip(),
        "source": "model",
    }


def _prefer_heuristic(heuristic: dict[str, Any], model_plan: dict[str, Any]) -> bool:
    heuristic_intent = str(heuristic.get("intent") or "").strip().lower()
    if heuristic_intent == "guide" and float(heuristic.get("confidence") or 0.0) >= 0.9:
        return True
    if heuristic_intent in {"create", "update", "workflow"} and float(heuristic.get("confidence") or 0.0) >= 0.95:
        if not bool(model_plan.get("should_route")):
            return True
    if float(model_plan.get("confidence") or 0.0) < 0.55:
        return True
    return False


def classify_prompt_internal(prompt: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    context = context or {}
    heuristic = detect_intent_heuristic(prompt, context)
    result = _enrich_plan(dict(heuristic), context)
    result["source"] = "heuristic"

    try:
        from . import ai as ai_api

        if ai_api._llm_chat_configured():
            model_plan = _normalize_model_plan(ai_api.plan_prompt_with_model(prompt, context), prompt, heuristic)
            if model_plan and not _prefer_heuristic(heuristic, model_plan):
                result = _enrich_plan(model_plan, context)
            elif model_plan:
                result["model_suggestion"] = {
                    "intent": model_plan.get("intent"),
                    "confidence": model_plan.get("confidence"),
                    "route_target": _route_target_for_plan(
                        str(model_plan.get("intent") or "").strip().lower(),
                        bool(model_plan.get("should_route")),
                    ),
                }
    except Exception:
        pass

    return result


@frappe.whitelist()
def classify_prompt(prompt: str, context: dict[str, Any] | str | None = None) -> dict[str, Any]:
    parsed_context = normalize_context_payload(context)
    return classify_prompt_internal(prompt, parsed_context)
