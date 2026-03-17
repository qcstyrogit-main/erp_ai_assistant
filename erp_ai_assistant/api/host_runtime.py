from typing import Any

from .chat import get_pending_action
from .context_resolver import normalize_context_payload
from .planner import classify_prompt_internal
from .resource_registry import get_resource_catalog_summary, read_resource
from .tool_registry import get_tool_catalog_summary, list_tool_specs
from .tool_router import route_prompt_pipeline


def _resource_arguments(name: str, context: dict[str, Any], conversation: str | None = None) -> dict[str, Any]:
    arguments: dict[str, Any] = {}
    if name == "doctype_schema" and context.get("doctype"):
        arguments["doctype"] = context.get("doctype")
    elif name == "pending_assistant_action" and conversation:
        arguments["conversation"] = conversation
    return arguments


def _read_recommended_resources(
    context: dict[str, Any],
    *,
    planner_result: dict[str, Any],
    conversation: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for name in planner_result.get("resource_names") or []:
        resource_name = str(name or "").strip()
        if not resource_name:
            continue
        try:
            payload[resource_name] = read_resource(
                resource_name,
                context=context,
                arguments=_resource_arguments(resource_name, context, conversation=conversation),
            )
        except Exception as exc:
            payload[resource_name] = {
                "ok": False,
                "type": "resource",
                "resource": resource_name,
                "message": str(exc) or "Resource read failed.",
                "data": None,
            }
    return payload


def build_host_session(
    prompt: str,
    context: dict[str, Any] | str | None = None,
    *,
    conversation: str | None = None,
    planner_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = normalize_context_payload(context)
    if conversation:
        current.setdefault("conversation", conversation)
    plan = planner_result or classify_prompt_internal(prompt, current)
    recommended_names = {str(name or "").strip() for name in (plan.get("tool_names") or []) if str(name or "").strip()}
    recommended_tools = [row for row in list_tool_specs() if str(row.get("name") or "").strip() in recommended_names]
    pending_action = get_pending_action(conversation) if conversation else None
    resources = _read_recommended_resources(current, planner_result=plan, conversation=conversation)
    return {
        "prompt": str(prompt or "").strip(),
        "context": current,
        "planner": plan,
        "pending_action": pending_action,
        "recommended_tools": recommended_tools,
        "recommended_resources": resources,
        "route_target": str(plan.get("route_target") or "").strip() or "provider_chat",
    }


def execute_host_turn(
    prompt: str,
    context: dict[str, Any] | str | None = None,
    *,
    conversation: str | None = None,
    planner_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session = build_host_session(
        prompt,
        context,
        conversation=conversation,
        planner_result=planner_result,
    )
    should_route = bool(session["pending_action"]) or session["route_target"] == "deterministic_router"
    routed = route_prompt_pipeline(
        prompt,
        context=session["context"],
        planner_result=session["planner"],
    ) if should_route else {"matched": False}
    return {
        "matched": bool((routed or {}).get("matched")),
        "result": routed,
        "session": session,
    }


def get_host_capabilities() -> dict[str, Any]:
    return {
        "ok": True,
        "type": "host_capabilities",
        "tool_catalog": get_tool_catalog_summary(),
        "resource_catalog": get_resource_catalog_summary(),
    }
