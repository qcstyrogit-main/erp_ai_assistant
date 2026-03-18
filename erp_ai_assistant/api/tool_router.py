from typing import Any

import frappe
from frappe import _

from .context_resolver import normalize_context_payload
from .entity_extractor import extract_prompt_entities


def route_prompt_pipeline(
    prompt: str,
    context: dict[str, Any] | str | None = None,
    *,
    planner_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    parsed_context = normalize_context_payload(context)
    entities = extract_prompt_entities(prompt, parsed_context)

    from .router import route_prompt

    result = route_prompt(prompt, context=parsed_context, planner_result=planner_result)
    if not isinstance(result, dict):
        result = {
            "ok": False,
            "matched": False,
            "type": "router",
            "action": None,
            "message": _("Router returned an invalid response."),
        }
    if "matched" not in result:
        result["matched"] = bool(result.get("parsed")) or str(result.get("type") or "").strip().lower() == "clarification"
    pipeline = {
        "intent": str((planner_result or {}).get("intent") or "").strip() or None,
        "route_target": (planner_result or {}).get("route_target"),
        "normalized_prompt": entities.get("normalized_prompt"),
        "target_doctype": entities.get("target_doctype"),
        "filters": entities.get("filters"),
        "export_requested": entities.get("export_requested"),
        "recommended_tools": (planner_result or {}).get("tool_names") or [],
        "recommended_resources": (planner_result or {}).get("resource_names") or [],
    }
    result["pipeline"] = pipeline
    return result


@frappe.whitelist()
def route_prompt(prompt: str, context: dict[str, Any] | str | None = None) -> dict[str, Any]:
    try:
        return route_prompt_pipeline(prompt, context)
    except frappe.PermissionError:
        return {"ok": False, "matched": True, "type": "router", "action": None, "message": _("Permission denied")}
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Tool Router Error")
        return {"ok": False, "matched": True, "type": "router", "action": None, "message": str(exc) or _("Unknown error")}
