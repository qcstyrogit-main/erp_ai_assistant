import json
from typing import Any


def normalize_context_payload(context: dict[str, Any] | str | None) -> dict[str, Any]:
    parsed = context
    if isinstance(context, str):
        try:
            parsed = json.loads(context)
        except Exception:
            parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    return parsed


def build_request_context(
    *,
    doctype: str | None = None,
    docname: str | None = None,
    route: str | None = None,
    user: str | None = None,
    base: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = dict(base or {})
    if doctype is not None:
        context["doctype"] = doctype
    if docname is not None:
        context["docname"] = docname
    if route is not None:
        context["route"] = route
    if user is not None:
        context["user"] = user
    return context
