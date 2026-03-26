from __future__ import annotations

from typing import Any

try:
    import jsonschema  # type: ignore
except Exception:
    jsonschema = None


def validate_tool_arguments(spec: dict[str, Any], arguments: dict[str, Any] | None) -> str | None:
    schema = spec.get("inputSchema") if isinstance(spec, dict) else {}
    schema = schema if isinstance(schema, dict) else {}
    payload = arguments or {}
    if not isinstance(payload, dict):
        return "Tool arguments must be a JSON object."

    if jsonschema:
        try:
            jsonschema.validate(payload, schema or {"type": "object"})
            return None
        except Exception as exc:  # pragma: no cover - depends on optional package
            return f"Invalid tool arguments: {exc}"

    required = schema.get("required") or []
    properties = schema.get("properties") or {}
    if not isinstance(properties, dict):
        properties = {}
    for field in required:
        if field not in payload:
            return f"Missing required field: {field}"

    if schema.get("additionalProperties") is False:
        extras = sorted(set(payload.keys()) - set(properties.keys()))
        if extras:
            return f"Unknown fields: {', '.join(extras)}"

    for key, definition in properties.items():
        if key not in payload or not isinstance(definition, dict):
            continue
        expected = definition.get("type")
        value = payload.get(key)
        if value is None or not expected:
            continue
        if expected == "string" and not isinstance(value, str):
            return f"Field '{key}' must be a string."
        if expected == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
            return f"Field '{key}' must be an integer."
        if expected == "number" and not isinstance(value, (int, float)):
            return f"Field '{key}' must be a number."
        if expected == "boolean" and not isinstance(value, bool):
            return f"Field '{key}' must be a boolean."
        if expected == "array" and not isinstance(value, list):
            return f"Field '{key}' must be an array."
        if expected == "object" and not isinstance(value, dict):
            return f"Field '{key}' must be an object."
    return None


def normalize_tool_result(tool_name: str, raw_result: Any) -> dict[str, Any]:
    if isinstance(raw_result, dict):
        result = dict(raw_result)
        result.setdefault("ok", bool(result.get("success", True)))
        result.setdefault("tool_name", tool_name)
        return result
    return {
        "ok": True,
        "tool_name": tool_name,
        "data": raw_result,
    }
