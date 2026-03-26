from __future__ import annotations

import json
from typing import Any

import requests

from . import ai as ai_helpers


def chat_with_tools(
    *,
    messages: list[dict[str, Any]],
    tools: dict[str, dict[str, Any]],
    model: str | None = None,
    images: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    provider = ai_helpers._provider_name()
    selected_model = ai_helpers._resolve_model(model)
    if provider in {"openai", "openai_compatible"}:
        return _openai_step(provider=provider, messages=messages, tools=tools, model=selected_model, images=images or [])
    return _anthropic_step(messages=messages, tools=tools, model=selected_model, images=images or [])


def _openai_step(*, provider: str, messages: list[dict[str, Any]], tools: dict[str, dict[str, Any]], model: str, images: list[dict[str, str]]) -> dict[str, Any]:
    base_url = str(
        ai_helpers._cfg(
            "OPENAI_BASE_URL",
            "https://api.openai.com" if provider == "openai" else "https://integrate.api.nvidia.com",
        )
        or ""
    ).rstrip("/")
    path = str(
        ai_helpers._cfg(
            "OPENAI_RESPONSES_PATH",
            ai_helpers.DEFAULT_OPENAI_RESPONSES_PATH if provider == "openai" else "/v1/chat/completions",
        )
        or ""
    )
    if not path.startswith("/"):
        path = f"/{path}"
    endpoint = f"{base_url}{path}"
    api_key = str(ai_helpers._cfg("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured.")

    headers = {"content-type": "application/json", "authorization": f"Bearer {api_key}"}
    uses_responses = path.rstrip("/").endswith("/responses")

    if uses_responses:
        payload = {
            "model": model,
            "instructions": _system_text(messages),
            "input": _to_responses_input(messages, images),
        }
        if tools:
            payload["tools"] = [_to_openai_tool_spec(name, spec) for name, spec in tools.items()]
            payload["tool_choice"] = "auto"
    else:
        payload = {
            "model": model,
            "messages": _to_openai_chat_messages(messages, images),
        }
        if tools:
            payload["tools"] = [{"type": "function", "function": _to_openai_function_spec(name, spec)} for name, spec in tools.items()]
            payload["tool_choice"] = "auto"

    response = requests.post(endpoint, headers=headers, json=payload, timeout=ai_helpers._llm_request_timeout_seconds())
    detail = ai_helpers._extract_error_detail(response) if response.status_code >= 400 else ""
    try:
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError(detail or str(exc) or "OpenAI-compatible request failed.")
    body = ai_helpers._parse_backend_json(response, endpoint)

    if uses_responses:
        return {
            "text": ai_helpers._openai_output_text(body),
            "tool_calls": _parse_openai_responses_tool_calls(body),
            "raw": body,
        }

    message = (((body.get("choices") or [{}])[0]).get("message") or {}) if isinstance(body, dict) else {}
    text = str(message.get("content") or "").strip()
    tool_calls = []
    for row in message.get("tool_calls") or []:
        if not isinstance(row, dict):
            continue
        function = row.get("function") or {}
        try:
            arguments = json.loads(function.get("arguments") or "{}")
        except Exception:
            arguments = {}
        tool_calls.append({
            "id": str(row.get("id") or function.get("name") or "tool-call"),
            "name": str(function.get("name") or "").strip(),
            "arguments": arguments,
        })
    return {"text": text, "tool_calls": tool_calls, "raw": body}


def _anthropic_step(*, messages: list[dict[str, Any]], tools: dict[str, dict[str, Any]], model: str, images: list[dict[str, str]]) -> dict[str, Any]:
    base_url = str(ai_helpers._cfg("ANTHROPIC_BASE_URL", "https://api.anthropic.com") or "").rstrip("/")
    path = str(ai_helpers._cfg("ANTHROPIC_MESSAGES_PATH", "/v1/messages") or "/v1/messages")
    if not path.startswith("/"):
        path = f"/{path}"
    endpoint = f"{base_url}{path}"

    api_key = str(ai_helpers._cfg("ANTHROPIC_API_KEY", "") or "").strip()
    auth_token = str(ai_helpers._cfg("ANTHROPIC_AUTH_TOKEN", "") or "").strip()
    if not api_key and not auth_token:
        raise RuntimeError("Anthropic authentication is not configured.")

    headers = {"content-type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    else:
        headers["authorization"] = f"Bearer {auth_token}"
    anthropic_version = ai_helpers._cfg("ANTHROPIC_VERSION", "2023-06-01")
    if anthropic_version:
        headers["anthropic-version"] = anthropic_version
    beta_param = ai_helpers._normalize_beta_param(ai_helpers._cfg("ANTHROPIC_BETA"))
    if beta_param:
        headers["anthropic-beta"] = beta_param

    payload = {
        "model": model,
        "max_tokens": ai_helpers._llm_request_max_tokens(),
        "stream": False,
        "system": _system_text(messages),
        "messages": _to_anthropic_messages(messages, images),
    }
    if tools:
        payload["tools"] = [_to_anthropic_tool_spec(name, spec) for name, spec in tools.items()]

    response = requests.post(endpoint, headers=headers, json=payload, timeout=ai_helpers._llm_request_timeout_seconds())
    detail = ai_helpers._extract_error_detail(response) if response.status_code >= 400 else ""
    try:
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError(detail or str(exc) or "Anthropic request failed.")
    body = ai_helpers._parse_backend_json(response, endpoint)

    text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    for block in body.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text" and block.get("text"):
            text_parts.append(str(block.get("text") or "").strip())
        elif block.get("type") == "tool_use":
            tool_calls.append(
                {
                    "id": str(block.get("id") or block.get("name") or "tool-call"),
                    "name": str(block.get("name") or "").strip(),
                    "arguments": block.get("input") if isinstance(block.get("input"), dict) else {},
                }
            )
    return {"text": "\n".join(part for part in text_parts if part).strip(), "tool_calls": tool_calls, "raw": body}


def _system_text(messages: list[dict[str, Any]]) -> str:
    return "\n\n".join(str(msg.get("content") or "").strip() for msg in messages if msg.get("role") == "system").strip()


def _to_openai_function_spec(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "description": str(spec.get("description") or name),
        "parameters": spec.get("inputSchema") or {"type": "object", "properties": {}, "additionalProperties": False},
    }


def _to_openai_tool_spec(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    function_spec = _to_openai_function_spec(name, spec)
    return {"type": "function", **function_spec}


def _to_anthropic_tool_spec(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "description": str(spec.get("description") or name),
        "input_schema": spec.get("inputSchema") or {"type": "object", "properties": {}, "additionalProperties": False},
    }


def _to_openai_chat_messages(messages: list[dict[str, Any]], images: list[dict[str, str]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    last_user_idx = max((idx for idx, msg in enumerate(messages) if msg.get("role") == "user"), default=-1)
    for idx, msg in enumerate(messages):
        role = str(msg.get("role") or "user")
        if role == "tool":
            payload.append({
                "role": "tool",
                "tool_call_id": msg.get("tool_call_id"),
                "content": str(msg.get("content") or ""),
            })
            continue
        content = str(msg.get("content") or "")
        if role == "assistant" and msg.get("tool_calls"):
            payload.append({"role": "assistant", "content": content, "tool_calls": msg.get("tool_calls")})
            continue
        if role == "user" and idx == last_user_idx and images:
            blocks: list[dict[str, Any]] = []
            if content:
                blocks.append({"type": "text", "text": content})
            for image in images:
                blocks.append({"type": "image_url", "image_url": {"url": f"data:{image['media_type']};base64,{image['data']}"}})
            payload.append({"role": "user", "content": blocks})
        else:
            payload.append({"role": role, "content": content})
    return payload


def _to_responses_input(messages: list[dict[str, Any]], images: list[dict[str, str]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    last_user_idx = max((idx for idx, msg in enumerate(messages) if msg.get("role") == "user"), default=-1)
    for idx, msg in enumerate(messages):
        role = str(msg.get("role") or "user")
        if role == "system":
            continue
        if role == "tool":
            payload.append({
                "type": "function_call_output",
                "call_id": msg.get("tool_call_id"),
                "output": str(msg.get("content") or ""),
            })
            continue
        if role == "assistant" and msg.get("tool_calls"):
            items: list[dict[str, Any]] = []
            if msg.get("content"):
                items.append({"type": "output_text", "text": str(msg.get("content") or "")})
            for tool_call in msg.get("tool_calls") or []:
                function = tool_call.get("function") or {}
                items.append({
                    "type": "function_call",
                    "call_id": tool_call.get("id"),
                    "name": function.get("name"),
                    "arguments": function.get("arguments"),
                })
            payload.append({"role": "assistant", "content": items})
            continue
        blocks: list[dict[str, Any]] = []
        content = str(msg.get("content") or "")
        if content:
            blocks.append({"type": "input_text", "text": content})
        if role == "user" and idx == last_user_idx and images:
            for image in images:
                blocks.append({"type": "input_image", "image_url": f"data:{image['media_type']};base64,{image['data']}"})
        payload.append({"role": role, "content": blocks})
    return payload


def _to_anthropic_messages(messages: list[dict[str, Any]], images: list[dict[str, str]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    last_user_idx = max((idx for idx, msg in enumerate(messages) if msg.get("role") == "user"), default=-1)
    for idx, msg in enumerate(messages):
        role = str(msg.get("role") or "user")
        if role == "system":
            continue
        if role == "tool":
            payload.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": msg.get("tool_call_id"),
                    "content": str(msg.get("content") or ""),
                }],
            })
            continue
        if role == "assistant" and msg.get("tool_calls"):
            content_blocks: list[dict[str, Any]] = []
            if msg.get("content"):
                content_blocks.append({"type": "text", "text": str(msg.get("content") or "")})
            for row in msg.get("tool_calls") or []:
                function = row.get("function") or {}
                arguments = function.get("arguments")
                if isinstance(arguments, str):
                    try:
                        arguments = json.loads(arguments)
                    except Exception:
                        arguments = {}
                content_blocks.append({
                    "type": "tool_use",
                    "id": row.get("id"),
                    "name": function.get("name"),
                    "input": arguments if isinstance(arguments, dict) else {},
                })
            payload.append({"role": "assistant", "content": content_blocks})
            continue
        content = str(msg.get("content") or "")
        blocks: list[dict[str, Any]] = []
        if content:
            blocks.append({"type": "text", "text": content})
        if role == "user" and idx == last_user_idx and images:
            for image in images:
                blocks.append({"type": "image", "source": {"type": "base64", "media_type": image["media_type"], "data": image["data"]}})
        payload.append({"role": role, "content": blocks or [{"type": "text", "text": ""}]})
    return payload


def _parse_openai_responses_tool_calls(body: dict[str, Any]) -> list[dict[str, Any]]:
    tool_calls: list[dict[str, Any]] = []
    for item in body.get("output") or []:
        if not isinstance(item, dict) or item.get("type") != "function_call":
            continue
        raw_arguments = item.get("arguments")
        if isinstance(raw_arguments, str):
            try:
                arguments = json.loads(raw_arguments)
            except Exception:
                arguments = {}
        else:
            arguments = raw_arguments if isinstance(raw_arguments, dict) else {}
        tool_calls.append({
            "id": str(item.get("call_id") or item.get("id") or item.get("name") or "tool-call"),
            "name": str(item.get("name") or "").strip(),
            "arguments": arguments,
        })
    return tool_calls
