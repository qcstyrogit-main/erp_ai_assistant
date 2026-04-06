# LLM Gateway
import frappe
from frappe import _
import json
import html
import re
import requests
import time
import base64
from typing import Any, Optional
from .infrastructure import *
from .infrastructure import (
    _humanize_tool_name,
    _prioritize_tool_specs,
    _run_tool,
    _stray_tool_call_response,
    _tool_call_signature,
    _validate_tool_result,
)
from .orchestrator import (
    _is_bulk_operation_request,
    _llm_user_prompt,
    _progress_update,
    _render_provider_tool_output,
    _tool_result_feedback_payload,
    _tool_round_limit_response,
    _verification_prompt,
)
from .ai_config import (
    DEFAULT_ANTHROPIC_REQUEST_TIMEOUT,
    DEFAULT_ANTHROPIC_VISION_MODEL,
    DEFAULT_OPENAI_MODEL,
    DEFAULT_OPENAI_RESPONSES_PATH,
    _cfg,
    _cfg_bool,
    _cfg_float,
    _llm_force_tool_use_enabled,
    _llm_max_tool_rounds,
    _llm_request_max_tokens,
    _llm_request_stream_enabled,
    _llm_temperature,
    _llm_top_p,
    _llm_verify_pass_enabled,
)
from .provider_settings import get_active_provider


def _endpoint_host(base_url: str) -> str:
    from .infrastructure import _endpoint_host as impl
    return impl(base_url)


def _erp_tool_system_prompt(prompt: str, context: dict[str, Any]) -> str:
    from .infrastructure import _erp_tool_system_prompt as impl
    return impl(prompt, context)


def _is_erp_intent(prompt: str, context: dict[str, Any]) -> bool:
    from .infrastructure import _is_erp_intent as impl
    return impl(prompt, context)


def _no_tools_available_response(prompt: str, context: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    from .infrastructure import _no_tools_available_response as impl
    return impl(prompt, context)


def _response_presentation_addendum() -> str:
    return (
        " Visible-answer rules: do not expose tool catalogs, schemas, argument lists, raw payloads, or execution traces "
        "unless the user explicitly asks for technical details. For record-list requests such as "
        "'last 5 Sales Invoices', answer with a short direct sentence, then a compact Markdown table using the most relevant columns, "
        "then a brief factual summary that highlights notable statuses such as Overdue or Cancelled and mentions total counts when available."
    )


def _fallback_model_candidates(selected_model: str | None = None) -> list[str]:
    from .infrastructure import _fallback_model_candidates as impl
    return impl(selected_model)


def _is_transient_provider_error(message: str) -> bool:
    from .infrastructure import _is_transient_provider_error as impl
    return impl(message)


def _tool_choice_mode(base_url: str, messages_path: str) -> str:
    from .infrastructure import _tool_choice_mode as impl
    return impl(base_url, messages_path)


def _tool_choice_payload(
    force_tool_use: bool,
    prompt: str,
    context: dict[str, Any],
    *,
    mode: str,
) -> Optional[str | dict[str, str]]:
    from .infrastructure import _tool_choice_payload as impl
    return impl(force_tool_use, prompt, context, mode=mode)

def _llm_request_timeout_seconds() -> float:
    """Get backend request timeout from config/env with safe bounds."""
    key = (
        "ERP_AI_ANTHROPIC_TIMEOUT_SECONDS"
        if _cfg("ERP_AI_ANTHROPIC_TIMEOUT_SECONDS") not in (None, "")
        else "ANTHROPIC_TIMEOUT_SECONDS"
    )
    return _cfg_float(
        key,
        DEFAULT_ANTHROPIC_REQUEST_TIMEOUT,
        minimum=5.0,
        maximum=600.0,
    )


def _provider_name() -> str:
    configured = str(_cfg("ERP_AI_PROVIDER", "")).strip().lower()
    if configured in {"openai", "openai compatible", "openai_compatible", "anthropic"}:
        if configured in {"openai compatible", "openai_compatible"}:
            return "openai_compatible"
        return configured
    return get_active_provider()


def _provider_compatibility_profile(
    provider: str,
    base_url: str,
    path: str = "",
    *,
    model: str | None = None,
) -> dict[str, Any]:
    host = _endpoint_host(base_url)
    normalized_model = str(model or "").strip().lower()

    profile = {
        "provider": provider,
        "profile": provider,
        "host": host,
        "path": str(path or "").strip(),
        "disable_tool_choice_by_default": False,
        "disable_sampling_by_default": False,
        "allow_textual_tool_fallback": True,
    }

    if provider == "openai":
        profile["profile"] = "openai"
        profile["allow_textual_tool_fallback"] = False
        return profile

    if provider == "anthropic":
        profile["profile"] = "anthropic"
        profile["allow_textual_tool_fallback"] = False
        return profile

    if "openrouter.ai" in host or normalized_model.startswith("openrouter/"):
        profile["profile"] = "openrouter"
        # OpenRouter routes across providers with varying tool_choice support.
        profile["disable_tool_choice_by_default"] = True
        return profile

    if "integrate.api.nvidia.com" in host or "build.nvidia.com" in host or normalized_model.startswith("nvidia/"):
        profile["profile"] = "nvidia"
        return profile

    profile["profile"] = "generic_openai_compatible"

    return profile


def _provider_chat_with_resilience(
    prompt: str,
    context: dict[str, Any],
    *,
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    errors: list[str] = []
    model_candidates = _fallback_model_candidates(model)
    if not model_candidates:
        model_candidates = [model] if model else [None]

    max_attempts = max(1, min(3, len(model_candidates)))
    for index in range(max_attempts):
        attempt_model = model_candidates[index]
        try:
            return _provider_chat(
                prompt,
                context,
                history=history,
                model=attempt_model,
                progress=progress,
                images=images,
            )
        except Exception as exc:
            message = str(exc) or "Unknown provider error"
            errors.append(message)
            if index + 1 < max_attempts and _is_transient_provider_error(message):
                continue
            if index + 1 < max_attempts and "model" in message.lower():
                continue
            break

    raise RuntimeError(" | ".join(errors[-2:]) if errors else "Unknown provider error")


def _provider_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    if _provider_name() == "openai":
        return _openai_chat(prompt, context, history=history, model=model, progress=progress, images=images)
    if _provider_name() == "openai_compatible":
        return _openai_compatible_chat(prompt, context, history=history, model=model, progress=progress, images=images)
    return _anthropic_chat(prompt, context, history=history, model=model, progress=progress, images=images)


def _parse_json_object_text(raw_text: Any) -> dict[str, Any]:
    if isinstance(raw_text, dict):
        return raw_text
    text = str(raw_text or "").strip()
    if not text:
        raise RuntimeError("Planner returned empty output.")
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError(f"Planner returned invalid JSON: {text[:240]}")


def _openai_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    api_key = _cfg("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OpenAI provider is selected but OPENAI_API_KEY is not configured.")

    model = _resolve_model(model)
    max_tool_rounds = _llm_max_tool_rounds()
    timeout_seconds = _llm_request_timeout_seconds()
    temperature = _llm_temperature()
    top_p = _llm_top_p()
    force_tool_use = _llm_force_tool_use_enabled()
    verify_pass_enabled = _llm_verify_pass_enabled()
    base_url = str(_cfg("OPENAI_BASE_URL", "https://api.openai.com")).rstrip("/")
    responses_path = str(_cfg("OPENAI_RESPONSES_PATH", DEFAULT_OPENAI_RESPONSES_PATH) or DEFAULT_OPENAI_RESPONSES_PATH)
    if not responses_path.startswith("/"):
        responses_path = f"/{responses_path}"
    endpoint = f"{base_url}{responses_path}"
    compat_profile = _provider_compatibility_profile("openai", base_url, responses_path, model=model)

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    system = _erp_tool_system_prompt(prompt, context) + _response_presentation_addendum()

    tool_specs = _prioritize_tool_specs(_openai_tool_specs(prompt, context))
    previous_response_id: str | None = None
    pending_input: Any = _build_openai_input(history, prompt, context, images)
    tool_events: list[str] = []
    rendered_payload = None
    rendered_feedback = None
    all_payloads: list[Any] = []
    had_tool_calls = False
    verification_requested = False
    tools_enabled = bool(tool_specs)
    if _is_erp_intent(prompt, context) and not tool_specs:
        return _no_tools_available_response(prompt, context)
    plain_text_retry_requested = False
    seen_tool_signatures: set[str] = set()
    last_tool_name: str | None = None

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload: dict[str, Any] = {
            "model": model,
            "instructions": system,
            "input": pending_input,
        }
        if tools_enabled:
            request_payload["tools"] = tool_specs
        if not compat_profile.get("disable_sampling_by_default") and _openai_supports_sampling_controls(model):
            request_payload["temperature"] = temperature
            request_payload["top_p"] = top_p
        if previous_response_id:
            request_payload["previous_response_id"] = previous_response_id
        tool_choice = _tool_choice_payload(force_tool_use, prompt, context, mode="openai")
        if tools_enabled and tool_choice is not None:
            request_payload["tool_choice"] = tool_choice

        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc

        response.raise_for_status()
        body = _parse_backend_json(response, endpoint)
        previous_response_id = str(body.get("id") or previous_response_id or "").strip() or None

        output_items = body.get("output") or []
        function_calls = [item for item in output_items if item.get("type") == "function_call"]
        approval_requests = [item for item in output_items if str(item.get("type") or "").startswith("mcp_approval")]
        mcp_calls = [item for item in output_items if item.get("type") == "mcp_call"]

        for mcp_call in mcp_calls:
            label = mcp_call.get("server_label") or mcp_call.get("name") or "mcp"
            status = mcp_call.get("status") or "completed"
            tool_events.append(f"mcp:{label} ({status})")

        if approval_requests:
            raise RuntimeError(
                "OpenAI MCP server requested approval. Set require_approval to 'never' in MCP server config or complete approval externally."
            )

        if function_calls and not tools_enabled:
            fallback_text = _openai_output_text(body)
            if fallback_text:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": fallback_text,
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider returned stray tool calls; retrying text-only")
                pending_input = "Answer the user's last message directly in plain text. Do not call tools."
                continue
            _progress_update(progress, stage="failed", done=True, error="No callable tools were available for this provider response.")
            return _stray_tool_call_response()

        if not function_calls:
            # Skip verification during bulk-create: it fires too early and cuts off
            # document creation before all N docs are created.
            _skip_verify = _is_bulk_operation_request(prompt, context)
            if had_tool_calls and verify_pass_enabled and not verification_requested and previous_response_id and not _skip_verify:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                pending_input = _verification_prompt(tool_events)
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": _openai_output_text(body) or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
                "all_payloads": all_payloads,
            }

        current_signatures = [
            _tool_call_signature(str(tool_call.get("name") or "").strip(), _parse_openai_tool_arguments(tool_call.get("arguments")))
            for tool_call in function_calls
        ]
        if current_signatures and all(signature in seen_tool_signatures for signature in current_signatures):
            if rendered_payload is not None:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": _render_provider_tool_output(last_tool_name, rendered_payload),
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider repeated the same tool calls; forcing final answer")
                pending_input = "You already have the tool results. Answer the user directly in plain text without any more tool calls."
                tools_enabled = False
                continue

        had_tool_calls = True
        pending_results = []
        for tool_call in function_calls:
            tool_name = str(tool_call.get("name") or "").strip()
            tool_input = _parse_openai_tool_arguments(tool_call.get("arguments"))
            seen_tool_signatures.add(_tool_call_signature(tool_name, tool_input))
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name, tool_input)}")
            try:
                tool_result = _run_tool(tool_name, tool_input, user=context.get("user"))
                _validate_tool_result(tool_name, tool_result)
                tool_feedback = _tool_result_feedback_payload(tool_name, tool_result)
                last_tool_name = tool_name
                rendered_payload = tool_result
                all_payloads.append(tool_result)
                tool_events.append(f"{tool_name} {tool_input}")
                pending_results.append(
                    {
                        "type": "function_call_output",
                        "call_id": tool_call.get("call_id"),
                        "output": json.dumps(tool_feedback, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_feedback = _tool_result_feedback_payload(tool_name, error_payload)
                tool_events.append(f"{tool_name} {tool_input} (error)")
                _progress_update(
                    progress,
                    stage="working",
                    step=f"Tool failed: {_humanize_tool_name(tool_name, tool_input)} — {str(exc) or 'Unknown tool error'}",
                )
                pending_results.append(
                    {
                        "type": "function_call_output",
                        "call_id": tool_call.get("call_id"),
                        "output": json.dumps(tool_feedback, default=str),
                    }
                )

        pending_input = pending_results

    _progress_update(progress, stage="working", step="Preparing response")
    return _tool_round_limit_response(last_tool_name, rendered_payload, rendered_feedback, tool_events, max_tool_rounds)


def _openai_compatible_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    api_key = _cfg("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OpenAI-compatible provider is selected but OPENAI_API_KEY is not configured.")

    model = _resolve_model(model)
    max_tool_rounds = _llm_max_tool_rounds()
    timeout_seconds = _llm_request_timeout_seconds()
    temperature = _llm_temperature()
    top_p = _llm_top_p()
    force_tool_use = _llm_force_tool_use_enabled()
    verify_pass_enabled = _llm_verify_pass_enabled()
    base_url = str(_cfg("OPENAI_BASE_URL", "https://integrate.api.nvidia.com")).rstrip("/")
    path = str(_cfg("OPENAI_RESPONSES_PATH", "/v1/chat/completions") or "/v1/chat/completions")
    if not path.startswith("/"):
        path = f"/{path}"
    endpoint = f"{base_url}{path}"
    compat_profile = _provider_compatibility_profile("openai_compatible", base_url, path, model=model)

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    system = _erp_tool_system_prompt(prompt, context) + _response_presentation_addendum()

    messages = _build_openai_compatible_messages(history, prompt, context, images)
    tool_specs = _prioritize_tool_specs(_openai_compatible_tool_specs(prompt, context))
    tool_events: list[str] = []
    rendered_payload = None
    rendered_feedback = None
    all_payloads: list[Any] = []
    had_tool_calls = False
    verification_requested = False
    disable_tool_choice = bool(compat_profile.get("disable_tool_choice_by_default"))
    tools_enabled = bool(tool_specs)
    if _is_erp_intent(prompt, context) and not tool_specs:
        return _no_tools_available_response(prompt, context)
    plain_text_retry_requested = False
    seen_tool_signatures: set[str] = set()
    last_tool_name: str | None = None
    disable_sampling_controls = bool(compat_profile.get("disable_sampling_by_default"))

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "system", "content": system}] + messages,
            "stream": False,
        }
        if tools_enabled and tool_specs:
            request_payload["tools"] = tool_specs
            tool_choice = None if disable_tool_choice else _tool_choice_payload(force_tool_use, prompt, context, mode="openai")
            if tool_choice is not None:
                request_payload["tool_choice"] = tool_choice
        if not disable_sampling_controls and _openai_compatible_supports_sampling_controls(model):
            request_payload["temperature"] = temperature
            request_payload["top_p"] = top_p

        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc

        if response.status_code >= 400:
            error_detail = _extract_error_detail(response)
            if tools_enabled and _is_tool_choice_function_none_error(error_detail):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected forced tool_choice; retrying without tool_choice",
                )
                continue
            if tools_enabled and _is_tool_choice_schema_error(error_detail):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected tool_choice schema; retrying with safer tool_choice",
                )
                continue
            if _is_sampling_parameter_error(error_detail):
                disable_sampling_controls = True
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected sampling controls; retrying without temperature/top_p",
                )
                continue
            if tools_enabled and _is_degraded_function_error(error_detail):
                tools_enabled = False
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Compatible API rejected tool calls; retrying text-only",
                )
                continue
            raise RuntimeError(f"AI endpoint rejected request ({endpoint}): {error_detail}")
        response.raise_for_status()
        body = _parse_backend_json(response, endpoint)
        choice = ((body.get("choices") or [{}])[0]) if isinstance(body.get("choices"), list) else {}
        message = choice.get("message") or {}
        tool_calls = message.get("tool_calls") or []
        text_body = str(message.get("content") or "").strip()
        if not tool_calls and text_body and compat_profile.get("allow_textual_tool_fallback"):
            textual_tool_call = _extract_textual_tool_call(text_body)
            if textual_tool_call is not None:
                tool_calls = [textual_tool_call]
                text_body = ""

        if tool_calls and not tools_enabled:
            if text_body:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": text_body,
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider returned stray tool calls; retrying text-only")
                messages.append({"role": "user", "content": "Answer directly in plain text. Do not call tools."})
                continue
            _progress_update(progress, stage="failed", done=True, error="No callable tools were available for this provider response.")
            return _stray_tool_call_response()

        if not tool_calls:
            _skip_verify_b = _is_bulk_operation_request(prompt, context)
            if had_tool_calls and verify_pass_enabled and not verification_requested and not _skip_verify_b:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                messages.append({"role": "user", "content": _verification_prompt(tool_events)})
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": text_body or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
                "all_payloads": all_payloads,
            }

        current_signatures = []
        for tool_call in tool_calls:
            function_payload = tool_call.get("function") or {}
            current_signatures.append(
                _tool_call_signature(
                    str(function_payload.get("name") or "").strip(),
                    _parse_openai_tool_arguments(function_payload.get("arguments")),
                )
            )
        if current_signatures and all(signature in seen_tool_signatures for signature in current_signatures):
            if rendered_payload is not None:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": _render_provider_tool_output(last_tool_name, rendered_feedback or rendered_payload),
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                _progress_update(progress, stage="working", step="Provider repeated the same tool calls; forcing final answer")
                tools_enabled = False
                messages.append({"role": "user", "content": "You already have the tool results. Answer directly in plain text without any more tool calls."})
                continue

        had_tool_calls = True
        messages.append({"role": "assistant", "content": text_body or "", "tool_calls": tool_calls})
        for tool_call in tool_calls:
            function_payload = tool_call.get("function") or {}
            tool_name = str(function_payload.get("name") or "").strip()
            tool_input = _parse_openai_tool_arguments(function_payload.get("arguments"))
            seen_tool_signatures.add(_tool_call_signature(tool_name, tool_input))
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name, tool_input)}")
            try:
                tool_result = _run_tool(tool_name, tool_input, user=context.get("user"))
                _validate_tool_result(tool_name, tool_result)
                tool_feedback = _tool_result_feedback_payload(tool_name, tool_result)
                last_tool_name = tool_name
                rendered_payload = tool_result
                all_payloads.append(tool_result)
                rendered_feedback = tool_feedback
                tool_events.append(f"{tool_name} {tool_input}")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_feedback = _tool_result_feedback_payload(tool_name, error_payload)
                rendered_feedback = tool_feedback
                tool_events.append(f"{tool_name} {tool_input} (error)")
                _progress_update(
                    progress,
                    stage="working",
                    step=f"Tool failed: {_humanize_tool_name(tool_name, tool_input)} — {str(exc) or 'Unknown tool error'}",
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.get("id"),
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )

    _progress_update(progress, stage="working", step="Preparing response")
    return _tool_round_limit_response(last_tool_name, rendered_payload, rendered_feedback, tool_events, max_tool_rounds)


def _anthropic_chat(
    prompt: str,
    context: dict[str, Any],
    history: Optional[list[dict[str, Any]]] = None,
    model: str | None = None,
    progress: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    api_key = _cfg("ANTHROPIC_API_KEY")
    auth_token = _cfg("ANTHROPIC_AUTH_TOKEN")
    model = _resolve_model(model)
    max_tokens = _llm_request_max_tokens()
    timeout_seconds = _llm_request_timeout_seconds()
    stream_enabled = _llm_request_stream_enabled()
    max_tool_rounds = _llm_max_tool_rounds()
    temperature = _llm_temperature()
    top_p = _llm_top_p()
    force_tool_use = _llm_force_tool_use_enabled()
    verify_pass_enabled = _llm_verify_pass_enabled()
    base_url = str(_cfg("ANTHROPIC_BASE_URL", "https://api.anthropic.com")).rstrip("/")
    messages_path = str(_cfg("ANTHROPIC_MESSAGES_PATH", "/v1/messages"))
    if not messages_path.startswith("/"):
        messages_path = f"/{messages_path}"
    compat_profile = _provider_compatibility_profile("anthropic", base_url, messages_path, model=model)
    tool_choice_mode = _tool_choice_mode(base_url, messages_path)

    beta_param = _normalize_beta_param(_cfg("ANTHROPIC_BETA"))

    endpoint = f"{base_url}{messages_path}"

    headers = {"content-type": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    elif auth_token:
        headers["authorization"] = f"Bearer {auth_token}"
    anthropic_version = _cfg("ANTHROPIC_VERSION", "2023-06-01")
    if anthropic_version and compat_profile.get("profile") == "anthropic" and tool_choice_mode == "anthropic":
        headers["anthropic-version"] = anthropic_version
    if beta_param and compat_profile.get("profile") == "anthropic" and tool_choice_mode == "anthropic":
        headers["anthropic-beta"] = beta_param
    tool_definitions = get_tool_definitions(user=context.get("user"))
    tool_specs = [
        {
            "name": name,
            "description": spec["description"],
            "input_schema": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
    ]
    tool_specs = _prioritize_tool_specs(tool_specs)
    if images:
        frappe.logger("erp_ai_assistant").info(
            "vision_request model=%s base_url=%s image_count=%s",
            model,
            base_url,
            len(images),
        )

    system = _erp_tool_system_prompt(prompt, context) + _response_presentation_addendum()
    messages: list[dict[str, Any]] = _build_messages_with_images(history, prompt, context, images)
    tool_events: list[str] = []
    rendered_payload = None
    rendered_feedback = None
    all_payloads: list[Any] = []
    had_tool_calls = False
    verification_requested = False
    tools_enabled = bool(tool_specs)
    if _is_erp_intent(prompt, context) and not tool_specs:
        return _no_tools_available_response(prompt, context)
    tool_choice_fallback_applied = False
    disable_tool_choice = False
    seen_tool_signatures: set[str] = set()
    plain_text_retry_requested = False
    last_tool_name: str | None = None

    for _round in range(max_tool_rounds):
        _progress_update(progress, stage="thinking", step="Thinking")
        request_payload = {
            "model": model,
            "max_tokens": max_tokens,
            "stream": stream_enabled,
            "temperature": temperature,
            "top_p": top_p,
            "system": system,
            "messages": messages,
        }
        if tools_enabled:
            request_payload["tools"] = tool_specs
            if not disable_tool_choice:
                tool_choice = _tool_choice_payload(
                    force_tool_use,
                    prompt,
                    context,
                    mode=tool_choice_mode,
                )
                if tool_choice is not None:
                    request_payload["tool_choice"] = tool_choice

        try:
            response = requests.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to AI endpoint failed ({endpoint}): {exc}") from exc

        if response.status_code >= 400:
            error_detail = _extract_error_detail(response)
            if tools_enabled and _is_tool_choice_function_none_error(error_detail):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider rejected forced tool_choice; retrying without tool_choice",
                )
                continue
            if (
                tools_enabled
                and not tool_choice_fallback_applied
                and tool_choice_mode != "openai"
                and _is_tool_choice_schema_error(error_detail)
            ):
                tool_choice_mode = "openai"
                tool_choice_fallback_applied = True
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider rejected Anthropic tool_choice; retrying OpenAI tool_choice format",
                )
                continue
            if tools_enabled and _is_degraded_function_error(error_detail):
                tools_enabled = False
                force_tool_use = False
                _progress_update(progress, stage="working", step="Provider rejected tool calls; retrying text-only")
                continue
        response.raise_for_status()
        content_type = (response.headers.get("content-type") or "").lower()
        if stream_enabled and "text/event-stream" in content_type:
            body = _parse_sse_stream(response, endpoint, progress=progress)
        else:
            body = _parse_backend_json(response, endpoint)
        content_blocks = body.get("content", [])
        messages.append({"role": "assistant", "content": content_blocks})

        text_chunks = [block.get("text", "") for block in content_blocks if block.get("type") == "text" and block.get("text")]
        tool_uses = [block for block in content_blocks if block.get("type") == "tool_use"]

        if not tool_uses:
            text_body = "\n".join(text_chunks)
            if tools_enabled and not disable_tool_choice and _is_tool_choice_function_none_error(text_body):
                disable_tool_choice = True
                force_tool_use = False
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider returned tool_choice error in body; retrying without tool_choice",
                )
                continue
            if tools_enabled and not disable_tool_choice and _is_tool_choice_schema_error(text_body):
                disable_tool_choice = True
                force_tool_use = False
                if tool_choice_mode != "openai":
                    tool_choice_mode = "openai"
                    tool_choice_fallback_applied = True
                _progress_update(
                    progress,
                    stage="working",
                    step="Provider returned tool_choice schema error in body; retrying with safer tool_choice",
                )
                continue
            _skip_verify_c = _is_bulk_operation_request(prompt, context)
            if had_tool_calls and verify_pass_enabled and not verification_requested and not _skip_verify_c:
                verification_requested = True
                _progress_update(progress, stage="working", step="Verifying ERP evidence")
                messages.append(
                    {
                        "role": "user",
                        "content": _verification_prompt(tool_events),
                    }
                )
                continue
            _progress_update(progress, stage="working", step="Preparing response")
            return {
                "text": "\n\n".join(chunk for chunk in text_chunks if chunk).strip() or "No response text returned.",
                "tool_events": tool_events,
                "payload": rendered_payload,
                "all_payloads": all_payloads,
            }

        current_signatures = [
            _tool_call_signature(str(tool_use.get("name") or "").strip(), tool_use.get("input", {}))
            for tool_use in tool_uses
        ]
        if current_signatures and all(signature in seen_tool_signatures for signature in current_signatures):
            if rendered_payload is not None:
                _progress_update(progress, stage="working", step="Preparing response")
                return {
                    "text": _render_provider_tool_output(last_tool_name, rendered_feedback or rendered_payload),
                    "tool_events": tool_events,
                    "payload": rendered_payload,
                }
            if not plain_text_retry_requested:
                plain_text_retry_requested = True
                tools_enabled = False
                _progress_update(progress, stage="working", step="Provider repeated the same tool calls; forcing final answer")
                messages.append(
                    {
                        "role": "user",
                        "content": "You already have the tool results. Answer directly in plain text without any more tool calls.",
                    }
                )
                continue

        had_tool_calls = True
        tool_results = []
        for tool_use in tool_uses:
            tool_name = tool_use["name"]
            tool_input = tool_use.get("input", {})
            seen_tool_signatures.add(_tool_call_signature(tool_name, tool_input))
            last_tool_name = tool_name
            _progress_update(progress, stage="working", step=f"Tool: {_humanize_tool_name(tool_name, tool_input)}")
            try:
                tool_result = _run_tool(tool_name, tool_input, user=context.get("user"))
                _validate_tool_result(tool_name, tool_result)
                tool_feedback = _tool_result_feedback_payload(tool_name, tool_result)
                tool_events.append(f"{tool_name} {tool_input}")
                rendered_payload = tool_result
                all_payloads.append(tool_result)
                rendered_feedback = tool_feedback
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )
            except Exception as exc:
                error_payload = {
                    "success": False,
                    "error": str(exc) or "Unknown tool error",
                    "tool": tool_name,
                    "input": tool_input,
                }
                tool_feedback = _tool_result_feedback_payload(tool_name, error_payload)
                rendered_feedback = tool_feedback
                tool_events.append(f"{tool_name} {tool_input} (error)")
                _progress_update(
                    progress,
                    stage="working",
                    step=f"Tool failed: {_humanize_tool_name(tool_name, tool_input)} — {str(exc) or 'Unknown tool error'}",
                )
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "is_error": True,
                        "content": json.dumps(tool_feedback, default=str),
                    }
                )

        messages.append({"role": "user", "content": tool_results})
        _progress_update(progress, stage="working", partial_text="")

    _progress_update(progress, stage="working", step="Preparing response")
    return _tool_round_limit_response(last_tool_name, rendered_payload, rendered_feedback, tool_events, max_tool_rounds)


def _extract_error_detail(response: requests.Response) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            return json.dumps(payload, default=str)
        return str(payload)
    except Exception:
        return (response.text or "").strip()


def _is_degraded_function_error(detail: str) -> bool:
    text = str(detail or "").lower()
    return "degraded function" in text or "cannot be invoked" in text


def _is_tool_choice_schema_error(detail: str) -> bool:
    text = str(detail or "").lower()
    if (
        "tool_choice" in text
        and "no endpoints found that support the provided 'tool_choice' value" in text
    ):
        return True
    required_fragments = (
        "tool_choice",
        "input should be 'auto', 'required' or 'none'",
        "input should be 'function'",
    )
    return all(fragment in text for fragment in required_fragments)


def _is_tool_choice_function_none_error(detail: str) -> bool:
    text = str(detail or "").lower()
    return (
        "tool_choice" in text
        and "invalid value for `function`" in text
        and "`none`" in text
    )


def _is_sampling_parameter_error(detail: str) -> bool:
    text = str(detail or "").lower()
    if not text:
        return False
    unsupported_markers = (
        "temperature",
        "top_p",
        "unsupported parameter",
        "extra inputs are not permitted",
        "unknown field",
        "unknown parameter",
    )
    return any(marker in text for marker in unsupported_markers) and (
        "temperature" in text or "top_p" in text
    )


def _openai_tool_specs(prompt: str, context: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions(user=(context or {}).get("user"))
    specs = [
        {
            "type": "function",
            "name": name,
            "description": spec["description"],
            "parameters": spec["inputSchema"],
        }
        for name, spec in tool_definitions.items()
    ]
    specs = _prioritize_tool_specs(specs)
    if _cfg_bool("ERP_AI_OPENAI_MCP_ENABLED", False):
        specs.extend(get_remote_mcp_servers())
    return specs


def _openai_compatible_tool_specs(prompt: str, context: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    tool_definitions = get_tool_definitions(user=(context or {}).get("user"))
    specs = [
        {
            "type": "function",
            "function": {
                "name": name,
                "description": spec["description"],
                "parameters": spec["inputSchema"],
            },
        }
        for name, spec in tool_definitions.items()
    ]
    return _prioritize_tool_specs(specs)


def _build_openai_input(
    history: Optional[list[dict[str, Any]]],
    prompt: str,
    context: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in (history or [])]
    normalized: list[dict[str, Any]] = []
    last_user_index = -1

    for row in rows:
        role = str(row.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _build_openai_input_content(str(row.get("content") or "").strip(), row.get("images"))
        if not content:
            continue
        normalized.append({"role": role, "content": content})
        if role == "user":
            last_user_index = len(normalized) - 1

    current_content = _build_openai_input_content(_llm_user_prompt(prompt, context), images)
    if current_content:
        if last_user_index >= 0 and images:
            normalized[last_user_index]["content"] = current_content
        elif not normalized or normalized[-1].get("role") != "user" or images:
            normalized.append({"role": "user", "content": current_content})
        else:
            normalized[-1]["content"] = current_content

    return normalized or [{"role": "user", "content": _build_openai_input_content(_llm_user_prompt(prompt, context), images)}]


def _build_openai_compatible_messages(
    history: Optional[list[dict[str, Any]]],
    prompt: str,
    context: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in (history or [])]
    normalized: list[dict[str, Any]] = []
    for row in rows:
        role = str(row.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _build_openai_compatible_content(str(row.get("content") or "").strip(), row.get("images"))
        if content in ("", []):
            continue
        normalized.append({"role": role, "content": content})

    current_content = _build_openai_compatible_content(_llm_user_prompt(prompt, context), images)
    normalized.append({"role": "user", "content": current_content})
    return normalized


def _build_openai_compatible_content(text: str, images: Optional[list[dict[str, str]]]) -> Any:
    blocks: list[dict[str, Any]] = []
    if str(text or "").strip():
        blocks.append({"type": "text", "text": text.strip()})
    for image in images or []:
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        blocks.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:{media_type};base64,{data}"},
            }
        )
    if str(text or "").strip() and not images:
        return text.strip()
    return blocks if blocks else (text or "").strip()


def _build_openai_input_content(text: str, images: Optional[list[dict[str, str]]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    if str(text or "").strip():
        blocks.append({"type": "input_text", "text": text.strip()})
    for image in images or []:
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        blocks.append(
            {
                "type": "input_image",
                "image_url": f"data:{media_type};base64,{data}",
            }
        )
    return blocks


def _openai_output_text(body: dict[str, Any]) -> str:
    top_level = str(body.get("output_text") or "").strip()
    if top_level:
        return top_level

    chunks: list[str] = []
    for item in body.get("output") or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            text = content.get("text") or content.get("output_text")
            if text:
                chunks.append(str(text).strip())
    return "\n\n".join(chunk for chunk in chunks if chunk).strip()


def _parse_openai_tool_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str):
        try:
            parsed = json.loads(arguments)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _extract_textual_tool_call(raw_text: Any) -> dict[str, Any] | None:
    text = str(raw_text or "").strip()
    if not text:
        return None

    unescaped_text = html.unescape(text)

    xml_tool_match = re.search(
        r"<tool_call>\s*<function=(?P<name>[\w\.\-]+)>\s*(?P<body>[\s\S]*?)\s*</function>\s*</tool_call>",
        unescaped_text,
        re.IGNORECASE,
    )
    if xml_tool_match:
        raw_name = str(xml_tool_match.group("name") or "").strip()
        tool_name = raw_name.split(".")[-1] if raw_name else ""
        normalized_name = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name, tool_name))
        arguments: dict[str, Any] = {}
        body = str(xml_tool_match.group("body") or "")
        for param_match in re.finditer(
            r"<parameter=(?P<key>[\w\.\-]+)>\s*(?P<value>[\s\S]*?)\s*</parameter>",
            body,
            re.IGNORECASE,
        ):
            key = str(param_match.group("key") or "").strip()
            raw_value = str(param_match.group("value") or "").strip()
            if not key:
                continue
            parsed_value: Any = raw_value
            if raw_value.startswith("{") or raw_value.startswith("["):
                try:
                    parsed_value = json.loads(raw_value)
                except Exception:
                    parsed_value = raw_value
            arguments[key] = parsed_value
        if normalized_name:
            return {
                "id": f"textual-{normalized_name}",
                "type": "function",
                "function": {
                    "name": normalized_name,
                    "arguments": json.dumps(arguments, default=str),
                },
            }

    # Handle <TOOLCALL>[{"name":...,"arguments":{...}}]</TOOLCALL> format
    toolcall_array_match = re.search(
        r"<TOOLCALL>\s*(?P<body>\[[\s\S]*?\])\s*</TOOLCALL>",
        unescaped_text,
    )
    if toolcall_array_match:
        body = str(toolcall_array_match.group("body") or "").strip()
        try:
            arr = json.loads(body)
        except Exception:
            arr = None
        if isinstance(arr, list) and arr:
            payload = arr[0] if isinstance(arr[0], dict) else {}
            raw_name = str(payload.get("name") or payload.get("tool_name") or "").strip()
            tool_name_tc = raw_name.split(".")[-1] if raw_name else ""
            normalized_tc = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name_tc, tool_name_tc))
            args_tc = payload.get("arguments") or payload.get("args") or {}
            if not isinstance(args_tc, dict):
                try:
                    args_tc = json.loads(str(args_tc))
                except Exception:
                    args_tc = {}
            if normalized_tc:
                return {
                    "id": f"textual-{normalized_tc}",
                    "type": "function",
                    "function": {
                        "name": normalized_tc,
                        "arguments": json.dumps(args_tc, default=str),
                    },
                }

    json_tool_match = re.search(r"<tool_call>\s*(?P<body>\{[\s\S]*?\})\s*</tool_call>", unescaped_text, re.IGNORECASE)
    if json_tool_match:
        body = str(json_tool_match.group("body") or "").strip()
        try:
            payload = json.loads(body)
        except Exception:
            payload = None
        if isinstance(payload, dict):
            raw_name = str(payload.get("tool_name") or payload.get("name") or "").strip()
            tool_name = raw_name.split(".")[-1] if raw_name else ""
            normalized_name = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name, tool_name))
            arguments = payload.get("args")
            if not isinstance(arguments, dict):
                arguments = payload.get("arguments")
            if not isinstance(arguments, dict):
                arguments = {}
            if normalized_name:
                return {
                    "id": f"textual-{normalized_name}",
                    "type": "function",
                    "function": {
                        "name": normalized_name,
                        "arguments": json.dumps(arguments, default=str),
                    },
                }

    inline_tool_match = re.search(
        r"<\|tool_call_begin\|>\s*(?P<name>[\w\.\-]+)(?::\d+)?\s*<\|tool_call_argument_begin\|>\s*(?P<args>\{[\s\S]*?\})\s*<\|tool_call_end\|>",
        unescaped_text,
        re.IGNORECASE,
    )
    if inline_tool_match:
        raw_name = str(inline_tool_match.group("name") or "").strip()
        tool_name = raw_name.split(".")[-1] if raw_name else ""
        normalized_name = TOOL_NAME_MAP.get(raw_name, TOOL_NAME_MAP.get(tool_name, tool_name))
        arguments = _parse_openai_tool_arguments(inline_tool_match.group("args"))
        if normalized_name:
            return {
                "id": f"textual-{normalized_name}",
                "type": "function",
                "function": {
                    "name": normalized_name,
                    "arguments": json.dumps(arguments, default=str),
                },
            }

    if "{" not in unescaped_text:
        return None

    try:
        payload = _parse_json_object_text(unescaped_text)
    except Exception:
        return None

    tool_name = str(payload.get("tool") or payload.get("name") or "").strip()
    args_payload: Any = None
    if tool_name:
        args_payload = (
            payload.get("args")
            if "args" in payload
            else payload.get("arguments")
            if "arguments" in payload
            else payload.get("input")
            if "input" in payload
            else payload.get("parameters")
        )
    else:
        function_payload = payload.get("function")
        if isinstance(function_payload, dict):
            tool_name = str(function_payload.get("name") or "").strip()
            args_payload = function_payload.get("arguments")

    if not tool_name:
        return None

    normalized_name = TOOL_NAME_MAP.get(tool_name, tool_name)
    arguments = _parse_openai_tool_arguments(args_payload)
    if not arguments and isinstance(args_payload, dict):
        arguments = args_payload

    return {
        "id": f"textual-{normalized_name}",
        "type": "function",
        "function": {
            "name": normalized_name,
            "arguments": json.dumps(arguments, default=str),
        },
    }


def _openai_supports_sampling_controls(model: str | None) -> bool:
    name = str(model or "").strip().lower()
    if not name:
        return True
    # OpenAI's GPT-5 family can reject temperature/top_p on the Responses API.
    return not name.startswith("gpt-5")


def _openai_compatible_supports_sampling_controls(model: str | None) -> bool:
    return True


def _build_messages_with_images(
    history: Optional[list[dict[str, Any]]],
    prompt: str,
    context: Optional[dict[str, Any]] = None,
    images: Optional[list[dict[str, str]]] = None,
) -> list[dict[str, Any]]:
    rows = []
    for item in history or []:
        row = dict(item)
        role = str(row.get("role") or "").strip().lower()
        if role == "user":
            row["content"] = _build_user_multimodal_content(str(row.get("content") or "").strip(), _build_image_blocks(row.get("images")))
        rows.append(row)
    image_blocks = _build_image_blocks(images)

    if not image_blocks:
        return rows or [{"role": "user", "content": _llm_user_prompt(prompt, context)}]

    merged_content = _build_user_multimodal_content(_llm_user_prompt(prompt, context), image_blocks)
    for index in range(len(rows) - 1, -1, -1):
        if str(rows[index].get("role") or "").strip().lower() == "user":
            rows[index]["content"] = merged_content
            return rows

    rows.append({"role": "user", "content": merged_content})
    return rows


def _build_user_multimodal_content(prompt: str, image_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    text = (prompt or "").strip()
    if text:
        blocks.append({"type": "text", "text": text})
    blocks.extend(image_blocks)
    if not blocks:
        blocks.append({"type": "text", "text": "Please analyze the attached image."})
    return blocks


def _build_image_blocks(images: Optional[list[dict[str, str]]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for image in images or []:
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        blocks.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": data,
                },
            }
        )
    return blocks


def _parse_message_attachments(raw: Any) -> list[dict[str, Any]]:
    if not raw:
        return []
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        attachments = payload.get("attachments") or []
        return [item for item in attachments if isinstance(item, dict)]
    return []


def _history_image_attachments(attachments: list[dict[str, Any]]) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    for attachment in attachments:
        file_url = str(attachment.get("file_url") or "").strip()
        media_type, data = _extract_base64_image(file_url)
        if not media_type or not data:
            continue
        images.append({"media_type": media_type, "data": data})
    return images


def _describe_message_attachments(attachments: list[dict[str, Any]]) -> list[str]:
    notes: list[str] = []
    for attachment in attachments[:4]:
        filename = str(attachment.get("filename") or "attachment").strip()
        label = str(attachment.get("label") or attachment.get("file_type") or "file").strip()
        if str(attachment.get("file_url") or "").startswith("data:image/"):
            notes.append(f"Attached image: {filename}")
        else:
            notes.append(f"Attached file: {filename} ({label})")
    return notes


def _merge_history_content_and_attachment_notes(content: str, notes: list[str]) -> str:
    body = str(content or "").strip()
    if not notes:
        return body
    note_text = "\n".join(notes)
    if not body:
        return note_text
    if note_text in body:
        return body
    return f"{body}\n\n{note_text}"


def _parse_prompt_images(images: str | list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if images in (None, "", []):
        return []

    payload: Any = images
    if isinstance(images, str):
        try:
            payload = json.loads(images)
        except Exception:
            return []

    if not isinstance(payload, list):
        return []

    parsed: list[dict[str, str]] = []
    for row in payload[:4]:
        if not isinstance(row, dict):
            continue
        data_url = str(row.get("data_url") or "").strip()
        media_type, data = _extract_base64_image(data_url)
        if not media_type or not data:
            continue
        if len(data) > 8_000_000:
            continue
        parsed.append({"media_type": media_type, "data": data})
    return parsed


def _extract_base64_image(data_url: str) -> tuple[str, str]:
    if not data_url.startswith("data:image/"):
        return "", ""
    marker = ";base64,"
    if marker not in data_url:
        return "", ""
    header, data = data_url.split(marker, 1)
    media_type = header.replace("data:", "", 1).strip().lower()
    if not media_type.startswith("image/"):
        return "", ""
    cleaned = re.sub(r"\s+", "", data or "")
    if not cleaned:
        return "", ""
    return media_type, cleaned


def _format_image_only_user_content(count: int) -> str:
    size = max(1, int(count or 0))
    return f"[Attached {size} image{'s' if size != 1 else ''}]"


def _build_prompt_image_attachments(images: Optional[list[dict[str, str]]]) -> dict[str, Any]:
    attachments: list[dict[str, str]] = []
    for index, image in enumerate(images or [], start=1):
        media_type = str(image.get("media_type") or "").strip().lower()
        data = str(image.get("data") or "").strip()
        if not media_type.startswith("image/") or not data:
            continue
        extension = media_type.split("/", 1)[1].split("+", 1)[0] or "png"
        attachments.append(
            {
                "label": "Image",
                "filename": f"image-{index}.{extension}",
                "file_type": extension,
                "file_url": f"data:{media_type};base64,{data}",
            }
        )
    return {"attachments": attachments}


def _response_rejects_images(text: Any) -> bool:
    body = str(text or "").lower()
    if not body:
        return False
    blocked_terms = [
        "i can't see",
        "i cannot see",
        "i don't see any image",
        "i do not see any image",
        "i don't see an image",
        "i do not see an image",
        "no image in your message",
        "can't view images",
        "cannot view images",
        "don't have the ability to view images",
        "do not have the ability to view images",
        "please upload the image",
    ]
    return any(term in body for term in blocked_terms)


@frappe.whitelist()
def get_available_models() -> dict[str, Any]:
    models = _available_llm_models()
    return {"models": models, "default_model": models[0] if models else None}


def _resolve_model(model: str | None) -> str:
    provider = _provider_name()
    is_openai_family = provider in {"openai", "openai_compatible"}
    default_key = "OPENAI_MODEL" if is_openai_family else "ANTHROPIC_MODEL"
    default_model = str(_cfg(default_key, DEFAULT_OPENAI_MODEL if is_openai_family else "claude-sonnet-4-6")).strip()
    available = _available_llm_models()
    requested = str(model or "").strip()
    if requested and requested in available:
        return requested
    return default_model


def _resolve_model_for_request(model: str | None, *, has_images: bool) -> str:
    """Resolve model, optionally routing image prompts to a vision-capable alias."""
    if has_images and _provider_name() in {"openai", "openai_compatible"}:
        vision_model = str(
            _cfg(
                "ERP_AI_OPENAI_VISION_MODEL",
                _cfg("OPENAI_VISION_MODEL", ""),
            )
        ).strip()
        if vision_model:
            available = _available_llm_models()
            if not available or vision_model in available:
                return vision_model
    if _provider_name() not in {"openai", "openai_compatible"} and has_images:
        vision_model = str(
            _cfg(
                "ERP_AI_ANTHROPIC_VISION_MODEL",
                _cfg("ANTHROPIC_VISION_MODEL", DEFAULT_ANTHROPIC_VISION_MODEL),
            )
        ).strip()
        if vision_model:
            available = _available_llm_models()
            if not available or vision_model in available:
                return vision_model
    return _resolve_model(model)


def _available_llm_models() -> list[str]:
    provider = _provider_name()
    is_openai_family = provider in {"openai", "openai_compatible"}
    models_key = "OPENAI_MODELS" if is_openai_family else "ANTHROPIC_MODELS"
    default_key = "OPENAI_MODEL" if is_openai_family else "ANTHROPIC_MODEL"
    vision_key = "OPENAI_VISION_MODEL" if is_openai_family else "ANTHROPIC_VISION_MODEL"
    default_value = DEFAULT_OPENAI_MODEL if is_openai_family else "claude-sonnet-4-6"
    raw = _cfg(models_key)
    models: list[str] = []

    if isinstance(raw, (list, tuple)):
        models = [str(item).strip() for item in raw if str(item or "").strip()]
    elif isinstance(raw, str):
        text = raw.strip()
        if text:
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        models = [str(item).strip() for item in parsed if str(item or "").strip()]
                except Exception:
                    models = []
            if not models:
                normalized = text.replace("\n", ",")
                models = [chunk.strip() for chunk in normalized.split(",") if chunk.strip()]

    default_model = str(_cfg(default_key, default_value)).strip()
    if default_model:
        models.append(default_model)
    vision_model = str(_cfg(vision_key, "")).strip()
    if vision_model:
        models.append(vision_model)

    unique: list[str] = []
    seen = set()
    for item in models:
        if item and item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


def _normalize_beta_param(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return "true" if value else None
    text = str(value).strip().lower()
    if text in {"", "0", "false", "none", "null", "off", "no"}:
        return None
    return text


def _parse_backend_json(response: requests.Response, endpoint: str) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        content_type = (response.headers.get("content-type") or "").lower()
        if "text/event-stream" in content_type:
            return _parse_sse_response(response.text, endpoint)
        preview = (response.text or "").strip().replace("\n", " ")[:300]
        content_type = response.headers.get("content-type", "")
        status = response.status_code
        detail = (
            f"AI endpoint returned non-JSON response (status={status}, content_type='{content_type}') "
            f"from {endpoint}. Body preview: {preview or '<empty>'}"
        )
        raise RuntimeError(detail) from exc
    if not isinstance(body, dict):
        raise RuntimeError(
            f"AI endpoint returned unexpected JSON payload type ({type(body).__name__}) from {endpoint}; expected object."
        )
    return body


def _parse_sse_response(text: str, endpoint: str) -> dict[str, Any]:
    return _parse_sse_events((text or "").splitlines(), endpoint)


def _parse_sse_stream(
    response: requests.Response,
    endpoint: str,
    progress: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    decoded_lines = []
    partial_text = ""
    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = str(raw_line)
        decoded_lines.append(line)
        stripped = line.strip()
        if not stripped.startswith("data:"):
            continue
        raw_data = stripped[len("data:") :].strip()
        if not raw_data or raw_data == "[DONE]":
            continue
        try:
            payload = json.loads(raw_data)
        except Exception:
            continue
        if payload.get("type") == "content_block_delta":
            delta = payload.get("delta") or {}
            if delta.get("type") == "text_delta":
                partial_text += str(delta.get("text") or "")
                _progress_update(progress, stage="thinking", partial_text=partial_text)
    return _parse_sse_events(decoded_lines, endpoint)


def _parse_sse_events(lines: list[str], endpoint: str) -> dict[str, Any]:
    blocks_by_index: dict[int, dict[str, Any]] = {}
    message_payload: dict[str, Any] = {}
    stop_reason = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        raw_data = line[len("data:") :].strip()
        if not raw_data or raw_data == "[DONE]":
            continue
        try:
            payload = json.loads(raw_data)
        except Exception:
            continue

        event_type = payload.get("type")
        if event_type == "message_start":
            message_payload = payload.get("message") or {}
            continue
        if event_type == "message_delta":
            if payload.get("delta", {}).get("stop_reason"):
                stop_reason = payload["delta"]["stop_reason"]
            continue
        if event_type == "content_block_start":
            index = int(payload.get("index", 0))
            block = payload.get("content_block") or {}
            blocks_by_index[index] = block
            continue
        if event_type == "content_block_delta":
            index = int(payload.get("index", 0))
            block = blocks_by_index.setdefault(index, {})
            delta = payload.get("delta") or {}
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                block["type"] = block.get("type") or "text"
                block["text"] = (block.get("text") or "") + (delta.get("text") or "")
            elif delta_type == "input_json_delta":
                block["_input_json"] = (block.get("_input_json") or "") + (delta.get("partial_json") or "")
            continue
        if event_type == "content_block_stop":
            index = int(payload.get("index", 0))
            block = blocks_by_index.get(index) or {}
            if block.get("type") == "tool_use" and block.get("_input_json"):
                try:
                    block["input"] = json.loads(block["_input_json"])
                except Exception:
                    block["input"] = {}
            block.pop("_input_json", None)
            blocks_by_index[index] = block

    content = [blocks_by_index[i] for i in sorted(blocks_by_index.keys()) if blocks_by_index[i]]
    if not content and message_payload.get("content"):
        content = message_payload.get("content") or []

    if not content:
        preview = "\n".join(lines).strip().replace("\n", " ")[:300]
        raise RuntimeError(
            f"AI endpoint returned SSE but no parsable content from {endpoint}. Body preview: {preview or '<empty>'}"
        )

    return {
        "id": message_payload.get("id"),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": message_payload.get("model"),
        "stop_reason": stop_reason or message_payload.get("stop_reason"),
    }




# --- Cross-imports to resolve dependencies ---
from .orchestrator import _progress_update, _tool_result_feedback_payload, _render_provider_tool_output, _verification_prompt, _tool_round_limit_response, _llm_user_prompt, _is_bulk_operation_request
