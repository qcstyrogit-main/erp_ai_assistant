"""
erp_ai_assistant.api.ai_config
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Configuration helpers extracted from ai.py as part of the PRODUCTION_REFACTOR_NOTES.md
modularisation plan.

All _cfg*, _llm_*, and DEFAULT_* symbols are defined here and re-exported.
The ai.py module imports from this module to avoid a Big Bang rewrite.

Resolution order for every setting (first truthy value wins):
  1. AI Provider Settings DocType (via provider_settings.get_provider_setting)
  2. site_config.json  (frappe.conf.get)
  3. Environment variables (os.getenv)
  4. Default value passed to the helper

Config key convention
---------------------
Settings may be specified with any capitalisation. The helpers check
lower, UPPER, and exact-case variants. ERP_AI_* prefixed keys take
priority over their legacy ANTHROPIC_* / OPENAI_* equivalents.

Public API
----------
  _cfg(key, default)
  _cfg_int(key, default, *, minimum, maximum)
  _cfg_float(key, default, *, minimum, maximum)
  _cfg_bool(key, default)

  _llm_request_max_tokens() -> int
  _llm_request_timeout_seconds() -> float
  _llm_request_stream_enabled() -> bool
  _llm_max_tool_rounds() -> int
  _llm_temperature() -> float
  _llm_top_p() -> float
  _llm_force_tool_use_enabled() -> bool
  _llm_verify_pass_enabled() -> bool
  _conversation_history_limit() -> int
  _provider_name() -> str
  _resolve_model(model) -> str
  _resolve_model_for_request(model, has_images) -> str

  DEFAULT_* module-level constants
"""
from __future__ import annotations

import os
from typing import Any

import frappe

from .provider_settings import get_active_provider, get_provider_setting

# ── Module-level defaults ────────────────────────────────────────────────────
DEFAULT_ANTHROPIC_MAX_TOKENS = 4000
DEFAULT_ANTHROPIC_REQUEST_TIMEOUT = 120.0
DEFAULT_ANTHROPIC_STREAM = True
DEFAULT_ANTHROPIC_MAX_TOOL_ROUNDS = 20
DEFAULT_ANTHROPIC_TEMPERATURE = 0.2
DEFAULT_ANTHROPIC_TOP_P = 0.9
DEFAULT_FORCE_TOOL_USE = True
DEFAULT_VERIFY_PASS = True
DEFAULT_ANTHROPIC_VISION_MODEL = ""
DEFAULT_OPENAI_MODEL = "gpt-5"
DEFAULT_OPENAI_RESPONSES_PATH = "/v1/responses"
DEFAULT_CONVERSATION_HISTORY_LIMIT = 12


# ── Low-level config helpers ─────────────────────────────────────────────────

def _cfg(key: str, default: Any = None) -> Any:
    """Resolve a configuration value from provider settings, site config, or env."""
    candidates = [key, key.lower(), key.upper()]
    for candidate in candidates:
        value = get_provider_setting(candidate)
        if value not in (None, ""):
            return value
    for candidate in candidates:
        value = frappe.conf.get(candidate)
        if value not in (None, ""):
            return value
    for candidate in candidates:
        value = os.getenv(candidate)
        if value not in (None, ""):
            return value
    return default


def _cfg_int(key: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    value = _cfg(key, default)
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    if minimum is not None and parsed < minimum:
        return default
    if maximum is not None and parsed > maximum:
        return default
    return parsed


def _cfg_float(
    key: str, default: float, *, minimum: float | None = None, maximum: float | None = None
) -> float:
    value = _cfg(key, default)
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    if minimum is not None and parsed < minimum:
        return default
    if maximum is not None and parsed > maximum:
        return default
    return parsed


def _cfg_bool(key: str, default: bool) -> bool:
    value = _cfg(key, default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


# ── LLM tuning helpers ───────────────────────────────────────────────────────

def _llm_request_max_tokens() -> int:
    """Get max tokens for chat calls from config/env with safe bounds."""
    key = (
        "ERP_AI_ANTHROPIC_MAX_TOKENS"
        if _cfg("ERP_AI_ANTHROPIC_MAX_TOKENS") not in (None, "")
        else "ANTHROPIC_MAX_TOKENS"
    )
    return _cfg_int(key, DEFAULT_ANTHROPIC_MAX_TOKENS, minimum=256, maximum=200000)


def _llm_request_timeout_seconds() -> float:
    """Get backend request timeout from config/env with safe bounds."""
    key = (
        "ERP_AI_ANTHROPIC_TIMEOUT_SECONDS"
        if _cfg("ERP_AI_ANTHROPIC_TIMEOUT_SECONDS") not in (None, "")
        else "ANTHROPIC_TIMEOUT_SECONDS"
    )
    return _cfg_float(key, DEFAULT_ANTHROPIC_REQUEST_TIMEOUT, minimum=5.0, maximum=600.0)


def _llm_request_stream_enabled() -> bool:
    """Get stream mode from config/env."""
    key = (
        "ERP_AI_ANTHROPIC_STREAM"
        if _cfg("ERP_AI_ANTHROPIC_STREAM") not in (None, "")
        else "ANTHROPIC_STREAM"
    )
    return _cfg_bool(key, DEFAULT_ANTHROPIC_STREAM)


def _llm_max_tool_rounds() -> int:
    """Get max assistant tool-calling rounds to prevent runaway loops."""
    key = (
        "ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS"
        if _cfg("ERP_AI_ANTHROPIC_MAX_TOOL_ROUNDS") not in (None, "")
        else "ANTHROPIC_MAX_TOOL_ROUNDS"
    )
    return _cfg_int(key, DEFAULT_ANTHROPIC_MAX_TOOL_ROUNDS, minimum=1, maximum=20)


def _llm_temperature() -> float:
    key = (
        "ERP_AI_ANTHROPIC_TEMPERATURE"
        if _cfg("ERP_AI_ANTHROPIC_TEMPERATURE") not in (None, "")
        else "ANTHROPIC_TEMPERATURE"
    )
    return _cfg_float(key, DEFAULT_ANTHROPIC_TEMPERATURE, minimum=0.0, maximum=2.0)


def _llm_top_p() -> float:
    key = (
        "ERP_AI_ANTHROPIC_TOP_P"
        if _cfg("ERP_AI_ANTHROPIC_TOP_P") not in (None, "")
        else "ANTHROPIC_TOP_P"
    )
    return _cfg_float(key, DEFAULT_ANTHROPIC_TOP_P, minimum=0.0, maximum=1.0)


def _llm_force_tool_use_enabled() -> bool:
    key = (
        "ERP_AI_FORCE_TOOL_USE"
        if _cfg("ERP_AI_FORCE_TOOL_USE") not in (None, "")
        else "FORCE_TOOL_USE"
    )
    return _cfg_bool(key, DEFAULT_FORCE_TOOL_USE)


def _llm_verify_pass_enabled() -> bool:
    key = (
        "ERP_AI_VERIFY_PASS"
        if _cfg("ERP_AI_VERIFY_PASS") not in (None, "")
        else "VERIFY_PASS"
    )
    return _cfg_bool(key, DEFAULT_VERIFY_PASS)


def _conversation_history_limit() -> int:
    key = (
        "ERP_AI_CONVERSATION_HISTORY_LIMIT"
        if _cfg("ERP_AI_CONVERSATION_HISTORY_LIMIT") not in (None, "")
        else "CONVERSATION_HISTORY_LIMIT"
    )
    return _cfg_int(key, DEFAULT_CONVERSATION_HISTORY_LIMIT, minimum=2, maximum=100)


# ── Provider / model resolution ──────────────────────────────────────────────

def _provider_name() -> str:
    """Return the active provider identifier string (lower-cased)."""
    return str(get_active_provider() or "").strip().lower() or "anthropic"


def _resolve_model(model: str | None = None) -> str:
    """Resolve the LLM model to use, falling back to provider defaults."""
    if model:
        return str(model).strip()
    provider = _provider_name()
    if provider in {"openai", "openai_compatible"}:
        raw = _cfg("OPENAI_MODEL", "") or _cfg("ERP_AI_OPENAI_MODEL", "")
        return str(raw).strip() or DEFAULT_OPENAI_MODEL
    raw = _cfg("ANTHROPIC_MODEL", "") or _cfg("ERP_AI_ANTHROPIC_MODEL", "")
    return str(raw).strip() or "claude-opus-4-5"


def _resolve_model_for_request(model: str | None = None, *, has_images: bool = False) -> str:
    """Resolve the model, preferring a vision model when images are attached."""
    if has_images:
        vision_model = str(_cfg("ANTHROPIC_VISION_MODEL", DEFAULT_ANTHROPIC_VISION_MODEL) or "").strip()
        if vision_model:
            return vision_model
    return _resolve_model(model)
