"""
erp_ai_assistant.api.rate_limit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-user prompt rate limiting using Frappe's Redis cache.

Default: 60 prompts per hour per user (configurable via site config or env).

Configuration keys (site_config.json or env):
    ERP_AI_RATE_LIMIT_PER_HOUR   integer  (default: 60)
    ERP_AI_RATE_LIMIT_ENABLED    1/0      (default: 1)

Usage:
    from .rate_limit import check_rate_limit
    check_rate_limit(user)   # raises frappe.ValidationError if over quota
"""

from __future__ import annotations

import os
import time

import frappe

_DEFAULT_PER_HOUR = 60
_WINDOW_SECONDS = 3600


def _cfg(key: str, default: str = "") -> str:
    for candidate in (key, key.lower(), key.upper()):
        v = frappe.conf.get(candidate) or os.getenv(candidate)
        if v not in (None, ""):
            return str(v)
    return default


def _rate_limit_enabled() -> bool:
    raw = _cfg("ERP_AI_RATE_LIMIT_ENABLED", "1")
    return str(raw).strip() not in ("0", "false", "no", "off")


def _per_hour_limit() -> int:
    raw = _cfg("ERP_AI_RATE_LIMIT_PER_HOUR", str(_DEFAULT_PER_HOUR))
    try:
        val = int(str(raw).strip())
        return max(1, val)
    except (ValueError, TypeError):
        return _DEFAULT_PER_HOUR


def _redis_key(user: str) -> str:
    window = int(time.time()) // _WINDOW_SECONDS
    return f"erp_ai:rate:{user}:{window}"


def check_rate_limit(user: str) -> None:
    """
    Increment the per-user hourly counter and raise ValidationError if
    the limit is exceeded. No-ops if rate limiting is disabled or Redis
    is unavailable.
    """
    if not _rate_limit_enabled():
        return

    limit = _per_hour_limit()
    key = _redis_key(user)

    try:
        cache = frappe.cache()
        current = cache.incr(key)
        if current == 1:
            # First request in this window — set TTL
            cache.expire(key, _WINDOW_SECONDS + 60)
        if current > limit:
            remaining_seconds = _WINDOW_SECONDS - (int(time.time()) % _WINDOW_SECONDS)
            minutes = remaining_seconds // 60
            raise frappe.ValidationError(
                f"Rate limit exceeded: {limit} prompts per hour. "
                f"Resets in {minutes} minute(s). "
                "Contact your System Manager to increase the limit via ERP_AI_RATE_LIMIT_PER_HOUR."
            )
    except frappe.ValidationError:
        raise
    except Exception:
        # Redis unavailable — fail open (don't block the user)
        pass


def get_rate_limit_status(user: str) -> dict:
    """Return current usage for the user (for display/debugging)."""
    limit = _per_hour_limit()
    key = _redis_key(user)
    try:
        current = int(frappe.cache().get(key) or 0)
    except Exception:
        current = 0
    return {
        "limit_per_hour": limit,
        "used": current,
        "remaining": max(0, limit - current),
        "enabled": _rate_limit_enabled(),
    }
