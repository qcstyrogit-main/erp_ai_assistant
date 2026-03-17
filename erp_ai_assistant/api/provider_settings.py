import json
from typing import Any

import frappe


SETTINGS_DOCTYPE = "AI Provider Settings"

PASSWORD_FIELDS = {
    "openai_api_key",
    "anthropic_api_key",
    "anthropic_auth_token",
}

SETTING_KEY_MAP = {
    "ERP_AI_PROVIDER": "provider",
    "OPENAI_API_KEY": "openai_api_key",
    "ERP_AI_OPENAI_API_KEY": "openai_api_key",
    "OPENAI_BASE_URL": "openai_base_url",
    "ERP_AI_OPENAI_BASE_URL": "openai_base_url",
    "OPENAI_MODEL": "openai_default_model",
    "ERP_AI_OPENAI_MODEL": "openai_default_model",
    "OPENAI_MODELS": "openai_models",
    "ERP_AI_OPENAI_MODELS": "openai_models",
    "OPENAI_VISION_MODEL": "openai_vision_model",
    "ERP_AI_OPENAI_VISION_MODEL": "openai_vision_model",
    "OPENAI_RESPONSES_PATH": "openai_responses_path",
    "ERP_AI_OPENAI_RESPONSES_PATH": "openai_responses_path",
    "ERP_AI_OPENAI_MCP_ENABLED": "enable_openai_mcp",
    "ERP_AI_OPENAI_MCP_SERVERS": "openai_mcp_servers_json",
    "ANTHROPIC_API_KEY": "anthropic_api_key",
    "ERP_AI_ANTHROPIC_API_KEY": "anthropic_api_key",
    "ANTHROPIC_AUTH_TOKEN": "anthropic_auth_token",
    "ERP_AI_ANTHROPIC_AUTH_TOKEN": "anthropic_auth_token",
    "ANTHROPIC_BASE_URL": "anthropic_base_url",
    "ERP_AI_ANTHROPIC_BASE_URL": "anthropic_base_url",
    "ANTHROPIC_MESSAGES_PATH": "anthropic_messages_path",
    "ERP_AI_ANTHROPIC_MESSAGES_PATH": "anthropic_messages_path",
    "ANTHROPIC_MODEL": "anthropic_default_model",
    "ERP_AI_ANTHROPIC_MODEL": "anthropic_default_model",
    "ANTHROPIC_MODELS": "anthropic_models",
    "ERP_AI_ANTHROPIC_MODELS": "anthropic_models",
    "ANTHROPIC_VISION_MODEL": "anthropic_vision_model",
    "ERP_AI_ANTHROPIC_VISION_MODEL": "anthropic_vision_model",
    "ERP_AI_TOOL_CHOICE_MODE": "tool_choice_mode",
}


def get_provider_setting(key: str) -> Any:
    fieldname = SETTING_KEY_MAP.get(str(key or "").strip())
    if not fieldname:
        return None
    if not frappe.db.exists("DocType", SETTINGS_DOCTYPE):
        return None

    try:
        if fieldname in PASSWORD_FIELDS:
            return frappe.get_cached_doc(SETTINGS_DOCTYPE).get_password(fieldname)
        return frappe.db.get_single_value(SETTINGS_DOCTYPE, fieldname)
    except Exception:
        return None


def get_active_provider() -> str:
    provider = str(get_provider_setting("ERP_AI_PROVIDER") or "").strip().lower()
    if provider in {"openai", "openai compatible", "openai_compatible", "anthropic"}:
        if provider in {"openai compatible", "openai_compatible"}:
            return "openai_compatible"
        return provider
    return "openai"


def get_remote_mcp_servers() -> list[dict[str, Any]]:
    raw = get_provider_setting("ERP_AI_OPENAI_MCP_SERVERS")
    if not raw:
        return []
    if isinstance(raw, list):
        rows = raw
    else:
        try:
            rows = json.loads(raw)
        except Exception:
            return []

    if not isinstance(rows, list):
        return []

    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = {
            "type": "mcp",
            "server_label": str(row.get("server_label") or "").strip(),
            "server_url": str(row.get("server_url") or "").strip(),
        }
        if not item["server_label"] or not item["server_url"]:
            continue
        server_description = str(row.get("server_description") or "").strip()
        authorization = str(row.get("authorization") or "").strip()
        connector_id = str(row.get("connector_id") or "").strip()
        if server_description:
            item["server_description"] = server_description
        if authorization:
            item["authorization"] = authorization
        if connector_id:
            item["connector_id"] = connector_id
        if row.get("allowed_tools"):
            item["allowed_tools"] = row.get("allowed_tools")
        require_approval = row.get("require_approval")
        if require_approval not in (None, "", {}):
            item["require_approval"] = require_approval
        cleaned.append(item)
    return cleaned
