from __future__ import annotations

import html
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import frappe
import requests


DEFAULT_HTTP_TIMEOUT = 20
DEFAULT_USER_AGENT = "ERP-AI-Assistant/1.0"


def cfg_value(key: str, default: Any = None) -> Any:
    for candidate in (key, key.lower(), key.upper()):
        value = frappe.conf.get(candidate)
        if value not in (None, ""):
            return value
        value = os.getenv(candidate)
        if value not in (None, ""):
            return value
    return default


def cfg_bool(key: str, default: bool = False) -> bool:
    value = cfg_value(key, default)
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def cfg_list(key: str) -> list[str]:
    value = cfg_value(key, [])
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def require_system_manager() -> None:
    if "System Manager" not in (frappe.get_roles() or []):
        frappe.throw("System Manager role is required for this tool.", frappe.PermissionError)


def site_public_root() -> Path:
    return Path(frappe.get_site_path("public", "files")).resolve()


def site_private_root() -> Path:
    return Path(frappe.get_site_path("private", "files")).resolve()


def assistant_workspace_root() -> Path:
    root = Path(frappe.get_site_path("private", "files", "assistant_tools")).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def allowed_roots() -> list[Path]:
    return [site_public_root(), site_private_root(), assistant_workspace_root()]


def safe_path(raw_path: str, *, must_exist: bool = False) -> Path:
    path = Path(str(raw_path or "")).expanduser()
    if not path.is_absolute():
        frappe.throw("Path must be absolute.", frappe.ValidationError)

    resolved = path.resolve(strict=False)
    roots = allowed_roots()
    if not any(os.path.commonpath([str(root), str(resolved)]) == str(root) for root in roots):
        allowed = ", ".join(str(root) for root in roots)
        frappe.throw(f"Path is outside allowed roots: {allowed}", frappe.PermissionError)

    if must_exist and not resolved.exists():
        frappe.throw(f"Path does not exist: {resolved}", frappe.DoesNotExistError)
    return resolved


def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def strip_html_text(raw_html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", str(raw_html or ""))
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\\s*>", "\n\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalized_domain(url_or_domain: str) -> str:
    value = str(url_or_domain or "").strip().lower()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    return parsed.netloc.lower().lstrip("www.")


def domain_allowed(url: str, allowed_domains: list[str] | None, blocked_domains: list[str] | None) -> bool:
    domain = normalized_domain(url)
    allowed = {normalized_domain(item) for item in (allowed_domains or []) if normalized_domain(item)}
    blocked = {normalized_domain(item) for item in (blocked_domains or []) if normalized_domain(item)}
    if blocked and domain in blocked:
        return False
    if allowed and domain not in allowed:
        return False
    return True


def http_get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_HTTP_TIMEOUT,
) -> requests.Response:
    request_headers = {"User-Agent": DEFAULT_USER_AGENT, **(headers or {})}
    response = requests.get(url, params=params, headers=request_headers, timeout=timeout)
    response.raise_for_status()
    return response


def json_result(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)
