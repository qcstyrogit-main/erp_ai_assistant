from __future__ import annotations

from typing import Any

import frappe

MAX_LIST_LIMIT = 5000
DESTRUCTIVE_CONFIRM_WORD = "DELETE"


def current_user() -> str:
    return str(frappe.session.user or "Guest")


def get_user_roles(user: str | None = None) -> list[str]:
    target = user or current_user()
    try:
        roles = frappe.get_roles(target) or []
    except Exception:
        roles = []
    return sorted({str(role).strip() for role in roles if str(role or '').strip()})


def is_system_manager(user: str | None = None) -> bool:
    return 'System Manager' in get_user_roles(user)


def clamp_limit(value: Any, default: int = 20, *, maximum: int = MAX_LIST_LIMIT) -> int:
    try:
        limit = int(value)
    except Exception:
        limit = default
    if limit < 1:
        return default
    return min(limit, maximum)


def ensure_doctype_access(doctype: str, ptype: str = 'read') -> None:
    if not str(doctype or '').strip():
        raise frappe.ValidationError('doctype is required')
    if not frappe.has_permission(doctype, ptype=ptype):
        raise frappe.PermissionError(f'User {current_user()} does not have {ptype} permission on {doctype}.')


def ensure_document_access(doc: Any, ptype: str = 'read') -> None:
    if hasattr(doc, 'check_permission'):
        doc.check_permission(ptype)
        return
    if not frappe.has_permission(doc.doctype, ptype=ptype, doc=doc):
        raise frappe.PermissionError(f'User {current_user()} does not have {ptype} permission on {doc.doctype} {doc.name}.')


def permission_summary(doctype: str) -> dict[str, bool]:
    return {
        'read': bool(frappe.has_permission(doctype, ptype='read')),
        'write': bool(frappe.has_permission(doctype, ptype='write')),
        'create': bool(frappe.has_permission(doctype, ptype='create')),
        'delete': bool(frappe.has_permission(doctype, ptype='delete')),
        'submit': bool(frappe.has_permission(doctype, ptype='submit')),
        'cancel': bool(frappe.has_permission(doctype, ptype='cancel')),
    }


def require_destruction_confirmation(arguments: dict[str, Any], *, action: str) -> None:
    confirmed = bool(arguments.get('confirmed'))
    confirmation_text = str(arguments.get('confirmation_text') or '').strip().upper()
    if confirmed and confirmation_text == DESTRUCTIVE_CONFIRM_WORD:
        return
    raise frappe.ValidationError(
        f"{action} is destructive. Re-run with confirmed=true and confirmation_text='{DESTRUCTIVE_CONFIRM_WORD}'."
    )
