from __future__ import annotations

import frappe
from frappe import _


def _auth_logger():
    return frappe.logger('erp_ai_assistant.auth')


@frappe.whitelist()
def who_am_i():
    """Return the current authenticated user context."""
    user = frappe.get_doc("User", frappe.session.user)
    return {
        "user": frappe.session.user,
        "full_name": user.full_name,
        "roles": frappe.get_roles(frappe.session.user),
    }


@frappe.whitelist(allow_guest=True)
def session_login(usr: str, pwd: str):
    """Optional session login wrapper for Desk/mobile clients.

    Disabled by default for production hardening. Enable explicitly with
    `erp_ai_enable_session_login = 1` in site_config.json when required.
    """
    if not frappe.conf.get('erp_ai_enable_session_login'):
        raise frappe.PermissionError(
            _('Session login wrapper is disabled. Use standard Frappe login or enable erp_ai_enable_session_login.')
        )

    frappe.local.login_manager.authenticate(usr, pwd)
    frappe.local.login_manager.post_login()
    _auth_logger().info('session_login succeeded for user=%s', usr)
    return who_am_i()


@frappe.whitelist()
def session_logout():
    frappe.local.login_manager.logout()
    return {"ok": True}


def has_workspace_access():
    """Desk app visibility gate."""
    return frappe.session.user != "Guest"
