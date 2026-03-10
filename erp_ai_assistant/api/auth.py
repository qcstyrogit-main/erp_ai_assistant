import frappe
from frappe import _


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
    """Session login wrapper for Desk/mobile clients.

    This is the short-term replacement for API-key-only flows.
    """
    frappe.local.login_manager.authenticate(usr, pwd)
    frappe.local.login_manager.post_login()
    return who_am_i()


@frappe.whitelist()
def session_logout():
    frappe.local.login_manager.logout()
    return {"ok": True}


def has_workspace_access():
    """Desk app visibility gate."""
    return frappe.session.user != "Guest"
