"""
erp_ai_assistant.patches.v0_2_0.open_conversation_permissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
v0.2.0 migration patch.

Opens AI Conversation and AI Message DocType permissions so that
non-System-Manager authenticated users can create and view their own
conversations. System Managers retain full access to all records.

This patch is idempotent — safe to re-run.
"""

import frappe


def execute() -> None:
    _open_doctype("AI Conversation")
    _open_doctype("AI Message")
    frappe.db.commit()


def _open_doctype(doctype: str) -> None:
    if not frappe.db.exists("DocType", doctype):
        return

    # Remove old System-Manager-only permissions
    frappe.db.delete("DocPerm", {"parent": doctype})

    # System Manager — full access to all records
    frappe.get_doc({
        "doctype": "DocPerm",
        "parent": doctype,
        "parenttype": "DocType",
        "parentfield": "permissions",
        "role": "System Manager",
        "permlevel": 0,
        "read": 1,
        "write": 1,
        "create": 1,
        "delete": 1,
        "export": 1,
        "if_owner": 0,
    }).insert(ignore_permissions=True)

    # All authenticated users — read/write/create their own records only
    frappe.get_doc({
        "doctype": "DocPerm",
        "parent": doctype,
        "parenttype": "DocType",
        "parentfield": "permissions",
        "role": "All",
        "permlevel": 0,
        "read": 1,
        "write": 1,
        "create": 1,
        "delete": 0,
        "export": 0,
        "if_owner": 1,
    }).insert(ignore_permissions=True)

    frappe.clear_cache(doctype=doctype)
