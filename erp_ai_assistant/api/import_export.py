"""Import/Export chat history functionality for ERP AI Assistant."""

import json
from datetime import datetime

import frappe
from frappe import _


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


@frappe.whitelist()
def export_history(start_date=None, end_date=None):
    """Export user's conversations and messages as JSON.

    Args:
        start_date: Optional start date filter
        end_date: Optional end date filter
    """
    if not frappe.session.user:
        frappe.throw(_("User not logged in"))

    # Get conversations for current user
    filters = {"owner": frappe.session.user}
    if start_date and end_date:
        filters["creation"] = ["between", [start_date, end_date]]
    elif start_date:
        filters["creation"] = [">=", start_date]
    elif end_date:
        filters["creation"] = ["<=", end_date]

    conversations = frappe.get_all(
        "AI Conversation",
        filters=filters,
        fields=["name", "title", "is_pinned", "status", "creation"],
        order_by="creation asc"
    )

    export_data = []
    for conv in conversations:
        message_fields = ["role", "content", "tool_events", "creation"]
        if frappe.db.has_column("AI Message", "attachments_json"):
            message_fields.append("attachments_json")
        messages = frappe.get_all(
            "AI Message",
            filters={"conversation": conv.name},
            fields=message_fields,
            order_by="creation asc"
        )

        export_data.append({
            "title": conv.title,
            "is_pinned": conv.is_pinned,
            "status": conv.status,
            "created": conv.creation,
            "messages": messages
        })

    filename = f"erp_ai_assistant_export_{frappe.session.user}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    frappe.local.response.filename = filename
    frappe.local.response.filecontent = json.dumps(export_data, indent=2, default=_json_default)
    frappe.local.response.type = "download"


@frappe.whitelist()
def import_history(file_data):
    """Import conversations and messages from JSON.

    Args:
        file_data: JSON string containing conversation data
    """
    if not frappe.session.user:
        frappe.throw(_("User not logged in"))

    try:
        import_data = json.loads(file_data)
        if not isinstance(import_data, list):
            import_data = [import_data]

        imported = []
        skipped = []

        for conv_data in import_data:
            # Check if conversation with same title already exists
            existing = frappe.db.exists("AI Conversation", {
                "title": conv_data.get("title"),
                "owner": frappe.session.user
            })

            if existing:
                skipped.append(conv_data.get("title"))
                continue

            # Create new conversation
            conv = frappe.get_doc({
                "doctype": "AI Conversation",
                "title": conv_data.get("title", "Imported Chat"),
                "is_pinned": conv_data.get("is_pinned", 0),
                "status": conv_data.get("status", "Open")
            })
            conv.insert(ignore_permissions=True)

            # Add messages
            messages = conv_data.get("messages", [])
            for msg_data in messages:
                msg_values = {
                    "doctype": "AI Message",
                    "conversation": conv.name,
                    "role": msg_data.get("role"),
                    "content": msg_data.get("content"),
                    "tool_events": msg_data.get("tool_events")
                }
                if frappe.db.has_column("AI Message", "attachments_json") and msg_data.get("attachments_json"):
                    msg_values["attachments_json"] = msg_data.get("attachments_json")
                msg = frappe.get_doc(msg_values)
                msg.insert(ignore_permissions=True)

            imported.append(conv.title)

        return {
            "imported": imported,
            "skipped": skipped,
            "count": len(imported)
        }

    except json.JSONDecodeError:
        frappe.throw(_("Invalid JSON file"))
    except Exception as e:
        frappe.throw(_("Import failed: {0}").format(str(e)))


@frappe.whitelist()
def bulk_delete(conversation_names):
    """Bulk delete multiple conversations.

    Args:
        conversation_names: JSON array of conversation names to delete
    """
    if not frappe.session.user:
        frappe.throw(_("User not logged in"))

    try:
        names = json.loads(conversation_names)
        if not isinstance(names, list):
            frappe.throw(_("Invalid conversation list"))

        deleted = []
        for name in names:
            # Check ownership
            conversation = frappe.get_doc("AI Conversation", name)
            if conversation.owner != frappe.session.user and "System Manager" not in frappe.get_roles():
                frappe.throw(_("Permission denied for {0}").format(name))

            frappe.delete_doc("AI Conversation", name, ignore_permissions=True)
            deleted.append(name)

        return {"deleted": deleted, "count": len(deleted)}

    except Exception as e:
        frappe.throw(_("Bulk delete failed: {0}").format(str(e)))
