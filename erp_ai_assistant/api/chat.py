import frappe
from frappe import _


CHAT_DOCTYPES = ("AI Conversation", "AI Message")


def _is_missing_doctype(exc: Exception, doctype: str) -> bool:
    if isinstance(exc, getattr(frappe.db, "TableMissingError", tuple())):
        return True
    args = getattr(exc, "args", ())
    return len(args) >= 2 and args[0] == "DocType" and args[1] == doctype


def _chat_storage_ready(raise_exception: bool = False) -> bool:
    for doctype in CHAT_DOCTYPES:
        try:
            frappe.db.get_table_columns(doctype)
        except Exception as exc:
            if _is_missing_doctype(exc, doctype):
                if raise_exception:
                    raise frappe.ValidationError(
                        _(
                            "ERP AI Assistant is installed but its DocTypes are not available on this site yet. "
                            "Run bench migrate for the affected site, then reload Desk."
                        )
                    )
                return False
            raise
    return True


def _get_conversation(name: str):
    _chat_storage_ready(raise_exception=True)
    doc = frappe.get_doc("AI Conversation", name)
    if doc.owner != frappe.session.user and "System Manager" not in frappe.get_roles(frappe.session.user):
        raise frappe.PermissionError
    return doc


@frappe.whitelist()
def list_conversations(search: str | None = None):
    if not _chat_storage_ready():
        return []

    filters = {"owner": frappe.session.user}
    rows = frappe.get_all(
        "AI Conversation",
        filters=filters,
        fields=["name", "title", "is_pinned", "modified", "status"],
        order_by="is_pinned desc, modified desc",
    )
    if search:
        needle = search.lower()
        rows = [row for row in rows if needle in (row.title or "").lower()]
    return rows


@frappe.whitelist()
def get_conversation(name: str):
    doc = _get_conversation(name)
    messages = frappe.get_all(
        "AI Message",
        filters={"conversation": name},
        fields=["name", "role", "content", "tool_events", "creation"],
        order_by="creation asc",
    )
    return {
        "conversation": doc.as_dict(),
        "messages": messages,
    }


@frappe.whitelist()
def create_conversation(title: str | None = None):
    _chat_storage_ready(raise_exception=True)
    doc = frappe.get_doc(
        {
            "doctype": "AI Conversation",
            "title": title or _("New chat"),
            "status": "Open",
        }
    )
    doc.insert(ignore_permissions=True)
    return doc.as_dict()


@frappe.whitelist()
def rename_conversation(name: str, title: str):
    doc = _get_conversation(name)
    doc.title = title
    doc.save()
    return doc.as_dict()


@frappe.whitelist()
def toggle_pin(name: str):
    doc = _get_conversation(name)
    doc.is_pinned = 0 if doc.is_pinned else 1
    doc.save()
    return {"name": doc.name, "is_pinned": doc.is_pinned}


@frappe.whitelist()
def delete_conversation(name: str):
    doc = _get_conversation(name)
    frappe.delete_doc("AI Conversation", doc.name, ignore_permissions=True)
    return {"ok": True}


@frappe.whitelist()
def add_message(conversation: str, role: str, content: str, tool_events: str | None = None):
    _chat_storage_ready(raise_exception=True)
    _get_conversation(conversation)
    message = frappe.get_doc(
        {
            "doctype": "AI Message",
            "conversation": conversation,
            "role": role,
            "content": content,
            "tool_events": tool_events,
        }
    )
    message.insert(ignore_permissions=True)
    return message.as_dict()
