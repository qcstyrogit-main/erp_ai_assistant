import frappe
from frappe.model.document import Document

SETTINGS_DOCTYPE = "AI Provider Settings"


class AIProviderSettings(Document):
    def on_update(self) -> None:
        """
        Flush the Frappe document cache whenever provider settings are saved.
        This ensures that rotated API keys and updated credentials are picked
        up immediately without requiring a bench restart or manual cache clear.
        """
        try:
            frappe.clear_document_cache(SETTINGS_DOCTYPE, SETTINGS_DOCTYPE)
        except Exception:
            pass
        try:
            frappe.cache().delete_key(f"document::{SETTINGS_DOCTYPE}::{SETTINGS_DOCTYPE}")
        except Exception:
            pass

