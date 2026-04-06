import json
import json
import re
from typing import Any

import frappe
from frappe import _
from frappe.utils.file_manager import save_file

from ..erp_tools import MUTATION_DOCTYPE_BLOCKLIST, resolve_doctype_name_internal
from .export import _build_excel_bytes, _slugify_filename


def _error(message: str, *, error_type: str = "validation_error") -> dict[str, Any]:
    return {"ok": False, "type": "file", "error_type": error_type, "message": message}


def _parse_json_arg(value: Any, default: Any) -> Any:
    if value in (None, "", []):
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _require_doctype_permission(doctype: str, ptype: str) -> None:
    if not frappe.has_permission(doctype, ptype=ptype):
        raise frappe.PermissionError(_("Not permitted to {0} {1}").format(ptype, doctype))


def _resolve_safe_export_target(target: str | None) -> dict[str, Any] | None:
    resolved_doctype = resolve_doctype_name_internal(target or "")
    if not resolved_doctype or resolved_doctype in MUTATION_DOCTYPE_BLOCKLIST:
        return None
    if not frappe.db.exists("DocType", resolved_doctype):
        return None
    meta = frappe.get_meta(resolved_doctype)
    if getattr(meta, "istable", 0):
        return None
    return {
        "doctype": resolved_doctype,
        "title": f"{resolved_doctype} List",
        "fields": _default_export_fields(resolved_doctype),
        "filter_fields": _filterable_export_fields(resolved_doctype),
    }


def _normalize_field_hint(value: Any) -> str:
    return re.sub(r"[\s_]+", " ", str(value or "").strip().lower())


def _exportable_meta_fields(doctype: str) -> dict[str, Any]:
    blocked_types = {
        "Section Break",
        "Column Break",
        "HTML",
        "Button",
        "Image",
        "Attach Image",
        "Table",
        "Table MultiSelect",
        "Fold",
    }
    meta = frappe.get_meta(doctype)
    fields: dict[str, Any] = {}
    for field in meta.fields:
        fieldname = str(field.fieldname or "").strip()
        fieldtype = str(field.fieldtype or "").strip()
        if not fieldname or fieldtype in blocked_types:
            continue
        fields[fieldname] = field
    return fields


def _field_label(fieldname: str, field: Any | None, doctype: str) -> str:
    if fieldname == "name":
        return f"{doctype} ID"
    label = str(getattr(field, "label", "") or "").strip()
    if label:
        return label
    return fieldname.replace("_", " ").title()


def _default_export_fields(doctype: str) -> list[str]:
    preferred = [
        "name",
        "status",
        "docstatus",
        "company",
        "posting_date",
        "transaction_date",
        "customer",
        "supplier",
        "party_name",
        "employee",
        "employee_name",
        "item_code",
        "item_name",
        "warehouse",
        "set_warehouse",
        "outstanding_amount",
        "grand_total",
        "modified",
    ]
    meta = frappe.get_meta(doctype)
    title_field = str(getattr(meta, "title_field", "") or "").strip()
    if title_field:
        preferred.insert(1, title_field)
    meta_fields = _exportable_meta_fields(doctype)
    selected: list[str] = []
    for fieldname in preferred:
        if fieldname == "name" and fieldname not in selected:
            selected.append(fieldname)
            continue
        if fieldname in meta_fields and fieldname not in selected:
            selected.append(fieldname)
        if len(selected) >= 8:
            return selected
    for fieldname in meta_fields.keys():
        if fieldname not in selected:
            selected.append(fieldname)
        if len(selected) >= 8:
            break
    return selected or ["name"]


def _filterable_export_fields(doctype: str) -> set[str]:
    allowed_types = {
        "Data",
        "Link",
        "Select",
        "Date",
        "Datetime",
        "Check",
        "Int",
        "Long Int",
        "Float",
        "Currency",
        "Percent",
        "Small Text",
        "Text",
        "Read Only",
    }
    fields = {"name", "owner", "modified_by", "creation", "modified", "docstatus"}
    for field in frappe.get_meta(doctype).fields:
        fieldname = str(field.fieldname or "").strip()
        fieldtype = str(field.fieldtype or "").strip()
        if fieldname and fieldtype in allowed_types:
            fields.add(fieldname)
    return fields


def _resolve_export_fields(doctype: str, requested_fields: list[str] | None, default_fields: list[str]) -> tuple[list[str], dict[str, str]]:
    meta_fields = _exportable_meta_fields(doctype)
    aliases: dict[str, str] = {}

    for fieldname, field in meta_fields.items():
        aliases[_normalize_field_hint(fieldname)] = fieldname
        aliases[_normalize_field_hint(fieldname.replace("_", " "))] = fieldname
        label = _field_label(fieldname, field, doctype)
        aliases[_normalize_field_hint(label)] = fieldname

    aliases[_normalize_field_hint("id")] = "name"
    aliases[_normalize_field_hint("record id")] = "name"
    aliases[_normalize_field_hint(f"{doctype} id")] = "name"

    title_field = str(getattr(frappe.get_meta(doctype), "title_field", "") or "").strip()
    if title_field:
        aliases[_normalize_field_hint("title")] = title_field
        aliases[_normalize_field_hint("name")] = title_field

    selected: list[str] = []
    if requested_fields:
        for raw_hint in requested_fields:
            fieldname = aliases.get(_normalize_field_hint(raw_hint))
            if fieldname and fieldname not in selected:
                selected.append(fieldname)

    if not selected:
        selected = [fieldname for fieldname in default_fields if fieldname == "name" or fieldname in meta_fields]

    labels = {
        fieldname: _field_label(fieldname, meta_fields.get(fieldname), doctype)
        for fieldname in selected
    }
    return selected, labels


def _rows_with_display_labels(rows: list[dict[str, Any]], labels: dict[str, str]) -> list[dict[str, Any]]:
    rendered: list[dict[str, Any]] = []
    for row in rows:
        rendered.append({labels.get(fieldname, fieldname): row.get(fieldname) for fieldname in labels})
    return rendered


def _save_bytes_as_file(
    file_name: str,
    content: bytes,
    *,
    attached_to_doctype: str,
    attached_to_name: str,
    is_private: int = 1,
) -> dict[str, Any]:
    file_doc = save_file(
        file_name,
        content,
        attached_to_doctype,
        attached_to_name,
        is_private=is_private,
    )
    return {
        "file_name": file_doc.file_name,
        "file_url": file_doc.file_url,
        "file_docname": file_doc.name,
        "is_private": file_doc.is_private,
    }


def export_employee_list_excel_internal(
    filters: dict[str, Any] | str | None = None,
    fields: list[str] | str | None = None,
) -> dict[str, Any]:
    parsed_fields = _parse_json_arg(fields, fields)
    if not isinstance(parsed_fields, list):
        parsed_fields = None
    return export_doctype_list_excel_internal("Employee", filters=filters, fields=parsed_fields)


def export_doctype_list_excel_internal(
    doctype: str,
    filters: dict[str, Any] | str | None = None,
    *,
    fields: list[str] | None = None,
    title: str | None = None,
) -> dict[str, Any]:
    config = _resolve_safe_export_target(doctype)
    if not config:
        return _error(_("Excel export is not enabled for {0}.").format(doctype), error_type="unsupported_doctype")

    resolved_doctype = str(config["doctype"]).strip()
    _require_doctype_permission(resolved_doctype, "read")
    parsed_filters = _parse_json_arg(filters, {}) or {}
    if not isinstance(parsed_filters, dict):
        parsed_filters = {}

    allowed_filter_fields = set(config.get("filter_fields") or set())
    safe_filters = {key: value for key, value in parsed_filters.items() if key in allowed_filter_fields and value not in (None, "")}
    selected_fields, field_labels = _resolve_export_fields(
        resolved_doctype,
        fields if isinstance(fields, list) else None,
        list(config.get("fields") or ["name"]),
    )
    rows = frappe.get_all(
        resolved_doctype,
        filters=safe_filters,
        fields=selected_fields,
        order_by="modified desc",
        limit_page_length=500,
    )
    if not rows:
        return _error(_("No {0} records matched the given filters.").format(resolved_doctype), error_type="not_found")

    export_title = title or str(config.get("title") or f"{resolved_doctype} List").strip()
    if safe_filters:
        export_title = f"{export_title} {' '.join(str(value) for value in safe_filters.values())}".strip()
    content = _build_excel_bytes(export_title, _rows_with_display_labels(rows, field_labels))
    file_name = f"{_slugify_filename(export_title)}.xlsx"
    file_payload = _save_bytes_as_file(
        file_name,
        content,
        attached_to_doctype="User",
        attached_to_name=frappe.session.user,
    )
    return {
        "ok": True,
        "type": "file",
        **file_payload,
        "message": f"{resolved_doctype} list Excel generated successfully",
        "data": {
            "doctype": resolved_doctype,
            "filters": safe_filters,
            "row_count": len(rows),
            "fields": selected_fields,
            "columns": [field_labels.get(fieldname, fieldname) for fieldname in selected_fields],
        },
    }


@frappe.whitelist()
def export_employee_list_excel(
    filters: dict[str, Any] | str | None = None,
    fields: list[str] | str | None = None,
) -> dict[str, Any]:
    try:
        return export_employee_list_excel_internal(filters, fields=fields)
    except frappe.PermissionError:
        return _error(_("You do not have permission to export Employee data."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Export Employee Excel Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


@frappe.whitelist()
def export_doctype_list_excel(
    doctype: str,
    filters: dict[str, Any] | str | None = None,
    fields: list[str] | str | None = None,
) -> dict[str, Any]:
    try:
        parsed_fields = _parse_json_arg(fields, fields)
        if not isinstance(parsed_fields, list):
            parsed_fields = None
        return export_doctype_list_excel_internal(doctype, filters=filters, fields=parsed_fields)
    except frappe.PermissionError:
        return _error(_("You do not have permission to export that data."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Export Doctype Excel Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")


def generate_document_pdf_internal(doctype: str, docname: str, print_format: str | None = None) -> dict[str, Any]:
    doctype_name = str(doctype or "").strip()
    name = str(docname or "").strip()
    if not doctype_name or not name:
        return _error(_("Doctype and docname are required"))
    if not frappe.db.exists(doctype_name, name):
        return _error(_("{0} {1} does not exist.").format(doctype_name, name), error_type="not_found")

    doc = frappe.get_doc(doctype_name, name)
    doc.check_permission("read")
    pdf_content = frappe.get_print(
        doctype_name,
        name,
        print_format=str(print_format or "").strip() or None,
        as_pdf=True,
    )
    file_name = f"{_slugify_filename(name)}.pdf"
    file_payload = _save_bytes_as_file(
        file_name,
        pdf_content,
        attached_to_doctype=doctype_name,
        attached_to_name=name,
    )
    return {
        "ok": True,
        "type": "file",
        **file_payload,
        "message": "PDF generated successfully",
        "data": {
            "doctype": doctype_name,
            "docname": name,
            "print_format": str(print_format or "").strip() or "Standard",
        },
    }


@frappe.whitelist()
def generate_document_pdf(doctype: str, docname: str, print_format: str | None = None) -> dict[str, Any]:
    try:
        return generate_document_pdf_internal(doctype=doctype, docname=docname, print_format=print_format)
    except frappe.PermissionError:
        return _error(_("You do not have permission to print that document."), error_type="permission_error")
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "ERP AI Assistant Generate PDF Error")
        return _error(str(exc) or _("Unknown error"), error_type="server_error")
