"""Export functionality for ERP AI Assistant - Excel, PDF, and Word export."""

import json
from io import BytesIO
from typing import Any, Dict, List

import frappe
from frappe.utils.file_manager import save_file


def _stringify_cell(value: Any) -> str:
    """Convert a Python value into a clean string for export cells."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    return str(value)


def _payload_to_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        rows = []
        for row in payload:
            if isinstance(row, dict):
                rows.append(row)
            else:
                rows.append({"value": _stringify_cell(row)})
        return rows
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return _payload_to_rows(payload.get("data"))
        if isinstance(payload.get("result"), list):
            return _payload_to_rows(payload.get("result"))
        compact = {k: v for k, v in payload.items() if k not in {"message", "success"}}
        if compact:
            return [compact]
    return [{"value": _stringify_cell(payload)}]


def _normalize_export_payload(payload: str) -> tuple[str, list[dict[str, Any]]]:
    data = json.loads(payload)
    title = data.get("title", "export")
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        rows = []
    return title, rows


def _slugify_filename(text: str) -> str:
    """Convert any text into a filesystem-safe filename."""
    import re

    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text)
    return text.lower() or "export"


@frappe.whitelist()
def export_to_excel(payload: str, filename: str | None = None):
    """Export payload data to Excel format."""
    try:
        import openpyxl  # noqa: F401
    except ImportError as exc:
        frappe.throw("Excel export requires openpyxl. Install with: pip install openpyxl")

    title, rows = _normalize_export_payload(payload)

    if not rows:
        frappe.throw("No data to export")

    bytes_content = _build_excel_bytes(title, rows)

    fname = _slugify_filename(filename or title)
    frappe.local.response.filename = f"{fname}.xlsx"
    frappe.local.response.filecontent = bytes_content
    frappe.local.response.type = "download"


@frappe.whitelist()
def export_to_pdf(payload: str, filename: str | None = None):
    """Export payload data to PDF format."""
    try:
        import reportlab  # noqa: F401
    except ImportError as exc:
        frappe.throw("PDF export requires reportlab. Install with: pip install reportlab")

    title, rows = _normalize_export_payload(payload)

    if not rows:
        frappe.throw("No data to export")

    bytes_content = _build_pdf_bytes(title, rows)

    fname = _slugify_filename(filename or title)
    frappe.local.response.filename = f"{fname}.pdf"
    frappe.local.response.filecontent = bytes_content
    frappe.local.response.type = "download"


@frappe.whitelist()
def export_to_word(payload: str, filename: str | None = None):
    """Export payload data to Word format."""
    try:
        import docx  # noqa: F401
    except ImportError as exc:
        frappe.throw("Word export requires python-docx. Install with: pip install python-docx")

    title, rows = _normalize_export_payload(payload)

    if not rows:
        frappe.throw("No data to export")

    bytes_content = _build_word_bytes(title, rows)

    fname = _slugify_filename(filename or title)
    frappe.local.response.filename = f"{fname}.docx"
    frappe.local.response.filecontent = bytes_content
    frappe.local.response.type = "download"


def create_message_artifacts(
    payload: Any,
    title: str,
    attached_to_doctype: str,
    attached_to_name: str,
) -> list[dict[str, str]]:
    rows = _payload_to_rows(payload)
    if not rows:
        return []

    attachments: list[dict[str, str]] = []
    base_name = _slugify_filename(title or "assistant-export")
    normalized_title = title or "Assistant Export"

    builders = [
        ("xlsx", "Excel", _build_excel_bytes),
        ("pdf", "PDF", _build_pdf_bytes),
        ("docx", "Word", _build_word_bytes),
    ]

    for ext, label, builder in builders:
        try:
            content = builder(normalized_title, rows)
            filename = f"{base_name}.{ext}"
            file_doc = save_file(
                fname=filename,
                content=content,
                dt=attached_to_doctype,
                dn=attached_to_name,
                is_private=1,
            )
            attachments.append(
                {
                    "label": label,
                    "filename": filename,
                    "file_url": file_doc.file_url,
                    "file_type": ext,
                }
            )
        except Exception:
            # Skip formats that fail due to missing libs or generation errors.
            continue

    return attachments


def _build_excel_bytes(title: str, rows: list[dict[str, Any]]) -> bytes:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = (title or "Export")[:30]

    headers = list(rows[0].keys())
    ws.append(headers)
    for row in rows:
        ws.append([_stringify_cell(row.get(header, "")) for header in headers])

    for column in ws.columns:
        max_length = 0
        col_cells = list(column)
        for cell in col_cells:
            val = _stringify_cell(cell.value)
            if len(val) > max_length:
                max_length = len(val)
        ws.column_dimensions[col_cells[0].column_letter].width = min(max_length + 2, 50)

    buffer = BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def _build_pdf_bytes(title: str, rows: list[dict[str, Any]]) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle

    headers = list(rows[0].keys())
    table_data = [headers]
    for row in rows:
        table_data.append([_stringify_cell(row.get(header, "")) for header in headers])

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, title=title)
    styles = getSampleStyleSheet()
    elements = [Paragraph(title, styles["Title"]), Paragraph("<br/>", styles["Normal"])]

    table = Table(table_data)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )
    elements.append(table)
    doc.build(elements)
    return buffer.getvalue()


def _build_word_bytes(title: str, rows: list[dict[str, Any]]) -> bytes:
    from docx import Document

    doc = Document()
    doc.add_heading(title, 0)

    headers = list(rows[0].keys())
    table = doc.add_table(rows=len(rows) + 1, cols=len(headers))
    table.style = "Light Grid"

    for i, header in enumerate(headers):
        table.rows[0].cells[i].text = header
    for row_idx, row in enumerate(rows):
        for col_idx, header in enumerate(headers):
            table.rows[row_idx + 1].cells[col_idx].text = _stringify_cell(row.get(header, ""))

    buffer = BytesIO()
    doc.save(buffer)
    return buffer.getvalue()
