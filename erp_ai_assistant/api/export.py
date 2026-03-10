"""Export functionality for ERP AI Assistant - Excel, PDF, and Word export."""

import json
from pathlib import Path
from typing import Any, Dict, List

import frappe


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


def _rows_to_table(title: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert a list of dict rows into a table structure for export."""
    if not rows:
        return {"headers": ["Result"], "rows": [{"Result": "No data"}]}

    # Extract all unique keys as headers
    headers = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)

    # Build table rows
    table_rows = []
    for row in rows:
        table_row = {header: _stringify_cell(row.get(header, "")) for header in headers}
        table_rows.append(table_row)

    return {"headers": headers, "rows": table_rows}


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
        from openpyxl import Workbook
    except ImportError as exc:
        frappe.throw("Excel export requires openpyxl. Install with: pip install openpyxl")

    data = json.loads(payload)
    title = data.get("title", "export")
    rows = data.get("rows", [])

    if not rows:
        frappe.throw("No data to export")

    wb = Workbook()
    ws = wb.active
    ws.title = title[:30]

    # Add headers
    headers = list(rows[0].keys())
    ws.append(headers)

    # Add data rows
    for row in rows:
        ws.append([row.get(header, "") for header in headers])

    # Auto-size columns
    for column in ws.columns:
        max_length = 0
        column = list(column)
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column[0].column_letter].width = adjusted_width

    fname = _slugify_filename(filename or title)
    frappe.local.response.filename = f"{fname}.xlsx"
    frappe.local.response.filecontent = wb.save_to_bytes()
    frappe.local.response.type = "binary"


@frappe.whitelist()
def export_to_pdf(payload: str, filename: str | None = None):
    """Export payload data to PDF format."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
    except ImportError as exc:
        frappe.throw("PDF export requires reportlab. Install with: pip install reportlab")

    data = json.loads(payload)
    title = data.get("title", "export")
    rows = data.get("rows", [])

    if not rows:
        frappe.throw("No data to export")

    # Build table data
    headers = list(rows[0].keys())
    table_data = [headers]
    for row in rows:
        table_data.append([_stringify_cell(row.get(header, "")) for header in headers])

    # Create PDF
    from io import BytesIO

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, title=title)

    styles = getSampleStyleSheet()
    elements = []

    # Title
    elements.append(Paragraph(title, styles["Title"]))
    elements.append(Paragraph("<br/>", styles["Normal"]))

    # Table
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

    fname = _slugify_filename(filename or title)
    frappe.local.response.filename = f"{fname}.pdf"
    frappe.local.response.filecontent = buffer.getvalue()
    frappe.local.response.type = "binary"


@frappe.whitelist()
def export_to_word(payload: str, filename: str | None = None):
    """Export payload data to Word format."""
    try:
        from docx import Document
        from docx.shared import Inches
    except ImportError as exc:
        frappe.throw("Word export requires python-docx. Install with: pip install python-docx")

    data = json.loads(payload)
    title = data.get("title", "export")
    rows = data.get("rows", [])

    if not rows:
        frappe.throw("No data to export")

    doc = Document()
    doc.add_heading(title, 0)

    # Add table
    headers = list(rows[0].keys())
    table = doc.add_table(rows=len(rows) + 1, cols=len(headers))
    table.style = "Light Grid"

    # Add headers
    for i, header in enumerate(headers):
        table.rows[0].cells[i].text = header

    # Add data rows
    for row_idx, row in enumerate(rows):
        for col_idx, header in enumerate(headers):
            table.rows[row_idx + 1].cells[col_idx].text = _stringify_cell(row.get(header, ""))

    from io import BytesIO

    buffer = BytesIO()
    doc.save(buffer)

    fname = _slugify_filename(filename or title)
    frappe.local.response.filename = f"{fname}.docx"
    frappe.local.response.filecontent = buffer.getvalue()
    frappe.local.response.type = "binary"
