import json

import frappe


@frappe.whitelist()
def export_report_payload(report_name: str, filters: str | None = None, format: str = "json"):
    """Run a report and return normalized export metadata.

    The Desk page can call this before generating XLSX/PDF/DOCX server-side.
    """
    parsed_filters = json.loads(filters) if filters else {}
    result = frappe.get_attr("frappe.desk.query_report.run")(report_name=report_name, filters=parsed_filters)
    return {
        "report_name": report_name,
        "format": format,
        "data": result,
    }
