# FAC Tool Blueprint

This document defines the minimum business-level FAC tools needed for `erp_ai_assistant` to behave like a strong ERP assistant.

The core problem today is not connection. It is tool granularity.

Generic tools such as `create_document` and `delete_document` are useful, but they force the model to guess too much about ERP behavior, mandatory fields, validation rules, report internals, and mutation confirmation.

## Design Principles

- Expose business actions, not just low-level CRUD primitives.
- Return normalized payloads that are easy for the assistant to validate.
- Require explicit mutation confirmation in the tool result.
- Prefer safe, schema-aware operations over free-form SQL or Python.
- Make target resolution deterministic when the user prompt is ambiguous.

## Minimum Tool Set

### `find_one_document`

Purpose:
- Resolve a natural-language target into one concrete ERP document.

Use cases:
- `update employee Macdenver birthday`
- `open customer ANICA`
- `submit sales order for customer Aqua Flask`

Input:
```json
{
  "doctype": "Employee",
  "query": "Macdenver Conti Magbojos",
  "filters": {},
  "fields": ["name", "employee_name", "status"],
  "allow_fuzzy_match": true
}
```

Output:
```json
{
  "success": true,
  "doctype": "Employee",
  "name": "HR-EMP-0001",
  "display_name": "Macdenver Conti Magbojos",
  "match_type": "fuzzy",
  "confidence": 0.94,
  "doc": {
    "name": "HR-EMP-0001",
    "employee_name": "Macdenver Conti Magbojos",
    "status": "Active"
  }
}
```

Rules:
- Return exactly one document or a structured ambiguity result.
- Do not silently pick one record if confidence is low.

### `update_document`

Purpose:
- Update one existing document safely.

Use cases:
- `update employee birthday`
- `change customer territory`
- `set sales order delivery date`

Input:
```json
{
  "doctype": "Employee",
  "name": "HR-EMP-0001",
  "fields": {
    "date_of_birth": "1993-04-30"
  },
  "comment": "Updated by ERP AI Assistant"
}
```

Output:
```json
{
  "success": true,
  "doctype": "Employee",
  "name": "HR-EMP-0001",
  "updated_fields": {
    "date_of_birth": "1993-04-30"
  },
  "modified": "2026-03-18 14:55:00.000000",
  "doc": {
    "name": "HR-EMP-0001",
    "employee_name": "Macdenver Conti Magbojos",
    "date_of_birth": "1993-04-30"
  }
}
```

Rules:
- Reject unknown fields.
- Enforce normal Frappe validation and permissions.
- Return the confirmed changed values.

### `set_document_fields`

Purpose:
- Apply partial field updates when the assistant has already resolved the target document.

Why keep this separate:
- `update_document` is the general mutation tool.
- `set_document_fields` is the simple field-patch tool for high-frequency assistant actions.

Input:
```json
{
  "doctype": "Customer",
  "name": "CUST-0001",
  "field_values": {
    "territory": "Philippines",
    "customer_group": "Commercial"
  }
}
```

Output:
```json
{
  "success": true,
  "doctype": "Customer",
  "name": "CUST-0001",
  "updated_fields": ["territory", "customer_group"],
  "doc": {
    "name": "CUST-0001",
    "customer_name": "ANICA",
    "territory": "Philippines",
    "customer_group": "Commercial"
  }
}
```

### `create_report`

Purpose:
- Create a real `Report` document with the required structure.

Use cases:
- `create report for sales order as of 2025`
- `create query report for employee turnover`

Input:
```json
{
  "report_name": "Sales Order 2025",
  "report_type": "Query Report",
  "ref_doctype": "Sales Order",
  "is_standard": "No",
  "module": "Selling",
  "query": "SELECT name, customer, transaction_date, grand_total FROM `tabSales Order` WHERE YEAR(transaction_date) = 2025",
  "filters": [
    {
      "fieldname": "company",
      "label": "Company",
      "fieldtype": "Link",
      "options": "Company"
    }
  ],
  "columns": [
    {
      "fieldname": "name",
      "label": "Sales Order",
      "fieldtype": "Link",
      "options": "Sales Order"
    },
    {
      "fieldname": "customer",
      "label": "Customer",
      "fieldtype": "Link",
      "options": "Customer"
    }
  ]
}
```

Output:
```json
{
  "success": true,
  "doctype": "Report",
  "name": "Sales Order 2025",
  "report_type": "Query Report",
  "ref_doctype": "Sales Order",
  "has_query": true,
  "has_script": false,
  "link": "/app/Form/Report/Sales%20Order%202025"
}
```

Rules:
- Reject creation if required report content is missing.
- If `report_type` is `Query Report`, require non-empty `query`.
- Return explicit booleans like `has_query` and `has_script`.

### `update_report`

Purpose:
- Modify an existing report definition safely.

Use cases:
- `remove company filter`
- `change report to show all companies`
- `add grand total column`

Input:
```json
{
  "name": "Detailed Sales Revenue - Qc Styropackaging Corp",
  "changes": {
    "query": "SELECT ...",
    "filters": [],
    "columns": [
      {
        "fieldname": "grand_total",
        "label": "Grand Total",
        "fieldtype": "Currency"
      }
    ]
  }
}
```

Output:
```json
{
  "success": true,
  "doctype": "Report",
  "name": "Detailed Sales Revenue - Qc Styropackaging Corp",
  "updated_parts": ["query", "filters", "columns"],
  "has_query": true
}
```

### `get_report_definition`

Purpose:
- Read the actual structure of a report for inspection or editing.

Use cases:
- `show me the current query`
- `what filters does this report use`
- `edit the report to remove company filter`

Input:
```json
{
  "name": "Detailed Sales Revenue - Qc Styropackaging Corp"
}
```

Output:
```json
{
  "success": true,
  "doctype": "Report",
  "name": "Detailed Sales Revenue - Qc Styropackaging Corp",
  "report_type": "Query Report",
  "ref_doctype": "Sales Invoice",
  "query": "SELECT ...",
  "columns": [],
  "filters": []
}
```

### `run_report`

Purpose:
- Execute a report with filters and return rows in a normalized format.

Use cases:
- `show sales revenue for 2025`
- `run sales order 2025 report`

Input:
```json
{
  "report_name": "Sales Order 2025",
  "filters": {
    "company": "Qc Styropackaging Corp"
  },
  "limit": 100
}
```

Output:
```json
{
  "success": true,
  "report_name": "Sales Order 2025",
  "columns": [
    {"fieldname": "name", "label": "Sales Order"},
    {"fieldname": "customer", "label": "Customer"}
  ],
  "rows": [
    {"name": "SO-0001", "customer": "ANICA"}
  ],
  "row_count": 1
}
```

### `export_report`

Purpose:
- Export an existing report to Excel or CSV.

Use cases:
- `export sales order 2025 report to excel`

Input:
```json
{
  "report_name": "Sales Order 2025",
  "filters": {
    "company": "Qc Styropackaging Corp"
  },
  "format": "xlsx"
}
```

Output:
```json
{
  "success": true,
  "report_name": "Sales Order 2025",
  "file_name": "sales-order-2025.xlsx",
  "file_url": "/files/sales-order-2025.xlsx",
  "row_count": 152
}
```

### `export_doctype_records`

Purpose:
- Export rows from a doctype without forcing the model to invent a report.

Use cases:
- `export sales invoice records for customer ANICA to excel`
- `download employee list with employee name and department`

Input:
```json
{
  "doctype": "Sales Invoice",
  "filters": {
    "customer": "ANICA"
  },
  "fields": ["name", "posting_date", "customer", "grand_total", "status"],
  "format": "xlsx",
  "file_name": "sales-invoices-anica.xlsx"
}
```

Output:
```json
{
  "success": true,
  "doctype": "Sales Invoice",
  "file_name": "sales-invoices-anica.xlsx",
  "file_url": "/files/sales-invoices-anica.xlsx",
  "row_count": 24
}
```

## Result Contract

Mutation tools should always return these top-level fields:

```json
{
  "success": true,
  "doctype": "Employee",
  "name": "HR-EMP-0001",
  "message": "Employee updated successfully"
}
```

If the tool cannot confirm the final state, it must return:
```json
{
  "success": false,
  "error": "Could not confirm updated document state"
}
```

This is important because the assistant should not claim success when FAC only partially completed an action.

## Why These Tools Matter

Without these tools, the model has to guess:
- which doctype field to use
- how to resolve a target name
- which report fields are mandatory
- how to structure filters and columns
- how to confirm mutation success

With these tools, the assistant becomes more reliable because the business logic moves into FAC, where it belongs.

## Priority Order

Implement in this order:

1. `find_one_document`
2. `update_document`
3. `export_doctype_records`
4. `create_report`
5. `get_report_definition`
6. `update_report`
7. `run_report`
8. `export_report`

This sequence gives the biggest assistant improvement fastest.
