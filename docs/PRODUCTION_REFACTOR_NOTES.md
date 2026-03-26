# Production refactor notes

This hardened package focuses the assistant on ERP work and removes high-risk packaging clutter.

## What changed
- Removed bundled `.git`, `.venv`, cache folders, `*.pyc`, and duplicate package trees.
- Reduced the default assistant tool catalog to a production-oriented subset.
- Disabled shell execution in `BashTool` for this build.
- Added `erp_ai_assistant.api.security` with shared permission and destructive-action checks.
- Hardened `tool_registry.py` to:
  - use `frappe.get_list(...)` for permission-aware listing
  - include permission summaries in read responses
  - require explicit confirmation for deletes
  - prefer draft-first document creation unless `confirmed_submit=true`
  - block generic global search without a target DocType
- Disabled the custom guest login wrapper by default. Enable only if needed with `erp_ai_enable_session_login = 1`.

## Recommended next rollout steps
1. Pass `doctype`, `docname`, `route`, and current filters from the frontend on every request.
2. Add an assistant audit DocType to log prompts, tools used, and touched records.
3. Add role-based tool catalogs per department.
4. Split `api/ai.py` into runtime, prompting, providers, and parser modules.
