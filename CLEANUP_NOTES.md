# Cleanup notes

This cleaned package removes local-only and duplicate artifacts that should not ship with the app source:

- `.git/`
- `.venv/`
- `__pycache__/` and compiled Python files
- `erp_ai_assistant.egg-info/`
- duplicate root `hooks.py`
- duplicate root `assistant_tools/`
- duplicate `erp_ai_assistant_README.md`
- local `erp_ai_assistant/test_assistant_bubble.html`

The Frappe app entrypoint remains `erp_ai_assistant/hooks.py`.
