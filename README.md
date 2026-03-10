# ERP AI Assistant (Web + Desk)

Installable Frappe/ERPNext app with:
- Desk floating assistant bubble (`/app`)
- Desk workspace page (`/app/assistant-workspace`)
- Web assistant page (`/assistant`)
- AI conversation DocTypes and API endpoints

## App Name
- Python module: `erp_ai_assistant`
- Bench install target: `erp_ai_assistant`

## Install On ERPNext
From your Bench folder:

```bash
bench get-app --branch main erp_ai_assistant D:/ai_assistant
bench --site <your-site> install-app erp_ai_assistant
bench --site <your-site> migrate
bench build
bench restart
```

If your Bench already has this app path linked, you can skip `get-app` and just run install/migrate/build.

## Usage
- Desk bubble appears automatically for logged-in users.
- Desk workspace route: `/app/assistant-workspace`
- Website route: `/assistant`

## Main Paths
- `erp_ai_assistant/hooks.py`
- `erp_ai_assistant/api/*.py`
- `erp_ai_assistant/doctype/ai_conversation/*`
- `erp_ai_assistant/doctype/ai_message/*`
- `erp_ai_assistant/public/js/assistant_bubble.js`
- `erp_ai_assistant/public/css/assistant_bubble.css`
- `erp_ai_assistant/www/assistant.html`
- `erp_ai_assistant/public/js/web_assistant.js`
- `erp_ai_assistant/public/css/web_assistant.css`
