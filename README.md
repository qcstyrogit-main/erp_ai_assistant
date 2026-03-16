# ERP AI Assistant

Installable Frappe/ERPNext app that turns ERPNext into a Claude Desktop-style web assistant backed by FAC.

It provides:
- a Desk floating assistant bubble
- a Desk assistant workspace
- a website assistant page
- conversation and message DocTypes
- multimodal chat with image attachments
- FAC-backed ERP tool calling
- downloadable Excel/PDF/Word artifacts generated from chat results

The app is intended to be the web counterpart of Claude Desktop for ERPNext:
- chat inside ERPNext
- call FAC tools with ERP permissions
- read ERP records and reports
- create/export files from tool results
- work with images when the configured model endpoint supports vision

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
- Configure provider credentials in `AI Provider Settings` after install/migrate.

## What The App Is
- UI host for an ERP-native AI assistant inside Frappe/ERPNext
- web client for FAC-backed tool use
- conversation layer for prompts, tool activity, attachments, and exports
- artifact renderer for downloadable responses

## Current Capabilities
- Chat with OpenAI, OpenAI-compatible, or Anthropic providers
- Attach images in the prompt and render them in chat
- Use FAC-discovered ERP tools with role and permission checks
- Run deterministic tool-first flows for common ERP intents
- Export tool results as Excel, PDF, or Word from the assistant response
- Use the assistant in Desk and on the website

## Architecture
- `erp_ai_assistant` is the UI and conversation app
- `frappe_assistant_core` is the tool backend
- the model provider handles reasoning
- FAC tool results are rendered back into chat responses and downloadable artifacts

In practice, this app behaves like:
1. user sends prompt in ERPNext
2. app sends prompt to model provider
3. model can call FAC-backed ERP tools
4. tool results are returned to the model
5. assistant renders final answer and optional downloadable files

## AI Provider Setup
1. Run:

```bash
bench --site <your-site> migrate
```

2. In Desk, open `AI Provider Settings`.
3. Choose `OpenAI` or `Anthropic`.
4. Set the matching API key and default model.
5. For OpenAI remote MCP tools, enable `Enable Remote MCP Servers For OpenAI` and provide JSON like:

```json
[
  {
    "server_label": "your-mcp",
    "server_url": "https://your-mcp-server.example.com",
    "authorization": "Bearer <token>",
    "require_approval": "never"
  }
]
```

## Environment Variable Fallbacks
If you prefer site config or environment variables, the app also reads:

- `ERP_AI_PROVIDER`
- `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_MODEL`, `OPENAI_MODELS`, `OPENAI_RESPONSES_PATH`
- `ERP_AI_OPENAI_MCP_ENABLED`, `ERP_AI_OPENAI_MCP_SERVERS`
- `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_MESSAGES_PATH`
- `ANTHROPIC_MODEL`, `ANTHROPIC_MODELS`, `ANTHROPIC_VISION_MODEL`

## Notes
- Vision depends on the configured endpoint actually supporting image input.
- FAC remains the source of truth for ERP tool availability and permission checks.
- Export attachments are generated on demand from chat payloads instead of relying on static saved documents.

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
