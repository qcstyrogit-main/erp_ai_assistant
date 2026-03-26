# ERP AI Assistant

Frappe/ERPNext app that adds an AI copilot to Desk and the website, stores chat history in DocTypes, and executes ERP actions through Frappe Assistant Core (FAC) when available.

## Overview

This app provides three connected layers:

- UI surfaces for Desk and web users
- provider-backed LLM orchestration for chat, tool calling, and image prompts
- ERP tool execution through FAC local registry first, then optional remote MCP fallback

The current codebase is not only a chat widget. It also includes:

- `AI Provider Settings` for provider and MCP configuration
- `AI Conversation` and `AI Message` DocTypes for chat persistence
- deterministic ERP helper APIs and internal tool/resource registries
- queued prompt execution with progress polling
- export/file generation helpers for Excel and PDF outputs

## Main Features

- Floating Desk assistant bubble injected from app hooks
- Full-page Desk workspace at `/app/assistant-workspace`
- Website assistant page at `/assistant`
- Conversation history with pin, rename, delete, and pending-action continuation support
- Context-aware prompting using current route, DocType, and document name
- Image input support in the Desk bubble and workspace/web assistant
- Provider support for OpenAI, OpenAI-compatible APIs, and Anthropic
- Model selection from configured allowed/default models
- FAC-native tool discovery and execution
- Internal tool and resource catalogs for ERP operations and metadata reads
- Attachment/export handling for generated files
- Connection test buttons for both AI provider and FAC MCP backend

## Runtime Architecture

Normal request flow:

`User Prompt`
-> `Desk/Web UI`
-> `erp_ai_assistant.api.assistant.handle_prompt`
-> `erp_ai_assistant.api.ai.enqueue_prompt`
-> `Queued LLM run`
-> `FAC local registry or remote MCP`
-> `AI Message / attachments / tool events`

Tool backend resolution order:

1. Local FAC registry, if `frappe_assistant_core` is installed on the same site and exposes tools
2. Remote FAC/MCP server configured in `AI Provider Settings`
3. Internal fallback catalogs for direct API access and metadata/resource reads

Important behavior:

- Local FAC is preferred over remote MCP.
- Remote MCP is optional, not required for same-site FAC deployments.
- Actual assistant capability depends on the tools FAC exposes at runtime.
- Prompts are queued with `frappe.enqueue(...)`, and the UI polls `get_prompt_progress` and `get_prompt_result`.

## Project Structure

Key areas in the repository:

- `erp_ai_assistant/hooks.py`: app metadata, Desk asset injection, workspace app launcher
- `erp_ai_assistant/api/ai.py`: LLM orchestration, prompt queueing, progress/result cache, model resolution
- `erp_ai_assistant/api/assistant.py`: whitelisted assistant endpoints and deterministic routing bridge
- `erp_ai_assistant/api/fac_client.py`: local FAC registry and remote MCP client
- `erp_ai_assistant/api/tool_registry.py`: internal tool catalog
- `erp_ai_assistant/api/resource_registry.py`: internal resource catalog
- `erp_ai_assistant/api/chat.py`: conversation/message persistence APIs
- `erp_ai_assistant/api/provider_settings.py`: settings and env/site-config resolution
- `erp_ai_assistant/api/auth.py`: workspace visibility and optional session helpers
- `erp_ai_assistant/public/js/assistant_bubble.js`: Desk floating assistant
- `erp_ai_assistant/public/js/web_assistant.js`: shared full-page assistant client
- `erp_ai_assistant/public/js/ai_provider_settings.js`: connection test buttons on settings form
- `erp_ai_assistant/doctype/ai_provider_settings/*`: single DocType for provider/FAC config
- `erp_ai_assistant/doctype/ai_conversation/*`: conversation DocType
- `erp_ai_assistant/doctype/ai_message/*`: message DocType
- `docs/fac_tool_blueprint.md`: recommended FAC business-tool roadmap
- `docs/PRODUCTION_REFACTOR_NOTES.md`: hardening/refactor notes

## Installation

From your Bench directory:

```bash
bench get-app erp_ai_assistant <repo-url-or-path>
bench --site <site-name> install-app erp_ai_assistant
bench --site <site-name> migrate
bench build
bench restart
```

If the app already exists in the bench, skip `bench get-app`.

## Requirements

- Frappe bench running Python 3.10+
- This app installed on the target site
- An AI provider credential configured in `AI Provider Settings` or via site config/environment variables
- Optional: `frappe_assistant_core` installed on the same site for local FAC-native tool execution

## Configuration

Open `AI Provider Settings` and configure the active provider.

Supported providers:

- `OpenAI`
- `OpenAI Compatible`
- `Anthropic`

Important settings:

- `provider`
- `tool_choice_mode`
- `openai_default_model`
- `openai_models`
- `openai_vision_model`
- `openai_api_key`
- `openai_base_url`
- `openai_responses_path`
- `enable_openai_mcp`
- `openai_mcp_servers_json`
- `anthropic_default_model`
- `anthropic_models`
- `anthropic_vision_model`
- `anthropic_api_key`
- `anthropic_auth_token`
- `anthropic_base_url`
- `anthropic_messages_path`
- `fac_mcp_url`
- `fac_mcp_authorization`
- `fac_mcp_timeout`

Notes:

- Use `https://api.openai.com` with `/v1/responses` for native OpenAI.
- OpenAI-compatible providers can point to custom bases such as NVIDIA-compatible endpoints and often use `/v1/chat/completions`.
- Vision models are selected automatically when image attachments are included.
- `Tool Choice Mode` is only for providers/proxies that need an explicit OpenAI-style or Anthropic-style tool-call format.

## Environment And Site Config Fallbacks

When settings are blank, the app also reads from site config or environment variables.

Common keys:

- `ERP_AI_PROVIDER`
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_MODELS`
- `OPENAI_VISION_MODEL`
- `OPENAI_RESPONSES_PATH`
- `ERP_AI_OPENAI_MCP_ENABLED`
- `ERP_AI_OPENAI_MCP_SERVERS`
- `ERP_AI_FAC_MCP_URL`
- `ERP_AI_FAC_MCP_AUTHORIZATION`
- `ERP_AI_FAC_MCP_TIMEOUT`
- `ANTHROPIC_API_KEY`
- `ANTHROPIC_AUTH_TOKEN`
- `ANTHROPIC_BASE_URL`
- `ANTHROPIC_MESSAGES_PATH`
- `ANTHROPIC_MODEL`
- `ANTHROPIC_MODELS`
- `ANTHROPIC_VISION_MODEL`
- `ERP_AI_TOOL_CHOICE_MODE`

The runtime also honors advanced LLM tuning keys in `api/ai.py`, including timeout, token, streaming, temperature, top-p, conversation-history, and tool-round limits.

## UI Surfaces

Desk bubble:

- injected globally through `app_include_js`
- available to authenticated Desk users
- supports conversation search, pinning, deleting, image attach/paste, model selection, stop action, and progress display

Desk workspace:

- route: `/app/assistant-workspace`
- app icon appears in the apps screen
- uses the shared web assistant client in Desk mode

Website assistant:

- route: `/assistant`
- uses the same shared web assistant styling and interaction model

## Stored Data

`AI Provider Settings`:

- single DocType
- writable by `System Manager`
- stores provider credentials, models, and FAC MCP connection data

`AI Conversation`:

- stores title, status, pin state, and pending action state
- currently permissioned for `System Manager` in the DocType definition
- API layer also restricts access to owner or `System Manager`

`AI Message`:

- stores role, content, tool events, and attachments JSON
- linked to `AI Conversation`

## Tooling Model

The assistant can work with both FAC-exposed tools and the app's internal catalogs.

Internal tool categories include:

- system
- read
- write
- workflow
- resource
- report
- file
- destructive

Examples of internal tools:

- `get_document`
- `list_documents`
- `create_document`
- `update_document`
- `delete_document`
- `get_doctype_info`
- `search_documents`
- `generate_report`
- `create_sales_order`
- `create_purchase_order`
- `create_quotation`
- `submit_erp_document`
- `cancel_erp_document`
- `run_workflow_action`
- `export_doctype_list_excel`
- `generate_document_pdf`

Internal resources include:

- `current_document`
- `doctype_schema`
- `available_doctypes`
- `current_page_context`
- `pending_assistant_action`

Security-related behavior in the internal registry:

- permission-aware document listing
- document/doctype access checks before read or write
- explicit confirmation required for destructive deletion
- draft-first submission flow for document creation
- generic global search blocked without a target DocType

## Public API

Main assistant endpoints:

- `erp_ai_assistant.api.assistant.handle_prompt`
- `erp_ai_assistant.api.assistant.ping_assistant`
- `erp_ai_assistant.api.assistant.answer_erp_query`
- `erp_ai_assistant.api.assistant.list_available_tools`
- `erp_ai_assistant.api.assistant.get_tool_catalog`
- `erp_ai_assistant.api.assistant.list_available_resources`
- `erp_ai_assistant.api.assistant.get_resource_catalog`
- `erp_ai_assistant.api.assistant.read_available_resource`
- `erp_ai_assistant.api.assistant.test_fac_mcp_connection`
- `erp_ai_assistant.api.assistant.test_ai_provider_connection`

Conversation endpoints:

- `erp_ai_assistant.api.chat.list_conversations`
- `erp_ai_assistant.api.chat.get_conversation`
- `erp_ai_assistant.api.chat.create_conversation`
- `erp_ai_assistant.api.chat.rename_conversation`
- `erp_ai_assistant.api.chat.toggle_pin`
- `erp_ai_assistant.api.chat.delete_conversation`

Queued prompt endpoints:

- `erp_ai_assistant.api.ai.enqueue_prompt`
- `erp_ai_assistant.api.ai.get_prompt_progress`
- `erp_ai_assistant.api.ai.get_prompt_result`
- `erp_ai_assistant.api.ai.get_available_models`

Authentication/helpers:

- `erp_ai_assistant.api.auth.who_am_i`
- `erp_ai_assistant.api.auth.session_login`
- `erp_ai_assistant.api.auth.session_logout`

MCP/FAC proxy endpoint:

- `erp_ai_assistant.api.fac_proxy.handle_mcp`

Supported MCP-style operations:

- `tools/list`
- `tools/call`
- `resources/list`
- `resources/read`

## Connection Testing

The `AI Provider Settings` form adds two custom buttons:

- `Test AI Provider`
- `Test MCP Connection`

These validate:

- provider endpoint reachability
- selected model and compatibility profile
- whether credentials are configured
- FAC connection mode and visible tool count

For same-site FAC deployments, a successful MCP test should usually report `mode: local_registry`.

## Example Prompts

Read and discovery:

- `how many active employees`
- `show me sales invoices for customer ANICA`
- `list available tools`
- `list available resources`
- `show me the schema for Sales Order`

Create and update:

- `create customer with customer name Aqua Flask`
- `create new employee with name Juan Dela Cruz`
- `create sales order for customer ABC with items ITEM-001 qty 2`
- `update employee EMP-0001 department to Finance`

Workflow and export:

- `submit sales order SO-0001`
- `cancel sales invoice SINV-0001`
- `approve leave application HR-LAP-0001`
- `export employee list to excel`
- `generate pdf for sales invoice SINV-0001`

## Operational Notes

- The assistant should only claim capabilities exposed by the current FAC catalog or internal tools actually available.
- If FAC does not expose a business tool such as `update_document`, the model may not be able to complete that action reliably through FAC alone.
- The current codebase still contains deterministic ERP helper routes alongside the FAC-native LLM path.
- `api/ai.py` is the main runtime file and is large; `docs/PRODUCTION_REFACTOR_NOTES.md` already recommends splitting it further.
- The optional `session_login` wrapper is disabled by default. Enable it only with `erp_ai_enable_session_login = 1` if you explicitly need it.

## FAC Tool Roadmap

For stronger ERP behavior, FAC should expose business-safe tools such as:

- `find_one_document`
- `update_document`
- `export_doctype_records`
- `create_report`
- `get_report_definition`
- `update_report`
- `run_report`
- `export_report`

Reference:

- [docs/fac_tool_blueprint.md](/d:/frappe_docker/development/frappe-bench/apps/erp_ai_assistant/docs/fac_tool_blueprint.md)
- [docs/PRODUCTION_REFACTOR_NOTES.md](/d:/frappe_docker/development/frappe-bench/apps/erp_ai_assistant/docs/PRODUCTION_REFACTOR_NOTES.md)

## License

MIT
