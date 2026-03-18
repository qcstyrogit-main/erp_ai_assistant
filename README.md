# ERP AI Assistant

Installable Frappe/ERPNext app that adds an assistant UI inside ERPNext and runs it in a FAC-native way.

## What It Is

This app is now designed to work with Frappe Assistant Core (FAC) as the primary tool backend.

Current runtime behavior:
- assistant prompt goes to the configured model provider
- FAC provides the live tool catalog for the session
- FAC executes tools directly
- same-site FAC registry is the primary path
- remote MCP is optional fallback only

This means the assistant should use the tools FAC actually exposes, not a static hardcoded tool list.

## Main Capabilities

- Desk floating assistant bubble
- Desk workspace entry
- website assistant page
- conversation and message DocTypes
- provider-backed chat with FAC tool calling
- FAC connection test from `AI Provider Settings`
- optional remote MCP configuration
- export/file artifact support when FAC or app tools expose it

## FAC-Native Flow

Normal assistant execution is now:

`User Prompt`
-> `Provider Chat`
-> `FAC Tool Discovery`
-> `FAC Tool Execution`
-> `Assistant Response`

Important notes:
- the assistant is FAC-native at runtime
- it uses the local FAC registry first when FAC is installed on the same site
- it does not rely on the old deterministic parser/router as the normal prompt path
- actual assistant capability depends on the tools FAC exposes

Example:
- if FAC does not expose `update_document`, the assistant should not pretend it can update documents

## App Name

- Python module: `erp_ai_assistant`
- Bench install target: `erp_ai_assistant`

## Install

From your Bench folder:

```bash
bench get-app --branch main erp_ai_assistant D:/ai_assistant
bench --site <your-site> install-app erp_ai_assistant
bench --site <your-site> migrate
bench build
bench restart
```

If the app is already present in your bench, skip `get-app`.

## Usage

- Desk bubble loads automatically from `hooks.py`
- Desk workspace route: `/app/assistant-workspace`
- website route: `/assistant`
- configure provider credentials in `AI Provider Settings`

## AI Provider Settings

Open `AI Provider Settings` and configure:
- provider
- model
- provider base URL / path if using OpenAI-compatible APIs

FAC-related fields:
- `FAC MCP URL`
- `FAC MCP Authorization`
- `FAC MCP Timeout (Seconds)`

Important:
- if FAC is installed on the same ERP site, the assistant will prefer the local FAC registry
- manual FAC MCP authorization is not required for the local same-site path
- remote MCP settings are mainly for fallback or external FAC servers

## Test FAC Connection

`AI Provider Settings` includes a `Test MCP Connection` button.

What it verifies:
- whether local FAC registry is available
- whether remote MCP fallback is reachable
- current tool count
- currently exposed tool names

Expected good result for same-site FAC:
- connected
- mode is effectively local FAC registry
- no manual authorization required

## Example Prompts

Read and search:
- `how many active employees`
- `show me sales invoices for customer ANICA`
- `pull out Macdenver Conti Magbojos details`
- `list available tools`
- `list available resources`

Create and workflow:
- `create customer with customer name Aqua Flask`
- `create new employee with name Juan Dela Cruz`
- `create sales order for customer ABC with items ITEM-001 qty 2`
- `submit sales order SO-0001`
- `cancel sales invoice SINV-0001`
- `approve leave application HR-LAP-0001`

Exports:
- `export employee list to excel`
- `export employee list to excel with fields employee id, employee name, department`
- `generate pdf for sales invoice SINV-0001`

## Important Limitation

The assistant can only do what FAC exposes.

If your FAC tool catalog does not expose a tool such as `update_document`, then prompts like:

`Update employee Macdenver Conti Magbojos birthday to April 30, 1993`

may still not complete as a real update.

That is a FAC capability issue, not an ERP AI Assistant connection issue.

## Recommended FAC Tool Roadmap

For this app to behave like a strong ERP assistant, FAC should expose business-level tools instead of relying only on generic CRUD primitives.

Recommended priority:
- `find_one_document`
- `update_document`
- `export_doctype_records`
- `create_report`
- `get_report_definition`
- `update_report`
- `run_report`
- `export_report`

Detailed tool contract:
- [docs/fac_tool_blueprint.md](/d:/frappe_docker/development/frappe-bench/apps/erp_ai_assistant/docs/fac_tool_blueprint.md)

Why this matters:
- `create_document` alone is too low-level for reliable report creation
- generic search tools are too broad for deterministic ERP actions
- explicit business tools let FAC validate inputs and confirm real success

## Public API Endpoints

Assistant:
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

MCP/FAC-style proxy:
- `erp_ai_assistant.api.fac_proxy.handle_mcp`

Supported MCP-style operations:
- `tools/list`
- `tools/call`
- `resources/list`
- `resources/read`

## Environment Variable / Site Config Fallbacks

The app also reads these when provider settings are not filled:

- `ERP_AI_PROVIDER`
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_MODELS`
- `OPENAI_RESPONSES_PATH`
- `ANTHROPIC_API_KEY`
- `ANTHROPIC_AUTH_TOKEN`
- `ANTHROPIC_BASE_URL`
- `ANTHROPIC_MESSAGES_PATH`
- `ANTHROPIC_MODEL`
- `ANTHROPIC_MODELS`
- `ANTHROPIC_VISION_MODEL`
- `ERP_AI_OPENAI_MCP_ENABLED`
- `ERP_AI_OPENAI_MCP_SERVERS`
- `ERP_AI_FAC_MCP_URL`
- `ERP_AI_FAC_MCP_AUTHORIZATION`
- `ERP_AI_FAC_MCP_TIMEOUT`

## Important Files

- `erp_ai_assistant/hooks.py`
- `erp_ai_assistant/api/assistant.py`
- `erp_ai_assistant/api/ai.py`
- `erp_ai_assistant/api/fac_client.py`
- `erp_ai_assistant/api/fac_proxy.py`
- `erp_ai_assistant/api/provider_settings.py`
- `erp_ai_assistant/api/resource_registry.py`
- `erp_ai_assistant/public/js/assistant_bubble.js`
- `erp_ai_assistant/public/js/web_assistant.js`
- `erp_ai_assistant/public/js/ai_provider_settings.js`

## Notes

- runtime is FAC-native
- same-site FAC local registry is preferred over remote MCP
- remote MCP is optional fallback
- provider quality still matters for tool selection and reasoning
- final business capability depends on FAC’s exposed tools, not on static assistant assumptions
