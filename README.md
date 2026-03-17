# ERP AI Assistant

Installable Frappe/ERPNext app that adds a Claude Desktop-style assistant inside ERPNext.

It includes:
- a Desk floating assistant bubble
- a Desk workspace entry
- a web assistant page
- conversation and message DocTypes
- natural-language ERP routing
- internal tool and resource registries
- Excel/PDF export support
- optional provider-backed chat and planning

## What It Is

This app is no longer just a chat frontend.

It now behaves like a small assistant host inside ERPNext:
- user prompt enters the assistant UI
- planner classifies the request
- parser/router resolves ERP intent and context
- tool registry exposes safe ERP/file actions
- resource registry exposes ERP context and metadata
- deterministic ERP execution runs when appropriate
- provider-backed chat can handle general conversation or broader planning

That makes the app much closer to a Claude Desktop-style architecture:
- discoverable tools
- discoverable resources
- planner-driven routing
- safe execution inside ERPNext

## Main Capabilities

- Chat in Desk and web
- General chat when an AI provider is configured
- Deterministic ERP read/list/search/count prompts
- Draft document creation for common ERP transactions
- Generic metadata-aware create/update flows
- Workflow actions like submit/cancel/approve
- Excel exports with user-requested fields
- Human-readable Excel column labels
- PDF generation for ERP documents
- Pending-action continuation in chat
- Tool catalog and resource catalog inspection

## Current Assistant Flow

The current backend shape is:

`User Prompt`
-> `Normalizer`
-> `Planner`
-> `Parser`
-> `Context Resolver`
-> `Clarification / Pending Action Logic`
-> `Tool Router`
-> `ERP Tools / File Tools`
-> `Structured Assistant Response`

There are now two important internal registries:

- `tool_registry.py`
  Exposes assistant actions like create, update, export, schema lookup, workflow, report execution, and generic document tools.
- `resource_registry.py`
  Exposes assistant-readable context like current document, doctype schema, available doctypes, current page context, and pending assistant action.

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
- Website route: `/assistant`
- Configure provider credentials in `AI Provider Settings`

## Example Prompts

Read and search:
- `how many active employees`
- `show me sales invoices for customer ANICA`
- `pull out Macdenver Conti Magbojos details`
- `list available tools`
- `list available resources`

Create and update:
- `create customer with customer name Aqua Flask`
- `create new employee with name Juan Dela Cruz`
- `update employee Macdenver Magbojos birthday to April 30, 1993`
- `create sales order for customer ABC with items ITEM-001 qty 2`

Exports:
- `export employee list to excel`
- `export employee list to excel with fields employee id, employee name, department`
- `export sales invoices for customer ANICA to excel with fields sales invoice id, customer, posting date, grand total`
- `generate pdf for sales invoice SINV-0001`

Workflow:
- `submit sales order SO-0001`
- `cancel sales invoice SINV-0001`
- `approve leave application HR-LAP-0001`

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

Router / planner:
- `erp_ai_assistant.api.router.route_prompt`
- `erp_ai_assistant.api.assistant.classify_prompt`

ERP actions:
- `erp_ai_assistant.api.assistant.create_sales_order`
- `erp_ai_assistant.api.assistant.create_quotation`
- `erp_ai_assistant.api.assistant.create_purchase_order`
- `erp_ai_assistant.api.assistant.create_erp_document`
- `erp_ai_assistant.api.assistant.update_erp_document`
- `erp_ai_assistant.api.assistant.submit_erp_document`
- `erp_ai_assistant.api.assistant.cancel_erp_document`
- `erp_ai_assistant.api.assistant.run_workflow_action`

Files:
- `erp_ai_assistant.api.assistant.export_doctype_list_excel`
- `erp_ai_assistant.api.file_tools.export_employee_list_excel`
- `erp_ai_assistant.api.file_tools.generate_document_pdf`

MCP/FAC-style proxy:
- `erp_ai_assistant.api.fac_proxy.handle_mcp`

Supported MCP-style operations:
- `tools/list`
- `tools/call`
- `resources/list`
- `resources/read`

## Provider Setup

Open `AI Provider Settings` in Desk and configure one of:
- `OpenAI`
- `OpenAI Compatible`
- `Anthropic`

General chat and model-backed planning depend on provider configuration.
Deterministic ERP routing still works even when no provider is configured.

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

## Important Files

- `erp_ai_assistant/hooks.py`
- `erp_ai_assistant/api/assistant.py`
- `erp_ai_assistant/api/ai.py`
- `erp_ai_assistant/api/planner.py`
- `erp_ai_assistant/api/router.py`
- `erp_ai_assistant/api/tool_registry.py`
- `erp_ai_assistant/api/resource_registry.py`
- `erp_ai_assistant/api/file_tools.py`
- `erp_ai_assistant/public/js/assistant_bubble.js`
- `erp_ai_assistant/public/js/web_assistant.js`

## Notes

- This app is closer to Claude Desktop in architecture, not by copying the desktop wrapper app, but by using:
  - planner
  - tool registry
  - resource registry
  - host-style routing and execution
- It is still a hybrid assistant, not a perfect "any prompt always works" system.
- The strongest current path is:
  - planner-guided ERP execution
  - deterministic tool execution
  - provider-backed fallback chat
