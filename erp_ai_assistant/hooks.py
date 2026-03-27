app_name = "erp_ai_assistant"
app_title = "ERP AI Assistant"
app_publisher = "ERP AI Assistant"
app_description = "FAC-backed ERP copilot for ERPNext/Frappe"
app_email = "support@example.com"
app_license = "MIT"

required_apps = []

app_include_js = [
    "/assets/erp_ai_assistant/js/assistant_bubble.js",
]

app_include_css = [
    "/assets/erp_ai_assistant/css/assistant_bubble.css",
]

add_to_apps_screen = [
    {
        "name": "erp_ai_assistant",
        "logo": "/assets/erp_ai_assistant/logo.svg",
        "title": "ERP AI Assistant",
        "route": "/app/assistant-workspace",
        "has_permission": "erp_ai_assistant.api.auth.has_workspace_access",
    }
]

doctype_js = {
    "AI Conversation": "public/js/ai_conversation.js",
    "AI Provider Settings": "public/js/ai_provider_settings.js",
}

permission_query_conditions = {}
has_permission = {}

# Production-oriented tool set: keep the default catalog focused on ERP work,
# file presentation, and user interaction. High-risk or non-ERP lifestyle tools
# are intentionally excluded from the default whole-company deployment.
assistant_tools = [
    "erp_ai_assistant.assistant_tools.web_tools.WebSearchTool",
    "erp_ai_assistant.assistant_tools.web_tools.WebFetchTool",
    "erp_ai_assistant.assistant_tools.web_tools.ImageSearchTool",
    "erp_ai_assistant.assistant_tools.file_ops.CreateFileTool",
    "erp_ai_assistant.assistant_tools.file_ops.StrReplaceTool",
    "erp_ai_assistant.assistant_tools.file_ops.ViewTool",
    "erp_ai_assistant.assistant_tools.file_ops.PresentFilesTool",
    "erp_ai_assistant.assistant_tools.ui_tools.AskUserInputTool",
    "erp_ai_assistant.assistant_tools.ui_tools.MessageComposeTool",
    "erp_ai_assistant.assistant_tools.ui_tools.ToolSearchTool",
]
