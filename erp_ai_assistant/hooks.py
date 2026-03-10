app_name = "erp_ai_assistant"
app_title = "ERP AI Assistant"
app_publisher = "OpenAI"
app_description = "FAC-backed AI assistant workspace inside ERP"
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
        "name": "erp-ai-assistant",
        "logo": "/assets/erp_ai_assistant/logo.svg",
        "title": "ERP AI Assistant",
        "route": "/app/assistant-workspace",
        "has_permission": "erp_ai_assistant.api.auth.has_workspace_access",
    }
]

doctype_js = {
    "AI Conversation": "public/js/ai_conversation.js",
}

permission_query_conditions = {}
has_permission = {}
