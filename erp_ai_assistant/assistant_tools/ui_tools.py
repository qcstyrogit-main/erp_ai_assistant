from __future__ import annotations

from typing import Any

import frappe
from frappe_assistant_core.core.base_tool import BaseTool


class AskUserInputTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "ask_user_input_v0"
        self.description = "Present clickable single-select, multi-select, or rank-priority questions to the user instead of asking in plain text."
        self.category = "ui"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {"questions": {"type": "array", "minItems": 1, "maxItems": 3, "items": {"type": "object"}}},
            "required": ["questions"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"widget": "ask_user_input_v0", "payload": arguments}


class MessageComposeTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "message_compose_v1"
        self.description = "Draft emails, Slack messages, or text messages with multiple strategic tone variants."
        self.category = "ui"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "kind": {"type": "string"},
                "summary_title": {"type": "string"},
                "variants": {"type": "array", "items": {"type": "object"}, "minItems": 1},
            },
            "required": ["kind", "variants"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"widget": "message_compose_v1", "payload": arguments}


class RecipeDisplayTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "recipe_display_v0"
        self.description = "Display an interactive recipe with adjustable servings and built-in step timers."
        self.category = "ui"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "description": {"type": ["string", "null"]},
                "base_servings": {"type": ["integer", "null"]},
                "ingredients": {"type": "array", "items": {"type": "object"}},
                "steps": {"type": "array", "items": {"type": "object"}},
                "notes": {"type": ["string", "null"]},
            },
            "required": ["ingredients", "steps", "title"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"widget": "recipe_display_v0", "payload": arguments}


class VisualizeShowWidgetTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "visualize_show_widget"
        self.description = "Render inline SVG diagrams, charts, or interactive HTML widgets directly in the chat."
        self.category = "ui"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "loading_messages": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 4},
                "widget_code": {"type": "string"},
            },
            "required": ["loading_messages", "title", "widget_code"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"widget": "visualize_show_widget", "payload": arguments}


class VisualizeReadMeTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "visualize_read_me"
        self.description = "Internal setup tool that loads design system guidelines before building visuals."
        self.category = "ui"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {"modules": {"type": "array", "items": {"type": "string"}}},
            "required": ["modules"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        modules = arguments.get("modules") or []
        guidelines = {
            "diagram": "Prefer clear hierarchy, labels, and directional flow.",
            "mockup": "Use realistic spacing, headers, and actionable controls.",
            "interactive": "Return self-contained HTML fragments with minimal dependencies.",
            "data_viz": "Use clear titles, axis labels, and color meaning.",
            "art": "Favor a clear visual concept over decorative noise.",
            "chart": "Use accessible contrast and direct value cues.",
        }
        return {
            "widget": "visualize_read_me",
            "modules": modules,
            "guidelines": {key: guidelines.get(key, "") for key in modules},
        }


class ToolSearchTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "tool_search"
        self.description = "Load deferred MCP tools by keyword."
        self.category = "resource"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 5, "minimum": 1, "maximum": 20},
            },
            "required": ["query"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from frappe_assistant_core.core.tool_registry import get_tool_registry

        query = str(arguments.get("query") or "").strip().lower()
        limit = max(1, min(int(arguments.get("limit") or 5), 20))
        registry = get_tool_registry()
        rows = []
        for tool in registry.get_available_tools(user=frappe.session.user):
            if not isinstance(tool, dict):
                continue
            haystack = " ".join(
                [
                    str(tool.get("name") or ""),
                    str(tool.get("description") or ""),
                    " ".join((tool.get("inputSchema") or {}).get("properties", {}).keys()),
                ]
            ).lower()
            if query in haystack:
                rows.append(
                    {
                        "name": tool.get("name"),
                        "description": tool.get("description"),
                        "inputSchema": tool.get("inputSchema"),
                    }
                )
        return {"query": query, "tools": rows[:limit]}
