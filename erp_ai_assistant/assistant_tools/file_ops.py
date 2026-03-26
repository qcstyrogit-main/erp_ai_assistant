from __future__ import annotations

from pathlib import Path
from typing import Any

import frappe
from frappe.utils.file_manager import save_file
from frappe_assistant_core.core.base_tool import BaseTool

from .common import read_text_file, require_system_manager, safe_path, site_private_root


class BashTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "bash_tool"
        self.description = "Run a bash command in a restricted bench context. Disabled by default."
        self.category = "admin"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The exact command to execute"},
                "description": {"type": "string", "description": "Why this command is being run"},
                "confirmed": {"type": "boolean", "default": False},
            },
            "required": ["command", "description"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        require_system_manager()
        raise ValueError(
            "bash_tool is disabled in the hardened build. Keep shell execution in a separate admin-only maintenance app if needed."
        )


class CreateFileTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "create_file"
        self.description = "Create a new file with content in the allowed filesystem roots."
        self.category = "file"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
                "path": {"type": "string"},
                "file_text": {"type": "string"},
            },
            "required": ["description", "path", "file_text"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        require_system_manager()
        path = safe_path(str(arguments.get("path") or ""))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(arguments.get("file_text") or ""), encoding="utf-8")
        return {"path": str(path), "bytes_written": path.stat().st_size}


class StrReplaceTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "str_replace"
        self.description = "Replace a unique string in a file with another string."
        self.category = "file"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
                "path": {"type": "string"},
                "old_str": {"type": "string"},
                "new_str": {"type": "string", "default": ""},
            },
            "required": ["description", "old_str", "path"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        require_system_manager()
        path = safe_path(str(arguments.get("path") or ""), must_exist=True)
        old_str = str(arguments.get("old_str") or "")
        new_str = str(arguments.get("new_str") or "")
        content = read_text_file(path)
        count = content.count(old_str)
        if count != 1:
            raise ValueError(f"old_str must appear exactly once. Found {count} matches.")
        updated = content.replace(old_str, new_str, 1)
        path.write_text(updated, encoding="utf-8")
        return {"path": str(path), "replacements": 1}


class ViewTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "view"
        self.description = "View text files, images, or directory listings with optional line range."
        self.category = "file"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
                "path": {"type": "string"},
                "view_range": {"type": ["array", "null"], "items": {"type": "integer"}, "minItems": 2, "maxItems": 2},
            },
            "required": ["description", "path"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        require_system_manager()
        path = safe_path(str(arguments.get("path") or ""), must_exist=True)
        if path.is_dir():
            return {
                "path": str(path),
                "type": "directory",
                "entries": [
                    {"name": child.name, "is_dir": child.is_dir(), "size": child.stat().st_size if child.is_file() else None}
                    for child in sorted(path.iterdir(), key=lambda item: item.name.lower())
                ],
            }

        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf"}:
            return {"path": str(path), "type": "binary", "size": path.stat().st_size}

        content = read_text_file(path).splitlines()
        view_range = arguments.get("view_range") or [1, min(len(content), 200)]
        start = max(1, int(view_range[0]))
        end = len(content) if int(view_range[1]) == -1 else max(start, int(view_range[1]))
        selected = content[start - 1 : end]
        return {
            "path": str(path),
            "type": "text",
            "start_line": start,
            "end_line": start + len(selected) - 1,
            "content": "\n".join(selected),
        }


class PresentFilesTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "present_files"
        self.description = "Make files visible and downloadable to the user in the chat interface."
        self.category = "file"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {"filepaths": {"type": "array", "items": {"type": "string"}, "minItems": 1}},
            "required": ["filepaths"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        files = []
        for raw_path in arguments.get("filepaths") or []:
            path = safe_path(str(raw_path), must_exist=True)
            payload = path.read_bytes()
            is_private = int(Path(path).resolve().is_relative_to(site_private_root()))
            file_doc = save_file(path.name, payload, "User", frappe.session.user, is_private=is_private)
            files.append(
                {
                    "path": str(path),
                    "file_name": file_doc.file_name,
                    "file_url": file_doc.file_url,
                    "file_docname": file_doc.name,
                }
            )
        return {"files": files}
