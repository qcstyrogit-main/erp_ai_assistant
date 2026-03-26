from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests
from frappe_assistant_core.core.base_tool import BaseTool

from .common import DEFAULT_HTTP_TIMEOUT, domain_allowed, http_get, normalized_domain, strip_html_text


class WebSearchTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "web_search"
        self.description = "Search the web for current information."
        self.category = "web"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "allowed_domains": {"type": "array", "items": {"type": "string"}},
                "blocked_domains": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["query"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        query = str(arguments.get("query") or "").strip()
        allowed_domains = arguments.get("allowed_domains") or []
        blocked_domains = arguments.get("blocked_domains") or []
        if not query:
            return {"results": []}

        response = http_get("https://duckduckgo.com/html/", params={"q": query}, timeout=DEFAULT_HTTP_TIMEOUT)
        matches = re.findall(
            r'(?is)<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="(?P<url>[^"]+)"[^>]*>(?P<title>.*?)</a>.*?<a[^>]+class="[^"]*result__snippet[^"]*"[^>]*>(?P<snippet>.*?)</a>',
            response.text,
        )
        results: list[dict[str, Any]] = []
        for raw_url, raw_title, raw_snippet in matches:
            url = self._normalize_result_url(raw_url)
            domain = normalized_domain(url)
            if not url or domain in {"duckduckgo.com"} or not domain_allowed(url, allowed_domains, blocked_domains):
                continue
            results.append(
                {
                    "title": strip_html_text(raw_title),
                    "url": url,
                    "snippet": strip_html_text(raw_snippet),
                    "domain": domain,
                }
            )
            if len(results) >= 8:
                break
        return {"query": query, "results": results}

    def _normalize_result_url(self, value: str) -> str:
        parsed = urlparse(value)
        if parsed.netloc.endswith("duckduckgo.com") and parsed.path == "/l/":
            target = parse_qs(parsed.query).get("uddg", [""])[0]
            return unquote(target)
        return value


class WebFetchTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "web_fetch"
        self.description = "Fetch the contents of a web page at a given URL."
        self.category = "web"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The URL to fetch"},
                "html_extraction_method": {"type": "string"},
                "text_content_token_limit": {"type": ["integer", "null"]},
                "allowed_domains": {"type": ["array", "null"], "items": {"type": "string"}},
                "blocked_domains": {"type": ["array", "null"], "items": {"type": "string"}},
                "web_fetch_pdf_extract_text": {"type": ["boolean", "null"]},
                "is_zdr": {"type": ["boolean", "null"]},
                "web_fetch_rate_limit_key": {"type": ["string", "null"]},
                "web_fetch_rate_limit_dark_launch": {"type": ["boolean", "null"]},
            },
            "required": ["url"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        url = str(arguments.get("url") or "").strip()
        allowed_domains = arguments.get("allowed_domains") or []
        blocked_domains = arguments.get("blocked_domains") or []
        if not domain_allowed(url, allowed_domains, blocked_domains):
            raise ValueError("URL domain is not allowed.")

        response = requests.get(
            url,
            headers={"User-Agent": "ERP-AI-Assistant/1.0"},
            timeout=DEFAULT_HTTP_TIMEOUT,
            stream=True,
        )
        response.raise_for_status()
        content_type = str(response.headers.get("content-type") or "").lower()
        body = response.content[:2_000_000]

        if "pdf" in content_type:
            return self._pdf_result(url, body, bool(arguments.get("web_fetch_pdf_extract_text")))

        html_text = body.decode(response.encoding or "utf-8", errors="replace")
        text = strip_html_text(html_text)
        token_limit = arguments.get("text_content_token_limit")
        if isinstance(token_limit, int) and token_limit > 0:
            text = self._trim_tokens(text, token_limit)

        title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_text)
        return {
            "url": url,
            "title": strip_html_text(title_match.group(1)) if title_match else "",
            "content_type": content_type,
            "text": text,
            "status_code": response.status_code,
        }

    def _trim_tokens(self, text: str, token_limit: int) -> str:
        words = text.split()
        if len(words) <= token_limit:
            return text
        return " ".join(words[:token_limit])

    def _pdf_result(self, url: str, body: bytes, extract_text: bool) -> dict[str, Any]:
        text = ""
        if extract_text:
            try:
                from io import BytesIO

                from pypdf import PdfReader

                reader = PdfReader(BytesIO(body))
                text = "\n\n".join((page.extract_text() or "").strip() for page in reader.pages).strip()
            except Exception:
                text = ""
        return {
            "url": url,
            "title": "",
            "content_type": "application/pdf",
            "text": text,
            "binary_size": len(body),
        }


class ImageSearchTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "image_search"
        self.description = "Search for and display images inline in the conversation."
        self.category = "web"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query to find relevant images"},
                "max_results": {"type": "integer", "minimum": 3, "maximum": 5, "default": 3},
            },
            "required": ["query"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        query = str(arguments.get("query") or "").strip()
        limit = int(arguments.get("max_results") or 3)
        limit = max(3, min(limit, 5))
        response = http_get(
            "https://commons.wikimedia.org/w/api.php",
            params={
                "action": "query",
                "format": "json",
                "generator": "search",
                "gsrsearch": query,
                "gsrnamespace": 6,
                "gsrlimit": limit,
                "prop": "imageinfo",
                "iiprop": "url",
                "iiurlwidth": 800,
            },
        )
        payload = response.json()
        pages = (payload.get("query") or {}).get("pages") or {}
        results: list[dict[str, Any]] = []
        for page in pages.values():
            imageinfo = (page.get("imageinfo") or [{}])[0]
            if not imageinfo.get("url"):
                continue
            results.append(
                {
                    "title": str(page.get("title") or "").replace("File:", ""),
                    "image_url": imageinfo.get("thumburl") or imageinfo.get("url"),
                    "source_url": imageinfo.get("descriptionurl") or imageinfo.get("url"),
                }
            )
        return {"query": query, "images": results[:limit]}
