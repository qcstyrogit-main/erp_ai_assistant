from typing import Any

from .provider_settings import get_active_provider
from .resource_registry import get_resource_catalog_summary
from .tool_registry import get_tool_catalog_summary


def get_provider_descriptor() -> dict[str, Any]:
    """Minimal provider abstraction hook for future Ollama/FAC/MCP integration."""
    provider = get_active_provider()
    tool_catalog = get_tool_catalog_summary()
    resource_catalog = get_resource_catalog_summary()
    return {
        "provider": provider,
        "supports_tools": provider in {"openai", "openai_compatible", "anthropic"},
        "supports_deterministic_router": True,
        "supports_internal_tool_registry": True,
        "supports_internal_resource_registry": True,
        "tool_count": tool_catalog.get("count", 0),
        "tool_categories": tool_catalog.get("categories", {}),
        "resource_count": resource_catalog.get("count", 0),
    }
