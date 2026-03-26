from __future__ import annotations

from typing import Any

from frappe_assistant_core.core.base_tool import BaseTool

from .common import cfg_value, http_get


class WeatherFetchTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "weather_fetch"
        self.description = "Fetch current weather conditions and forecast for a location."
        self.category = "integration"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "location_name": {"type": "string"},
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
            },
            "required": ["latitude", "location_name", "longitude"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        response = http_get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": arguments.get("latitude"),
                "longitude": arguments.get("longitude"),
                "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,weather_code,wind_speed_10m",
                "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
                "timezone": "auto",
                "forecast_days": 3,
            },
        )
        return {"location_name": arguments.get("location_name"), "forecast": response.json()}


class FetchSportsDataTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "fetch_sports_data"
        self.description = "Fetch live scores, standings, and game stats for major sports leagues."
        self.category = "integration"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "league": {"type": "string"},
                "data_type": {"type": "string"},
                "team": {"type": ["string", "null"]},
                "game_id": {"type": ["string", "null"]},
            },
            "required": ["data_type", "league"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        base_url = str(cfg_value("ERP_AI_SPORTS_API_URL", "") or "").strip()
        if not base_url:
            raise ValueError("Sports API is not configured. Set ERP_AI_SPORTS_API_URL.")
        response = http_get(base_url.rstrip("/") + "/sports", params=arguments)
        return response.json()


class PlacesSearchTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "places_search"
        self.description = "Search for places, businesses, restaurants, and attractions using Google Places API."
        self.category = "integration"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "queries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "max_results": {"type": "integer", "default": 5},
                        },
                        "required": ["query"],
                    },
                    "minItems": 1,
                    "maxItems": 10,
                },
                "location_bias_lat": {"type": ["number", "null"]},
                "location_bias_lng": {"type": ["number", "null"]},
                "location_bias_radius": {"type": ["number", "null"]},
            },
            "required": ["queries"],
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        api_key = str(cfg_value("ERP_AI_GOOGLE_MAPS_API_KEY", "") or "").strip()
        if not api_key:
            raise ValueError("Google Places API key is not configured. Set ERP_AI_GOOGLE_MAPS_API_KEY.")

        queries = []
        for query_row in arguments.get("queries") or []:
            params = {"query": query_row.get("query"), "key": api_key}
            lat = arguments.get("location_bias_lat")
            lng = arguments.get("location_bias_lng")
            radius = arguments.get("location_bias_radius")
            if lat is not None and lng is not None:
                params["location"] = f"{lat},{lng}"
                if radius is not None:
                    params["radius"] = radius

            response = http_get("https://maps.googleapis.com/maps/api/place/textsearch/json", params=params)
            payload = response.json()
            limit = max(1, min(int(query_row.get("max_results") or 5), 10))
            queries.append({"query": query_row.get("query"), "results": (payload.get("results") or [])[:limit]})
        return {"queries": queries}


class PlacesMapDisplayTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.name = "places_map_display_v0"
        self.description = "Display locations or multi-day itineraries on an interactive map."
        self.category = "ui"
        self.source_app = "erp_ai_assistant"
        self.inputSchema = {
            "type": "object",
            "properties": {
                "title": {"type": ["string", "null"]},
                "narrative": {"type": ["string", "null"]},
                "mode": {"type": ["string", "null"]},
                "show_route": {"type": ["boolean", "null"]},
                "travel_mode": {"type": ["string", "null"]},
                "locations": {"type": ["array", "null"]},
                "days": {"type": ["array", "null"]},
            },
            "additionalProperties": False,
        }

    def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
        mode = arguments.get("mode") or ("itinerary" if arguments.get("days") else "markers")
        return {"widget": "places_map_display_v0", "mode": mode, "payload": arguments}
