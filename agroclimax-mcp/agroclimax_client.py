from __future__ import annotations

import time
from typing import Any

import httpx


class AgroClimaXApiError(RuntimeError):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class AgroClimaXClient:
    def __init__(self, *, base_url: str, api_key: str, timeout_seconds: float = 20.0, cache_ttl_seconds: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.cache_ttl_seconds = cache_ttl_seconds
        self._response_cache: dict[tuple[str, tuple[tuple[str, str], ...]], tuple[float, dict[str, Any]]] = {}

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _cache_key(self, path: str, params: dict[str, Any] | None) -> tuple[str, tuple[tuple[str, str], ...]]:
        normalized_params = tuple(sorted((str(key), str(value)) for key, value in (params or {}).items()))
        return (path, normalized_params)

    async def _request(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        cache_key = self._cache_key(path, params)
        cached = self._response_cache.get(cache_key)
        now = time.monotonic()
        if cached is not None and now - cached[0] <= self.cache_ttl_seconds:
            return cached[1]

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(f"{self.base_url}{path}", params=params, headers=self._headers())
        if response.status_code >= 400:
            detail: str
            try:
                payload = response.json()
                detail = str(payload.get("detail") or payload)
            except Exception:
                detail = response.text or f"HTTP {response.status_code}"
            raise AgroClimaXApiError(response.status_code, detail)
        payload = response.json()
        self._response_cache[cache_key] = (now, payload)
        return payload

    async def find_field(self, name: str, *, limit: int = 5) -> dict[str, Any]:
        return await self._request("/api/v1/integrations/mcp/fields/search", params={"q": name, "limit": limit})

    async def find_paddock(self, name: str, *, field_id: str | None = None, limit: int = 10) -> dict[str, Any]:
        params: dict[str, Any] = {"q": name, "limit": limit}
        if field_id:
            params["field_id"] = field_id
        return await self._request("/api/v1/integrations/mcp/paddocks/search", params=params)

    async def get_field_current_status(self, field_id: str) -> dict[str, Any]:
        return await self._request(f"/api/v1/integrations/mcp/fields/{field_id}/current-status")

    async def get_field_historical_trend(self, field_id: str, *, days: int = 30) -> dict[str, Any]:
        return await self._request(f"/api/v1/integrations/mcp/fields/{field_id}/historical-trend", params={"days": days})

    async def get_paddock_historical_trend(self, paddock_id: str, *, days: int = 30) -> dict[str, Any]:
        return await self._request(f"/api/v1/integrations/mcp/paddocks/{paddock_id}/historical-trend", params={"days": days})

    async def get_latest_satellite_coverage(self, field_id: str) -> dict[str, Any]:
        return await self._request(f"/api/v1/integrations/mcp/fields/{field_id}/latest-satellite-coverage")

    async def get_active_weather_alerts(self, field_id: str) -> dict[str, Any]:
        return await self._request(f"/api/v1/integrations/mcp/fields/{field_id}/active-weather-alerts")

    async def get_paddock_alert_history(self, paddock_id: str, *, days: int = 30) -> dict[str, Any]:
        return await self._request(f"/api/v1/integrations/mcp/paddocks/{paddock_id}/alert-history", params={"days": days})
