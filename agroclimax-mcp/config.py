from __future__ import annotations

from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class MCPSettings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    agroclimax_api_url: str = "http://127.0.0.1:8000"
    agroclimax_api_key: str = ""
    mcp_server_port: int = 8090
    mcp_client_bearer_tokens: Annotated[list[str], NoDecode] = Field(default_factory=list)
    mcp_allowed_hosts: Annotated[list[str], NoDecode] = Field(default_factory=list)

    @field_validator("mcp_client_bearer_tokens", mode="before")
    @classmethod
    def parse_client_tokens(cls, value: object) -> list[str]:
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [part.strip() for part in str(value).split(",") if part.strip()]

    @field_validator("mcp_allowed_hosts", mode="before")
    @classmethod
    def parse_allowed_hosts(cls, value: object) -> list[str]:
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [part.strip() for part in str(value).split(",") if part.strip()]


settings = MCPSettings()
