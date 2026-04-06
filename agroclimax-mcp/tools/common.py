from __future__ import annotations

from typing import Annotated, Any

from mcp.types import CallToolResult, TextContent


ToolPayloadResult = Annotated[CallToolResult, dict[str, Any]]


def tool_result(*, text: str, payload: dict) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structuredContent=payload,
        isError=False,
    )
