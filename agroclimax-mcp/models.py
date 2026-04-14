from __future__ import annotations

from pydantic import BaseModel


class ToolEnvelope(BaseModel):
    title: str
    summary_text: str
    data: dict
