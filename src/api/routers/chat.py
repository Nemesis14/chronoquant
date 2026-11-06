"""Chat: POST /api/chat (SSE over the headless local claude CLI) + GET /api/chat/config.

Session state lives in the claude CLI (``--resume <id>``); the API stores no conversation.
Client disconnect (Stop button / AbortController) cancels the generator, which kills the
claude process tree.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from api.chat import runner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])

MAX_CONCURRENT = 2
SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
_slots = threading.BoundedSemaphore(MAX_CONCURRENT)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=8000)
    session_id: str | None = Field(default=None, max_length=128)


def _error(status: int, detail: str, code: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"detail": detail, "code": code})


async def _events(message: str, session_id: str | None) -> AsyncIterator[str]:
    cancel = threading.Event()
    try:
        async for event, data in runner.stream_chat_async(message, session_id, cancel):
            yield runner.sse(event, data)
    finally:
        cancel.set()
        _slots.release()


@router.post("", response_model=None)
async def chat(req: ChatRequest) -> Any:
    """Body ``{message, session_id|null}`` -> SSE: session, delta, thinking, tool, done, error."""
    try:
        runner.resolve_claude_bin()
    except runner.ChatError as exc:
        return _error(503, exc.message, exc.code)
    if not _slots.acquire(blocking=False):
        return _error(429, f"At most {MAX_CONCURRENT} chats may run at once; try again shortly.", "busy")
    return StreamingResponse(
        _events(req.message, req.session_id), media_type="text/event-stream", headers=SSE_HEADERS
    )


@router.get("/config")
def chat_config() -> dict[str, Any]:
    """Header / diagnostics data for the chat UI."""
    cfg = runner.chat_config()
    try:
        binary_ok = bool(runner.resolve_claude_bin())
    except runner.ChatError:
        binary_ok = False
    return {
        "model": cfg["model"],
        "timeout_s": cfg["timeout_s"],
        "tools": list(runner.ALLOWED_TOOLS),
        "mcp_ok": True,
        "binary_ok": binary_ok,
        "max_concurrent": MAX_CONCURRENT,
    }
