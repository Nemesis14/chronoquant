"""chronoquant MCP server (FastMCP, streamable HTTP, in-process in the API).

Mounted at ``/mcp/chronoquant`` by ``api.main``; also runnable over stdio::

    uv run python -m cq_mcp.server
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import anyio.to_thread
from mcp.server.fastmcp import FastMCP
from starlette.routing import Route

from cq_mcp import tools_data, tools_jobs, tools_models, tools_reports, tools_sentiment

SERVER_NAME = "chronoquant"
MOUNT_PATH = "/mcp/chronoquant"

INSTRUCTIONS = (
    "ChronoQuant trading research data. All tools only read, except `enqueue_sync` and `sentiment_refresh` (they start worker jobs). "
    "Prefer the specific tools over `query_sql`; DuckDB aliases for SQL: live. lab. reg."
)

TOOLS: tuple[Callable[..., str], ...] = (
    tools_models.get_status, tools_data.list_tables, tools_data.query_sql, tools_data.get_candles,
    tools_data.get_predictions, tools_data.get_latest_signal, tools_models.get_active_strategy,
    tools_models.get_trades, tools_models.get_equity, tools_models.list_models, tools_models.get_model,
    tools_models.get_registry, tools_reports.list_reports, tools_reports.read_report,
    tools_jobs.list_jobs, tools_jobs.enqueue_sync, tools_sentiment.sentiment_latest,
    tools_sentiment.sentiment_refresh,
)


def _threaded(fn: Callable[..., str]) -> Callable[..., Any]:
    """Async wrapper running the blocking tool in a worker thread (keeps the event loop free)."""

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> str:
        return await anyio.to_thread.run_sync(functools.partial(fn, *args, **kwargs))

    return wrapper


def build_server() -> FastMCP:
    """Create a fresh FastMCP instance with every tool registered."""
    server = FastMCP(SERVER_NAME, instructions=INSTRUCTIONS, streamable_http_path=MOUNT_PATH)
    for fn in TOOLS:
        server.add_tool(_threaded(fn), name=fn.__name__, description=inspect.getdoc(fn))
    return server


MCP_SERVER = build_server()


class _McpEndpoint:
    """ASGI endpoint resolving the (lifespan-rebuilt) session manager at call time."""

    def __init__(self, server: FastMCP) -> None:
        self.server = server

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        await self.server.session_manager.handle_request(scope, receive, send)


def mcp_route() -> Route:
    """Route for ``/mcp/chronoquant`` (register on the app router BEFORE the SPA fallback)."""
    return Route(MOUNT_PATH, endpoint=_McpEndpoint(MCP_SERVER))


@asynccontextmanager
async def mcp_lifespan() -> AsyncIterator[None]:
    """Start the streamable-HTTP session manager (run() is once per instance: rebuild each time)."""
    MCP_SERVER._session_manager = None  # noqa: SLF001
    MCP_SERVER.streamable_http_app()
    async with MCP_SERVER.session_manager.run():
        yield


if __name__ == "__main__":
    MCP_SERVER.run()
