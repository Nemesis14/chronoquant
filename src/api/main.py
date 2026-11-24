"""ChronoQuant BFF: FastAPI app, routers, static frontend + SPA fallback.

Run (repo root)::

    uv run python -m uvicorn api.main:app --app-dir src --host 127.0.0.1 --port 8600
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import utils
from api.errors import ApiError, register_error_handlers
from api.routers import (
    chat,
    equity,
    health,
    logs,
    market,
    predictions,
    registry,
    scout,
    strat,
    trades,
)
from cq_mcp.server import mcp_lifespan, mcp_route

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEV_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]

# Routers owned by other tasks; included only when their module exists (keeps the import safe).
OPTIONAL_ROUTERS = ("jobs", "reports")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    logger.info("ChronoQuant API starting")
    async with mcp_lifespan():
        yield
    logger.info("ChronoQuant API stopped")


def _include_optional_routers(app: FastAPI) -> None:
    for name in OPTIONAL_ROUTERS:
        module_name = f"api.routers.{name}"
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise  # the router exists but one of its own imports is broken
            logger.info("optional router %s not present, skipping", name)
            continue
        app.include_router(module.router)


def _mount_frontend(app: FastAPI, dist: Path) -> None:
    """Serve the built SPA (``frontend/dist``) with an index.html fallback.

    Registered AFTER every API / MCP route.  Unknown ``/api/*`` paths answer a JSON 404,
    never ``index.html``.
    """
    if (dist / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")
    dist_root = dist.resolve()

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str, request: Request) -> FileResponse:
        if full_path == "api" or full_path.startswith("api/"):
            raise ApiError(404, "not_found", "Not Found")
        candidate = (dist / full_path).resolve()
        if full_path and candidate.is_file() and dist_root in candidate.parents:
            return FileResponse(candidate)
        # no-cache (t948): the browser must revalidate the shell so a fresh `npm run build`
        # (new hashed /assets) is picked up on navigation, not only on reload
        return FileResponse(dist / "index.html", headers={"Cache-Control": "no-cache"})


def create_app(frontend_dist: Path | None = None) -> FastAPI:
    """Build the application (factory so tests can point at a temporary dist)."""
    app = FastAPI(title="ChronoQuant API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=DEV_ORIGINS, allow_methods=["*"], allow_headers=["*"]
    )
    register_error_handlers(app)

    for router_module in (health, market, predictions, scout, trades, equity, strat, registry, logs, chat):
        app.include_router(router_module.router)
    _include_optional_routers(app)
    # /mcp/chronoquant: route on the app router (no Mount => no 307 slash redirect), BEFORE the SPA fallback
    app.router.routes.append(mcp_route())

    dist = frontend_dist if frontend_dist is not None else Path(utils.repo_root()) / "frontend" / "dist"
    if (dist / "index.html").is_file():
        _mount_frontend(app, dist)
    else:
        @app.get("/api/{path:path}", include_in_schema=False)
        def api_not_found(path: str) -> None:
            raise ApiError(404, "not_found", "Not Found")

    return app


app = create_app()


if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
