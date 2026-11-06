"""Unified error format: ``{"detail": "<human readable>", "code": "<machine code>"}``."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.deps import DbBusyError

logger = logging.getLogger(__name__)


class ApiError(Exception):
    """Domain error mapped to the unified JSON error body."""

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail


def not_found(detail: str = "not found") -> ApiError:
    return ApiError(404, "not_found", detail)


def no_data(detail: str = "no data") -> ApiError:
    return ApiError(404, "no_data", detail)


def bad_request(detail: str) -> ApiError:
    return ApiError(400, "bad_request", detail)


def _body(status_code: int, code: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail, "code": code})


def _http_code(status_code: int) -> str:
    return {
        404: "not_found",
        409: "conflict",
        503: "unavailable",
    }.get(status_code, "bad_request" if status_code < 500 else "internal_error")


def register_error_handlers(app: FastAPI) -> None:
    """Install the exception handlers on ``app``."""

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return _body(exc.status_code, exc.code, exc.detail)

    @app.exception_handler(DbBusyError)
    async def _db_busy(_: Request, exc: DbBusyError) -> JSONResponse:
        return _body(503, "db_busy", str(exc) or "database is busy (sync in progress)")

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return _body(exc.status_code, _http_code(exc.status_code), str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0] if exc.errors() else {}
        loc = ".".join(str(p) for p in first.get("loc", ()) if p != "query")
        return _body(422, "bad_request", f"{loc}: {first.get('msg', 'invalid request')}".strip(": "))

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error: %s", exc)
        return _body(500, "internal_error", "internal server error")
