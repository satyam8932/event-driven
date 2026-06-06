from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request

from app.api.routes import health, jobs
from app.db.engine import close_engine
from app.infra.broker import close_connection
from app.infra.redis import close_redis
from app.logging import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    configure_logging()
    yield
    await close_engine()
    await close_redis()
    await close_connection()


app = FastAPI(title="GenAI Pipeline API", lifespan=lifespan)
app.include_router(jobs.router)
app.include_router(health.router)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, _exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


if __name__ == "__main__":
    uvicorn.run("app.api.main:app", host="0.0.0.0", port=8000, reload=False)
