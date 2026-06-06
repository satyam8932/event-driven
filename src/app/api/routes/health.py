from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import HealthResponse
from app.db.engine import get_engine
from app.infra.redis import get_redis
from app.logging import get_logger

log = get_logger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    services: dict[str, str] = {}

    try:
        async with get_engine().connect() as conn:
            await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        services["postgres"] = "ok"
    except Exception:
        services["postgres"] = "error"

    try:
        await get_redis().ping()
        services["redis"] = "ok"
    except Exception:
        services["redis"] = "error"

    overall = "ok" if all(v == "ok" for v in services.values()) else "degraded"
    return HealthResponse(status=overall, services=services)
