FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN pip install uv

# ── Production builder ────────────────────────────────────────────────────────
FROM base AS builder

COPY pyproject.toml .
RUN uv pip install --system .

COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini .

# ── Dev/test builder (adds test deps) ────────────────────────────────────────
FROM builder AS test-builder

RUN uv pip install --system ".[dev]"

COPY tests/ tests/

# ── Production final ──────────────────────────────────────────────────────────
FROM base AS final

COPY --from=builder /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

WORKDIR /app

COPY scripts/ scripts/
RUN chmod +x scripts/*.sh 2>/dev/null || true

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m"]
