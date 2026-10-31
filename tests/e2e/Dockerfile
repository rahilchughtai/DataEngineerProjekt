FROM docker.io/library/python:3.12.16-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /usr/local/bin/uv

WORKDIR /app
COPY tests/e2e/pyproject.toml tests/e2e/uv.lock ./
RUN uv sync --frozen --no-dev --no-cache
COPY tests/e2e/*.py ./
COPY tests/e2e/fixtures ./fixtures
COPY seaweedfs/s3.template.json ./

RUN useradd --system --uid 10001 e2e
USER e2e
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
ENTRYPOINT ["pytest", "-v"]
