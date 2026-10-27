FROM docker.io/library/python:3.12.16-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-cache

# Ohne Root-Rechte laufen (Least Privilege).
RUN useradd --system --uid 10001 ingestion
USER ingestion
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "ingestion"]
