FROM docker.io/library/python:3.12.16-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /usr/local/bin/uv

# Build context is the repo root so the shared library (libs/pipeline_common) ends up in the
# image. The layout matches the repo because uv.lock refers to ../../libs/pipeline_common.
WORKDIR /app
COPY libs/pipeline_common/pyproject.toml ./libs/pipeline_common/
COPY libs/pipeline_common/src ./libs/pipeline_common/src
WORKDIR /app/services/ingestion
COPY services/ingestion/pyproject.toml services/ingestion/uv.lock ./
RUN uv sync --frozen --no-dev --no-cache
COPY services/ingestion/ingest.py ./

# Run without root privileges (least privilege).
RUN useradd --system --uid 10001 ingestion
USER ingestion
ENV PATH=/app/services/ingestion/.venv/bin:$PATH PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "ingest.py"]
