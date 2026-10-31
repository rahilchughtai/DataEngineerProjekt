FROM docker.io/library/python:3.12.16-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /usr/local/bin/uv

# Build-Kontext ist das Repo-Root, damit die gemeinsame Bibliothek (libs/pipeline_common) ins Image
# kommt. Das Layout bleibt wie im Repo, weil uv.lock den Pfad ../../libs/pipeline_common kennt.
WORKDIR /app
COPY libs/pipeline_common/pyproject.toml ./libs/pipeline_common/
COPY libs/pipeline_common/src ./libs/pipeline_common/src
WORKDIR /app/services/ingestion
COPY services/ingestion/pyproject.toml services/ingestion/uv.lock ./
RUN uv sync --frozen --no-dev --no-cache
COPY services/ingestion/ingest.py services/ingestion/init-buckets.py ./

# Ohne Root-Rechte laufen (Least Privilege).
RUN useradd --system --uid 10001 ingestion
USER ingestion
ENV PATH=/app/services/ingestion/.venv/bin:$PATH PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "ingest.py"]
