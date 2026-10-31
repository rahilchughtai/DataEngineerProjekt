"""Fixtures des End-to-End-Tests.

Der Test läuft als Container e2e-tests am Ende der Kette aus docker-compose.test.yml. Compose
startet ihn erst, wenn alle Schritte erfolgreich beendet sind; er liest danach nur noch den Lake.
"""

import os
from pathlib import Path

import pyarrow as pa
import pytest

from lake import Lake, Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture(scope="session")
def lake(settings: Settings) -> Lake:
    return Lake(settings)


@pytest.fixture(scope="session")
def run_id() -> str:
    """Die Run-ID, die run-init für diesen Lauf ins gemeinsame Volume geschrieben hat."""
    return Path(os.environ["PIPELINE_RUN_ID_FILE"]).read_text(encoding="utf-8").strip()


@pytest.fixture(scope="session")
def tables(lake: Lake, settings: Settings) -> dict[str, dict[str, pa.Table]]:
    """Schicht → Tabelle → Zeilen; Bronze prüft der Test byte-genau gegen die Quelldateien."""
    return {layer: lake.tables(settings.buckets[layer]) for layer in ("silver", "gold")}
