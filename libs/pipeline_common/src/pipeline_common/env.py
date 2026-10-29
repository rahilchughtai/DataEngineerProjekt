"""Konfiguration aus Umgebungsvariablen."""

import os


class ConfigError(ValueError):
    pass


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Umgebungsvariable {name} fehlt (siehe .env.example)")
    return value


def optional(name: str, default: str) -> str:
    return os.environ.get(name, default)
