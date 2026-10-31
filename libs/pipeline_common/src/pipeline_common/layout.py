"""Ablage im Lake, die mehrere Services gemeinsam kennen müssen."""

from pipeline_common.periods import Month


def month_prefix(dataset: str, month: Month) -> str:
    """Präfix eines Monats relativ zum Bucket: Ingestion schreibt Bronze dorthin, Job 1 liest."""
    return f"{dataset}/{month.partition}"


def runs_key(dataset: str, run_id: str) -> str:
    """Key des Reports relativ zum Bucket; jeder Schritt legt ihn in seiner Schicht ab."""
    return f"_runs/{dataset}/{run_id}.json"
