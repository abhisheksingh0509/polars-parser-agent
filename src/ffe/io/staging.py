"""Scrap paper. Workers write here in parallel; the commit step files it."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl


def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def add_lineage(df: pl.DataFrame, member: str, job_id: str, spec_hash: str) -> pl.DataFrame:
    """Every row carries where it came from. _src_line_no is added by the parser."""
    return df.with_columns(
        pl.lit(member, pl.Utf8).alias("_src_file"),
        pl.lit(job_id, pl.Utf8).alias("_job_id"),
        pl.lit(spec_hash, pl.Utf8).alias("_spec_hash"),
        pl.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast(
            pl.Datetime("us")
        ).alias("_ingested_at"),
    )


def write(df: pl.DataFrame, root: Path, job_id: str, kind: str, member: str) -> str:
    out = root / job_id / kind
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{_safe(member)}.parquet"
    df.write_parquet(path)
    return str(path)
