"""The table's data files. Workers write these in parallel.

**These are not scratch.** `sink.commit` registers them with Iceberg's
`add_files`, which records each path and reads its footer for statistics --
it never copies or rewrites a row. So the file a worker writes here *is* the
file the table reads forever after. Deleting one breaks the table.

They therefore live where a reader would look for them, beside the metadata
pyiceberg writes for the same table:

    <warehouse>/<namespace>/<table>/data/<job_id>/<member>.parquet
    <warehouse>/<namespace>/<table>/metadata/...        <- pyiceberg's

Rejects are a table of their own (`<table>_rejects`), so they land under their
own directory rather than in a sibling of the good rows.

The invariant that makes this safe: **every file under a table's `data/` is
either registered in a snapshot or is being written right now.** A job that
fails a gate deletes what it wrote before returning (see `runner.run`), because
at that point the commit has not run and nothing can reference it.
"""

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


def table_dir(warehouse: Path, table_name: str) -> Path:
    """`bronze.people` -> `<warehouse>/bronze/people`, matching pyiceberg."""
    return warehouse.joinpath(*[_safe(p) for p in table_name.split(".")])


def write(df: pl.DataFrame, warehouse: Path, table_name: str, job_id: str, member: str) -> str:
    """Write one member's rows as a data file of `table_name`. Returns the path."""
    out = table_dir(warehouse, table_name) / "data" / job_id
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{_safe(member)}.parquet"
    df.write_parquet(path)
    return str(path)


def discard(paths: list[str]) -> int:
    """Remove data files a job wrote but never committed, and any job directory
    left empty. Only ever called before `sink.commit` has run for the job, so
    nothing can be referencing these -- see the invariant in the module doc."""
    removed = 0
    parents = set()
    for p in paths:
        f = Path(p)
        if f.is_file():
            f.unlink()
            removed += 1
        parents.add(f.parent)
    for d in parents:
        if d.is_dir() and not any(d.iterdir()):
            d.rmdir()
    return removed
