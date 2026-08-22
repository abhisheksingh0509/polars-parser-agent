"""The Iceberg commit. One writer, one snapshot, no data rewritten.

Workers wrote Parquet in parallel. This runs single-threaded at the end and
registers those files with `add_files`, which reads the Parquet footers for
statistics rather than copying any rows.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog


def catalog(warehouse: Path) -> SqlCatalog:
    """Local SQLite catalog + filesystem warehouse.

    Paths go through as_posix()/as_uri() rather than str(): on Windows a plain
    str() yields backslashes and a bare drive letter, which neither SQLAlchemy
    nor pyiceberg's FileIO will parse. Both forms are identical on POSIX.
    """
    warehouse = warehouse.resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "ffe",
        **{
            "uri": f"sqlite:///{(warehouse / 'catalog.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
        },
    )


def commit(warehouse: Path, table_name: str, parquet_paths: list[str]) -> dict:
    """Register staged Parquet into an Iceberg table as a single snapshot."""
    if not parquet_paths:
        return {"table": table_name, "files": 0, "snapshot_id": None, "rows": 0}

    schemas = {pq.read_schema(p) for p in parquet_paths}
    if len(schemas) > 1:
        raise RuntimeError(
            f"staged files for {table_name} do not share one schema "
            f"({len(schemas)} distinct). All members of a feed must produce the "
            f"same columns and types before they can be committed together."
        )
    arrow_schema = schemas.pop()

    cat = catalog(warehouse)
    namespace, _, short = table_name.rpartition(".")
    namespace = namespace or "default"
    cat.create_namespace_if_not_exists(namespace)

    table = cat.create_table_if_not_exists(
        identifier=f"{namespace}.{short}", schema=arrow_schema
    )
    table.add_files([Path(p).resolve().as_uri() for p in parquet_paths])
    table.refresh()

    snap = table.current_snapshot()
    return {
        "table": f"{namespace}.{short}",
        "files": len(parquet_paths),
        "snapshot_id": snap.snapshot_id if snap else None,
        "rows": int(snap.summary.get("total-records", 0)) if snap else 0,
    }


def scan(warehouse: Path, table_name: str):
    """Read a table back, for verification."""
    import polars as pl

    cat = catalog(warehouse)
    namespace, _, short = table_name.rpartition(".")
    table = cat.load_table(f"{namespace or 'default'}.{short}")
    return pl.from_arrow(table.scan().to_arrow())
