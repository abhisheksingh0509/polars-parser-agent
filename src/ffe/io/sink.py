"""The Iceberg commit. One writer, one snapshot, no data rewritten.

Workers wrote the table's data files in parallel. This runs single-threaded at
the end and registers them with `add_files`, which reads their Parquet footers
for statistics rather than copying any rows -- so those files, where they lie,
become the table. See `datafiles` for what that means for their lifetime.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform

from ..core.report import ParseError


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


def _drift(table_name: str, message: str, observed: dict) -> ParseError:
    """Schema drift is a spec decision, not a crash.

    Raised rather than returned so it cannot be ignored, and shaped like every
    other error in the tool: the caller reads `field` and `hint` to know that
    the fix is one line of policy, not a code change.
    """
    return ParseError(
        code="schema_drift",
        message=f"{table_name}: {message}",
        field="policy.schema_change",
        observed=observed,
        hint="Set policy.schema_change to 'evolve' to add the new columns as "
        "nullable. Evolution only ever ADDS columns -- a changed type is "
        "still a hard failure, and re-typing must be done deliberately.",
        blame="spec",
    )


def _unify(table_name: str, schemas: list[pa.Schema], evolve: bool) -> pa.Schema:
    """One arrow schema for a set of data files, or a structured refusal."""
    distinct = list({s: None for s in schemas})
    if len(distinct) == 1:
        return distinct[0]

    names = [{f.name for f in s} for s in distinct]
    added = sorted(set().union(*names) - set.intersection(*names))
    observed = {"distinct_schemas": len(distinct), "columns_not_in_every_file": added}

    if not evolve:
        raise _drift(
            table_name,
            f"the job's data files do not share one schema ({len(distinct)} distinct). "
            f"Columns missing from at least one file: {added or 'none -- types differ'}",
            observed,
        )
    try:
        # promote_options="default" would silently widen int->double and the
        # like. Left strict on purpose: adding a column is safe, retyping one
        # rewrites the meaning of data already committed.
        return pa.unify_schemas(distinct)
    except pa.ArrowException as exc:  # ArrowTypeError on a conflict, not ArrowInvalid
        raise _drift(
            table_name,
            f"data files disagree on a column TYPE, which evolution will not "
            f"resolve: {exc}",
            observed,
        ) from exc


def _partition(table, table_name: str, columns: list[str]) -> None:
    """Give a table an identity partition spec, or refuse for a stated reason.

    Only ever applied to a table with no snapshot. Iceberg can evolve a spec on
    a populated table, but the existing files keep the old one, so the result is
    a table partitioned two ways -- correct, and confusing enough that it should
    be a deliberate migration rather than a side effect of a load.
    """
    known = {f.name for f in table.schema().fields}
    missing = [c for c in columns if c not in known]
    if missing:
        raise ParseError(
            code="partition_column_missing",
            message=f"{table_name}: cannot partition by {missing}; the parser "
            f"does not produce {'that column' if len(missing) == 1 else 'those columns'}",
            field="target.partition_by",
            observed={"requested": columns, "available": sorted(known)},
            hint="Name a column the parser actually emits. `ffe dry-run` prints "
            "the full schema.",
            blame="spec",
        )

    by_id = {f.field_id: f.name for f in table.schema().fields}
    current = [by_id.get(f.source_id) for f in table.spec().fields]
    if current == columns:
        return  # already partitioned exactly as asked

    if table.current_snapshot() is not None:
        raise ParseError(
            code="partition_spec_conflict",
            message=f"{table_name}: already holds data partitioned by "
            f"{current or '(nothing)'}, which does not match the requested {columns}",
            field="target.partition_by",
            observed={"table_partitioned_by": current, "requested": columns},
            hint="An existing table's layout is not changed by a load. Write to a "
            "new table, or migrate the existing one deliberately.",
            blame="spec",
        )

    with table.update_spec() as update:
        for column in columns:
            update.add_field(column, IdentityTransform(), column)
    table.refresh()


def commit(
    warehouse: Path,
    table_name: str,
    parquet_paths: list[str],
    schema_change: str = "fail",
    partition_by: list[str] | None = None,
) -> dict:
    """Register a job's data files into an Iceberg table as a single snapshot."""
    if not parquet_paths:
        return {"table": table_name, "files": 0, "snapshot_id": None, "rows": 0}

    evolve = schema_change == "evolve"
    arrow_schema = _unify(
        table_name, [pq.read_schema(p) for p in parquet_paths], evolve
    )

    cat = catalog(warehouse)
    namespace, _, short = table_name.rpartition(".")
    namespace = namespace or "default"
    cat.create_namespace_if_not_exists(namespace)

    table = cat.create_table_if_not_exists(
        identifier=f"{namespace}.{short}", schema=arrow_schema
    )

    # Drift across JOBS, not just within one: the table already exists and the
    # feed has since grown a column. add_files refuses a file wider than the
    # table, so widen the table first when policy allows it. A file NARROWER
    # than the table is always fine -- Iceberg reads the absent column as null.
    if evolve:
        with table.update_schema() as update:
            update.union_by_name(arrow_schema)
        table.refresh()

    if partition_by:
        _partition(table, table_name, partition_by)

    try:
        table.add_files([Path(p).resolve().as_uri() for p in parquet_paths])
    except ValueError as exc:
        if "more than one partition value" in str(exc):
            raise ParseError(
                code="mixed_partition_file",
                message=f"{table_name}: one data file spans several partition "
                f"values, which add_files cannot register: {exc}",
                field="target.partition_by",
                observed={"partition_by": partition_by or []},
                hint="Staging writes one file per member, so this means a single "
                "member mixes partition values -- for a date, an archive holding "
                "more than one business day. Either split the source, or "
                "partition on something constant within a member.",
                blame="file",
            ) from exc
        if "more columns" not in str(exc):
            raise
        raise _drift(
            table_name,
            f"the job's data files carry columns the existing table does not have: {exc}",
            {"table_columns": [f.name for f in table.schema().fields]},
        ) from exc
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
