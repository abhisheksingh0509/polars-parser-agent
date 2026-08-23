"""Eight verbs. Every one emits JSON. This is the whole AI story: no agent lives
here, but any agent can drive it.

Exit codes are part of the contract:
  0  ok
  2  your spec is wrong   (fix the YAML and retry)
  3  the file is wrong    (truncated, corrupt -- don't retry, go ask a human)
  1  we crashed           (a bug in ffe)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer

from .core import plugins
from .core.engine import parse
from .core.profile import profile as run_profile
from .core.report import ParseError
from .core.spec import FeedSpec

app = typer.Typer(add_completion=False, help="Flat-file ingestion engine.")

ROOT = Path.cwd()
EXIT_SPEC, EXIT_FILE, EXIT_BUG = 2, 3, 1


def _emit(payload: dict, code: int = 0) -> None:
    typer.echo(json.dumps(payload, indent=2, default=str))
    if code:
        raise typer.Exit(code)


def _fail(exc: ParseError) -> None:
    _emit(
        {"ok": False, **exc.to_dict()},
        EXIT_FILE if exc.blame == "file" else EXIT_SPEC,
    )


def _options(pairs: list[str] | None) -> dict:
    """`--option business_date=2026-08-23` -> {"business_date": "2026-08-23"}.

    Values stay strings. The framework has no opinion on what a parser wants;
    interpreting them is the plugin's job, same as `parser.options` in the YAML.
    """
    out: dict[str, str] = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key.strip():
            _emit(
                {
                    "ok": False,
                    "error": "bad_option",
                    "message": f"--option expects key=value, got {pair!r}",
                    "field": "--option",
                    "hint": "Example: --option business_date=2026-08-23",
                    "blame": "spec",
                },
                EXIT_SPEC,
            )
        out[key.strip()] = value
    return out


def _load(spec_path: Path) -> FeedSpec:
    try:
        return FeedSpec.from_yaml(spec_path)
    except Exception as exc:
        _emit(
            {
                "ok": False,
                "error": "invalid_spec",
                "message": str(exc),
                "hint": "The YAML does not match the FeedSpec schema. "
                "Run `ffe schema` to see the accepted shape.",
                "blame": "spec",
            },
            EXIT_SPEC,
        )


# --------------------------------------------------------------------------- #


@app.command()
def profile(sample: Path, cap: int = 100):
    """Measure a sample file's structure. Deterministic, no model involved."""
    _emit({"ok": True, "profile": run_profile(sample.read_bytes(), cap=cap)})


@app.command(name="dry-run")
def dry_run(
    spec: Path,
    sample: Path,
    head: int = 20,
    plugin_dir: Optional[Path] = typer.Option(None, help="directory of parser plugins"),
    option: list[str] = typer.Option(
        [], "--option", help="per-run parser input, key=value; repeatable"
    ),
):
    """Parse a sample and report. Writes nothing, anywhere, ever."""
    feed = _load(spec)
    plugins.load_dir(plugin_dir or ROOT / "plugins")
    # Parsed before the try: _emit raises typer.Exit to set the exit code, and
    # the `except Exception` below would otherwise swallow it into a bug report.
    ctx = plugins.ParseContext(member=sample.name, options=_options(option))
    try:
        result = parse(feed.parser, sample.read_bytes(), ctx)
    except ParseError as exc:
        _fail(exc)
        return
    except Exception as exc:
        _emit(
            {"ok": False, "error": "unhandled", "message": f"{type(exc).__name__}: {exc}"},
            EXIT_BUG,
        )
        return

    report = result.report
    payload = {
        "ok": True,
        "gates": {
            "ragged_lines": report.ragged_lines,
            "unknown_tags": report.unknown_tags,
            "reject_ratio": round(report.reject_ratio, 4),
            "all_null_columns": [
                c.name
                for c in report.columns
                if report.rows_parsed and c.null_count == report.rows_parsed
            ],
            # the trailer is free, exact ground truth when the file declares it
            "trailer_row_count": (
                "n/a"
                if report.trailer_declared_rows is None
                else "match"
                if report.trailer_declared_rows
                == report.rows_parsed + report.rows_rejected
                else f"MISMATCH: declared {report.trailer_declared_rows}, "
                f"parsed {report.rows_parsed + report.rows_rejected}"
            ),
            "passed": report.ragged_lines == 0
            and report.unknown_tags == 0
            and report.reject_ratio <= feed.policy.max_reject_ratio
            and (
                report.trailer_declared_rows is None
                or report.trailer_declared_rows
                == report.rows_parsed + report.rows_rejected
            ),
        },
        "report": report.to_dict(),
        "schema": {c.name: c.dtype for c in report.columns},
        "head": json.loads(result.frame.head(head).write_json()),
        "rejects": json.loads(result.rejects.head(head).write_json())
        if result.rejects is not None and len(result.rejects)
        else [],
    }
    _emit(payload)


@app.command()
def lint(spec: Path):
    """Validate a feed spec without touching any file."""
    feed = _load(spec)
    _emit(
        {
            "ok": True,
            "name": feed.name,
            "parser": feed.parser.kind,
            "target": feed.target.table,
            "spec_hash": feed.hash(),
        }
    )


@app.command(name="new-parser")
def new_parser(ref: str):
    """Scaffold a plugin, its test, and its feed spec, all correctly shaped."""
    from .scaffold import new_parser as scaffold

    written = scaffold(ref, ROOT)
    _emit(
        {
            "ok": True,
            "written": written,
            "next": [
                f"drop a <=100 row sample at {written['fixture']}",
                f"implement parse() in {written['plugin']}",
                f"ffe dry-run {written['feed']} {written['fixture']}",
            ],
        }
    )


@app.command()
def run(
    spec: Path,
    workspace: Path = typer.Option(Path("./_ffe"), help="warehouse (data + metadata), ledger"),
    table: Optional[str] = typer.Option(
        None, help="override target.table, e.g. sandbox.my_test"
    ),
    source: Optional[str] = typer.Option(
        None, help="override source.pattern, e.g. C:/drops/*.zip"
    ),
    workers: Optional[int] = None,
    executor: str = typer.Option("thread", help="serial | thread | process"),
    option: list[str] = typer.Option(
        [], "--option", help="per-run parser input, key=value; repeatable"
    ),
):
    """The real ingestion: fan out over members, then one Iceberg commit."""
    from .io.runner import run as run_job

    feed = _load(spec)
    if table:
        feed.target.table = table
    if source:
        feed.source.pattern = source
    plugins.load_dir(ROOT / "plugins")
    result = run_job(feed, workspace, plugin_dir=str(ROOT / "plugins"),
                     workers=workers, executor=executor, options=_options(option))
    _emit(
        {"ok": result.status in ("ok", "partial"), **result.to_dict()},
        0 if result.status in ("ok", "partial") else EXIT_FILE,
    )


@app.command()
def explain(spec: Path, workspace: Path = Path("./_ffe")):
    """What this feed does, plus how its last few runs went."""
    from .io.ledger import Ledger
    from .io.source import resolve

    feed = _load(spec)
    members = resolve(feed.source.pattern, feed.source.members)
    history = []
    if (Path(workspace) / "ledger.db").exists():
        history = Ledger(Path(workspace) / "ledger.db").history(feed.name)
    _emit(
        {
            "ok": True,
            "name": feed.name,
            "parser": feed.parser.kind,
            "source": feed.source.pattern,
            "target": feed.target.table,
            "policy": feed.policy.model_dump(),
            "members_matched": len(members),
            "members_sample": [m.label for m in members[:10]],
            "recent_jobs": history,
        }
    )


@app.command()
def tables(workspace: Path = typer.Option(Path("./_ffe"), help="which warehouse")):
    """What is in the warehouse: tables, row counts, snapshots."""
    from .io.sink import catalog

    warehouse = Path(workspace) / "warehouse"
    if not (warehouse / "catalog.db").exists():
        _emit(
            {
                "ok": True,
                "warehouse": str(warehouse),
                "tables": [],
                "note": "no warehouse here yet -- run a feed first",
            }
        )
        return

    cat = catalog(warehouse)
    found = []
    for (namespace,) in cat.list_namespaces():
        for _, name in cat.list_tables(namespace):
            tbl = cat.load_table(f"{namespace}.{name}")
            snap = tbl.current_snapshot()
            found.append(
                {
                    "table": f"{namespace}.{name}",
                    "rows": int(snap.summary.get("total-records", 0)) if snap else 0,
                    "files": int(snap.summary.get("total-data-files", 0)) if snap else 0,
                    "snapshots": len(tbl.metadata.snapshots),
                    "columns": [f.name for f in tbl.schema().fields],
                }
            )
    _emit({"ok": True, "warehouse": str(warehouse), "tables": found})


@app.command()
def schema():
    """The FeedSpec JSON schema. For a human or an agent authoring a spec."""
    _emit({"ok": True, "schema": FeedSpec.model_json_schema()})


def main() -> None:  # pragma: no cover
    try:
        app()
    except SystemExit:
        raise
    except Exception as exc:
        json.dump({"ok": False, "error": "unhandled", "message": str(exc)}, sys.stdout)
        sys.exit(EXIT_BUG)


if __name__ == "__main__":  # pragma: no cover
    main()
