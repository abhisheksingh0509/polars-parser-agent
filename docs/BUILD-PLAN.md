# Build plan

For picking this up on another machine — in particular a Windows machine with
GitHub Copilot. Read [`DESIGN.md`](DESIGN.md) for *why*; this is *what to do*, in
order, with a verifiable exit criterion for each step.

Agent instructions live in
[`../.github/copilot-instructions.md`](../.github/copilot-instructions.md) and
Copilot reads them automatically. Nothing below repeats them.

Commands are PowerShell. On macOS/Linux swap `.venv\Scripts\python.exe` for
`.venv/bin/python`.

---

## Part 1 — get it green on Windows

Do this first and do not skip the verification. Everything else assumes it.

```powershell
uv venv --python 3.12
uv pip install -e ".[dev]"
.venv\Scripts\python.exe -m pytest -q          # expect: 30 passed
.venv\Scripts\python.exe scripts\smoke.py      # expect: 19/19, "smoke test OK"
```

**Exit criterion: 30 tests and 19 smoke checks pass on Windows.**

Known Windows failure modes, all already handled — if one reappears, it is a
regression, not a new problem:

| Symptom | Cause | Where |
|---|---|---|
| `Could not parse SQLAlchemy URL` / `unsupported scheme` on commit | a Windows path passed as `str()` — backslashes and a bare drive letter | `io/sink.py` uses `as_posix()` / `as_uri()` |
| A fixture parses differently than on macOS | git rewrote CRLF in a fixture | `.gitattributes` marks `tests/fixtures/**` binary |
| `UnicodeEncodeError` writing output | cp1252 console | CLI emits ASCII-escaped JSON |
| `--executor process` hangs or re-runs the CLI | spawn re-imports the worker | `_process` is module level in `io/runner.py` — keep it there |
| pyarrow / pyiceberg wheel won't install | Python 3.13+ | pin 3.12 |

If tests fail for any *other* reason, fix the code, add a test that pins it, and
note it in the table above.

---

## Part 2 — migrate the real in-house parsers

This is the actual office work, and the reason the framework exists: your team
already writes a parser per reference feed. Keep doing that. Stop rewriting the
storage and processing layer around each one.

### Per parser

1. **Get a sample.** ≤100 rows, scrubbed of anything sensitive. It gets committed
   to `tests/fixtures/`, so it must be shareable.

2. **Profile it before writing anything.**
   ```powershell
   .venv\Scripts\ffe.exe profile tests\fixtures\<name>.txt
   ```
   Read `structure_hint`. If it says `native`, `sentinel`, or `record_tag`, try
   config-only first — you may not need the existing parser's code at all.

3. **Try a spec.** Copy the closest file in `feeds/`, then:
   ```powershell
   .venv\Scripts\ffe.exe dry-run feeds\<name>.yaml tests\fixtures\<name>.txt
   ```
   Iterate on `gates` and the error `candidates` until `gates.passed` is true.
   A surprising share of "we needed a custom parser" feeds are expressible this
   way — the custom code was usually carrying the file handling, not the parsing.

4. **Only if that can't pass, scaffold a plugin.**
   ```powershell
   .venv\Scripts\ffe.exe new-parser <ref>
   ```
   Then port the existing parser into `parse()` and **delete** everything that:
   - opens files, walks directories, or unzips
   - connects to a database or writes output
   - retries, logs to a file, or manages temp directories
   - re-implements reject handling or lineage

   A parser that took a file path now takes `bytes`. That is usually the only
   signature change.

5. **Convert raised exceptions on bad rows into rejects.** This is normally the
   biggest behavioural change: existing parsers tend to abort a whole file on one
   malformed row. Append to `bad` with a `_reject_reason` instead, and let
   `max_reject_ratio` decide whether the job survives.

6. **Add `_src_line_no`** to every row, good and rejected.

7. **Make the generated test real** — assert the expected columns, dtypes, and
   row count, not just "it returned something".

8. **`dry-run`, then have the feed's owner confirm the DataFrame** before any
   `ffe run`.

**Exit criterion per parser:** `gates.passed` true on the sample, its generated
test asserts real expectations, `pytest` green, and a human has looked at the
DataFrame.

### Ordering advice

Do the two or three *simplest* feeds first, even if they're low value. They
validate the workflow while the cost of discovering a framework gap is low. Save
the gnarliest legacy parser until the pattern is established.

Track it plainly in a table in this file as you go — feed name, strategy used,
plugin or config-only, date. That table is how you'll know whether the built-ins
are pulling their weight or whether a fourth structure strategy is warranted.

---

## Part 3 — remaining framework work

In the order I'd do it. Each is independent; none blocks Part 2.

### 3.1 Partitioned tables

Currently commits are unpartitioned. Partitioning on `_ingest_date` requires that
one staged Parquet file maps to exactly one partition.

- Add `target.partition_by: [_ingest_date]` to `FeedSpec`.
- In `io/staging.py`, split a member's frame by partition value before writing,
  so a file is never mixed.
- In `io/sink.py`, create the table with a `PartitionSpec` and confirm
  `add_files` accepts the staged files.
- **Test:** two ingest dates in one job produce two partitions and one snapshot.

Watch for: `add_files` is stricter about partitioned tables than unpartitioned
ones. If it fights, fall back to single-threaded `table.append()` over the staged
files — the staging boundary makes that a contained swap.

### 3.2 Fixed-width as a built-in strategy

Real reference data wants this declaratively. `plugins/acme_positions.py` is the
working reference; promote it.

- Add `FixedWidthData` to `spec.py`: a list of `{name, start, end, dtype}`.
- Add a `copybook` header parser for feeds that declare their own layout.
- Reuse `coerce.py` unchanged — rejects and lineage come free.
- **Test:** `acme_positions.txt` parses from YAML alone, with output identical to
  the plugin's. Keep the plugin as the extension-path example.

### 3.3 Schema-drift policy

`add_files` already refuses staged files whose schemas disagree, with a clear
error. What's missing is a policy for "the feed grew a column".

- Add `policy.schema_change: fail | evolve`, default `fail`.
- On `evolve`, use pyiceberg's schema evolution to add nullable columns only.
  Never widen or retype silently.
- **Test:** a member with an extra column fails by default and evolves when asked.

### 3.4 Object-store sources

`io/source.py` is local-path only. In a Microsoft shop this is most likely ADLS /
Azure Blob rather than S3.

- Add an `fsspec`-backed source so `pattern` accepts `abfs://` / `az://` / `s3://`.
- Keep `Member` picklable — it must stay paths-and-strings, never an open handle.
- Point the Iceberg warehouse at the same store, and swap `SqlCatalog` for the
  REST catalog if the org runs one.
- **Test:** the existing suite against a local `fsspec` memory or file backend, so
  it doesn't need cloud credentials in CI.

### 3.5 Scheduling

Deliberately absent. `ffe run` is a single command with a JSON result and a
non-zero exit on failure, so whatever the org already uses — Airflow, ADF,
Dagster, Task Scheduler — can call it. Do not build a scheduler.

---

## Part 4 — deployment shape

The prototype needs no services. Graduate only when there's a reason:

| Step | Swap | Reason to bother |
|---|---|---|
| now | SQLite catalog, filesystem warehouse, SQLite ledger | works, zero ops |
| later | Iceberg REST catalog, object-store warehouse | more than one writer, or shared tables |
| later | Postgres ledger | more than one machine running jobs |
| optional | Docker image of the CLI | reproducible runs on a scheduler |

All four are interface swaps behind `io/sink.py` and `io/ledger.py`. Nothing in
`ffe.core` changes.

---

## Appendix — rebuilding from scratch

If only the docs travel and not the code, `DESIGN.md` plus this module list is
enough to reconstruct it. Build in this order; each step is testable alone.

| # | Module | Responsibility | Depends on |
|---|---|---|---|
| 1 | `core/spec.py` | `FeedSpec` pydantic models; discriminated union on `parser.kind` | — |
| 2 | `core/report.py` | `ParseReport`, `ParseResult`, `ParseError` (with `field` / `candidates` / `hint` / `blame`) | — |
| 3 | `core/framer.py` | `bytes -> Lines(list[str], base)`. Encoding, BOM, CRLF. **No object per line.** | 1 |
| 4 | `core/structure.py` | `Lines -> Layout` of index regions. `sentinel` + `record_tag`. | 1, 2, 3 |
| 5 | `core/header.py` | header region `-> Schema`. `ddl`, `name_row`, `supplied`. | 1, 2, 4 |
| 6 | `core/coerce.py` | cast with `strict=False`; route newly-null rows to rejects with the original value. **Never raises.** | 1 |
| 7 | `core/engine.py` | `parse(spec, bytes) -> ParseResult`. Delimiter check with measured candidates. Polars does the parsing. | 1–6 |
| 8 | `core/plugins.py` | `@register`, `ParserPlugin`, `load_dir` | 2 |
| 9 | `core/profile.py` | deterministic profile + `structure_hint`. No model. | — |
| 10 | `io/source.py` | `pattern -> [Member]`. **`Member` must pickle** — paths, never handles. | — |
| 11 | `io/staging.py` | Parquet write + lineage columns | — |
| 12 | `io/ledger.py` | SQLite job/member history | — |
| 13 | `io/sink.py` | `add_files` commit. Paths via `as_posix()`/`as_uri()`. | 11 |
| 14 | `io/runner.py` | fan out, apply gates, then **one** commit. `_process` module level. | 7, 10–13 |
| 15 | `scaffold.py` | `new-parser` templates | — |
| 16 | `cli.py` | seven verbs, JSON out, exit codes 2/3/1 | all |

Write the tests from `tests/` alongside — they encode the contracts more
precisely than prose can. Start with the two worked examples in `DESIGN.md` as
fixtures; if those parse correctly the design is being followed.

Two rules that are easy to lose and expensive to retrofit:

- **`core` imports nothing from `io`.** That is what makes `dry-run` unable to
  write, and it is worth an explicit check.
- **No Python object per line.** Measured at 20x the cost of the actual parse.
