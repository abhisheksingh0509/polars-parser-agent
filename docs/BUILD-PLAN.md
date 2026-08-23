# Build plan

What to do, in order, with a verifiable exit criterion for each step. Read
[`DESIGN.md`](DESIGN.md) for *why*; this is *what to do*.

There are two ways to use this file:

| If you are | Read |
|---|---|
| **working in this repo** | [Part 1](#part-1--get-it-green) to confirm it's green, then [Part 2](#part-2--migrate-the-real-in-house-parsers) — migrating the in-house parsers is the actual work. [Part 3](#part-3--remaining-framework-work) and [Part 4](#part-4--deployment-shape) are what's left to build. |
| **recreating the project from scratch, with only the docs** | [Part 5](#part-5--recreating-this-from-the-docs). `DESIGN.md` plus that part is the whole specification — no other file needs to travel. |

The repo itself runs on macOS. Windows is used only for the second case, which is
why Part 5 restates the invariants that would otherwise live only in the code and
in the agent instructions.

Agent instructions live in
[`../.github/copilot-instructions.md`](../.github/copilot-instructions.md), which
Claude Code and Copilot read automatically. Parts 1–4 don't repeat them.

Every command goes through `uv run`.

---

## Part 1 — get it green

Do this first and do not skip the verification. Everything else assumes it.

```bash
uv sync --extra dev
uv run pytest -q                    # expect: green
uv run python scripts/smoke.py      # expect: "smoke test OK"
```

`uv sync` reads `requires-python` and installs Python 3.12 itself, so there is
nothing to pick by hand. Any skip is the notebook test —
`uv run --all-extras pytest -q` runs it too.

**Exit criterion: the test suite and the smoke checks both pass.**

If something fails, check [the traps in Part 5](#traps-that-have-already-cost-a-day)
before debugging — every one of them is already handled here, so a reappearance is
a regression rather than a new problem. If it fails for any *other* reason, fix the
code, add a test that pins it, and add it to that table.

---

## Part 2 — migrate the real in-house parsers

This is the actual office work, and the reason the framework exists: your team
already writes a parser per reference feed. Keep doing that. Stop rewriting the
storage and processing layer around each one.

### Per parser

1. **Get a sample.** ≤100 rows, scrubbed of anything sensitive. It gets committed
   to `tests/fixtures/`, so it must be shareable.

2. **Profile it before writing anything.**
   ```bash
   uv run ffe profile tests/fixtures/<name>.txt
   ```
   Read `structure_hint`. If it says `native`, `sentinel`, or `record_tag`, try
   config-only first — you may not need the existing parser's code at all.

3. **Try a spec.** Copy the closest file in `feeds/`, then:
   ```bash
   uv run ffe dry-run feeds/<name>.yaml tests/fixtures/<name>.txt
   ```
   Iterate on `gates` and the error `candidates` until `gates.passed` is true.
   A surprising share of "we needed a custom parser" feeds are expressible this
   way — the custom code was usually carrying the file handling, not the parsing.

4. **Only if that can't pass, scaffold a plugin.**
   ```bash
   uv run ffe new-parser <ref>
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
test asserts real expectations, `uv run pytest` green, and a human has looked at the
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

### 3.1 Partitioned tables — **done**

`target.partition_by: [business_date]` gives the table an identity `PartitionSpec`
in `sink.py`. Partition on the feed's business date, not ingest time — a re-load
must land in the day it belongs to.

The partition-aware staging split this item used to call for turned out to be
**unnecessary**: `staging.write` already emits one Parquet per member, and a
member holds one business day, so every staged file maps to exactly one partition
value for free. The invariant is per *file*, not per archive.

Refusals are structured errors, not crashes: `partition_column_missing`,
`partition_spec_conflict` (the table already holds data laid out differently — a
load never re-lays-out an existing table), and `mixed_partition_file` (one member
spans two values; `add_files` catches this itself).

Not done, and deliberately: partition transforms other than identity (`day`,
`bucket`, `truncate`), and migrating a populated table onto a new spec.

### 3.2 Fixed-width as a built-in strategy

Real reference data wants this declaratively. `plugins/acme_positions.py` is the
working reference; promote it.

- Add `FixedWidthData` to `spec.py`: a list of `{name, start, end, dtype}`.
- Add a `copybook` header parser for feeds that declare their own layout.
- Reuse `coerce.py` unchanged — rejects and lineage come free.
- **Test:** `acme_positions.txt` parses from YAML alone, with output identical to
  the plugin's. Keep the plugin as the extension-path example.

### 3.3 Schema-drift policy — **done**

`policy.schema_change: fail | evolve`, default `fail`. On `evolve`, `sink.py`
unifies the staged schemas and calls `union_by_name` on the table before
`add_files`, which adds nullable columns only; a changed column *type* is still
a hard failure. Catches drift within one job and across jobs, and fails as a
structured `schema_drift` error (`blame: spec`) rather than an unhandled crash.
Covered by five tests in `test_pipeline.py`.

Two things worth knowing if you touch it: `pa.unify_schemas` raises
`ArrowTypeError`, not `ArrowInvalid`, on a type conflict; and `add_files`
rejects a file *wider* than the table but accepts a *narrower* one, reading the
absent column as null.

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

## Part 5 — recreating this from the docs

This is the path where the code doesn't travel and the docs do: rebuilding the
project from scratch on another machine, from [`DESIGN.md`](DESIGN.md) and this
part. Those two files are the whole specification — `DESIGN.md` carries the *why*,
the FeedSpec shape, the plugin SPI, the error design, and two worked examples with
their expected output; this part carries the build order and the invariants.

### Before writing any code

Python **3.12** specifically. 3.13+ is ahead of the stable pyiceberg and pyarrow
wheels, and you will lose an afternoon to a build failure that has nothing to do
with your code.

So the first thing in the new `pyproject.toml` is the constraint, not a dependency:

```toml
[project]
requires-python = ">=3.12,<3.13"
dependencies = ["polars>=1.0", "pydantic>=2.7", "pyarrow>=16",
                "pyiceberg[sql-sqlite]>=0.7", "typer>=0.12", "pyyaml>=6"]

[project.optional-dependencies]
dev = ["pytest>=8"]

[project.scripts]
ffe = "ffe.cli:app"
```

From then on `uv sync --extra dev` supplies the interpreter as well as the
environment, so nobody has to have a 3.12 installed or choose one by hand. Commit
the resulting `uv.lock`.

### Two invariants that are cheap now and expensive later

Both are architectural. Retrofitting either one means rewriting the layer.

- **`core` imports nothing from `io`.** This is what makes `dry-run` structurally
  incapable of writing anywhere — not a convention anyone has to remember, a fact
  about the import graph. Worth an explicit test.
- **No Python object per line.** Measured at 20x the cost of the parse itself.
  Structure detection finds byte boundaries; Polars parses. Fixing this after the
  fact took throughput from 1.6M to 5.2M rows/s.

### Build order

Each step is testable alone, and depends only on what came before it.

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
| 16 | `cli.py` | eight verbs, JSON out, exit codes 2/3/1 | all |

Write the tests alongside, not afterwards — they encode the contracts more
precisely than prose can. Use the two worked examples in `DESIGN.md` as your first
fixtures: if those parse to the output shown there, the design is being followed.

### Traps that have already cost a day

Every one of these is handled in the current code. If you are rebuilding, get them
right the first time; if you are working in this repo and one reappears, it's a
regression.

| Symptom | Cause | The fix that is already in place |
|---|---|---|
| `Could not parse SQLAlchemy URL` / `unsupported scheme` on commit | a Windows path handed over as `str()` — backslashes and a bare drive letter, which neither pyiceberg nor SQLAlchemy will parse | `io/sink.py` goes through `Path.as_posix()` / `Path.as_uri()` |
| a fixture parses differently on another machine | git rewrote CRLF inside a test fixture | `.gitattributes` marks `tests/fixtures/**` as binary; the framer normalises CRLF and strips a BOM, pinned by `test_crlf_and_bom_parse_identically_to_lf` |
| `UnicodeEncodeError` when printing a result | a cp1252 console meeting a non-ASCII value | the CLI emits ASCII-escaped JSON on purpose |
| `--executor process` hangs, or re-runs the CLI from the top | `spawn` re-imports the worker, so worker code must be importable at module level | `_process` in `io/runner.py` is module level — not a closure, not a lambda |
| pyarrow / pyiceberg wheels won't install | Python 3.13+ | pin 3.12 |

### How you know you're done

- the two worked examples from `DESIGN.md` parse to the output documented there
- a multi-member archive fans out and lands in **one** Iceberg snapshot, with
  `_src_file` and `_src_line_no` on every row
- a feed over `max_reject_ratio` commits **nothing** — not a partial load
- `dry-run` cannot write, and you have a test asserting the import graph that
  guarantees it
