# Working in this repo

This repo lands flat-file feeds in Iceberg. Engineers write only the parse logic;
the framework owns zip handling, parallelism, staging, the Iceberg commit,
rejects, and lineage.

**Never write ingestion code.** If you find yourself opening a zip, writing
Parquet, or calling pyiceberg, stop — that already exists and you are in the
wrong layer.

## Environment

Everything runs through `uv`. One setup command, then `uv run` in front of every
command — identical on Windows, macOS and Linux, so never emit a
`.venv\Scripts\…` or `.venv/bin/…` path.

```bash
uv sync --extra dev
uv run pytest
```

`uv sync` reads `requires-python` (`>=3.12,<3.13`) and installs Python **3.12**
itself — 3.13+ is ahead of stable pyiceberg/pyarrow wheels. Do not pick an
interpreter by hand, and do not use `python setup.py`, `pip install` into the
global interpreter, or conda.

If `uv` is genuinely unavailable, fall back to a venv and drop the `uv run`
prefix from everything below:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

## Onboarding a feed

Someone gives you a sample file (100 rows is enough) and asks to onboard a feed:

```bash
uv run ffe profile <sample>                 # 1. measure. read structure_hint.
uv run ffe lint <feed.yaml>                 # 2. after writing a spec, check it
uv run ffe dry-run <feed.yaml> <sample>     # 3. parse it. writes nothing.
uv run ffe run <feed.yaml>                  # 4. ONLY after a human approves
```

**Always try config-only first.** `profile` returns a `structure_hint` naming a
strategy. Most feeds need one YAML file and no Python at all. Only scaffold a
plugin when `dry-run` cannot be made to pass:

```bash
uv run ffe new-parser <ref>                 # writes plugin + test + feed spec
```

Show the human the `head` from `dry-run` and wait. **Do not run `ffe run`
without approval** — it writes to the warehouse.

## Reading dry-run

`gates.passed` is the only thing that matters. When false, look at:

| Gate | Meaning if non-zero |
|---|---|
| `ragged_lines` | rows whose field count disagrees with the header |
| `unknown_tags` | record tags not declared in the spec |
| `reject_ratio` | rows that failed to cast, over the policy limit |
| `all_null_columns` | almost always a column-alignment bug |
| `trailer_row_count` | `MISMATCH` means rows were lost — never ship this |

`trailer_row_count` is exact ground truth when the file declares a count. Trust
it over everything else.

## Reading errors

Errors are JSON and say what to change. Act on `field`, `candidates`, `hint` —
do not guess:

```json
{"error": "delimiter_mismatch", "field": "parser.data.delimiter",
 "candidates": [{"value": "|", "columns": 5, "consistency": 1.0}],
 "hint": "Set parser.data.delimiter to '|'.", "blame": "spec"}
```

`blame` decides your next move, and the exit code mirrors it:

- **`blame: spec`** (exit 2) — you can fix this. Edit the YAML and retry.
- **`blame: file`** (exit 3) — the file is truncated or corrupt. **Stop
  retrying** and tell the human. No spec change will help.
- exit 1 — a bug in `ffe`, not in the spec. Report it, don't work around it.

The exit code is `$LASTEXITCODE` in PowerShell, `$?` in bash/zsh. `uv run`
passes the CLI's exit code through unchanged.

## Choosing a parser kind

Read `structure_hint.strategy` from `profile`:

| Hint | Use | Copy from |
|---|---|---|
| `native` | one stable width, delimiter on every line | set `kind: native` |
| `sentinel` | banner lines (`*`) separate regions | `feeds/banner-ddl-feed.yaml` |
| `record_tag` | field 0 is a record code (`F`/`H`/`I`/`E`) | `feeds/pipe-tagged-feed.yaml` |
| `unknown` | scaffold a plugin | `plugins/acme_positions.py` |

Copy the closest existing feed spec rather than writing one from scratch.
`uv run ffe schema` prints the full accepted shape, generated from the pydantic
models.

Fixed-width is **not** a built-in strategy — it needs a plugin.
`plugins/acme_positions.py` is the reference implementation.

## The plugin contract

```python
from ffe.core.plugins import ParseContext, ParserPlugin, register
from ffe.core.report import ParseResult

@register("my-feed")
class MyFeed(ParserPlugin):
    def parse(self, raw: bytes, ctx: ParseContext) -> ParseResult: ...
```

- Return a Polars DataFrame. **Never raise on bad data** — bad rows go into
  `rejects` with a `_reject_reason` column holding the *original* value.
- Add `_src_line_no` to every row, good and rejected. Without it a rejected row
  can't be traced back to its line.
- Set `report.trailer_declared_rows` if the file declares a count.
- Do **not** import anything from `ffe.io`. Plugins never touch storage.
- Read tunables from `ctx.options`, not module constants.

## Migrating an existing in-house parser

Wrap, don't rewrite. Existing parsers usually already produce rows; they just
also do their own file and storage handling. Strip that out:

1. `uv run ffe new-parser <ref>` to get the correctly shaped stub.
2. Move the row-producing logic into `parse()`. Delete everything that opens
   files, walks directories, unzips, connects to a database, or writes output —
   the framework does all of it.
3. Replace raised exceptions on bad rows with an append to `bad`.
4. Add `_src_line_no`.
5. Drop a real sample (≤100 rows, scrubbed of anything sensitive) into
   `tests/fixtures/` and make the generated test assert the expected columns and
   row count.
6. `dry-run`, then have the owner of the feed confirm the DataFrame.

A parser that took a file path now takes `bytes`. That is usually the only
signature change needed.

## Performance rule

Polars parses 150k rows in about 1 ms, so any per-line Python work dominates.
Keep hot loops at the C level — list slices, `str.count`, `str.join` — and let
Polars do the parsing. **Do not build one object per line**: that was measured at
20x the cost of the parse itself, and fixing it took throughput from 1.6M to
5.2M rows/s.

Default executor is `thread`. Do not switch to `process` "for speed" — it was
measured slower on every benchmark, because spawning workers that each re-import
Polars costs more than the parsing saves until a job exceeds roughly 4M rows.

## Windows specifics that have already bitten

- **Paths into pyiceberg / SQLAlchemy** must go through `Path.as_posix()` or
  `Path.as_uri()`. A plain `str()` gives backslashes and a bare drive letter,
  which neither will parse. See `src/ffe/io/sink.py`.
- **Line endings**: `.gitattributes` marks `tests/fixtures/**` as binary so git
  never rewrites them. Do not remove that. The framer normalises CRLF and strips
  a BOM; `test_crlf_and_bom_parse_identically_to_lf` pins it.
- **`--executor process`** uses spawn on Windows, so worker code must stay
  importable at module level. `_process` in `runner.py` is module level for this
  reason — do not turn it into a closure or a lambda.
- Console output is ASCII-escaped JSON on purpose, so a cp1252 console can't
  crash on a non-ASCII value.

## When a production file fails after the sample passed

Expected, and the reject table is the feedback loop:

```sql
SELECT _src_file, _src_line_no, _reject_reason
FROM bronze.<feed>_rejects WHERE _job_id = '<job>' LIMIT 50
```

Those rows are the input for refining the parser. Add the failing shape to the
fixture, tighten the parser, re-run the test. Never widen `max_reject_ratio` to
make a job pass.

## Testing

Every feed's sample is committed to `tests/fixtures/` and pinned by a test. This
is deliberate: each feed onboarded makes the suite stronger.

```bash
uv run pytest -q                    # must be green before proposing anything
uv run python scripts/smoke.py      # end-to-end: zip -> Iceberg -> read back
```

`pytest` reports 30 passed and 1 skipped; the skip is the notebook test, which
needs the extra dependencies (`uv run --all-extras pytest -q` gives 32 passed).
