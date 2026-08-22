# Flatfile Ingestion Engine (`ffe`)

Lands bespoke flat-file feeds in Iceberg. **You write the parse function; the
framework owns the plumbing** — zip handling, parallelism, staging, the Iceberg
commit, rejects, and lineage.

Onboarding a new feed is **one YAML file**, plus a parser class only when the
built-ins can't express the format.

**New here? Start with [`docs/GETTING-STARTED.md`](docs/GETTING-STARTED.md)** — plain
language, step by step. It ends with a worked example of
[onboarding a file of your own](docs/GETTING-STARTED.md#testing-your-own-file),
from first look to a test that pins it. Or run
[`notebooks/explore.ipynb`](notebooks/explore.ipynb) to watch the whole path from a
messy file to a queryable table.

| Doc | For |
|---|---|
| [`docs/GETTING-STARTED.md`](docs/GETTING-STARTED.md) | using it, in plain language — and testing your own file |
| [`notebooks/explore.ipynb`](notebooks/explore.ipynb) | seeing it work, interactively |
| [`docs/DESIGN.md`](docs/DESIGN.md) | why it's built this way, plus measurements |
| [`docs/BUILD-PLAN.md`](docs/BUILD-PLAN.md) | extending it, or migrating existing parsers |
| [`.claude/commands/onboard-feed.md`](.claude/commands/onboard-feed.md) | onboarding a feed with Claude Code: `/onboard-feed <sample>` |
| [`.github/copilot-instructions.md`](.github/copilot-instructions.md) | read automatically by Copilot / Claude Code |

---

## Setup

Everything runs through [uv](https://docs.astral.sh/uv/). One command, identical
on Windows, macOS and Linux:

```bash
uv sync --extra dev
```

That creates `.venv` and installs Python **3.12** itself, from `requires-python` —
you do not need a system 3.12, and you should not pick one by hand, because 3.13+
is ahead of stable pyiceberg/pyarrow wheels. `uv.lock` is committed, so the
resolution is identical every time.

uv is genuinely required, not a convenience: it is what supplies the interpreter.

```bash
uv run pytest -q                    # expect: 30 passed, 1 skipped
uv run python scripts/smoke.py      # expect: 19/19, "smoke test OK"
```

If either fails on a fresh clone, fix that before anything else — see
[`docs/BUILD-PLAN.md`](docs/BUILD-PLAN.md) Part 1.

No services needed. It runs on a SQLite Iceberg catalog and a filesystem
warehouse out of the box.

### To run the notebook

```bash
uv run --all-extras jupyter lab notebooks/explore.ipynb
```

It's committed without saved outputs, so run it top to bottom — takes about 15
seconds and cleans up after itself.

`--all-extras` adds the notebook dependencies, which also un-skip the two tests
that execute the notebook: `uv run --all-extras pytest -q` gives **32 passed**.

---

## Using it

`uv run` puts the CLI on the path — no activation, no platform-specific path to
the executable.

```bash
uv run ffe profile sample.txt                        # measure structure. no model involved.
uv run ffe lint feeds/my-feed.yaml                   # validate the spec
uv run ffe dry-run feeds/my-feed.yaml sample.txt     # parse a sample. writes nothing.
uv run ffe run feeds/my-feed.yaml                    # fan out, gate, one Iceberg commit
uv run ffe explain feeds/my-feed.yaml                # config + recent job history
uv run ffe new-parser acme-positions                 # scaffold plugin + test + feed spec
uv run ffe tables                                    # what's in the warehouse
uv run ffe schema                                    # the full accepted FeedSpec shape
```

The output table comes from `target.table` in the feed file, and can be overridden
per run without editing it:

```bash
uv run ffe run feeds/my-feed.yaml --table sandbox.my_test --source "C:/drops/*.zip"
```

Every command emits JSON. Exit codes are part of the contract: **2** = your spec
is wrong (fix and retry), **3** = the file is wrong (stop retrying), **1** = a
bug in `ffe`.

### Driving it with Claude Code

That JSON, plus errors that name the field to change, is what makes the onboarding
loop agent-drivable — so it ships as a slash command:

```
/onboard-feed trades.txt bronze.trades
```

It profiles the sample, copies the nearest spec, and iterates `dry-run` on the
structured errors until `gates.passed`, then **stops for your approval** — `ffe run`
is deliberately outside the command's allowed tools, so writing to the warehouse
needs a human. Details and the reasoning are in
[`docs/GETTING-STARTED.md`](docs/GETTING-STARTED.md#driving-this-with-claude).

### A feed spec

```yaml
name: pipe-tagged-feed
source:
  pattern: "data/issues/*.zip"     # file, zip, directory, or glob
  members: "*.txt"                 # which entries inside the archive
parser:
  kind: sectioned                  # native | sectioned | plugin
  structure:
    strategy: record_tag           # field 0 names the record type
    delimiter: "|"
    records:
      F: { kind: file_header, fields: [tag, file_id, business_date] }
      H: { kind: header }
      I: { kind: data }
      E: { kind: trailer, fields: [tag, entity] }
  header: { parser: name_row, skip_leading_fields: 3 }
  data:   { parser: delimited, delimiter: "|", drop_columns: [tag] }
  coerce: { schema_override: { id: int64 } }
  validate:
    require_trailer: true
    promote_fields: [business_date, file_id]
target:
  table: bronze.issues
policy:
  max_reject_ratio: 0.01           # exceeded -> job fails, nothing commits
```

---

## Built-in parsers

| Kind | For | Example |
|---|---|---|
| `native` | a file that is just a file — CSV/TSV | set `kind: native` |
| `sectioned` / `sentinel` | banner lines (`*`) delimit regions | `feeds/banner-ddl-feed.yaml` |
| `sectioned` / `record_tag` | field 0 is a record code (`F`/`H`/`I`/`E`) | `feeds/pipe-tagged-feed.yaml` |
| `plugin` | your code, for anything else | `plugins/acme_positions.py` |

Fixed-width is deliberately a plugin, not a built-in — it's the reference
example of the extension path.

## Layout

| Path | What |
|---|---|
| `src/ffe/core/` | parsers. Pure functions, **no I/O** — which is why `dry-run` cannot write |
| `src/ffe/io/` | source resolution, fan-out, staging, ledger, Iceberg sink |
| `src/ffe/cli.py` | the eight verbs |
| `feeds/` | one YAML per feed. Versioned config, reviewed like code |
| `plugins/` | your parser code |
| `tests/fixtures/` | every feed's sample, pinned by a test |
| `scripts/smoke.py` | end-to-end acceptance check |

## How parallelism works

1. List the archive's members — nothing read yet.
2. N workers, each parsing its own member onto its own staged Parquet file.
   **No worker touches Iceberg.**
3. One single-threaded `add_files` commit registers every staged file in a single
   snapshot. No data is rewritten; Iceberg reads the Parquet footers for stats.

Default executor is `thread`. Measured on an M-series Mac: parsing runs at ~5M
rows/s single-threaded, so at small volumes a process pool costs more to start
than it saves — processes lost every benchmark. Use `--executor process` only
when a job's aggregate parse time is measured in seconds (roughly 4M+ rows).
Numbers and method are in [`docs/DESIGN.md`](docs/DESIGN.md#measured-not-assumed).

## Guarantees worth knowing

- **`dry-run` cannot write anywhere.** `ffe.core` has no I/O at all; it's
  structural, not a convention.
- **Bad rows never kill a job.** They go to `<table>_rejects` with the original
  value, the reason, and `_src_line_no`.
- **A broken feed doesn't land partially.** Exceed `max_reject_ratio` and nothing
  commits.
- **Every row is traceable** to its archive, member, and source line number.
- **Types don't drift.** Schema inference is a spec-authoring aid, frozen into
  the spec — never a runtime behaviour.
