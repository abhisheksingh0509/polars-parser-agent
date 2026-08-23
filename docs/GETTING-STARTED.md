# Getting started

No prior knowledge of this project assumed. By the end you'll have run it on a
file that ships with the repo, then on **one of your own**, and pinned that with
a test so it stays working.

- [What this solves](#what-this-solves)
- [Install](#install)
- [See it work first](#see-it-work-first) — on files already in the repo
- [**Testing your own file**](#testing-your-own-file) — the six steps
- [When your file needs actual code](#when-your-file-needs-actual-code)
- [Driving this with Claude](#driving-this-with-claude) — `/onboard-feed`, and how the loop converges
- [Reference](#reference) — commands, lineage columns, where things get saved

---

## What this solves

Some files are easy: a normal CSV that any tool opens. Others arrive with banner
lines, a header written in the vendor's own mini-language, a letter at the start
of each row saying what kind of row it is, a row count at the bottom. Ask ten
vendors for reference data and you get ten shapes.

Somebody has to write the code that reads each shape. That part is fair enough —
the person who knows the feed should own it.

The problem is everything *around* it. Each of those small parsers ends up also
opening the zip, looping the members, running them in parallel, writing the
output, committing it, dealing with the rows that were bad, and remembering where
each row came from. That's the same work every time, it's most of the code, and
it's where the bugs live.

This project owns that part. You supply two things — **what the file looks like**
and **which table it goes in** — and get parallel reading, bad-row quarantine and
row-level traceability without writing any of it.

```
your zip                                              Iceberg tables
  ├── file1.txt ─┐                                  ┌─ bronze.trades
  ├── file2.txt ─┼─►  parsed in parallel  ─► one ─► │
  └── file3.txt ─┘        by N workers      commit  └─ bronze.trades_rejects
                                                          (the bad rows)
```

---

## Install

You need [uv](https://docs.astral.sh/uv/). One command, same on Windows, Mac and
Linux:

```bash
uv sync --extra dev
```

That creates `.venv` and picks Python **3.12** for you — which matters, because
3.13+ is ahead of the storage libraries' stable wheels.

```bash
uv run pytest -q                    # must be green
uv run python scripts/smoke.py      # smoke test OK
```

No database, no Docker, no cloud account. Everything runs on a SQLite catalog and
a folder on your disk.

Every command below starts with `uv run`, which runs it inside this project's
environment. If you'd rather not type it, activate the venv once
(`source .venv/bin/activate`) and drop the prefix.

---

## See it work first

Before pointing it at anything of yours, spend two minutes on a file that's
already here and known-good. If this doesn't work, nothing else will.

```bash
# 1. what does this file look like?
uv run ffe profile tests/fixtures/pipe_tagged.txt

# 2. parse it. writes nothing, anywhere.
uv run ffe dry-run feeds/pipe-tagged-feed.yaml tests/fixtures/pipe_tagged.txt

# 3. actually load it, into a throwaway warehouse
uv run ffe run feeds/pipe-tagged-feed.yaml --source tests/fixtures/pipe_tagged.txt --table sandbox.demo --workspace _ffe/demo

# 4. what landed?
uv run ffe tables --workspace _ffe/demo
```

Step 4 prints:

```json
{ "table": "sandbox.demo", "rows": 2, "files": 1, "snapshots": 1,
  "columns": ["id", "name", "_src_line_no", "business_date", "file_id",
              "_src_file", "_job_id", "_spec_hash", "_ingested_at"] }
```

Two rows landed, and the file itself only declared `id` and `name` — everything
else was added for you. `rm -rf _ffe/demo` when you're done; nothing outside that
folder was touched.

Prefer to see it with the data in front of you?
[`notebooks/explore.ipynb`](../notebooks/explore.ipynb) walks the same ground and
prints every table as it goes:

```bash
uv run --all-extras jupyter lab notebooks/explore.ipynb
```

---

## Testing your own file

Six steps. Step 4 is a loop — expect to go round it a few times. That's the normal
way to use this, not a sign you're doing it wrong.

Claude Code can run this whole loop for you with one command
([below](#driving-this-with-claude)). Read this section first anyway: it ends with
you approving a table, and you can't approve what you can't judge.

### 1. Get a small sample

**100 lines is plenty.** A plain `.txt` or `.csv` is fine — it does not need to be
zipped, and it does not need to be the whole file.

One thing to decide now: this sample ends up committed to `tests/fixtures/` in
step 6, so scrub anything sensitive before you start. Real structure, fake values.

### 2. Ask what it sees

```bash
uv run ffe profile trades.txt
```

The only part you need is `structure_hint`:

```json
"structure_hint": {
  "strategy": "record_tag",
  "delimiter": "|",
  "tags": ["E", "F", "H", "I"],
  "why": "field 0 holds a few short repeated codes and line width varies by code"
}
```

Read that as: *each row starts with a letter saying what kind of row it is, and
the columns are separated by pipes.*

No AI is involved — it counts characters and measures line widths. By default it
only looks at the first 100 lines (`--cap` changes that).

There are four possible answers, and each has a real example in this repo you can
run the same command against:

| `strategy` | Meaning | Start from | Profile this to see it |
|---|---|---|---|
| `native` | an ordinary CSV/TSV | six lines of YAML (below) | `tests/fixtures/plain.csv` |
| `sentinel` | banner lines like `***` split the file into sections | `feeds/banner-ddl-feed.yaml` | `tests/fixtures/banner_ddl.txt` |
| `record_tag` | field 0 is a code letter on every row | `feeds/pipe-tagged-feed.yaml` | `tests/fixtures/pipe_tagged.txt` |
| `unknown` | it can't work this one out | [needs code](#when-your-file-needs-actual-code) | `tests/fixtures/acme_positions.txt` |

`unknown` is not a failure. It usually means fixed-width — field positions counted
in characters instead of separated by anything — which is deliberately not a
built-in.

### 3. Write the spec — by copying

Make a file in `feeds/`. **Don't write it from scratch**; copy the one the table
above pointed you at and edit that.

If the hint was `native`, this really is the whole spec:

```yaml
name: plain
source:
  pattern: "data/plain/*.csv"
parser:
  kind: native
  delimiter: ","
target:
  table: bronze.plain
```

For the `record_tag` sample above, `feeds/trades.yaml` looks like this. Four
blocks, and only the middle one is feed-specific:

```yaml
name: trades

source:                                # WHERE THE FILES COME FROM
  pattern: "data/trades/*.zip"          #   a file, a zip, a folder, or a glob
  members: "*.txt"                      #   which entries inside the archive

parser:                                # WHAT THE FILE LOOKS LIKE
  kind: sectioned                       #   copied from feeds/pipe-tagged-feed.yaml
  structure:
    strategy: record_tag                #   ← straight from structure_hint
    delimiter: "|"                      #   ← straight from structure_hint
    records:
      F: { kind: file_header, fields: [tag, file_id, business_date] }
      H: { kind: header }
      I: { kind: data }
      E: { kind: trailer, fields: [tag, entity] }
  header:
    parser: name_row                    #   column names come from the H row
    skip_leading_fields: 3              #   ...after skipping H|date|entity
  data:
    parser: delimited
    delimiter: "|"
    drop_columns: [tag]                 #   the I isn't data, don't keep it

target:                                # WHERE IT GOES
  table: bronze.trades

policy:                                # WHEN TO REFUSE
  max_reject_ratio: 0.01                #   >1% bad rows: commit nothing at all
```

Check the shape before using it:

```bash
uv run ffe lint feeds/trades.yaml
```

`lint` only reads the YAML — it never opens your data file. A clean lint means the
spec is *valid*, not that it's *right*. That's step 4.

### 4. Dry-run until the gates pass

```bash
uv run ffe dry-run feeds/trades.yaml trades.txt
```

`dry-run` **cannot write anything anywhere.** That's structural rather than a
promise: the parsing code has no I/O in it at all. Run it as often as you like.

**Look at `gates` first, and at `passed` before anything else:**

```json
"gates": { "ragged_lines": 0, "unknown_tags": 0, "reject_ratio": 0.0,
           "all_null_columns": [], "trailer_row_count": "n/a", "passed": true }
```

| Gate | What a bad value means |
|---|---|
| `ragged_lines` | some rows have a different number of fields than the header |
| `unknown_tags` | rows carry a code letter your spec doesn't declare |
| `reject_ratio` | share of rows that couldn't be converted to their type |
| `all_null_columns` | a whole column came out empty — nearly always misaligned columns |
| `trailer_row_count` | the file declared how many rows it holds and you got a different number. `n/a` just means it didn't declare one |

> **The one trap worth knowing:** `dry-run` exits **0** even when `gates.passed`
> is `false`. A non-zero exit means it couldn't parse the file at all; failing
> gates mean it parsed, but the result is suspect. Judge it by `gates.passed`,
> never by the exit code.

Then read `head` — that's your actual data, as rows. **Check the columns are the
ones you expect** before going further.

#### If it can't parse at all

You get told which setting is wrong, so you don't have to guess:

```json
{ "error": "delimiter_mismatch",
  "message": "delimiter ',' splits the data rows into 1 column, but the header declares 4",
  "field": "parser.data.delimiter",
  "candidates": [ { "value": "|", "columns": 4, "consistency": 1.0 } ],
  "hint": "Set parser.data.delimiter to '|'.",
  "blame": "spec" }
```

Change `parser.data.delimiter` to `|` and run again. One field decides what to do
next, and the exit code mirrors it:

- **`"blame": "spec"`** (exit 2) — your description is wrong. Fix the YAML, retry.
- **`"blame": "file"`** (exit 3) — the file is truncated or corrupt. **Stop
  retrying.** No spec change will help; go back to whoever sent it.

#### Every column came out as text

Correct, and on purpose. Types are never guessed, so a column can't silently
change type next month when the vendor sends different data. Say what you want:

```yaml
  coerce:
    schema_override: { trade_id: int64, qty: int64 }
```

```json
"schema": { "trade_id": "Int64", "symbol": "String",
            "qty": "Int64", "_src_line_no": "UInt32" }
```

Dates stay text too. This tool stores data the way the file wrote it; convert
dates downstream, where you know the format.

#### Some rows are bad

Bad rows never stop the job and never crash the parser. They're set aside with the
reason and their original text:

```json
"gates": { "reject_ratio": 0.3333, "passed": false },
"rejects": [
  { "trade_id": "1002", "symbol": "VOLV-B", "qty": "NOTANUMBER",
    "_src_line_no": 4,
    "_reject_reason": "qty: cannot cast 'NOTANUMBER' to int64" }
]
```

`_src_line_no: 4` is line 4 of the file you handed it, so you can go and look.

`passed` went false because 33% exceeds the `max_reject_ratio: 0.01` from your
spec. Decide which of the two it is:

- **The file is fine and your spec is wrong** — maybe `qty` really can be blank,
  or isn't an integer at all. Fix the spec.
- **The file really is that broken** — go back to the sender.

**Don't raise `max_reject_ratio` to make the message go away.** That number exists
to stop 80% of a broken file quietly landing and someone finding out three weeks
later.

### 5. Load it somewhere you can throw away

Only once `gates.passed` is true. Point it straight at your sample, into a sandbox
table and a workspace you can delete:

```bash
uv run ffe run feeds/trades.yaml --source trades.txt --table sandbox.my_trades --workspace _ffe/scratch
```

`--source` and `--table` override the spec for this run only, so you can test
without editing the YAML you just got right.

```json
{ "ok": true, "members": 1, "rows": 3, "rejects": 0, "status": "ok",
  "commit": { "table": "sandbox.my_trades", "files": 1, "rows": 3,
              "snapshot_id": 5232672464871556654 } }
```

One `snapshot_id` — however many files went in, they land in a single commit.

If the reject gate trips you get the refusal instead, and nothing is written:

```json
{ "ok": false, "status": "failed", "rows": 2, "rejects": 1,
  "reject_ratio": 0.3333333333333333, "commit": {},
  "errors": [ { "error": "reject_ratio_exceeded",
                "message": "reject ratio 0.333 exceeds max_reject_ratio 0.01; nothing committed" } ] }
```

Exit code **3**, an empty `commit`, and no table created — not a partial load you
have to clean up afterwards. Have a look at what did land:

```bash
uv run ffe tables --workspace _ffe/scratch
```

Then `rm -rf _ffe/scratch` and do it for real, with `source.pattern` in the spec
pointing at where the files actually arrive:

```bash
uv run ffe run feeds/trades.yaml
```

### 6. Pin it as a test

This is the step that makes your work survive everyone else's. Three small things:

**1.** Put the sample in `tests/fixtures/`, and the spec stays in `feeds/`:

```bash
cp trades.txt tests/fixtures/trades.txt
```

**2.** Write `tests/test_trades.py`, asserting what the file *should* produce — the
real columns, the real types, the real row count:

```python
from pathlib import Path

from ffe.core.engine import parse
from ffe.core.spec import FeedSpec

ROOT = Path(__file__).parent.parent
FIX = Path(__file__).parent / "fixtures"


def test_trades_sample_parses():
    spec = FeedSpec.from_yaml(ROOT / "feeds" / "trades.yaml")
    result = parse(spec.parser, (FIX / "trades.txt").read_bytes())

    assert result.frame.columns == ["trade_id", "symbol", "qty", "_src_line_no"]
    assert str(result.frame.schema["qty"]) == "Int64"
    assert len(result.frame) == 3
    assert result.rejects is None or len(result.rejects) == 0
```

**3.** Run it:

```bash
uv run pytest -q
```

`assert len(result.frame) > 0` isn't worth writing — it passes on a completely
misaligned parse. Assert the column names and the dtypes; those are what break.

Why bother: your feed is now protected against everyone who touches the engine
later, and the suite gets stronger with each feed onboarded instead of staying the
same size. It's also how you handle the vendor changing format — when a real file
starts producing rejects, add that shape to the fixture and tighten the spec.

---

## When your file needs actual code

Some formats can't be described in YAML. Fixed-width is the usual one, where field
positions are counted in characters instead of separated by anything — `profile`
reports `unknown` for these.

Scaffold it:

```bash
uv run ffe new-parser trades
```

You get three files, already the right shape:

- `plugins/trades.py` — your code goes here
- `tests/test_trades.py` — the test from step 6, pre-wired
- `feeds/trades.yaml` — the spec, with `kind: plugin`

You fill in **one function**. It receives the bytes of one file and returns a
table. No zip handling, no parallelism, no saving — all of that is still done for
you. `plugins/acme_positions.py` is a complete working example to copy.

Two rules:

1. **Never raise on a bad row.** Append it to `rejects` with a `_reject_reason`
   instead, and let `max_reject_ratio` decide whether the job survives. Existing
   in-house parsers usually abort a whole file on one bad row; that's the main
   behaviour to change when porting one in.
2. **Add `_src_line_no` to every row**, good and rejected. Without it a rejected
   row can't be traced back to its line.

Then pick step 4 back up — everything from there is the same.

---

## Driving this with Claude

Every command prints JSON, every error names the field to change, and the exit
codes separate "your spec is wrong" from "the file is wrong". None of that is
incidental — [`DESIGN.md`](DESIGN.md#designing-errors-for-a-model-reader) calls the
error format *"the single highest-leverage decision in this whole document"*,
because an agent's next action is decided entirely by what the last command
printed. Which means step 4 is a loop something else can turn.

### The command

```
/onboard-feed trades.txt bronze.trades
```

It lives in [`.claude/commands/onboard-feed.md`](../.claude/commands/onboard-feed.md)
— checked in, reviewed like code, so everyone gets the same workflow. It walks the
same six steps and stops where you'd want it to:

```
> /onboard-feed trades.txt

  ⏺ uv run ffe profile trades.txt
      structure_hint: record_tag, delimiter "|", tags E F H I
  ⏺ Write feeds/trades.yaml            (copied from pipe-tagged-feed.yaml)
  ⏺ uv run ffe lint feeds/trades.yaml                 → ok
  ⏺ uv run ffe dry-run feeds/trades.yaml trades.txt   → exit 2
      delimiter_mismatch · parser.data.delimiter · candidate "|"
  ⏺ Edit feeds/trades.yaml             (delimiter "," → "|")
  ⏺ uv run ffe dry-run feeds/trades.yaml trades.txt   → gates.passed

  head(20), schema, and the spec diff — approve before I run `ffe run`?
```

### Why it converges instead of flailing

Four properties, in the order they matter:

1. **`profile` measures; the model only interprets.** Delimiters, line-width
   histograms and position-0 token frequencies come from arithmetic. The only
   interpretive leap is "position 0 is a record tag" — and it's handed the
   measurements rather than asked to produce them.
2. **The error *is* the prompt.** `field` says what to change, `candidates` gives
   measured alternatives, `hint` spells out the edit. There's nothing left to
   invent, which is why it usually takes one or two passes.
3. **`blame` separates retry from stop.** `spec` means iterate; `file` means the
   input is truncated or corrupt and no amount of retrying helps. Without that
   split an agent will happily burn twenty turns on a broken file.
4. **`dry-run` cannot write.** Not by convention — `ffe.core` has no I/O in it at
   all. Iteration is free and side-effect-free, so there's no reason to be timid
   about it.

### What it deliberately won't do

- **Run `ffe run` on its own.** That verb is *not* in the command's `allowed-tools`,
  so it triggers a permission prompt — the approval gate is enforced by the
  permission system rather than by asking the model nicely. Don't click through it
  out of habit.
- **Loosen `max_reject_ratio` to make a gate pass.** If rows are being rejected,
  either the types are wrong or the file is broken; hiding it is neither.
- **Cycle forever.** It stops after five dry-run attempts and reports what it tried
  and where it got stuck.
- **Write ingestion code.** Zip handling, Parquet, pyiceberg — all of it already
  exists, and reaching for it means being in the wrong layer.

### Without the command

A plain sentence works too:

```
> onboard tests/fixtures/trades.txt as a feed into bronze.trades
```

[`CLAUDE.md`](../CLAUDE.md) points at
[`.github/copilot-instructions.md`](../.github/copilot-instructions.md), which
carries the workflow, the gate table, the plugin contract and the approval rule —
so the contract is loaded whether or not you type a command name. The command just
makes it explicit, repeatable, and reviewable.

### Your half of the loop

Claude can tell you `gates.passed` is true. It cannot tell you the feed is right.
Only you know that `qty` should have been an integer, that a column named `entity`
is really the desk code, or that 3% rejects is normal for this vendor on a Monday.
So when it stops and shows you `head` — read the columns, check the dtypes, look at
what got rejected. That's the step the tooling can't do for you, and it's why the
command stops there instead of finishing the job.

---

## Reference

### The columns starting with `_`

Added to every row, so you never have to wonder where a value came from:

| Column | What it tells you |
|---|---|
| `_src_file` | which archive, and which member inside it |
| `_src_line_no` | **which line of the original file** |
| `_job_id` | which run put this row here |
| `_spec_hash` | which version of your spec was used |
| `_ingested_at` | when |

`_src_line_no` is the one you'll be glad of. When someone asks "where did this
strange value come from?", you can point at the exact line.

### What actually happens when you run it

Worth knowing, because it explains why the files end up where they do.

**1. It reads the list of what's in your zip — not the contents.** A zip has an
index at the end, like a table of contents. That's all it opens at this point, so
pointing at a 2 GB archive costs nothing until there's real work to do.

**2. Each file inside the zip gets its own worker, running at the same time.**
Every worker opens its *own* copy of the zip. That looks wasteful and isn't: a
single shared zip handle remembers where it last read from, so two workers
sharing one would keep yanking each other's place in the file.

**3. Each worker writes its own Parquet file.** One input file in, one Parquet
out, under the table it belongs to. No worker writes to the same place as any
other, so nothing has to take turns or wait for a lock.

**4. Once every worker is done, the checks run** — the same gates you saw in
dry-run, but now across all the files at once. Some things can only be checked
here: whether every file agrees on its columns, for instance, is a question about
the whole set, not about any one file.

**5. If a check fails, the Parquet is deleted and nothing is recorded.** The job
exits `3` with an empty `commit`. You get nothing rather than most of a broken
load, which is the point.

**6. If the checks pass, one final step tells Iceberg those files are now part of
the table.** Just one, single-threaded, at the very end.

That last step is the part people expect to be slow, and isn't. It does **not**
copy or rewrite your data. It opens the end of each Parquet file, where Parquet
keeps a small summary — how many rows, the smallest and largest value per column
— writes that summary into a list, and points the table at the new list. Your
files stay exactly where the workers wrote them.

Two things follow from that, and both are the reason it's built this way:

- **It's all-or-nothing.** Adding to that list is one atomic operation, so either
  every file from your run is in the table or none of it is. There's no state
  where half a drop has landed.
- **The Parquet files *are* the table.** They were never a temporary copy on the
  way somewhere else. That's why they live inside `warehouse/`, and why deleting
  one takes rows out of your table — see below.

Only that final step ever talks to Iceberg. Because it's the only writer, and
it runs once, there's never a question of two things committing at the same time
and having to sort out who won.

### Where things get saved

Everything lives under `_ffe/`, or wherever `--workspace` points:

```
_ffe/
├── warehouse/   the Iceberg tables — this is the real output
│   └── <ns>/<table>/
│       ├── data/       one Parquet per input file. NOT temporary: the commit
│       │               registers these in place, so they are the table's rows
│       └── metadata/   snapshots and manifests, written by pyiceberg
└── ledger.db    history: every run, every file, how it went
```

Nothing under `warehouse/` is scratch. A job's Parquet is registered where it
lies rather than copied, so deleting a data file removes rows from the table —
the metadata will still point at it. A job that fails a gate cleans up after
itself, so what remains is only ever live data.

To read a table in Python:

```python
from pathlib import Path
from ffe.io.sink import scan

df = scan(Path("_ffe/warehouse"), "bronze.trades")
print(df.head())

bad = scan(Path("_ffe/warehouse"), "bronze.trades_rejects")
print(bad.select(["_src_file", "_src_line_no", "_reject_reason"]))
```

Run that with `uv run python` so the imports resolve. To start completely fresh,
delete the `_ffe` folder — your input files and specs are untouched.

### All the commands

| Command | What it does | Writes anything? |
|---|---|---|
| `uv run ffe profile <file>` | measures a file, suggests a strategy | no |
| `uv run ffe lint <feed>` | checks the YAML is valid | no |
| `uv run ffe dry-run <feed> <file>` | parses a sample, shows the table | **no, by construction** |
| `uv run ffe run <feed>` | the real thing | yes |
| `uv run ffe tables` | what's in the warehouse | no |
| `uv run ffe explain <feed>` | resolved settings, how many files match, run history | no |
| `uv run ffe new-parser <name>` | scaffolds plugin + test + spec | creates files |
| `uv run ffe schema` | every setting a spec accepts | no |

Useful flags on `run`: `--table` and `--source` to override the spec for a single
run, `--workspace` to keep output somewhere disposable, `--workers` for the
fan-out width.

Every command prints JSON, and exit codes are part of the contract: **2** your
spec is wrong, **3** the file is wrong, **1** a bug in `ffe`.

### When it goes wrong

**`delimiter_mismatch`, but my delimiter is right.**
The header is being read from the wrong place, so the expected column count is
wrong. Compare `observed.columns_expected` with `observed.columns_found` in the
error, and check `header.skip_leading_fields`.

**`all_null_columns` lists a column that definitely has values.**
The columns are shifted. Usually `drop_columns` or `skip_leading_fields` is off by
one, so the data is landing under the wrong names.

**`trailer_row_count` says MISMATCH.**
Rows were lost. The file told you how many it holds and you produced a different
number, so trust it over everything else and never ship it.

**Nothing happened — 0 files found.**
`source.pattern` matched nothing. `uv run ffe explain feeds/trades.yaml` prints
`members_matched` and the first few names it resolved.

**It's slower with more workers.**
Normal, and not worth chasing. Parsing runs at millions of rows a second, so on a
small job starting extra workers costs more than it saves. The default is right
until a job takes minutes.

---

## Where to go next

- [`docs/DESIGN.md`](DESIGN.md) — why it's built this way, and the measurements
  behind the decisions
- [`docs/BUILD-PLAN.md`](BUILD-PLAN.md) — migrating existing in-house parsers, and
  what's still to build
- [`.github/copilot-instructions.md`](../.github/copilot-instructions.md) — the
  same workflow, written for an AI agent to drive
