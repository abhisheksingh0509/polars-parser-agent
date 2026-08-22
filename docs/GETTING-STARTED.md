# Getting started

Plain-language walkthrough. No prior knowledge of this project needed.

## What this is

Some data files are easy — a normal CSV that any tool can open. Others are not:
they have banner lines, a header written in their own mini-language, a letter at
the start of each row saying what kind of row it is, a trailer at the bottom. Ask
ten vendors for reference data and you get ten shapes.

Someone has to write code that reads each of those shapes. That part is fine and
unavoidable — the person who knows the feed should own it.

The problem is what surrounds it. Every one of those little parsers ends up also
doing: open the zip, loop the files inside, run them in parallel, write the
output, save it to the warehouse, deal with the rows that were bad, remember
where each row came from. That's the same work every time, it's most of the code,
and it's where the bugs live.

**This project does all of that for you.** You describe your file, or in hard
cases write a small function that reads one file. Everything else is handled.

## The idea in one picture

```
your zip file                                            Iceberg table
  ├── file1.txt  ─┐                                    ┌─ bronze.issues
  ├── file2.txt  ─┤   read in parallel    save once     │
  ├── file3.txt  ─┼─►  by N workers   ─►  in one go  ─► │
  └── ...        ─┘                                     └─ bronze.issues_rejects
                                                             (the bad rows)
```

You supply: **what the file looks like** and **which table it goes in.**
You get: parallel reading, bad-row handling, and full traceability, free.

---

## Step 1 — Install it

You need Python **3.12** exactly. Newer versions don't have working versions of
the storage libraries yet.

**Windows (PowerShell):**
```powershell
uv venv --python 3.12
uv pip install -e ".[dev]"
```

**Mac / Linux:**
```bash
uv venv --python 3.12 && uv pip install -e ".[dev]"
```

Check it worked:
```bash
pytest -q                    # should say 30 passed
python scripts/smoke.py      # should say smoke test OK
```

Nothing else to install. No database, no Docker, no cloud account.

Below, `ffe` means `.venv/bin/ffe` on Mac/Linux or `.venv\Scripts\ffe.exe` on
Windows.

---

## Step 2 — Look at your file first

Take a small sample of your file — **100 lines is plenty**. Ask the tool what it
sees:

```bash
ffe profile mysample.txt
```

It measures the file and prints what it found. The part to read is
`structure_hint`:

```json
"structure_hint": {
  "strategy": "record_tag",
  "delimiter": "|",
  "tags": ["E", "F", "H", "I"],
  "why": "field 0 holds a few short repeated codes and line width varies by code"
}
```

That's the tool saying: *"each row starts with a letter that says what kind of row
it is, and the columns are separated by pipes."*

There are four possible answers:

| It says | Meaning | What to do |
|---|---|---|
| `native` | It's a normal CSV | Easiest case. Step 3. |
| `sentinel` | Lines of `***` split the file into sections | Step 3. |
| `record_tag` | Each row starts with a code letter | Step 3. |
| `unknown` | It can't work this one out | Skip to "Hard files" below. |

No AI is involved here. It's counting characters.

---

## Step 3 — Describe your file

Make a file in `feeds/`, for example `feeds/my-feed.yaml`. **Don't write it from
scratch** — copy the closest example:

- `feeds/pipe-tagged-feed.yaml` — if the hint said `record_tag`
- `feeds/banner-ddl-feed.yaml` — if the hint said `sentinel`

A feed file says three things:

```yaml
name: my-feed

source:                                   # 1. WHERE THE FILES COME FROM
  pattern: "data/drops/*.zip"             #    a file, a zip, a folder, a pattern
  members: "*.txt"                        #    which files inside the zip

parser:                                   # 2. WHAT THE FILE LOOKS LIKE
  kind: sectioned
  ...                                     #    copied from an example

target:                                   # 3. WHERE IT GOES
  table: bronze.my_data                   #    your output table name

policy:
  max_reject_ratio: 0.01                  #    if more than 1% of rows are bad,
                                          #    stop and save nothing
```

Check the file is valid before using it:

```bash
ffe lint feeds/my-feed.yaml
```

---

## Step 4 — Test it without saving anything

This is the important step. `dry-run` reads your sample and shows you the result.
**It cannot save anything anywhere**, so try it as many times as you like:

```bash
ffe dry-run feeds/my-feed.yaml mysample.txt
```

Look at `gates` first:

```json
"gates": { "ragged_lines": 0, "unknown_tags": 0, "reject_ratio": 0.0,
           "trailer_row_count": "match", "passed": true }
```

`passed: true` means it read the file cleanly. Then look at `head` — that's your
actual data as a table. **Check the columns are what you expect** before going
further.

### If it didn't work

You get a message that tells you what to change. You don't have to guess:

```json
{ "error": "delimiter_mismatch",
  "field": "parser.data.delimiter",
  "candidates": [ { "value": "|", "columns": 5, "consistency": 1.0 } ],
  "hint": "Set parser.data.delimiter to '|'." }
```

Read it as: *"the setting called `parser.data.delimiter` is wrong; try `|`, which
gives 5 columns on every line."* Change that one line in your YAML and run
`dry-run` again.

One field matters a lot — `blame`:

- `"blame": "spec"` → **your description is wrong.** Fix the YAML and retry.
- `"blame": "file"` → **the file itself is broken** (cut short, corrupted). Don't
  keep trying. Go ask whoever sent it.

---

## Step 5 — Run it for real

Only after `dry-run` looks right:

```bash
ffe run feeds/my-feed.yaml
```

This one *does* save. It reads all the files in the zip at the same time, then
saves everything into the table in a single step.

Want it somewhere else without editing the YAML? Override it:

```bash
ffe run feeds/my-feed.yaml --table sandbox.just_testing
```

The result tells you what happened:

```json
{ "ok": true, "members": 40, "rows": 1639, "rejects": 0,
  "commit": { "table": "bronze.my_data", "files": 40, "snapshot_id": 4118718724834384944 } }
```

40 files read, 1639 rows saved, all in **one** save (`snapshot_id`).

---

## Step 6 — Look at what landed

```bash
ffe tables
```

```
bronze.my_data              1639 rows  40 files  1 snapshot(s)
bronze.my_data_rejects         0 rows   0 files  0 snapshot(s)
```

To actually look at the data, open
[`notebooks/explore.ipynb`](../notebooks/explore.ipynb). It walks through every
step above and shows the tables as you go.

Or in a few lines of Python:

```python
from pathlib import Path
from ffe.io.sink import scan

df = scan(Path("_ffe/warehouse"), "bronze.my_data")
print(df.head())
```

You'll notice extra columns starting with `_`. Those are added for you:

| Column | What it tells you |
|---|---|
| `_src_file` | which zip and which file inside it |
| `_src_line_no` | **which line of the original file** |
| `_job_id` | which run put this row here |
| `_spec_hash` | which version of your description was used |
| `_ingested_at` | when |

`_src_line_no` is the one you'll thank yourself for. When someone asks "where did
this weird value come from?", you can point at the exact line.

---

## Step 7 — When some rows are bad

Bad rows never stop the job. They go to a separate table with the reason:

```python
bad = scan(Path("_ffe/warehouse"), "bronze.my_data_rejects")
print(bad.select(["_src_file", "_src_line_no", "_reject_reason"]))
```

```
issues.zip!issue_003.txt   8   id: cannot cast 'NOTANUMBER' to int64
```

The bad row keeps its **original text**, so you can see what actually arrived.

**But if too many rows are bad, nothing is saved at all.** That's
`max_reject_ratio`. The point is to never quietly load 80% of a broken file and
have someone find out three weeks later.

If that happens: look at the reject rows, fix your description (or go back to the
sender), and re-run. **Don't raise `max_reject_ratio` to make the error go away.**

---

## Hard files: when the built-ins can't read yours

Some formats can't be described in YAML — fixed-width columns are the usual case,
where field positions are counted in characters instead of separated by a
delimiter.

For those, you write a small function. Start with:

```bash
ffe new-parser my-weird-feed
```

That creates three files for you, already the right shape:

- `plugins/my_weird_feed.py` — where your code goes
- `tests/test_my_weird_feed.py` — a test, ready to fill in
- `feeds/my-weird-feed.yaml` — the feed description

Open the plugin file and fill in one function: it gets the contents of one file
and returns a table. **That's all you write.** No zip handling, no saving, no
parallelism — that's still done for you.

There's a complete working example to copy: `plugins/acme_positions.py`.

Two rules for your function:

1. **Never crash on a bad row.** Put it in `rejects` with a reason instead.
2. **Add `_src_line_no`** to every row, so bad rows can be traced.

Then go back to Step 4 and carry on as normal.

---

## Where things are saved

Everything lives under `_ffe/` (or whatever you pass to `--workspace`):

```
_ffe/
├── staging/     temporary files, one per input file. Safe to delete.
├── warehouse/   the Iceberg tables. This is the real output.
└── ledger.db    history: every run, every file, how it went
```

To see the history of a feed:

```bash
ffe explain feeds/my-feed.yaml
```

To throw everything away and start fresh, delete the `_ffe` folder. Your input
files and your feed descriptions are untouched.

---

## All the commands

| Command | What it does | Saves anything? |
|---|---|---|
| `ffe profile <file>` | measures a file, suggests a strategy | no |
| `ffe lint <feed>` | checks your YAML is valid | no |
| `ffe dry-run <feed> <file>` | reads a sample, shows the table | **no** |
| `ffe run <feed>` | the real thing | yes |
| `ffe tables` | what's in the warehouse | no |
| `ffe explain <feed>` | settings + run history | no |
| `ffe new-parser <name>` | creates plugin + test + feed files | creates files |
| `ffe schema` | all the settings a feed file accepts | no |

---

## Common problems

**"It says `delimiter_mismatch` but my delimiter is right."**
The header is probably being read from the wrong place, so the tool expects the
wrong number of columns. Check `observed.columns_expected` against
`observed.columns_found` in the error.

**"All my columns are text, I wanted numbers."**
Types are not guessed — that's on purpose, so a column doesn't silently change
type next month. Say what you want:
```yaml
coerce:
  schema_override: { id: int64, amount: float64 }
```

**"My dates came out as text."**
Correct, and deliberate. This tool saves data the way the file wrote it. Convert
dates in whatever you use downstream.

**"Nothing happened — 0 files found."**
Your `source.pattern` didn't match anything. Check it with
`ffe explain feeds/my-feed.yaml` — it shows how many files matched.

**"It's slower with more workers."**
Normal. Reading is very fast, so on small jobs starting extra workers costs more
than it saves. The default is already the right setting; leave it alone unless a
job takes minutes.
