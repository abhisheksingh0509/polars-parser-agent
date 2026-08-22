# Runbook — from an approved sample to production data

`/onboard-feed` stops the moment `gates.passed` is true on a 100-row sample. This
file is what happens next: pointing that spec at the real drop and landing it in
Iceberg, without going back and forth.

The worked example throughout is **`msci-test`**, onboarded from
`tests/fixtures/msci_test.txt` and pointed at `data/msci_test/*.zip`. Substitute
your own feed name and paths. Its shape — **one zip per business day, several
files inside** — drives the partitioning discussion at the end; the numbers in
the steps come from a hand-built sample archive and are illustration, not the
real volumes.

**Preconditions:** `gates.passed: true` on the sample, `uv run pytest` green, and
a human has looked at the DataFrame. If any of those is false, you are still in
onboarding — go back to
[`.github/copilot-instructions.md`](../.github/copilot-instructions.md).

---

## The short version

```bash
uv run pytest -q                                                  # 1. suite green
uv run ffe lint     feeds/msci-test.yaml                          # 2. spec valid
uv run ffe explain  feeds/msci-test.yaml                          # 3. members resolve
unzip -o data/msci_test/*.zip -d /tmp/msci                        # 4. extract, then
uv run ffe dry-run  feeds/msci-test.yaml /tmp/msci/<member>.txt   #    dry-run each member
uv run ffe run      feeds/msci-test.yaml --table sandbox.msci_test --workspace ./_x
uv run ffe tables   --workspace ./_x                              # 5. trial run, verify
uv run ffe run      feeds/msci-test.yaml                          # 6. for real
                    # ... --option business_date=2026-08-23 to override the file
uv run ffe tables                                                 # 7. verify
```

Steps 1–5 write nothing to the real warehouse. Step 6 is the only irreversible
one — see [Re-running is not idempotent](#re-running-is-not-idempotent) before
you run it twice.

---

## Step 1 — suite green

```bash
uv run pytest -q
```

Expect `30 passed, 1 skipped` plus your feed's generated test. The skip is the
notebook test (`uv run --all-extras pytest -q` gives 32 passed). A red suite
before you touch production data means you are debugging two things at once.

## Step 2 — point the spec at the real drop

Onboarding writes `source.pattern` with a `# TODO` marker because it only ever
saw a loose sample file. Set it to where the files actually arrive and delete the
marker:

```yaml
source:
  pattern: "data/msci_test/*.zip"   # file, zip, directory, or glob
  members: "*"                      # which entries inside the archive
```

`pattern` is resolved with `glob` relative to the **current working directory**,
so run `ffe` from the repo root or make the pattern absolute. A pattern matching
several archives is a **multi-day backfill** — see
[The unit of a business day is the zip](#the-unit-of-a-business-day-is-the-zip). `members` is
matched with `fnmatch` against both the full archive path and the bare filename,
so `"*.txt"` catches `sub/dir/taxonomy.txt`.

```bash
uv run ffe lint feeds/msci-test.yaml
```

Emits `name`, `parser`, `target`, and `spec_hash`. Exit 2 means the YAML does not
match the schema — `uv run ffe schema` prints the accepted shape.

Note `spec_hash` covers the **parser block only** — `name`, `source`, `target`,
and `policy` are excluded. Changing the output table does not change the hash;
changing parse logic does. That is deliberate: the hash on every landed row
answers "which parse produced this", not "which table did it go to".

## Step 3 — confirm the framework sees every member

```bash
uv run ffe explain feeds/msci-test.yaml
```

| Field | For `msci-test` | If it's wrong |
|---|---|---|
| `members_matched` | `3` | `0` → `source.pattern` doesn't match. `1` on a multi-file zip → `members` glob is too narrow. |
| `members_sample` | `mock_taxonomy_data.zip!taxonomy_setup_20260823.txt`, … | The `zip!member` form confirms the archive was opened, not treated as one opaque file. |
| `recent_jobs` | `[]` on a first run | Non-empty means this feed already landed — read [Re-running](#re-running-is-not-idempotent). |

This reads the zip **central directory only**. No member is decompressed and no
parser runs, so it is safe against a multi-GB drop.

## Step 4 — dry-run the members the sample never covered

> **`dry-run` takes a plain file, not an archive.** It does `sample.read_bytes()`
> and hands those bytes straight to the parser. Point it at a `.zip` and the
> parser receives compressed binary. Extract first.

```bash
mkdir -p /tmp/msci && unzip -o data/msci_test/mock_taxonomy_data.zip -d /tmp/msci
for f in /tmp/msci/*.txt; do
  echo "== $f"; uv run ffe dry-run feeds/msci-test.yaml "$f" | head -40
done
```

`gates.passed` is the only verdict. When false:

| Gate | Non-zero means | Fix |
|---|---|---|
| `ragged_lines` | field count disagrees with the header | delimiter or header config |
| `unknown_tags` | a record tag the spec doesn't declare | add it to `structure.records` |
| `reject_ratio` | rows failed to cast, over policy | fix the parser — **never widen `max_reject_ratio`** |
| `all_null_columns` | almost always column misalignment | check `skip_leading_fields` / `drop_columns` |
| `trailer_row_count` | `MISMATCH` = rows were lost | never ship this; the declared count is ground truth |

Two things to check across members that a single dry-run cannot show you:

1. **Identical `schema` keys and dtypes in every member.** The commit stages one
   Parquet per member and `sink.commit` refuses a set that does not share one
   schema. Catching that here costs seconds; catching it at commit time costs a
   failed job. For `msci-test` the risk is real but currently benign: the plugin
   derives column names from each member's own header row and only falls back to
   `FALLBACK_COLUMNS` if there isn't one. All three members carry the same
   header, so they agree.
2. **`trailer_row_count: "match"` on each.** Sum the declared counts across
   members — that total is what step 7 must show. In the sample archive it is
   5 + 3 + 4 = **12**.

`blame` in an error payload decides your next move, and the exit code mirrors it:
`spec` / exit 2 → edit the YAML and retry; `file` / exit 3 → the file is
truncated or corrupt, **stop retrying** and go ask a human; exit 1 → a bug in
`ffe`, report it rather than working around it.

## Step 5 — trial run into a sandbox

Identical to production except for the table and the workspace, so a surprise
costs nothing:

```bash
uv run ffe run    feeds/msci-test.yaml --table sandbox.msci_test --workspace ./_x
uv run ffe tables --workspace ./_x
```

`--workspace` holds staging, the job ledger, and the warehouse; `./_x` is
gitignored. Expect:

```
status: ok      members_ok: 3      rows: 12      commit.files: 3
commit.snapshot_id: <not null>
```

`files: 3` with **one** `snapshot_id` is the whole design in one line: three
workers staged three Parquet files in parallel, and a single writer registered
them as one atomic snapshot. More than one snapshot means something committed
twice.

Then throw it away: `rm -rf ./_x`.

## Step 6 — the real run

```bash
uv run ffe run feeds/msci-test.yaml
```

Writes to `target.table` in the default `./_ffe` workspace. This is the step
`/onboard-feed` deliberately cannot perform — `ffe run` is outside its
`allowed-tools` so that landing data always costs a human decision.

`status` is `ok` (everything parsed), `partial` (some members failed but the
reject ratio held — **check `errors`**), or `failed` (nothing committed).

Two gates can refuse the commit outright, and both leave the warehouse untouched:

- `reject_ratio` over `policy.max_reject_ratio`
- any member failing while `policy.on_reject: fail`

Tuning for throughput, in the order worth trying: `--workers N`, then
`--executor serial` for small members. **Do not reach for `--executor process`
"for speed"** — it lost every benchmark, because each worker re-imports Polars,
and it does not pay off until roughly 4M rows.

## Step 7 — verify what landed

```bash
uv run ffe tables
uv run ffe explain feeds/msci-test.yaml     # recent_jobs now has this job_id
```

Check `rows` equals the trailer total from step 4 (12 for `msci-test`), and that
the column list carries the five lineage columns added at staging time:

| Column | Is |
|---|---|
| `_src_file` | `archive.zip!member.txt` — which member the row came from |
| `_src_line_no` | line in that member; added by the **parser**, not the framework |
| `_job_id` | the run |
| `_spec_hash` | which parse logic produced it |
| `_ingested_at` | UTC naive `Datetime("us")` — **load** time, not business date |

The business date is a *parser* column, not lineage — for `msci-test` it is
`business_date`. See [Supplying the business date](#supplying-the-business-date).

To read rows back:

```python
from ffe.io import sink
df = sink.scan(Path("_ffe/warehouse"), "bronze.msci_test")
```

## Step 8 — when a production file fails after the sample passed

Expected. A 100-row sample cannot contain the one row in 4 million with an
embedded delimiter, and the reject table is the feedback loop:

```sql
SELECT _src_file, _src_line_no, _reject_reason
FROM bronze.msci_test_rejects WHERE _job_id = '<job>' LIMIT 50
```

Rejects land in `<table>_rejects` as their own commit, so a partial job is still
fully traceable. The loop is ordinary engineering: add the failing shape to
`tests/fixtures/`, tighten the parser, re-run the test, re-run the feed.
`_reject_reason` holds the **original** value, so the row is reconstructable.

Never widen `max_reject_ratio` to make a job pass.

---

## Re-running is not idempotent

`ffe run` allocates a fresh `job_id` every invocation and never consults the
ledger before processing. Running the same feed against the same drop twice
appends the rows twice, under two `job_id`s, in two snapshots. Nothing warns you.

So before re-running: `uv run ffe explain <spec>` and look at `recent_jobs`. If
the drop already landed, either move the source files aside or roll the Iceberg
table back to the prior snapshot. Dedupe downstream on `_src_file` +
`_src_line_no` if you need it cheaply.

## The unit of a business day is the zip

**One archive holds exactly one day's data.** It may hold many files, but they
all belong to the same business date. This is a property of the feed, not of
`ffe`, and it is the assumption everything below rests on — check it before
reusing any of this for another feed.

> The zip currently in `data/msci_test/` is a hand-built **sample** and violates
> this: it carries three dates in one archive. Don't model the real naming
> convention or the partition design on it.

### Generating test drops

`msci-test-gen.py` builds archives in the real shape — one per business day,
several members inside, each member stating its own date and row count:

```bash
uv run python msci-test-gen.py --clean               # a week into data/msci_test
uv run python msci-test-gen.py --days 14 --parts 4
```

Output is deterministic, so regenerating produces identical bytes rather than
churn. Two flags exist to exercise the failure paths rather than the happy one:

| Flag | Produces | Exercises |
|---|---|---|
| `--drift` | a `Region` column from day 4 onwards | `policy.schema_change` — fails by default, lands with `evolve` |
| `--bad-rows N` | N malformed rows per archive | the reject table and `max_reject_ratio` |

`--bad-rows` will trip the default `max_reject_ratio: 0.01`, which is the point;
raise it **in a sandbox spec only**, never in the production one.

Two consequences worth internalising:

- **The business date belongs to the archive, not the member.** Deriving it from
  a member's filename is reading the wrong thing — a real member may not carry a
  date at all, and if it does it is redundant.
- **A glob that matches several zips is a multi-day job**, and that is fine —
  it is how a backfill works. What must stay true is that each *staged file*
  carries a single date, which the assumption gives you for free.

## Supplying the business date

Two ways, and they layer. `engine` merges `{**spec.options, **ctx.options}`, so
precedence is **per-run beats per-spec beats the file** — someone passing a date
explicitly is correcting what the drop says.

**1. The parser reads it from the file.** Each member carries
`# Generated: YYYY-MM-DD`, so `plugins/msci_test.py` parses that into a
`business_date` column of type `Date`. Nothing to pass; the common case is free.

**2. The run supplies it.** `--option key=value`, repeatable, on both `run` and
`dry-run`:

```bash
uv run ffe run     feeds/msci-test.yaml --option business_date=2026-08-23
uv run ffe dry-run feeds/msci-test.yaml sample.txt --option business_date=2026-08-23
```

For a backfill of a mislabelled drop, or any feed whose files don't state their
own date. The flag is deliberately generic rather than a `--date`: the framework
has no opinion on what a parser wants, exactly as with `parser.options` in the
YAML. Values arrive as **strings**; interpreting them is the plugin's job.

A date that won't parse lands **null** rather than raising. A silently-wrong date
is worse than an empty one, and `all_null_columns` puts it in front of you on the
next dry-run.

Same mechanism for anything else per-run — a cutoff, a region, a mode. Read it in
the plugin from `ctx.options`, never from a module constant.

## Schema drift — when the feed grows a column

`policy.schema_change` decides, and it defaults to `fail`:

```yaml
policy:
  schema_change: fail     # fail | evolve
```

**`fail`** refuses the commit and lands nothing. The error names the knob and the
column, and `blame: spec` / exit 2 means you can fix it:

```json
{"error": "schema_drift", "field": "policy.schema_change",
 "observed": {"distinct_schemas": 2, "columns_not_in_every_file": ["region"]},
 "hint": "Set policy.schema_change to 'evolve' to add the new columns as nullable…",
 "blame": "spec"}
```

Default to leaving this alone. A column you did not expect is more often a parser
bug — a misread header, a shifted delimiter — than a real upstream change, and
failing costs you one job while evolving on a bug quietly corrupts a table.

**`evolve`** adds the new columns as nullable and commits. It catches drift in
both directions:

- *within one job* — members of the same archive disagree
- *across jobs* — the table already exists and today's drop is wider

Rows loaded before the column existed read back `null`; a member missing a column
the table has is always fine, since Iceberg reads an absent column as null.

**Evolution only ever adds.** A column whose *type* changed is still a hard
failure under `evolve`, with its own error — adding a column is safe, retyping
one rewrites the meaning of data already committed. Do that deliberately, not as
a side effect of a load.

## Partitioning

`target.partition_by` takes column names and gives the table an identity
partition spec:

```yaml
target:
  table: bronze.msci_test
  partition_by: [business_date]
```

Partition on the **business date, not `_ingested_at`** — a re-load must land in
the day it belongs to, not the day you ran it.

This is cheap here because of the feed's shape. `add_files` refuses a Parquet
file spanning two partition values, so normally partitioning means splitting
frames before staging. But staging already writes **one file per member**, and
one archive is one business day, so every staged file holds exactly one date
already. No split, no rewrite: `add_files` still just reads the footers.

The invariant that actually matters is **per file**, not per archive. An archive
mixing days is fine as long as each member inside it is single-day — that is why
the three-date sample archive still partitions correctly into three days.

Three refusals, each a structured error rather than a crash:

| Error | When | Fix |
|---|---|---|
| `partition_column_missing` | `partition_by` names a column the parser doesn't emit | name one it does — `dry-run` prints the schema |
| `partition_spec_conflict` | the table already holds data laid out differently | a load never re-lays-out an existing table; write to a new one or migrate deliberately |
| `mixed_partition_file` | one member spans two partition values | split the source, or partition on something constant within a member |

The second is the one to expect: **adding `partition_by` to a feed that has
already landed data will fail**, by design. Prove the change on a sandbox table
(`--table sandbox.x --workspace ./_x`), then point it at a fresh production
table.

Two things that surprise people:

- **No Hive-style directories appear.** `add_files` registers files where they
  already sit and never moves them; partitioning is metadata. Pruning works
  regardless — `table.scan(row_filter="business_date == '2026-08-23'")` reads
  only the matching files.
- **The reject table is deliberately not partitioned.** A rejected row may have
  failed on the very column being partitioned on. Rejects are small and you
  query them by `_job_id`.

## Step 9 — commit the feed

The sample is the regression test, and every feed onboarded makes the suite
stronger. Commit:

```
feeds/<feed>.yaml
plugins/<feed>.py            # only if it needed a plugin
tests/fixtures/<sample>      # <=100 rows, scrubbed
tests/test_<feed>.py
```

Not the production data — `data/` is gitignored, and so are the `_ffe` / `_x`
workspaces. `.gitattributes` marks `tests/fixtures/**` binary so git never
rewrites line endings; leave that alone, it is pinned by
`test_crlf_and_bom_parse_identically_to_lf`.

Then log the feed in the [`BUILD-PLAN.md`](BUILD-PLAN.md) Part 2 table — name,
strategy, plugin or config-only, date. That table is how you learn whether the
built-in strategies are pulling their weight or whether a fourth is warranted.
