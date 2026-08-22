# Runbook — from an approved sample to production data

`/onboard-feed` stops the moment `gates.passed` is true on a 100-row sample. This
file is what happens next: pointing that spec at the real drop and landing it in
Iceberg, without going back and forth.

The worked example throughout is **`msci-test`**, onboarded from
`tests/fixtures/msci_test.txt` and pointed at `data/msci_test/*.zip`. Substitute
your own feed name and paths.

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
so run `ffe` from the repo root or make the pattern absolute. `members` is
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
2. **`trailer_row_count: "match"` on each.** For `msci-test` that is 5, 3, and 4
   rows — **12 total**, the number step 7 must show.

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

## Known gap — no business date, and no partitioning

Two distinct gaps that get conflated. Both are real today.

**1. Tables are unpartitioned.** `sink.commit` calls
`create_table_if_not_exists(identifier, schema)` with no `PartitionSpec`, and
`FeedSpec.Target` accepts only `table`. Every commit appends to one flat table.
Scoped as [`BUILD-PLAN.md` §3.1](BUILD-PLAN.md) — `target.partition_by`, a
partition-aware split in `staging.py` so one staged file maps to exactly one
partition value, and a `PartitionSpec` in `sink.py`. The staging split is the
part that matters: `add_files` is markedly stricter about partitioned tables, and
a file spanning two partition values will be refused.

**2. There is no business date to partition *on*.** `_ingested_at` is when the
loader ran — re-load a 2026-08-23 file today and it is stamped today. The feed's
own date lives in the data (`taxonomy_setup_20260823.txt`, and a
`# Generated: 2026-08-23` comment inside each member), and nothing currently
lifts it onto the rows.

Three ways to supply it, in increasing order of work:

| Approach | How | Cost |
|---|---|---|
| **Parser derives it** | The plugin already receives `ctx.member` (`archive.zip!taxonomy_setup_20260823.txt`). Parse the date out and emit a `business_date` column. | Small, and it is source-faithful — the value comes from the file. **Best fit for `msci-test` today.** |
| **Static, via the spec** | `parser.options` in the YAML reaches the plugin as `ctx.options` — `engine.py` merges `{**spec.options, **ctx.options}`. Read a tunable from there. | Free, but it is per-**spec**, not per-**run**. Wrong for a value that changes each drop. |
| **Per-run CLI argument** | Not implemented. `ffe run` exposes `--table`, `--source`, `--workers`, `--executor`; there is no `--option k=v` or `--date`, and `runner.py` constructs `ParseContext(member=…, job_id=…)` without threading anything else through. | Requires a CLI flag, a field on `FeedSpec` or the run call, and plumbing into `ParseContext`. |

Note the design tension before picking: `promote_fields` already lifts a business
date onto every row for `sectioned` feeds, and
[`DESIGN.md` open question 5](DESIGN.md) asks whether that belongs in bronze at
all, since it is a transform and bronze is meant to be source-faithful. Deriving
the date from the filename is the same argument in a different coat — worth
deciding once, for all feeds, rather than per feed.

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
