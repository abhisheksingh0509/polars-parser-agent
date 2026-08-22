---
description: Onboard a flat-file feed from a sample — profile it, write the spec, and iterate dry-run until the gates pass
argument-hint: <path/to/sample.txt> [target.table]
allowed-tools: Bash(uv run ffe profile:*), Bash(uv run ffe lint:*), Bash(uv run ffe dry-run:*), Bash(uv run ffe explain:*), Bash(uv run ffe tables:*), Bash(uv run ffe schema:*), Bash(uv run ffe new-parser:*), Bash(uv run pytest:*), Read, Write, Edit, Glob, Grep
---

Onboard the sample file `$1` as a feed in this repo. Target table: `$2` — if that
was not given, use `bronze.<sample stem>` and say so rather than asking.

Follow the workflow in `.github/copilot-instructions.md`. It is the contract; this
file only sequences it. **Never write ingestion code** — no zip handling, no
Parquet, no pyiceberg. If you find yourself there, you are in the wrong layer.

## 1. Measure before deciding anything

```
uv run ffe profile $1
```

Report `structure_hint.strategy` and its `why` verbatim. This is arithmetic, not
inference — do not argue with it. It reads the first 100 lines only.

## 2. Copy the nearest spec, never write one from scratch

| `strategy` | Copy | Then |
|---|---|---|
| `native` | nothing — six lines of YAML, `kind: native` | set `delimiter` |
| `sentinel` | `feeds/banner-ddl-feed.yaml` | set `sentinel`, check the block ordinals |
| `record_tag` | `feeds/pipe-tagged-feed.yaml` | set `delimiter`, rewrite `records` from the profile's tags |
| `unknown` | go to step 6 | |

Write it to `feeds/<name>.yaml`. Take `delimiter` and the record tags from the
profile output, not from a guess. `uv run ffe schema` prints the full accepted
shape if you need a field you have not seen before.

Then validate the shape, which never opens the data file:

```
uv run ffe lint feeds/<name>.yaml
```

## 3. Iterate dry-run — this is the loop

```
uv run ffe dry-run feeds/<name>.yaml $1
```

`dry-run` cannot write anywhere, by construction, so run it as often as you need.
Read the result in this order:

**First, `gates.passed`.** Not the exit code — `dry-run` exits 0 even when the
gates fail. A non-zero exit means it could not parse at all; failing gates mean it
parsed and the result is not trustworthy. Both need work, for different reasons.

**On a structured error**, act on `field`, `candidates` and `hint`. Change exactly
that one field and retry. Do not guess at a second change in the same pass — you
lose the signal about which one worked. `blame` decides whether to continue:

- `"blame": "spec"` (exit 2) — yours to fix. Edit the YAML, retry.
- `"blame": "file"` (exit 3) — **stop immediately.** The file is truncated or
  corrupt. No spec change helps. Report it and let the human go back to the sender.

**On failing gates**, fix the cause named by the gate:

| Gate | Almost always means |
|---|---|
| `ragged_lines` | wrong delimiter, or the header is being read from the wrong block |
| `unknown_tags` | a record code the spec does not declare — add it, or the file changed |
| `all_null_columns` | columns are shifted; check `drop_columns` and `skip_leading_fields` |
| `reject_ratio` | values that will not cast — see below |
| `trailer_row_count` | `MISMATCH` means rows were lost. Exact ground truth. Never ship it |

**Never widen `max_reject_ratio` to make a gate pass.** That number exists to stop
a broken file landing quietly. If rejects are legitimate, the spec's types are
wrong; if the file is broken, that is the human's problem to take up with the
sender. Say which one you think it is.

Stop after **five** attempts and report what you changed, what each attempt
produced, and where you are stuck. Do not keep cycling.

## 4. Types, deliberately

Every column arrives as `String` unless the spec says otherwise — inference is not
a runtime behaviour here, so a vendor cannot silently change a column's type next
month. Read `head`, propose `coerce.schema_override` for the columns that are
clearly numeric, and leave dates as text unless asked.

## 5. Show your work and stop

When `gates.passed` is true, print for the human:

- `head` — the actual rows
- `schema` — final column names and dtypes
- any `rejects`, with `_reject_reason` and `_src_line_no`
- the diff of the spec you wrote

**Then stop and ask.** `ffe run` writes to the warehouse and is deliberately not
in this command's allowed tools, so it will ask for permission — do not treat that
prompt as a formality to click through. When the human approves, the safe first
real run is against the sample, into a throwaway workspace:

```
uv run ffe run feeds/<name>.yaml --source $1 --table sandbox.<name> --workspace _ffe/scratch
uv run ffe tables --workspace _ffe/scratch
```

Exit 3 with an empty `commit` means a gate refused the load and nothing was
written — that is the system working, not a failure to work around.

## 6. If the strategy was `unknown`

The format needs code — usually fixed-width, which is deliberately not a built-in.

```
uv run ffe new-parser <name>
```

Implement `parse()` in the generated `plugins/<name>.py`, copying the shape of
`plugins/acme_positions.py`. Two rules that are not negotiable: **never raise on a
bad row** (append it to `rejects` with a `_reject_reason` holding the original
value), and **add `_src_line_no` to every row**, good and rejected, or a rejected
row cannot be traced. Do not import anything from `ffe.io`. Then return to step 3.

## 7. Pin it, or the work does not survive

Once the human has approved the DataFrame:

1. Copy the sample to `tests/fixtures/<stem>.txt` — it is committed, so confirm
   with the human that it carries nothing sensitive.
2. Write `tests/test_<stem>.py` asserting the **real** expectations: exact column
   list, the dtypes that matter, the row count, and that rejects are empty.
   `assert len(frame) > 0` is worthless — it passes on a completely misaligned
   parse.
3. `uv run pytest -q` and report the count.

Finish with a summary: the strategy chosen, what you changed and why, the final
gates, and the test you added.
