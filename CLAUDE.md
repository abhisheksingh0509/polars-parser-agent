# Working in this repo

The full working instructions live in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md) — read that
file first and follow it. It is the single source of truth for the onboarding
workflow, the plugin contract, gates, error handling, and the performance rules.

It is kept there rather than here because that path is picked up automatically by
both Claude Code and GitHub Copilot. There is no separate Claude-specific
guidance; nothing in that file is Copilot-specific.

## Onboarding a feed

`/onboard-feed <sample> [table]` runs the whole loop — profile, write the spec,
iterate `dry-run` on the structured errors, then stop for a human to approve the
DataFrame. It lives in
[`.claude/commands/onboard-feed.md`](.claude/commands/onboard-feed.md) and
sequences the workflow in the instructions above rather than restating it.

`ffe run` is deliberately outside that command's `allowed-tools`: writing to the
warehouse should cost a permission prompt.

What happens *after* that approval — pointing the spec at the real drop, the
sandbox trial run, verifying the commit, and the fact that re-running a feed
double-loads it — is [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

## Platform

This repo runs on **macOS**. Nothing here needs to work on Windows: Windows is
used only to recreate the project from scratch out of
[`docs/DESIGN.md`](docs/DESIGN.md) and
[`docs/BUILD-PLAN.md`](docs/BUILD-PLAN.md) Part 5, which is why those two files
carry the platform traps and the build order.

## Commands

Everything runs through `uv` — `uv sync --extra dev` once, then `uv run` in front
of every command:

```bash
uv run ffe <verb> …
uv run pytest -q                    # 30 passed, 1 skipped
uv run python scripts/smoke.py      # 19/19, smoke test OK
```

`uv.lock` is committed, so `uv sync` resolves identically every time. Don't hand
uv an interpreter — `requires-python` pins 3.12 and it picks that itself.
