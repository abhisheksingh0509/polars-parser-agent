# Working in this repo

The full working instructions live in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md) — read that
file first and follow it. It is the single source of truth for the onboarding
workflow, the plugin contract, gates, error handling, and the performance rules.

It is kept there rather than here so that GitHub Copilot picks it up
automatically on the Windows machines this project is also developed on. There is
no separate Claude-specific guidance; nothing in that file is Copilot-specific.

## Commands

Everything runs through `uv` — `uv sync --extra dev` once, then `uv run` in front
of every command (`uv run ffe …`, `uv run pytest`, `uv run python scripts/smoke.py`).
That is identical here and on Windows, so there is nothing to translate and no
`.venv/bin/…` or `.venv\Scripts\…` path to construct.

The one platform difference: the exit code, which the instructions call
`$LASTEXITCODE`, is `$?` in zsh.
