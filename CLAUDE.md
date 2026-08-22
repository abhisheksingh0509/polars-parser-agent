# Working in this repo

The full working instructions live in
[`.github/copilot-instructions.md`](.github/copilot-instructions.md) — read that
file first and follow it. It is the single source of truth for the onboarding
workflow, the plugin contract, gates, error handling, and the performance rules.

It is kept there rather than here so that GitHub Copilot picks it up
automatically on the Windows machines this project is also developed on. There is
no separate Claude-specific guidance; nothing in that file is Copilot-specific.

## macOS / Linux equivalents

That file spells commands out for PowerShell. Locally:

| Windows | macOS / Linux |
|---|---|
| `.venv\Scripts\ffe.exe` | `.venv/bin/ffe` |
| `.venv\Scripts\python.exe` | `.venv/bin/python` |
| `.venv\Scripts\Activate.ps1` | `source .venv/bin/activate` |
| `$LASTEXITCODE` | `$?` |
