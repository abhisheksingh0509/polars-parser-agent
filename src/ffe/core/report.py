"""ParseReport: one object, read by the ledger, the human, and whatever agent
is driving dry-run. Deliberately not three metric paths."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

import polars as pl


@dataclass
class ColumnStat:
    name: str
    dtype: str
    null_count: int
    cast_failures: int = 0


@dataclass
class ParseReport:
    parser: str
    lines_total: int = 0
    lines_consumed: int = 0
    rows_parsed: int = 0
    rows_rejected: int = 0
    ragged_lines: int = 0
    unknown_tags: int = 0
    trailer_declared_rows: int | None = None
    trailer_seen: bool = False
    columns: list[ColumnStat] = field(default_factory=list)
    spec_hash: str = ""
    duration_ms: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def reject_ratio(self) -> float:
        total = self.rows_parsed + self.rows_rejected
        return (self.rows_rejected / total) if total else 0.0


@dataclass
class ParseResult:
    """What every parser returns. Plugins return this too."""

    frame: pl.DataFrame
    rejects: pl.DataFrame | None = None
    report: ParseReport | None = None


class ParseError(Exception):
    """Raised for problems the *spec* can fix.

    An agent reads `field`, `observed` and `candidates` to decide its next move,
    so populate them. The error message is the prompt.
    """

    def __init__(
        self,
        code: str,
        message: str,
        field: str | None = None,
        observed: dict | None = None,
        candidates: list[dict] | None = None,
        hint: str | None = None,
        blame: str = "spec",
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field
        self.observed = observed or {}
        self.candidates = candidates or []
        self.hint = hint
        self.blame = blame  # "spec" (you can fix it) | "file" (the file is wrong)

    def to_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "field": self.field,
            "observed": self.observed,
            "candidates": self.candidates,
            "hint": self.hint,
            "blame": self.blame,
        }
