"""Parser for msci-test: MSCI/GICS-style taxonomy reference data.

Layout:
  #...                  <- comment lines, ignored
  [SCHEMA_START]
  Sector_ID|Industry_Group_ID|Industry_ID|Sub_Industry_ID::Label_EN::Label_FR
  [DATA_BLOCK]
  10|1010|101010|10101010::Energy (Oil & Gas)::Énergie   <- data
  [SCHEMA_END]
  TRLR_COUNT:5

Every row (the header and each data row) follows the same mixed-delimiter
grammar: four `|`-separated fields, the last of which is itself
`::`-separated into three. That is the shape no built-in strategy covers,
which is why this file needs a plugin rather than a spec.

Business date: one archive holds one day. The date is carried in the file's
`# Generated: YYYY-MM-DD` comment, so the parser reads it from there. A run can
override it with `--option business_date=YYYY-MM-DD` for a backfill or a
mislabelled drop; `ctx.options` wins over the file, because someone passing it
explicitly is correcting what the file says.

Contract:
  * return a ParseResult holding a Polars DataFrame
  * bad rows go into `rejects` with a `_reject_reason` column -- never raise
  * add `_src_line_no` so a rejected row can be traced to its line
  * do NOT open files, touch Iceberg, or import anything from ffe.io
"""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from ffe.core.plugins import ParseContext, ParserPlugin, register
from ffe.core.report import ParseReport, ParseResult

DATE_COMMENT = re.compile(r"^#\s*Generated:\s*(\d{4})-(\d{2})-(\d{2})\s*$")

FALLBACK_COLUMNS = [
    "Sector_ID",
    "Industry_Group_ID",
    "Industry_ID",
    "Sub_Industry_ID",
    "Label_EN",
    "Label_FR",
]


def _business_date(value: str | None) -> date | None:
    """`2026-08-23` -> date. Anything else -> None, never an exception."""
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _split_row(text: str) -> list[str] | None:
    """Split a header or data row: 4 `|` fields, the last `::`-split into 3."""
    parts = text.split("|")
    if len(parts) != 4:
        return None
    tail = parts[3].split("::")
    if len(tail) != 3:
        return None
    return parts[:3] + tail


@register("msci-test")
class MsciTest(ParserPlugin):
    def parse(self, raw: bytes, ctx: ParseContext) -> ParseResult:
        lines = raw.decode("utf-8", errors="replace").splitlines()

        columns = None
        in_data = False
        declared = None
        generated = None
        rows, bad = [], []

        for lineno, text in enumerate(lines, start=1):
            stripped = text.strip()
            if not stripped or stripped.startswith("#"):
                match = DATE_COMMENT.match(stripped)
                if match:
                    generated = "-".join(match.groups())
                continue
            if stripped == "[SCHEMA_START]":
                continue
            if stripped == "[DATA_BLOCK]":
                in_data = True
                continue
            if stripped == "[SCHEMA_END]":
                in_data = False
                continue
            if stripped.startswith("TRLR_COUNT:"):
                declared = int(stripped.split(":", 1)[1].strip())
                continue

            if not in_data:
                # the header row between SCHEMA_START and DATA_BLOCK names
                # the columns -- measured from the file, not hardcoded.
                columns = _split_row(stripped) or columns
                continue

            tokens = _split_row(stripped)
            if tokens is None:
                bad.append(
                    {
                        "_src_line_no": lineno,
                        "raw_line": stripped,
                        "_reject_reason": "expected 4 '|' fields with a "
                        "3-part '::' tail on the last field",
                    }
                )
                continue

            row = dict(zip(columns or FALLBACK_COLUMNS, tokens))
            row["_src_line_no"] = lineno
            rows.append(row)

        names = columns or FALLBACK_COLUMNS
        frame = pl.DataFrame(
            rows,
            schema={**{n: pl.Utf8 for n in names}, "_src_line_no": pl.UInt32},
        )

        # One archive is one business day, so this is a constant per member.
        # Null rather than a guess when neither source supplies it -- a
        # silently-wrong date is worse than an empty one, and the
        # all_null_columns gate surfaces it on the very next dry-run.
        business_date = _business_date(
            ctx.options.get("business_date") or generated
        )
        frame = frame.with_columns(
            pl.lit(business_date, pl.Date).alias("business_date")
        )
        rejects = (
            pl.DataFrame(
                bad,
                schema={
                    "_src_line_no": pl.UInt32,
                    "raw_line": pl.Utf8,
                    "_reject_reason": pl.Utf8,
                },
            )
            if bad
            else None
        )
        return ParseResult(
            frame=frame,
            rejects=rejects,
            report=ParseReport(
                parser="plugin/msci-test",
                lines_total=len(lines),
                lines_consumed=len(rows) + len(bad),
                rows_parsed=len(frame),
                rows_rejected=len(bad),
                trailer_declared_rows=declared,
                trailer_seen=declared is not None,
            ),
        )
