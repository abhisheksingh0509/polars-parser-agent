"""Parser for acme-positions: fixed-width, mainframe-style.

Fixed-width is deliberately NOT a built-in strategy, so this is the honest
example of the plugin path: the format the declarative spec can't express.

Layout:
  HDR  + date(8)  + label
  D    + id(3)    + name(20) + amount(10)   <- data
  TRL  + count(6)

Contract:
  * return a ParseResult holding a Polars DataFrame
  * bad rows go into `rejects` with a `_reject_reason` column -- never raise
  * add `_src_line_no` so a rejected row can be traced to its line
  * do NOT open files, touch Iceberg, or import anything from ffe.io
"""

from __future__ import annotations

import polars as pl

from ffe.core.plugins import ParseContext, ParserPlugin, register
from ffe.core.report import ParseReport, ParseResult

FIELDS = [("id", 1, 4), ("name", 4, 24), ("amount", 24, 34)]


@register("acme-positions")
class AcmePositions(ParserPlugin):
    def parse(self, raw: bytes, ctx: ParseContext) -> ParseResult:
        scale = int(ctx.options.get("amount_scale", 100))
        lines = raw.decode("utf-8", errors="replace").splitlines()

        rows, bad = [], []
        declared = None

        for lineno, text in enumerate(lines, start=1):
            if not text.strip():
                continue
            if text.startswith("TRL"):
                declared = int(text[3:9])
                continue
            if not text.startswith("D"):
                continue  # HDR and anything else is metadata

            row = {n: text[a:b].strip() for n, a, b in FIELDS}
            row["_src_line_no"] = lineno
            try:
                row["amount"] = int(row["amount"]) / scale
                row["id"] = int(row["id"])
            except ValueError as exc:
                bad.append({**row, "_reject_reason": f"{exc}"})
                continue
            rows.append(row)

        frame = pl.DataFrame(
            rows,
            schema={
                "id": pl.Int64,
                "name": pl.Utf8,
                "amount": pl.Float64,
                "_src_line_no": pl.UInt32,
            },
        )
        rejects = (
            pl.DataFrame(bad, schema={
                "id": pl.Utf8, "name": pl.Utf8, "amount": pl.Utf8,
                "_src_line_no": pl.UInt32, "_reject_reason": pl.Utf8,
            })
            if bad
            else None
        )
        return ParseResult(
            frame=frame,
            rejects=rejects,
            report=ParseReport(
                parser="plugin/acme-positions",
                lines_total=len(lines),
                lines_consumed=len(rows) + len(bad),
                rows_parsed=len(frame),
                rows_rejected=len(bad),
                trailer_declared_rows=declared,
                trailer_seen=declared is not None,
            ),
        )
