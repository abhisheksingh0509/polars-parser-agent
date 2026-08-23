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

The *shape* is read from `ctx.options`, not hardcoded here: `fields` carries
one `[name, start, end, type]` per column and its length sets how many columns
come out, and `data_tag` / `trailer_tag` name the record markers. So the next
sender who ships this layout with renamed columns, different offsets, or `R`
instead of `D` is a feed-spec change and not a second plugin. The defaults
below reproduce this feed, so a fresh scaffold runs unedited.

A malformed *option* raises ParseError(blame="spec") -- that is a spec problem
the dry-run loop can act on. A malformed *row* still never raises.
"""

from __future__ import annotations

import polars as pl

from ffe.core.plugins import ParseContext, ParserPlugin, register
from ffe.core.report import ParseError, ParseReport, ParseResult

# name, start, end, type. `amount` is an integer scaled by `amount_scale`.
DEFAULT_FIELDS = [
    ["id", 1, 4, "int"],
    ["name", 4, 24, "str"],
    ["amount", 24, 34, "amount"],
]
DTYPES = {"str": pl.Utf8, "int": pl.Int64, "amount": pl.Float64}


def _fields(value) -> list[tuple[str, int, int, str]]:
    """Validate the field table. A bad table is a spec problem, so it raises."""

    def bad(message: str, observed) -> ParseError:
        return ParseError(
            "bad_option",
            f"parser.options.fields {message}",
            field="parser.options.fields",
            observed={"value": observed},
            hint="Each entry is [name, start, end, type] with type one of "
            f"{sorted(DTYPES)}; start < end; names distinct.",
        )

    if not isinstance(value, list) or not value:
        raise bad("must be a non-empty list of [name, start, end, type]", value)

    out, seen = [], set()
    for entry in value:
        if not isinstance(entry, (list, tuple)) or len(entry) != 4:
            raise bad("entries must be exactly [name, start, end, type]", entry)
        name, start, end, kind = entry
        if not isinstance(name, str) or not name.strip():
            raise bad("needs a non-empty column name", entry)
        if name == "_src_line_no":
            raise bad("may not name a column '_src_line_no'", entry)
        if name in seen:
            raise bad(f"repeats the column name {name!r}", entry)
        if not isinstance(start, int) or not isinstance(end, int) or start >= end:
            raise bad("needs integer offsets with start < end", entry)
        if kind not in DTYPES:
            raise bad(f"has unknown type {kind!r}", entry)
        seen.add(name)
        out.append((name, start, end, kind))
    return out


@register("acme-positions")
class AcmePositions(ParserPlugin):
    def parse(self, raw: bytes, ctx: ParseContext) -> ParseResult:
        scale = int(ctx.options.get("amount_scale", 100))
        fields = _fields(ctx.options.get("fields", DEFAULT_FIELDS))
        data_tag = str(ctx.options.get("data_tag", "D"))
        trailer_tag = str(ctx.options.get("trailer_tag", "TRL"))
        lines = raw.decode("utf-8", errors="replace").splitlines()

        rows, bad = [], []
        declared = None

        for lineno, text in enumerate(lines, start=1):
            if not text.strip():
                continue
            if text.startswith(trailer_tag):
                count = text[len(trailer_tag):].strip()
                try:
                    declared = int(count)
                except ValueError:
                    # Not a spec problem and not a row -- the trailer is the
                    # row-count ground truth, so an unreadable one is the file's
                    # fault and no spec edit fixes it.
                    raise ParseError(
                        "bad_trailer",
                        f"trailer row count is not a number: {count!r}",
                        field=None,
                        observed={"line": lineno, "value": count},
                        hint="The file's trailer is corrupt; go back to the sender.",
                        blame="file",
                    ) from None
                continue
            if not text.startswith(data_tag):
                continue  # HDR and anything else is metadata

            # Keep the raw slices: a rejected row must carry the *original*
            # value, so casting writes into a second dict.
            raw_row = {n: text[a:b].strip() for n, a, b, _ in fields}
            row = {"_src_line_no": lineno}
            try:
                for name, _, _, kind in fields:
                    value = raw_row[name]
                    if kind == "int":
                        row[name] = int(value)
                    elif kind == "amount":
                        row[name] = int(value) / scale
                    else:
                        row[name] = value
            except ValueError as exc:
                bad.append({**raw_row, "_src_line_no": lineno, "_reject_reason": f"{exc}"})
                continue
            rows.append(row)

        frame = pl.DataFrame(
            rows,
            schema={**{n: DTYPES[k] for n, _, _, k in fields}, "_src_line_no": pl.UInt32},
        )
        rejects = (
            pl.DataFrame(bad, schema={
                **{n: pl.Utf8 for n, _, _, _ in fields},
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
