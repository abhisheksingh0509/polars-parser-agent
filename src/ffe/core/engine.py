"""parse(spec, raw) -> ParseResult.

Pure function. No I/O, no config discovery, no network. That is what makes
`dry-run` safe by construction rather than by care.

Performance rule, measured not assumed: Polars parses 150k rows in ~1ms, so any
per-line Python work dominates. Everything here stays at the C level -- list
slices, str.count, str.join -- and Polars does the actual parsing.
"""

from __future__ import annotations

import io
import time

import polars as pl

from . import coerce as coerce_stage
from . import header as header_stage
from . import plugins, structure
from .framer import Lines, frame
from .report import ColumnStat, ParseError, ParseReport, ParseResult
from .spec import (
    DelimitedData,
    NativeParser,
    Parser,
    PluginParser,
    RecordTagStructure,
    SectionedParser,
)
from .structure import Layout

LINE_NO = "_src_line_no"
_CANDIDATE_DELIMS = [",", "|", ";", "\t", "~", "^", ":"]


# --------------------------------------------------------------------------- #
# delimiter checking: fail loudly and usefully, with measured alternatives
# --------------------------------------------------------------------------- #


def _measure(sample: list[str], delim: str) -> tuple[int, float]:
    counts: dict[int, int] = {}
    for t in sample:
        n = t.count(delim) + 1
        counts[n] = counts.get(n, 0) + 1
    modal = max(counts, key=lambda k: counts[k])
    return modal, counts[modal] / sum(counts.values())


def _check_delimiter(sample: list[str], names: list[str], cfg: DelimitedData) -> None:
    """The error payload is what an agent reads to pick its next move, so it
    carries measured alternatives rather than a generic complaint."""
    want = len(names)
    modal, consistency = _measure(sample, cfg.delimiter)
    if modal == want:
        return

    # A candidate that yields the *expected* width earns a lower consistency
    # bar than an unrelated one: a couple of ragged lines shouldn't hide the
    # right answer. Consistency is reported so the reader can judge.
    primary, secondary = [], []
    for cand in _CANDIDATE_DELIMS:
        if cand == cfg.delimiter:
            continue
        m, c = _measure(sample, cand)
        entry = {"value": cand, "columns": m, "consistency": round(c, 3)}
        if m == want and c >= 0.5:
            primary.append(entry)
        elif m > 1 and c >= 0.9:
            secondary.append(entry)
    primary.sort(key=lambda e: -e["consistency"])
    candidates = primary + secondary

    observed = {
        "delimiter": cfg.delimiter,
        "columns_found": modal,
        "columns_expected": want,
        "consistency": round(consistency, 3),
        "expected_names": names,
    }

    if modal == 1:
        raise ParseError(
            "delimiter_mismatch",
            f"delimiter {cfg.delimiter!r} splits the data rows into 1 column, "
            f"but the header declares {want}",
            field="parser.data.delimiter",
            observed=observed,
            candidates=candidates[:4],
            hint=(
                f"Set parser.data.delimiter to {candidates[0]['value']!r}."
                if candidates
                else "No candidate delimiter yields the expected column count; "
                "the header may be misread, or this file needs a plugin."
            ),
        )

    raise ParseError(
        "column_count_mismatch",
        f"data rows have {modal} columns but the header declares {want} ({names})",
        field="parser.header",
        observed=observed,
        candidates=candidates[:4],
        hint="The header and the data disagree on width. Either the header is "
        "read from the wrong block, skip_leading_fields is off by "
        f"{want - modal}, or the tag column needs dropping.",
    )


# --------------------------------------------------------------------------- #
# the delimited read
# --------------------------------------------------------------------------- #


def _read_delimited(
    lines: Lines, idx, names: list[str], cfg: DelimitedData
) -> tuple[pl.DataFrame, int]:
    texts = lines.texts
    if isinstance(idx, range) and idx.step == 1:
        seq = texts[idx.start : idx.stop]
        line_nos = range(lines.no(idx.start), lines.no(idx.stop))
    else:
        seq = [texts[i] for i in idx]
        line_nos = [lines.no(i) for i in idx]

    if not seq:
        return pl.DataFrame({n: pl.Series([], pl.Utf8) for n in names}), 0

    n = len(names)
    _check_delimiter(seq[:200], names, cfg)
    joined = "\n".join(seq)

    # One C-level count answers "is anything ragged?" for the common case.
    # Only when the totals disagree do we pay for a per-line pass. (Two rows
    # with compensating errors would slip through this fast path; that has
    # never been worth a 150k-iteration loop on every file.)
    ragged = 0
    if joined.count(cfg.delimiter) != len(seq) * (n - 1):
        ragged = sum(1 for t in seq if t.count(cfg.delimiter) + 1 != n)

    try:
        df = pl.read_csv(
            io.BytesIO(joined.encode()),
            separator=cfg.delimiter,
            has_header=False,
            schema={name: pl.Utf8 for name in names},
            quote_char=cfg.quote,
            truncate_ragged_lines=(cfg.ragged == "truncate"),
        )
    except Exception as exc:
        raise ParseError(
            "unreadable_data_block",
            f"Polars could not read the data block with delimiter "
            f"{cfg.delimiter!r}: {exc}",
            field="parser.data",
            observed={"delimiter": cfg.delimiter, "expected_columns": n},
            hint="If the delimiter is right, the block may contain embedded "
            "quotes or newlines -- try quote: null, or use a plugin.",
        ) from exc

    return df.with_columns(pl.Series(LINE_NO, line_nos, pl.UInt32)), ragged


# --------------------------------------------------------------------------- #


def _stats(df: pl.DataFrame, failures: dict[str, int]) -> list[ColumnStat]:
    return [
        ColumnStat(
            name=c,
            dtype=str(df.schema[c]),
            null_count=int(df[c].null_count()),
            cast_failures=failures.get(c, 0),
        )
        for c in df.columns
    ]


def _parse_native(spec: NativeParser, raw: bytes) -> ParseResult:
    kwargs = dict(
        separator=spec.delimiter,
        has_header=spec.has_header,
        infer_schema_length=0,  # read as text; coerce decides types and rejects
        truncate_ragged_lines=True,
    )
    if spec.columns:
        kwargs["new_columns"] = spec.columns
    df = pl.read_csv(io.BytesIO(raw), **kwargs)

    offset = 2 if spec.has_header else 1
    df = df.with_columns(
        pl.Series(LINE_NO, range(offset, offset + len(df)), pl.UInt32)
    )
    good, rejects, failures = coerce_stage.apply(
        df, spec.coerce.schema_override, spec.coerce
    )
    header_lines = 1 if spec.has_header else 0
    return ParseResult(
        frame=good,
        rejects=rejects,
        report=ParseReport(
            parser="native",
            lines_total=len(df) + header_lines,
            lines_consumed=len(df),
            rows_parsed=len(good),
            rows_rejected=len(rejects),
            columns=_stats(good, failures),
        ),
    )


def _file_header_values(layout: Layout, spec: SectionedParser) -> dict[str, str]:
    region = layout.first("file_header")
    if region is None or not len(region):
        return {}
    if not isinstance(spec.structure, RecordTagStructure):
        return {}
    rec = spec.structure.records.get(region.record_type or "")
    if rec is None or not rec.fields:
        return {}
    values = layout.texts(region)[0].split(spec.structure.delimiter)
    return {
        name: values[i].strip() for i, name in enumerate(rec.fields) if i < len(values)
    }


def _data_index(layout: Layout):
    regions = layout.every("data")
    if len(regions) == 1:
        return regions[0].idx
    merged: list[int] = []
    for r in regions:
        merged.extend(r.idx)
    merged.sort()
    return merged


def _parse_sectioned(spec: SectionedParser, raw: bytes) -> ParseResult:
    lines = frame(raw, spec.framer)
    layout = structure.split(lines, spec.structure)

    struct_delim = getattr(spec.structure, "delimiter", spec.data.delimiter)
    schema = header_stage.resolve(layout, spec.header, struct_delim)

    # a record-tag file carries its tag as field 0 of every data row
    prefix = ["tag"] if isinstance(spec.structure, RecordTagStructure) else []
    read_names = prefix + schema.names

    idx = _data_index(layout)
    if not len(idx):
        raise ParseError(
            "no_data_rows",
            "the data region contained no lines",
            field="parser.structure",
            hint="Check which block ordinal or record tag is the data region.",
        )

    df, ragged = _read_delimited(lines, idx, read_names, spec.data)

    drop = [c for c in spec.data.drop_columns if c in df.columns]
    if drop:
        df = df.drop(drop)

    promoted = _file_header_values(layout, spec)
    for name in spec.validate_.promote_fields:
        if name in promoted:
            df = df.with_columns(pl.lit(promoted[name], pl.Utf8).alias(name))

    # types declared by the file, then explicit overrides from the spec
    targets = {**schema.dtypes, **spec.coerce.schema_override}
    good, rejects, failures = coerce_stage.apply(df, targets, spec.coerce)

    trailer = layout.first("trailer")
    trailer_seen = bool(trailer and len(trailer))
    if spec.validate_.require_trailer and not trailer_seen:
        raise ParseError(
            "trailer_missing",
            "policy requires a trailer record but none was found",
            field="parser.validate.require_trailer",
            hint="Either the file is truncated, or the trailer is not being "
            "recognised by the structure config.",
            blame="file",
        )
    if spec.validate_.trailer_token and trailer_seen:
        got = layout.texts(trailer)[0].strip()
        if got != spec.validate_.trailer_token:
            raise ParseError(
                "trailer_mismatch",
                f"expected trailer {spec.validate_.trailer_token!r}, found {got!r}",
                field="parser.validate.trailer_token",
                observed={"found": got},
                candidates=[{"trailer_token": got}],
            )

    return ParseResult(
        frame=good,
        rejects=rejects,
        report=ParseReport(
            parser=f"sectioned/{spec.structure.strategy}",
            lines_total=len(lines),
            lines_consumed=len(idx),
            rows_parsed=len(good),
            rows_rejected=len(rejects),
            ragged_lines=ragged,
            unknown_tags=layout.unknown_tags,
            trailer_seen=trailer_seen,
            columns=_stats(good, failures),
        ),
    )


def parse(
    spec: Parser, raw: bytes, ctx: plugins.ParseContext | None = None
) -> ParseResult:
    started = time.perf_counter()
    ctx = ctx or plugins.ParseContext()

    if isinstance(spec, NativeParser):
        result = _parse_native(spec, raw)
    elif isinstance(spec, SectionedParser):
        result = _parse_sectioned(spec, raw)
    elif isinstance(spec, PluginParser):
        ctx.options = {**spec.options, **ctx.options}
        result = plugins.get(spec.ref).parse(raw, ctx)
        if result.report is None:
            result.report = ParseReport(
                parser=f"plugin/{spec.ref}",
                rows_parsed=len(result.frame),
                rows_rejected=len(result.rejects) if result.rejects is not None else 0,
                columns=_stats(result.frame, {}),
            )
        result.report.parser = f"plugin/{spec.ref}"
        if not result.report.columns:
            # A plugin builds its own ParseReport and rarely fills this in.
            # Without it `all_null_columns` has nothing to check, so the gate
            # silently passes for every plugin feed -- measure it here instead.
            result.report.columns = _stats(result.frame, {})
    else:
        raise ValueError(f"unsupported parser kind: {spec!r}")

    result.report.duration_ms = int((time.perf_counter() - started) * 1000)
    return result
