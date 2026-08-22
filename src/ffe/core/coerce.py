"""Cast to the target types, and route rows that fail into rejects.

The rule: never raise. One bad date in row 3,000,000 must not kill the job.
"""

from __future__ import annotations

import polars as pl

from .spec import Coerce

_DTYPES: dict[str, pl.DataType] = {
    "utf8": pl.Utf8,
    "string": pl.Utf8,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "float64": pl.Float64,
    "bool": pl.Boolean,
    "date": pl.Date,
}


def dtype(name: str) -> pl.DataType:
    key = name.lower()
    if key not in _DTYPES:
        raise ValueError(f"unknown dtype {name!r}; known: {sorted(_DTYPES)}")
    return _DTYPES[key]


def apply(
    df: pl.DataFrame, targets: dict[str, str], cfg: Coerce
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, int]]:
    """Returns (good, rejects, cast_failures_by_column).

    Rejects carry the *original* uncast values, so you can see what was wrong.
    """
    original = df
    if cfg.trim:
        df = df.with_columns(
            pl.col(c).str.strip_chars()
            for c in df.columns
            if df.schema[c] == pl.Utf8
        )
    if cfg.empty_as_null:
        df = df.with_columns(
            pl.when(pl.col(c).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(c))
            .alias(c)
            for c in df.columns
            if df.schema[c] == pl.Utf8
        )

    failures: dict[str, int] = {}
    bad_masks: dict[str, pl.Series] = {}

    for name, want in targets.items():
        if name not in df.columns:
            continue
        target = dtype(want)
        col = df[name]
        if col.dtype == target:
            continue
        cast = col.cast(target, strict=False)
        bad = col.is_not_null() & cast.is_null()
        n = int(bad.sum())
        if n:
            failures[name] = n
            bad_masks[name] = bad
        df = df.with_columns(cast.alias(name))

    if not bad_masks:
        empty = original.head(0).with_columns(
            pl.lit(None, pl.Utf8).alias("_reject_reason")
        )
        return df, empty, failures

    combined = bad_masks[next(iter(bad_masks))]
    for mask in list(bad_masks.values())[1:]:
        combined = combined | mask

    reasons = []
    for i in range(len(original)):
        why = [
            f"{name}: cannot cast {original[name][i]!r} to {targets[name]}"
            for name, mask in bad_masks.items()
            if mask[i]
        ]
        reasons.append("; ".join(why) if why else None)

    rejects = original.with_columns(
        pl.Series("_reject_reason", reasons, pl.Utf8)
    ).filter(combined)
    good = df.filter(~combined)
    return good, rejects, failures
