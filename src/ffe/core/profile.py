"""Deterministic structural profile. No model involved.

Code measures; a model only interprets. Delimiter counts, line shapes and
first-field tokens come from arithmetic -- the interpretive leap ("position 0
is a record tag, H is the header, E is the trailer") is the only part worth
handing to a model, and it gets handed the measurements.
"""

from __future__ import annotations

from collections import Counter

CANDIDATES = [",", "|", ";", "\t", "~", "^", ":"]
SAMPLE_CAP = 100


def profile(raw: bytes, cap: int = SAMPLE_CAP) -> dict:
    encoding = "utf-8"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        encoding = "latin-1"
        text = raw.decode("latin-1")

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln for ln in text.split("\n")]
    while lines and not lines[-1].strip():
        lines.pop()

    truncated = len(lines) > cap
    body = lines[:cap]
    nonblank = [ln for ln in body if ln.strip()]

    # Measure each candidate two ways, because the difference is diagnostic:
    # a flat CSV has its delimiter on *every* line; a sectioned file has it only
    # inside the data block. `coverage` low + `consistency` high => sectioned.
    delimiters = {}
    for cand in CANDIDATES:
        present = [ln for ln in nonblank if cand in ln]
        if not present:
            continue
        counts = Counter(len(ln.split(cand)) for ln in present)
        modal, hits = counts.most_common(1)[0]
        delimiters[cand] = {
            "modal_columns": modal,
            "lines_containing": len(present),
            "coverage": round(len(present) / len(nonblank), 3),
            "consistency": round(hits / len(present), 3),
        }

    best = max(
        delimiters.items(),
        key=lambda kv: (
            kv[1]["consistency"] * kv[1]["coverage"],
            kv[1]["modal_columns"],
        ),
        default=(None, None),
    )[0]

    first_field = {}
    if best:
        tokens = Counter(ln.split(best)[0].strip() for ln in nonblank)
        # a record-tag file has few distinct, short values at position 0
        if len(tokens) <= 12 and all(len(t) <= 4 for t in tokens):
            first_field = dict(tokens.most_common(12))

    banners = Counter(
        ln.strip()
        for ln in body
        if ln.strip() and len(set(ln.strip())) == 1 and len(ln.strip()) <= 3
    )

    # Order matters. A flat table has one line width; a record-tag file's width
    # varies *by tag*, which is what separates them when the sample is small.
    uniform = bool(best) and delimiters[best]["consistency"] >= 0.95 and delimiters[best]["coverage"] >= 0.95
    tagged = (
        bool(first_field)
        and len(first_field) > 1
        and not uniform
        and not all(t.isdigit() for t in first_field)
    )

    if banners:
        sentinel = banners.most_common(1)[0][0]
        hint = {
            "strategy": "sentinel",
            "sentinel": sentinel,
            "why": f"{banners[sentinel]} lines consist only of {sentinel!r}",
        }
    elif uniform:
        hint = {
            "strategy": "native",
            "delimiter": best,
            "columns": delimiters[best]["modal_columns"],
            "why": f"{best!r} appears on every line with one stable width",
        }
    elif tagged:
        hint = {
            "strategy": "record_tag",
            "delimiter": best,
            "tags": sorted(first_field),
            "why": "field 0 holds a few short repeated codes and line width "
                   "varies by code",
        }
    else:
        hint = {
            "strategy": "unknown",
            "why": "no built-in strategy matched these measurements; this file "
                   "probably needs a plugin",
        }

    return {
        "encoding": encoding,
        "structure_hint": hint,
        "bytes": len(raw),
        "lines_total": len(lines),
        "lines_profiled": len(body),
        "truncated": truncated,
        "blank_lines": len(body) - len(nonblank),
        "line_lengths": {
            "min": min((len(ln) for ln in nonblank), default=0),
            "max": max((len(ln) for ln in nonblank), default=0),
            "distinct": len({len(ln) for ln in nonblank}),
        },
        "delimiters": delimiters,
        "best_delimiter": best,
        "first_field_tokens": first_field,
        "repeated_char_lines": dict(banners),
        "head": body[:60],
        "tail": lines[-20:] if len(lines) > 80 else [],
    }
