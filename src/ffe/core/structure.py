"""lines -> labelled regions. The stage that varies most between feeds.

A Region holds *indices*, not text. For a banner file those indices are a
contiguous range, which lets the data stage hand Polars a plain list slice.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .framer import Lines
from .report import ParseError
from .spec import RecordTagStructure, SentinelStructure


@dataclass
class Region:
    kind: str  # header | data | trailer | meta | file_header
    idx: range | list[int] = field(default_factory=list)
    record_type: str | None = None

    def __len__(self) -> int:
        return len(self.idx)

    @property
    def contiguous(self) -> bool:
        return isinstance(self.idx, range) and self.idx.step == 1


@dataclass
class Layout:
    lines: Lines
    regions: list[Region] = field(default_factory=list)
    unknown_tags: int = 0

    def first(self, kind: str) -> Region | None:
        return next((r for r in self.regions if r.kind == kind), None)

    def every(self, kind: str) -> list[Region]:
        return [r for r in self.regions if r.kind == kind]

    def texts(self, region: Region | None) -> list[str]:
        """Materialise a region. Only call this for small regions (header,
        trailer) -- the data region is streamed by the data stage instead."""
        if region is None:
            return []
        t = self.lines.texts
        if region.contiguous:
            return t[region.idx.start : region.idx.stop]
        return [t[i] for i in region.idx]


# --------------------------------------------------------------------------- #


def _sentinel(lines: Lines, cfg: SentinelStructure) -> Layout:
    texts = lines.texts
    sentinel = cfg.sentinel

    # boundaries as index ranges; no per-line objects
    chunks: list[range] = []
    start: int | None = None
    for i, t in enumerate(texts):
        if t.strip() == sentinel:
            if start is not None:
                chunks.append(range(start, i))
                start = None
            elif not cfg.collapse_runs:
                chunks.append(range(i, i))
        elif start is None:
            start = i
    if start is not None:
        chunks.append(range(start, len(texts)))

    layout = Layout(lines=lines)
    for block in cfg.blocks:
        if block.ordinal >= len(chunks):
            if block.optional:
                continue
            raise ParseError(
                "block_missing",
                f"expected a {block.kind} block at ordinal {block.ordinal} but "
                f"splitting on {sentinel!r} yielded only {len(chunks)} block(s)",
                field="parser.structure.blocks",
                observed={"blocks_found": len(chunks), "sentinel": sentinel},
                hint=f"Either the sentinel is wrong, or ordinal {block.ordinal} "
                f"should be marked optional.",
            )
        span = chunks[block.ordinal]
        idx: range | list[int] = range(span.start + block.skip_leading, span.stop)
        if block.drop_blank:
            idx = [i for i in idx if texts[i].strip()]
        layout.regions.append(Region(kind=block.kind, idx=idx))
    return layout


def _record_tag(lines: Lines, cfg: RecordTagStructure) -> Layout:
    texts = lines.texts
    delim = cfg.delimiter
    kinds = {tag: rec.kind for tag, rec in cfg.records.items()}

    buckets: dict[str, list[int]] = {}
    tag_counts: dict[str, int] = {}
    unknown = 0

    # str.find + slice avoids allocating a field list per line
    if cfg.tag_position == 0:
        for i, t in enumerate(texts):
            if not t or not t.strip():
                continue
            cut = t.find(delim)
            tag = (t if cut < 0 else t[:cut]).strip()
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            if tag in kinds:
                buckets.setdefault(tag, []).append(i)
            else:
                unknown += 1
                if cfg.unknown_tag == "fail":
                    raise ParseError(
                        "unknown_tag",
                        f"line {lines.no(i)} has record tag {tag!r}, not declared",
                        field="parser.structure.records",
                        observed={"tag_counts": tag_counts},
                        hint=f"Add {tag!r} to parser.structure.records, or set "
                        f"unknown_tag to 'reject'.",
                    )
    else:
        for i, t in enumerate(texts):
            if not t.strip():
                continue
            fields = t.split(delim)
            tag = (
                fields[cfg.tag_position].strip()
                if len(fields) > cfg.tag_position
                else ""
            )
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            if tag in kinds:
                buckets.setdefault(tag, []).append(i)
            else:
                unknown += 1

    layout = Layout(
        lines=lines,
        regions=[
            Region(kind=kinds[tag], idx=idx, record_type=tag)
            for tag, idx in buckets.items()
        ],
        unknown_tags=unknown,
    )

    if not any(r.kind == "data" for r in layout.regions):
        raise ParseError(
            "no_data_records",
            "no line matched a record type declared as kind: data",
            field="parser.structure.records",
            observed={"tag_counts": tag_counts},
            candidates=[
                {"tag": t, "lines": n}
                for t, n in sorted(tag_counts.items(), key=lambda kv: -kv[1])[:5]
            ],
            hint="Point one of the observed tags at kind: data.",
        )
    return layout


def split(lines: Lines, cfg) -> Layout:
    if isinstance(cfg, SentinelStructure):
        return _sentinel(lines, cfg)
    if isinstance(cfg, RecordTagStructure):
        return _record_tag(lines, cfg)
    raise ValueError(f"unsupported structure strategy: {cfg!r}")
