"""header region -> column names (+ declared types, when the file says so)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .report import ParseError
from .spec import DdlHeader, NameRowHeader, SuppliedHeader
from .structure import Layout, Region


@dataclass
class Schema:
    names: list[str] = field(default_factory=list)
    # only populated when the file declares types (e.g. a DDL header)
    dtypes: dict[str, str] = field(default_factory=dict)
    widths: dict[str, int] = field(default_factory=dict)


def _ddl(texts: list[str], cfg: DdlHeader) -> Schema:
    rx = re.compile(cfg.pattern)
    schema = Schema()
    unmatched: list[str] = []
    for text in texts:
        m = rx.match(text.strip())
        if not m:
            unmatched.append(text.strip())
            continue
        g = m.groupdict()
        name = g["name"]
        schema.names.append(name)
        declared = g.get("type", "").lower()
        schema.dtypes[name] = cfg.type_map.get(declared, "utf8")
        if g.get("width"):
            schema.widths[name] = int(g["width"])

    if not schema.names:
        raise ParseError(
            "header_unparsed",
            "no line in the header block matched the DDL pattern",
            field="parser.header.pattern",
            observed={"header_lines": unmatched[:10], "pattern": cfg.pattern},
            hint="Adjust parser.header.pattern, or use header.parser: name_row "
            "if the header is a single row of column names.",
        )
    return schema


def _name_row(texts: list[str], cfg: NameRowHeader, delimiter: str) -> Schema:
    if not texts:
        raise ParseError(
            "header_missing",
            "the header region is empty",
            field="parser.structure",
            hint="Check which block or record tag is the header.",
        )
    fields = [f.strip() for f in texts[0].split(delimiter)]
    names = fields[cfg.skip_leading_fields :]
    if not names:
        raise ParseError(
            "header_all_skipped",
            f"skip_leading_fields={cfg.skip_leading_fields} consumed every field "
            f"of the header row (it has {len(fields)})",
            field="parser.header.skip_leading_fields",
            observed={"header_fields": fields},
            candidates=[
                {"skip_leading_fields": i, "names": fields[i:]}
                for i in range(len(fields))
                if fields[i:]
            ][:4],
            hint="Lower skip_leading_fields.",
        )
    return Schema(names=names)


def resolve(layout: Layout, cfg, delimiter: str) -> Schema:
    if isinstance(cfg, SuppliedHeader):
        return Schema(names=list(cfg.columns))
    region = layout.first("header")
    if region is None:
        raise ParseError(
            "header_missing",
            f"header.parser is {cfg.parser!r} but the file produced no header region",
            field="parser.header",
            hint="Use header.parser: supplied with an explicit column list if this "
            "file has no header.",
        )
    texts = layout.texts(region)
    if isinstance(cfg, DdlHeader):
        return _ddl(texts, cfg)
    if isinstance(cfg, NameRowHeader):
        return _name_row(texts, cfg, delimiter)
    raise ValueError(f"unsupported header parser: {cfg!r}")
