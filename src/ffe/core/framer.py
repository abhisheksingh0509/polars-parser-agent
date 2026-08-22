"""bytes -> lines. The only place encoding is dealt with.

Lines are plain strings in one list, and a line *number* is an index plus a
base. There is deliberately no object per line: at 150k lines, allocating one
costs ~20x more than the actual Polars parse.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .spec import Framer


@dataclass
class Lines:
    texts: list[str] = field(default_factory=list)
    base: int = 1  # the file line number of texts[0]

    def no(self, i: int) -> int:
        return i + self.base

    def __len__(self) -> int:
        return len(self.texts)


def frame(raw: bytes, cfg: Framer) -> Lines:
    text = raw.decode(cfg.encoding, errors="replace")
    if text.startswith("﻿"):
        text = text[1:]
    if "\r" in text:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
    parts = text.split("\n")
    if cfg.strip_trailing_blank:
        while parts and not parts[-1].strip():
            parts.pop()
    return Lines(texts=parts)
