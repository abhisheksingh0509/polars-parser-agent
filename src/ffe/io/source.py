"""Resolve a pattern into members. The caller never learns whether it was a
file or an archive.

Member is deliberately a plain frozen dataclass holding *paths*, not open
handles -- so it pickles cleanly to a worker process, and every worker is
forced to open its own handle on the archive. A shared handle remembers where
it was last reading; two workers sharing one trip over each other.
"""

from __future__ import annotations

import fnmatch
import glob
import zipfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Member:
    container: str
    name: str
    kind: str  # "file" | "zip"
    size: int

    @property
    def label(self) -> str:
        return self.name if self.kind == "file" else f"{Path(self.container).name}!{self.name}"

    def read(self) -> bytes:
        if self.kind == "zip":
            with zipfile.ZipFile(self.container) as zf:  # own handle, per worker
                return zf.read(self.name)
        return Path(self.container).read_bytes()


def resolve(pattern: str, members: str = "*") -> list[Member]:
    out: list[Member] = []
    for path in sorted(glob.glob(pattern, recursive=True)):
        p = Path(path)
        if p.is_dir():
            continue
        if p.suffix.lower() == ".zip":
            with zipfile.ZipFile(p) as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    if not (
                        fnmatch.fnmatch(info.filename, members)
                        or fnmatch.fnmatch(Path(info.filename).name, members)
                    ):
                        continue
                    out.append(Member(str(p), info.filename, "zip", info.file_size))
        else:
            out.append(Member(str(p), str(p), "file", p.stat().st_size))
    return out
