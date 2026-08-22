"""The parser SPI. This is the primary extension point.

    @register("acme-positions")
    class AcmePositions(ParserPlugin):
        def parse(self, raw, ctx): ...
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass, field
from pathlib import Path

from .report import ParseResult

_REGISTRY: dict[str, type["ParserPlugin"]] = {}


@dataclass
class ParseContext:
    """Everything a plugin is allowed to know."""

    member: str = "<memory>"
    job_id: str = "adhoc"
    options: dict = field(default_factory=dict)


class ParserPlugin:
    """Implement parse(). Return a table. Bad rows go in rejects, never raise."""

    def parse(self, raw: bytes, ctx: ParseContext) -> ParseResult:  # pragma: no cover
        raise NotImplementedError


def register(ref: str):
    def deco(cls: type[ParserPlugin]) -> type[ParserPlugin]:
        _REGISTRY[ref] = cls
        return cls

    return deco


def get(ref: str) -> ParserPlugin:
    if ref not in _REGISTRY:
        raise KeyError(
            f"no parser registered as {ref!r}. known: {sorted(_REGISTRY)}. "
            f"Did you forget to load the plugins directory?"
        )
    return _REGISTRY[ref]()


def known() -> list[str]:
    return sorted(_REGISTRY)


def load_dir(path: str | Path) -> list[str]:
    """Import every .py in a directory so its @register calls fire."""
    path = Path(path)
    if not path.is_dir():
        return []
    loaded = []
    for py in sorted(path.glob("*.py")):
        if py.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(f"ffe_plugin_{py.stem}", py)
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            loaded.append(py.stem)
    return loaded
