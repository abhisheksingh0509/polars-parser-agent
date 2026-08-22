"""FeedSpec: the one config object. Binds source -> parser -> target."""

from __future__ import annotations

from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# shared pieces
# --------------------------------------------------------------------------- #


class Framer(BaseModel):
    encoding: str = "utf-8"
    strip_trailing_blank: bool = True


class Coerce(BaseModel):
    trim: bool = True
    empty_as_null: bool = True
    # name -> polars dtype name, e.g. {"id": "int64"}
    schema_override: dict[str, str] = Field(default_factory=dict)


# --------------------------------------------------------------------------- #
# structure strategies
# --------------------------------------------------------------------------- #


class Block(BaseModel):
    kind: Literal["header", "data", "trailer", "meta"]
    ordinal: int
    skip_leading: int = 0
    drop_blank: bool = False
    optional: bool = False


class SentinelStructure(BaseModel):
    strategy: Literal["sentinel"] = "sentinel"
    sentinel: str = "*"
    collapse_runs: bool = True
    blocks: list[Block]


class TagRecord(BaseModel):
    kind: Literal["header", "data", "trailer", "file_header"]
    fields: list[str] | None = None


class RecordTagStructure(BaseModel):
    strategy: Literal["record_tag"] = "record_tag"
    delimiter: str = "|"
    tag_position: int = 0
    records: dict[str, TagRecord]
    unknown_tag: Literal["reject", "ignore", "fail"] = "reject"


Structure = Annotated[
    SentinelStructure | RecordTagStructure, Field(discriminator="strategy")
]


# --------------------------------------------------------------------------- #
# header parsers
# --------------------------------------------------------------------------- #


class DdlHeader(BaseModel):
    """Header block is a mini-DDL: one 'name type(width)' per line."""

    parser: Literal["ddl"] = "ddl"
    pattern: str = r"^(?P<name>\S+)\s+(?P<type>\w+)\((?P<width>\d+)\)$"
    type_map: dict[str, str] = Field(
        default_factory=lambda: {"string": "utf8", "int": "int64", "dec": "float64"}
    )


class NameRowHeader(BaseModel):
    """Header block is one delimited row of column names."""

    parser: Literal["name_row"] = "name_row"
    skip_leading_fields: int = 0


class SuppliedHeader(BaseModel):
    """No header in the file; the spec names the columns."""

    parser: Literal["supplied"] = "supplied"
    columns: list[str]


Header = Annotated[
    DdlHeader | NameRowHeader | SuppliedHeader, Field(discriminator="parser")
]


# --------------------------------------------------------------------------- #
# data parser
# --------------------------------------------------------------------------- #


class DelimitedData(BaseModel):
    parser: Literal["delimited"] = "delimited"
    delimiter: str = ","
    quote: str | None = '"'
    drop_columns: list[str] = Field(default_factory=list)
    ragged: Literal["truncate", "fail"] = "truncate"


class Validate(BaseModel):
    trailer_token: str | None = None
    require_trailer: bool = False
    # lift fields off the file_header record onto every data row
    promote_fields: list[str] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# the three parser kinds
# --------------------------------------------------------------------------- #


class NativeParser(BaseModel):
    """A file that is just a file. Polars does all the work."""

    kind: Literal["native"] = "native"
    format: Literal["csv"] = "csv"
    delimiter: str = ","
    has_header: bool = True
    columns: list[str] | None = None
    coerce: Coerce = Field(default_factory=Coerce)


class SectionedParser(BaseModel):
    """A file with regions: banners, header blocks, record tags, trailers."""

    kind: Literal["sectioned"] = "sectioned"
    framer: Framer = Field(default_factory=Framer)
    structure: Structure
    header: Header
    data: DelimitedData = Field(default_factory=DelimitedData)
    coerce: Coerce = Field(default_factory=Coerce)
    validate_: Validate = Field(default_factory=Validate, alias="validate")

    model_config = {"populate_by_name": True}


class PluginParser(BaseModel):
    """Your code. Registered by name."""

    kind: Literal["plugin"] = "plugin"
    ref: str
    options: dict = Field(default_factory=dict)


Parser = Annotated[
    NativeParser | SectionedParser | PluginParser, Field(discriminator="kind")
]


# --------------------------------------------------------------------------- #
# the feed
# --------------------------------------------------------------------------- #


class Source(BaseModel):
    pattern: str
    members: str = "*"


class Target(BaseModel):
    table: str  # "namespace.table"


class Policy(BaseModel):
    on_reject: Literal["quarantine", "fail"] = "quarantine"
    max_reject_ratio: float = 0.01
    workers: int | None = None
    # "the feed grew a column" -- fail by default, because a schema you did not
    # ask for is usually a parser bug, not a real change upstream.
    schema_change: Literal["fail", "evolve"] = "fail"


class FeedSpec(BaseModel):
    name: str
    source: Source
    parser: Parser
    target: Target
    policy: Policy = Field(default_factory=Policy)

    @classmethod
    def from_yaml(cls, path) -> FeedSpec:
        with open(path) as fh:
            return cls.model_validate(yaml.safe_load(fh))

    def hash(self) -> str:
        import hashlib

        canon = self.model_dump_json(exclude={"name", "source", "target", "policy"})
        return hashlib.sha256(canon.encode()).hexdigest()[:16]
