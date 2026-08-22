"""The two worked examples are permanent regression fixtures. Example A in
particular: '*' is both the section delimiter and leading noise, which is the
case that catches structure-detection regressions."""

from pathlib import Path

import pytest

from ffe.core.engine import parse
from ffe.core.report import ParseError
from ffe.core.spec import FeedSpec

ROOT = Path(__file__).parent.parent
FIX = Path(__file__).parent / "fixtures"


def feed(name: str) -> FeedSpec:
    return FeedSpec.from_yaml(ROOT / "feeds" / f"{name}.yaml")


# --------------------------------------------------------------------------- #
# example A -- banner delimited, self-describing DDL header
# --------------------------------------------------------------------------- #


def test_banner_ddl():
    r = parse(feed("banner-ddl-feed").parser, (FIX / "banner_ddl.txt").read_bytes())
    assert r.frame.columns == ["name", "age", "_src_line_no"]
    assert str(r.frame.schema["age"]) == "Int64"  # type came from the DDL
    assert r.frame["name"].to_list() == ["ABhishek", "ANkita", "Nilanjana"]
    assert len(r.rejects) == 0
    assert r.report.trailer_seen
    # 'ANkita,20,' has a trailing delimiter: parsed, but reported
    assert r.report.ragged_lines == 1


def test_banner_leading_sentinel_run_is_one_boundary():
    """Three leading '*' lines must collapse to one boundary, not three blocks."""
    r = parse(feed("banner-ddl-feed").parser, (FIX / "banner_ddl.txt").read_bytes())
    assert r.report.rows_parsed == 3


def test_src_line_no_points_at_the_original_file():
    r = parse(feed("banner-ddl-feed").parser, (FIX / "banner_ddl.txt").read_bytes())
    lines = (FIX / "banner_ddl.txt").read_text().splitlines()
    for row in r.frame.iter_rows(named=True):
        assert lines[row["_src_line_no"] - 1].startswith(row["name"])


# --------------------------------------------------------------------------- #
# example B -- tag per record
# --------------------------------------------------------------------------- #


def test_pipe_tagged():
    r = parse(feed("pipe-tagged-feed").parser, (FIX / "pipe_tagged.txt").read_bytes())
    assert r.frame.columns == ["id", "name", "_src_line_no", "business_date", "file_id"]
    assert r.frame["id"].to_list() == [1, 2]
    assert "tag" not in r.frame.columns  # the I was dropped
    assert r.report.unknown_tags == 0
    assert len(r.rejects) == 0


def test_promoted_fields_land_on_every_row():
    """business_date lives in the F record; every data row needs it."""
    r = parse(feed("pipe-tagged-feed").parser, (FIX / "pipe_tagged.txt").read_bytes())
    assert r.frame["business_date"].to_list() == ["20260608", "20260608"]
    assert r.frame["file_id"].to_list() == ["issue.1", "issue.1"]
    # bronze keeps source-faithful types: the date stays as the file wrote it
    assert str(r.frame.schema["business_date"]) == "String"


def test_unknown_tag_is_counted_not_fatal():
    raw = (FIX / "pipe_tagged.txt").read_bytes().replace(b"I|2|", b"X|2|")
    r = parse(feed("pipe-tagged-feed").parser, raw)
    assert r.report.rows_parsed == 1
    assert r.report.unknown_tags == 1


# --------------------------------------------------------------------------- #
# rejects: never raise, always traceable
# --------------------------------------------------------------------------- #


def test_bad_row_is_quarantined_not_fatal():
    spec = feed("banner-ddl-feed").parser
    r = parse(spec, (FIX / "ragged.txt").read_bytes())
    assert r.report.rows_parsed == 2
    assert r.report.rows_rejected == 1
    assert r.report.reject_ratio == pytest.approx(1 / 3)


def test_reject_row_keeps_the_original_value_and_a_reason():
    r = parse(feed("banner-ddl-feed").parser, (FIX / "ragged.txt").read_bytes())
    bad = r.rejects.row(0, named=True)
    assert bad["id"] == "NOTANUMBER"  # the raw text, not a null
    assert bad["_src_line_no"] == 8
    assert "cannot cast" in bad["_reject_reason"]


# --------------------------------------------------------------------------- #
# errors an agent has to act on
# --------------------------------------------------------------------------- #


def test_wrong_delimiter_names_the_field_and_measures_the_fix():
    spec = feed("banner-ddl-feed").parser
    spec.data.delimiter = "|"
    with pytest.raises(ParseError) as exc:
        parse(spec, (FIX / "banner_ddl.txt").read_bytes())
    err = exc.value
    assert err.code == "delimiter_mismatch"
    assert err.field == "parser.data.delimiter"
    assert err.blame == "spec"
    assert err.candidates[0]["value"] == ","  # measured, not guessed
    assert "," in err.hint


def test_truncated_file_is_blamed_on_the_file_not_the_spec():
    raw = (FIX / "pipe_tagged.txt").read_bytes().replace(b"E|issue\n", b"")
    with pytest.raises(ParseError) as exc:
        parse(feed("pipe-tagged-feed").parser, raw)
    assert exc.value.code == "trailer_missing"
    assert exc.value.blame == "file"  # retrying with a new spec won't help


def test_wrong_header_block_reports_a_width_disagreement():
    spec = feed("pipe-tagged-feed").parser
    spec.header.skip_leading_fields = 1  # keeps date+entity as if they were columns
    with pytest.raises(ParseError) as exc:
        parse(spec, (FIX / "pipe_tagged.txt").read_bytes())
    assert exc.value.code == "column_count_mismatch"
    assert exc.value.observed["columns_expected"] == 5


# --------------------------------------------------------------------------- #
# portability: the office machine writes CRLF and Windows editors add a BOM
# --------------------------------------------------------------------------- #


def test_crlf_and_bom_parse_identically_to_lf():
    lf = parse(feed("banner-ddl-feed").parser, (FIX / "banner_ddl.txt").read_bytes())
    crlf = parse(feed("banner-ddl-feed").parser, (FIX / "banner_ddl_crlf.txt").read_bytes())
    assert crlf.frame.equals(lf.frame)
    assert crlf.report.rows_parsed == lf.report.rows_parsed
    # the BOM must not end up glued to the first column name
    assert crlf.frame.columns == lf.frame.columns


def test_bom_is_stripped_not_treated_as_data():
    r = parse(feed("banner-ddl-feed").parser, (FIX / "banner_ddl_crlf.txt").read_bytes())
    assert not any(c.startswith("﻿") for c in r.frame.columns)
