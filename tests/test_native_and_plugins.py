"""The fast path (a file that is just a file) and the plugin SPI."""

from pathlib import Path

import pytest

from ffe.core.engine import parse
from ffe.core.plugins import load_dir
from ffe.core.report import ParseError
from ffe.core.spec import Coerce, FeedSpec, NativeParser, PluginParser

ROOT = Path(__file__).parent.parent
FIX = Path(__file__).parent / "fixtures"


def test_native_csv_needs_no_code():
    spec = NativeParser(coerce=Coerce(schema_override={"id": "int64", "score": "int64"}))
    r = parse(spec, (FIX / "plain.csv").read_bytes())
    assert r.frame["id"].to_list() == [1, 2]
    assert str(r.frame.schema["score"]) == "Int64"
    assert r.frame["_src_line_no"].to_list() == [2, 3]  # header is line 1


def test_native_types_are_frozen_by_the_spec_not_inferred():
    """No schema_override means everything stays text. Inference is a
    spec-authoring aid, never a runtime behaviour."""
    r = parse(NativeParser(), (FIX / "plain.csv").read_bytes())
    assert all(str(d) == "String" for n, d in r.frame.schema.items() if n != "_src_line_no")


def test_plugin_spi_end_to_end():
    load_dir(ROOT / "plugins")
    r = parse(PluginParser(ref="acme-positions"), (FIX / "acme_positions.txt").read_bytes())
    assert r.frame["name"].to_list() == ["Abhishek", "Nilanjana"]
    assert r.frame["amount"].to_list() == [125.0, 340.0]
    assert r.report.parser == "plugin/acme-positions"


def test_plugin_reject_and_trailer_contract():
    load_dir(ROOT / "plugins")
    r = parse(PluginParser(ref="acme-positions"), (FIX / "acme_positions.txt").read_bytes())
    assert r.report.rows_rejected == 1
    assert r.rejects.row(0, named=True)["_src_line_no"] == 4
    # the trailer declares 3 records; we saw 2 good + 1 rejected
    assert r.report.trailer_declared_rows == 3
    assert r.report.rows_parsed + r.report.rows_rejected == 3


def test_plugin_options_reach_the_plugin():
    load_dir(ROOT / "plugins")
    r = parse(
        PluginParser(ref="acme-positions", options={"amount_scale": 1}),
        (FIX / "acme_positions.txt").read_bytes(),
    )
    assert r.frame["amount"].to_list() == [12500.0, 34000.0]


def test_plugin_shape_comes_from_the_spec_not_the_code():
    """Same bytes, renamed columns, no code change -- the field table is config.
    Pins the plugin-contract rule: hardcoding it would need a second plugin."""
    load_dir(ROOT / "plugins")
    r = parse(
        PluginParser(ref="acme-positions", options={
            "fields": [["Position_Id", 1, 4, "int"], ["Holder", 4, 24, "str"]],
        }),
        (FIX / "acme_positions.txt").read_bytes(),
    )
    assert r.frame.columns == ["Position_Id", "Holder", "_src_line_no"]
    assert r.frame["Holder"].to_list() == ["Abhishek", "Nilanjana", "BADAMOUNT"]
    assert str(r.frame.schema["Position_Id"]) == "Int64"


def test_plugin_rejects_hold_the_original_value_not_a_cast_one():
    load_dir(ROOT / "plugins")
    r = parse(PluginParser(ref="acme-positions"), (FIX / "acme_positions.txt").read_bytes())
    bad = r.rejects.row(0, named=True)
    assert bad["amount"] == "00000ABCDE"  # the raw slice, not a partial cast
    assert bad["id"] == "003"
    assert "00000ABCDE" in bad["_reject_reason"]


def test_plugin_bad_option_is_blamed_on_the_spec():
    load_dir(ROOT / "plugins")
    with pytest.raises(ParseError) as caught:
        parse(
            PluginParser(ref="acme-positions", options={"fields": [["id", 4, 1, "int"]]}),
            (FIX / "acme_positions.txt").read_bytes(),
        )
    assert caught.value.blame == "spec"
    assert caught.value.field == "parser.options.fields"


def test_acme_spec_states_its_shape_rather_than_inheriting_it():
    """A fixed-width file has no header, so the spec is the only place the shape
    can be declared -- and the committed spec must declare it, not fall back to
    the plugin's defaults, or a column rename never shows up in a diff.

    Also the only coverage of feeds/acme-positions.yaml being loadable at all.
    """
    load_dir(ROOT / "plugins")
    spec = FeedSpec.from_yaml(ROOT / "feeds" / "acme-positions.yaml")
    assert "fields" in spec.parser.options, "the spec must state its own columns"

    r = parse(spec.parser, (FIX / "acme_positions.txt").read_bytes())
    assert r.frame.columns == ["id", "name", "amount", "_src_line_no"]
    assert [str(d) for d in r.frame.dtypes] == ["Int64", "String", "Float64", "UInt32"]
    assert r.frame["amount"].to_list() == [125.0, 340.0]
