"""The plumbing: zip fan-out, staging, the single Iceberg commit, the gates."""

import zipfile
from pathlib import Path

import pytest

from ffe.core.spec import FeedSpec
from ffe.io import sink
from ffe.io.ledger import Ledger
from ffe.io.runner import run
from ffe.io.source import resolve

ROOT = Path(__file__).parent.parent
MEMBER = (
    "F|issue.{n}|20260822\nH|20260822|issue|id|name\n"
    "I|1|Abhishek\nI|2|Nilanjana\nE|issue\n"
)


@pytest.fixture
def zip_of(tmp_path):
    def build(count: int, bad: int = 0) -> Path:
        path = tmp_path / "drop.zip"
        with zipfile.ZipFile(path, "w") as z:
            for n in range(count):
                z.writestr(f"issue_{n:03d}.txt", MEMBER.format(n=n))
            for n in range(bad):
                z.writestr(
                    f"broken_{n:03d}.txt",
                    "F|x|20260822\nH|20260822|issue|id|name\nI|NOTANUM|X\nE|issue\n",
                )
        return path

    return build


def feed_for(zip_path: Path, table: str, **policy) -> FeedSpec:
    spec = FeedSpec.from_yaml(ROOT / "feeds" / "pipe-tagged-feed.yaml")
    spec.source.pattern = str(zip_path)
    spec.target.table = table
    for k, v in policy.items():
        setattr(spec.policy, k, v)
    return spec


# --------------------------------------------------------------------------- #


def test_zip_members_are_listed_without_unpacking(zip_of):
    members = resolve(str(zip_of(5)), "*.txt")
    assert len(members) == 5
    assert all(m.kind == "zip" for m in members)
    assert "!" in members[0].label  # archive!member


def test_member_is_picklable_so_each_worker_opens_its_own_handle(zip_of):
    import pickle

    member = resolve(str(zip_of(1)), "*.txt")[0]
    revived = pickle.loads(pickle.dumps(member))
    assert revived.read() == member.read()


def test_many_members_land_in_one_snapshot(tmp_path, zip_of):
    spec = feed_for(zip_of(12), "bronze.t_many")
    result = run(spec, tmp_path / "ws", workers=4)

    assert result.status == "ok"
    assert result.members == 12 and result.members_ok == 12
    assert result.rows == 24  # 2 rows per member
    assert result.commit["files"] == 12
    assert result.commit["snapshot_id"] is not None  # ONE snapshot, not 12

    df = sink.scan(tmp_path / "ws" / "warehouse", "bronze.t_many")
    assert len(df) == 24
    assert df["_src_file"].n_unique() == 12  # every row traceable to its member


def test_lineage_is_on_every_row(tmp_path, zip_of):
    run(feed_for(zip_of(3), "bronze.t_lineage"), tmp_path / "ws", workers=2)
    df = sink.scan(tmp_path / "ws" / "warehouse", "bronze.t_lineage")
    for col in ("_src_file", "_src_line_no", "_job_id", "_spec_hash", "_ingested_at"):
        assert col in df.columns
        assert df[col].null_count() == 0


def test_reject_ratio_gate_commits_nothing(tmp_path, zip_of):
    """The whole point: don't quietly land 99.9% of a broken feed."""
    spec = feed_for(zip_of(1, bad=3), "bronze.t_gate", max_reject_ratio=0.01)
    result = run(spec, tmp_path / "ws", workers=2)

    assert result.status == "failed"
    assert result.reject_ratio > 0.01
    assert result.commit == {}  # nothing committed at all
    assert result.errors[0]["error"] == "reject_ratio_exceeded"
    with pytest.raises(Exception):
        sink.scan(tmp_path / "ws" / "warehouse", "bronze.t_gate")


def test_rejects_get_their_own_table(tmp_path, zip_of):
    spec = feed_for(zip_of(9, bad=1), "bronze.t_rej", max_reject_ratio=0.5)
    result = run(spec, tmp_path / "ws", workers=2)

    assert result.rejects == 1
    assert result.reject_commit["files"] == 1
    rej = sink.scan(tmp_path / "ws" / "warehouse", "bronze.t_rej_rejects")
    assert rej["_reject_reason"][0].startswith("id:")
    assert rej["_src_line_no"][0] == 3  # traceable back to the line


def test_one_broken_member_does_not_take_the_job_down(tmp_path, zip_of):
    path = zip_of(4)
    with zipfile.ZipFile(path, "a") as z:
        z.writestr("garbage.txt", "this file has no structure at all\n")

    result = run(feed_for(path, "bronze.t_partial"), tmp_path / "ws", workers=2)
    assert result.status == "partial"
    assert result.members_ok == 4 and result.failed == 1
    assert result.errors[0]["member"].endswith("garbage.txt")
    assert result.errors[0]["field"]  # the error says what to change
    assert result.commit["files"] == 4  # the good members still landed


def test_ledger_records_every_member(tmp_path, zip_of):
    run(feed_for(zip_of(6), "bronze.t_ledger"), tmp_path / "ws", workers=3)
    ledger = Ledger(tmp_path / "ws" / "ledger.db")
    rows = ledger.conn.execute("SELECT status, COUNT(*) FROM members GROUP BY 1").fetchall()
    assert dict(rows) == {"ok": 6}
    assert ledger.history("pipe-tagged-feed")[0]["status"] == "ok"


def test_empty_source_is_not_an_error(tmp_path):
    spec = feed_for(tmp_path / "nothing" / "*.zip", "bronze.t_empty")
    result = run(spec, tmp_path / "ws")
    assert result.status == "empty" and result.members == 0


@pytest.mark.parametrize("executor", ["serial", "thread", "process"])
def test_all_executors_give_identical_results(tmp_path, zip_of, executor):
    spec = feed_for(zip_of(6), f"bronze.t_{executor}")
    result = run(spec, tmp_path / executor, workers=4, executor=executor)
    assert result.rows == 12 and result.status == "ok"


def test_run_options_reach_the_parser_and_stay_picklable(tmp_path, zip_of):
    """Per-run options ride the worker payload, so they must survive spawn."""
    import pickle

    from ffe.core.plugins import ParseContext

    opts = {"business_date": "2026-08-23"}
    member = resolve(str(zip_of(1)), "*.txt")[0]
    payload = (member, feed_for(zip_of(1), "bronze.x"), "s", "job", None, opts)
    assert pickle.loads(pickle.dumps(payload))[5] == opts

    # and the default is an empty dict, never None -- ctx.options is indexed
    assert ParseContext().options == {}


# --------------------------------------------------------------------------- #
# schema drift: "the feed grew a column"
# --------------------------------------------------------------------------- #


@pytest.fixture
def drifting_zip(tmp_path):
    """Two members, the second carrying an extra field the first lacks."""

    def build(name: str = "drift.zip") -> Path:
        path = tmp_path / name
        with zipfile.ZipFile(path, "w") as z:
            z.writestr(
                "narrow.txt",
                "F|a|20260822\nH|20260822|issue|id|name\nI|1|Abhishek\nE|issue\n",
            )
            z.writestr(
                "wide.txt",
                "F|b|20260822\nH|20260822|issue|id|name|region\n"
                "I|2|Nilanjana|EMEA\nE|issue\n",
            )
        return path

    return build


def test_schema_drift_fails_the_job_by_default(tmp_path, drifting_zip):
    spec = feed_for(drifting_zip(), "bronze.drift_fail")
    assert spec.policy.schema_change == "fail"

    result = run(spec, tmp_path / "ws", plugin_dir=str(ROOT / "plugins"))

    assert result.status == "failed"
    assert result.commit == {}  # nothing landed
    drift = [e for e in result.errors if e.get("error") == "schema_drift"]
    assert drift, result.errors
    # the error must name the knob and the column, not just complain
    assert drift[0]["field"] == "policy.schema_change"
    assert drift[0]["blame"] == "spec"
    assert "region" in drift[0]["observed"]["columns_not_in_every_file"]


def test_schema_drift_evolves_when_asked(tmp_path, drifting_zip):
    spec = feed_for(drifting_zip(), "bronze.drift_evolve", schema_change="evolve")
    result = run(spec, tmp_path / "ws", plugin_dir=str(ROOT / "plugins"))

    assert result.status == "ok", result.errors
    df = sink.scan(tmp_path / "ws" / "warehouse", "bronze.drift_evolve")
    assert "region" in df.columns
    assert len(df) == 2
    # the member that never had the column reads back null, not an error
    assert sorted(df["region"].to_list(), key=lambda v: (v is None, v)) == ["EMEA", None]


def test_a_column_added_in_a_later_job_evolves_the_existing_table(tmp_path):
    """Drift across jobs, not just within one -- the common real case."""
    ws = tmp_path / "ws"
    first = tmp_path / "day1.zip"
    with zipfile.ZipFile(first, "w") as z:
        z.writestr("a.txt", "F|a|20260822\nH|20260822|issue|id|name\nI|1|A\nE|issue\n")
    second = tmp_path / "day2.zip"
    with zipfile.ZipFile(second, "w") as z:
        z.writestr(
            "b.txt",
            "F|b|20260823\nH|20260823|issue|id|name|region\nI|2|B|EMEA\nE|issue\n",
        )

    ok = run(feed_for(first, "bronze.grew", schema_change="evolve"), ws,
             plugin_dir=str(ROOT / "plugins"))
    assert ok.status == "ok"

    grown = run(feed_for(second, "bronze.grew", schema_change="evolve"), ws,
                plugin_dir=str(ROOT / "plugins"))
    assert grown.status == "ok", grown.errors

    df = sink.scan(ws / "warehouse", "bronze.grew")
    assert len(df) == 2 and "region" in df.columns


def test_the_same_growth_is_refused_without_the_policy(tmp_path):
    """Default policy must catch cross-job drift too, not only within a job."""
    ws = tmp_path / "ws"
    first = tmp_path / "day1.zip"
    with zipfile.ZipFile(first, "w") as z:
        z.writestr("a.txt", "F|a|20260822\nH|20260822|issue|id|name\nI|1|A\nE|issue\n")
    second = tmp_path / "day2.zip"
    with zipfile.ZipFile(second, "w") as z:
        z.writestr(
            "b.txt",
            "F|b|20260823\nH|20260823|issue|id|name|region\nI|2|B|EMEA\nE|issue\n",
        )

    assert run(feed_for(first, "bronze.strict"), ws,
               plugin_dir=str(ROOT / "plugins")).status == "ok"

    blocked = run(feed_for(second, "bronze.strict"), ws,
                  plugin_dir=str(ROOT / "plugins"))
    assert blocked.status == "failed"
    assert blocked.errors[0]["error"] == "schema_drift"
    assert len(sink.scan(ws / "warehouse", "bronze.strict")) == 1  # unchanged


def test_evolve_adds_columns_but_never_retypes_one():
    """Adding a column is safe. Retyping rewrites data already committed."""
    import pyarrow as pa

    from ffe.core.report import ParseError
    from ffe.io.sink import _unify

    narrow = pa.schema([("id", pa.string())])
    wider = pa.schema([("id", pa.string()), ("region", pa.string())])
    retyped = pa.schema([("id", pa.int64())])

    assert [f.name for f in _unify("t", [narrow, wider], evolve=True)] == [
        "id",
        "region",
    ]
    with pytest.raises(ParseError) as caught:
        _unify("t", [narrow, retyped], evolve=True)
    assert caught.value.code == "schema_drift"
    assert "TYPE" in caught.value.message


# --------------------------------------------------------------------------- #
# partitioning: one archive is one business day
# --------------------------------------------------------------------------- #


def _day_zip(path: Path, day: str, ids: list[str]) -> Path:
    """One archive, one business day, several members -- the real feed shape."""
    with zipfile.ZipFile(path, "w") as z:
        for n, ident in enumerate(ids):
            z.writestr(
                f"part_{n}.txt",
                f"F|f{n}|{day}\nH|{day}|issue|id|name\nI|{ident}|N{ident}\nE|issue\n",
            )
    return path


def partitioned(zip_path: Path, table: str, by=("business_date",), **policy):
    spec = feed_for(zip_path, table, **policy)
    spec.target.partition_by = list(by)
    return spec


def test_a_partitioned_table_gets_one_partition_per_day(tmp_path):
    ws = tmp_path / "ws"
    day1 = _day_zip(tmp_path / "d1.zip", "20260823", ["1", "2"])
    day2 = _day_zip(tmp_path / "d2.zip", "20260824", ["3"])

    first = run(partitioned(day1, "bronze.parts"), ws, plugin_dir=str(ROOT / "plugins"))
    assert first.status == "ok", first.errors
    second = run(partitioned(day2, "bronze.parts"), ws, plugin_dir=str(ROOT / "plugins"))
    assert second.status == "ok", second.errors

    table = sink.catalog(ws / "warehouse").load_table("bronze.parts")
    assert [f.name for f in table.spec().fields] == ["business_date"]
    # two days, three rows, and each job is still exactly one snapshot
    assert len(table.scan().to_arrow()) == 3
    assert len(table.metadata.snapshots) == 2

    partitions = {f.file.partition[0] for f in table.scan().plan_files()}
    assert len(partitions) == 2

    pruned = table.scan(row_filter="business_date == '20260823'").to_arrow()
    assert len(pruned) == 2


def test_many_members_of_one_day_land_in_one_partition(tmp_path):
    """The assumption that makes this cheap: no staging split needed."""
    ws = tmp_path / "ws"
    day = _day_zip(tmp_path / "d.zip", "20260823", [str(n) for n in range(6)])

    result = run(partitioned(day, "bronze.oneday"), ws, plugin_dir=str(ROOT / "plugins"))
    assert result.status == "ok", result.errors
    assert result.commit["files"] == 6  # six members, six staged files

    table = sink.catalog(ws / "warehouse").load_table("bronze.oneday")
    assert len({f.file.partition[0] for f in table.scan().plan_files()}) == 1


def test_partitioning_by_a_column_the_parser_does_not_emit_is_refused(tmp_path):
    result = run(
        partitioned(_day_zip(tmp_path / "d.zip", "20260823", ["1"]),
                    "bronze.nocol", by=("no_such_column",)),
        tmp_path / "ws", plugin_dir=str(ROOT / "plugins"),
    )
    assert result.status == "failed"
    err = result.errors[0]
    assert err["error"] == "partition_column_missing"
    assert err["field"] == "target.partition_by"
    assert "no_such_column" in err["observed"]["requested"]


def test_an_existing_unpartitioned_table_is_not_silently_repartitioned(tmp_path):
    """A load must not change the layout of a table that already holds data."""
    ws = tmp_path / "ws"
    flat = _day_zip(tmp_path / "d1.zip", "20260823", ["1"])
    assert run(feed_for(flat, "bronze.wasflat"), ws,
               plugin_dir=str(ROOT / "plugins")).status == "ok"

    later = run(partitioned(_day_zip(tmp_path / "d2.zip", "20260824", ["2"]),
                            "bronze.wasflat"),
                ws, plugin_dir=str(ROOT / "plugins"))
    assert later.status == "failed"
    assert later.errors[0]["error"] == "partition_spec_conflict"
    assert later.errors[0]["observed"]["table_partitioned_by"] == []
    assert len(sink.scan(ws / "warehouse", "bronze.wasflat")) == 1  # untouched


def test_rejects_are_not_partitioned(tmp_path, zip_of):
    """A rejected row may have failed on the partition column itself."""
    ws = tmp_path / "ws"
    spec = feed_for(zip_of(4, bad=1), "bronze.rej", max_reject_ratio=0.5)
    spec.target.partition_by = ["business_date"]

    result = run(spec, ws, plugin_dir=str(ROOT / "plugins"))
    assert result.status == "ok", result.errors
    assert result.reject_commit["rows"] >= 1

    cat = sink.catalog(ws / "warehouse")
    assert [f.name for f in cat.load_table("bronze.rej").spec().fields] == [
        "business_date"
    ]
    assert cat.load_table("bronze.rej_rejects").spec().fields == ()  # unpartitioned
