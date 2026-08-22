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
