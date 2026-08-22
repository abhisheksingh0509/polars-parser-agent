"""End-to-end acceptance check. Cross-platform, no shell required.

    python scripts/smoke.py

Builds a throwaway zip, runs a real ingestion, reads the result back out of
Iceberg, asserts the gates behave, and cleans up. Exits non-zero on failure so
CI or an agent can use it as a pass/fail signal.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from ffe.core.plugins import load_dir  # noqa: E402
from ffe.core.spec import FeedSpec  # noqa: E402
from ffe.io import sink  # noqa: E402
from ffe.io.runner import run  # noqa: E402

GOOD = (
    "F|issue.{n}|20260822\n"
    "H|20260822|issue|id|name\n"
    "I|1|Abhishek\nI|2|Nilanjana\nI|3|Ankita\n"
    "E|issue\n"
)
BAD = (
    "F|issue.x|20260822\n"
    "H|20260822|issue|id|name\n"
    "I|NOTANUMBER|Broken\n"
    "E|issue\n"
)

checks: list[tuple[str, bool, str]] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    checks.append((label, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}{'  -- ' + detail if detail else ''}")


def feed(zip_path: Path, table: str, **policy) -> FeedSpec:
    spec = FeedSpec.from_yaml(ROOT / "feeds" / "pipe-tagged-feed.yaml")
    spec.source.pattern = str(zip_path)
    spec.target.table = table
    for key, value in policy.items():
        setattr(spec.policy, key, value)
    return spec


def main() -> int:
    load_dir(ROOT / "plugins")
    work = Path(tempfile.mkdtemp(prefix="ffe-smoke-"))
    try:
        print(f"workspace: {work}\n")

        # ---- a clean 20-member archive ---------------------------------
        clean = work / "clean.zip"
        with zipfile.ZipFile(clean, "w", zipfile.ZIP_DEFLATED) as z:
            for n in range(20):
                z.writestr(f"issue_{n:03d}.txt", GOOD.format(n=n))

        print("1. parallel fan-out into one Iceberg snapshot")
        result = run(feed(clean, "bronze.smoke"), work / "ws", workers=4)
        check("job succeeded", result.status == "ok", result.status)
        check("all 20 members parsed", result.members_ok == 20, str(result.members_ok))
        check("60 rows landed", result.rows == 60, str(result.rows))
        check("20 staged files", result.commit.get("files") == 20, str(result.commit))
        check("ONE snapshot", result.commit.get("snapshot_id") is not None)

        print("\n2. read it back out of Iceberg")
        df = sink.scan(work / "ws" / "warehouse", "bronze.smoke")
        check("row count matches", len(df) == 60, str(len(df)))
        check("every member traceable", df["_src_file"].n_unique() == 20)
        lineage = ["_src_file", "_src_line_no", "_job_id", "_spec_hash", "_ingested_at"]
        check("lineage complete", all(c in df.columns for c in lineage))
        check("no null lineage", all(df[c].null_count() == 0 for c in lineage))
        check("promoted business_date present", "20260822" in df["business_date"].to_list())

        # ---- a feed that is mostly broken -------------------------------
        print("\n3. the reject gate refuses to commit a broken feed")
        dirty = work / "dirty.zip"
        with zipfile.ZipFile(dirty, "w") as z:
            z.writestr("ok.txt", GOOD.format(n=0))
            for n in range(3):
                z.writestr(f"bad_{n}.txt", BAD)

        gated = run(feed(dirty, "bronze.smoke_gate", max_reject_ratio=0.01), work / "ws", workers=2)
        check("job failed as designed", gated.status == "failed", gated.status)
        check("nothing was committed", gated.commit == {}, str(gated.commit))
        check(
            "reason is the ratio gate",
            any(e.get("error") == "reject_ratio_exceeded" for e in gated.errors),
        )

        print("\n4. rejects are quarantined when the ratio allows it")
        kept = run(feed(dirty, "bronze.smoke_rej", max_reject_ratio=0.9), work / "ws", workers=2)
        check("good rows still landed", kept.rows == 3, str(kept.rows))
        check("bad rows quarantined", kept.rejects == 3, str(kept.rejects))
        rej = sink.scan(work / "ws" / "warehouse", "bronze.smoke_rej_rejects")
        check("reject keeps the original value", "NOTANUMBER" in rej["id"].to_list())
        check("reject carries a reason", rej["_reject_reason"].null_count() == 0)
        check("reject carries its line number", rej["_src_line_no"].to_list() == [3, 3, 3])

        print("\n5. executors agree")
        rows = {}
        for mode in ("serial", "thread", "process"):
            r = run(feed(clean, f"bronze.smoke_{mode}"), work / mode, workers=4, executor=mode)
            rows[mode] = r.rows
        check("identical results", len(set(rows.values())) == 1, str(rows))

    finally:
        shutil.rmtree(work, ignore_errors=True)

    failed = [label for label, ok, _ in checks if not ok]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed")
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    print("smoke test OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
