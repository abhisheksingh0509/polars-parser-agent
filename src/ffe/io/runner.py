"""Parallel across files, sequential within a file, single at the finish line.

  1. list what's inside (nothing read yet)
  2. N workers, each parsing its own member onto its own scrap paper
  3. one commit at the end

No worker ever touches Iceberg. That is what makes the commit conflict-free.
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from ..core import plugins
from ..core.engine import parse
from ..core.report import ParseError
from ..core.spec import FeedSpec
from . import sink, staging
from .ledger import Ledger
from .source import Member, resolve


@dataclass
class JobResult:
    job_id: str
    feed: str
    members: int = 0
    members_ok: int = 0
    failed: int = 0
    rows: int = 0
    rejects: int = 0
    reject_ratio: float = 0.0
    status: str = "ok"
    commit: dict = field(default_factory=dict)
    reject_commit: dict = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        return d


# --------------------------------------------------------------------------- #
# runs inside a worker process -- must be module level to be picklable
# --------------------------------------------------------------------------- #


def _process(args) -> dict:
    member, spec, staging_root, job_id, plugin_dir, options = args
    if plugin_dir:
        plugins.load_dir(plugin_dir)

    rec: dict = {"member": member.label}
    try:
        raw = member.read()
        result = parse(
            spec.parser,
            raw,
            plugins.ParseContext(
                member=member.label, job_id=job_id, options=dict(options)
            ),
        )
        spec_hash = spec.hash()

        good = staging.add_lineage(result.frame, member.label, job_id, spec_hash)
        rec["staged"] = staging.write(good, Path(staging_root), job_id, "good", member.label)

        if result.rejects is not None and len(result.rejects):
            bad = staging.add_lineage(result.rejects, member.label, job_id, spec_hash)
            rec["staged_rejects"] = staging.write(
                bad, Path(staging_root), job_id, "reject", member.label
            )

        rep = result.report
        rec.update(
            status="ok",
            parser=rep.parser,
            rows_parsed=rep.rows_parsed,
            rows_rejected=rep.rows_rejected,
            ragged=rep.ragged_lines,
            duration_ms=rep.duration_ms,
        )
    except ParseError as exc:
        rec.update(status="failed", error=exc.to_dict())
    except Exception as exc:  # a broken file must not take the job down
        rec.update(
            status="failed",
            error={"error": "unhandled", "message": f"{type(exc).__name__}: {exc}"},
        )
    return rec


# --------------------------------------------------------------------------- #


def run(
    spec: FeedSpec,
    workspace: Path,
    plugin_dir: str | None = None,
    workers: int | None = None,
    job_id: str | None = None,
    executor: str = "thread",
    options: dict | None = None,
) -> JobResult:
    """`options` is per-run parser input -- a business date, a cutoff, a mode.

    It reaches a plugin as `ctx.options`, layered over `parser.options` from the
    spec so a run can override the YAML without editing it. A plain dict, so the
    payload stays picklable for `--executor process`.
    """
    job_id = job_id or uuid.uuid4().hex[:12]
    workspace = Path(workspace)
    staging_root = workspace / "staging"
    ledger = Ledger(workspace / "ledger.db")

    members: list[Member] = resolve(spec.source.pattern, spec.source.members)
    result = JobResult(job_id=job_id, feed=spec.name, members=len(members))
    ledger.start(job_id, spec.name, spec.target.table, len(members))

    if not members:
        result.status = "empty"
        ledger.finish(job_id, "empty", 0, 0)
        return result

    n = workers or spec.policy.workers or min(8, (os.cpu_count() or 2))
    opts = dict(options or {})
    payloads = [
        (m, spec, str(staging_root), job_id, plugin_dir, opts) for m in members
    ]

    # ---- step 2: parallel, isolated, nothing shared -----------------------
    # Executor choice is a real tradeoff, not a detail:
    #   serial  - fewest surprises; fastest when members are small
    #   thread  - no spawn cost; Polars releases the GIL for its own work
    #   process - the only real win when a plugin loops rows in Python
    if n == 1 or len(members) == 1 or executor == "serial":
        records = [_process(p) for p in payloads]
    else:
        pool_cls = ThreadPoolExecutor if executor == "thread" else ProcessPoolExecutor
        with pool_cls(max_workers=n) as pool:
            records = list(pool.map(_process, payloads))

    good_files, reject_files = [], []
    for rec in records:
        ledger.member(job_id, rec)
        if rec["status"] == "ok":
            result.members_ok += 1
            result.rows += rec.get("rows_parsed", 0)
            result.rejects += rec.get("rows_rejected", 0)
            good_files.append(rec["staged"])
            if rec.get("staged_rejects"):
                reject_files.append(rec["staged_rejects"])
        else:
            result.failed += 1
            result.errors.append({"member": rec["member"], **rec["error"]})

    total = result.rows + result.rejects
    result.reject_ratio = (result.rejects / total) if total else 0.0

    # ---- gate: don't quietly land 99.9% of a broken feed ------------------
    if result.reject_ratio > spec.policy.max_reject_ratio:
        result.status = "failed"
        msg = (
            f"reject ratio {result.reject_ratio:.3f} exceeds "
            f"max_reject_ratio {spec.policy.max_reject_ratio}; nothing committed"
        )
        result.errors.append({"error": "reject_ratio_exceeded", "message": msg})
        ledger.finish(job_id, "failed", result.rows, result.rejects, error=msg)
        return result

    if result.failed and spec.policy.on_reject == "fail":
        result.status = "failed"
        ledger.finish(
            job_id, "failed", result.rows, result.rejects,
            error=f"{result.failed} member(s) failed to parse; nothing committed",
        )
        return result

    # ---- step 3: one writer, one snapshot --------------------------------
    # Schema drift surfaces here rather than in a worker, because it is a
    # property of the SET of staged files, not of any one member. It is a
    # policy decision with a structured error, so it fails the job cleanly
    # instead of escaping as an unhandled crash.
    try:
        result.commit = sink.commit(
            workspace / "warehouse",
            spec.target.table,
            good_files,
            schema_change=spec.policy.schema_change,
        )
        if reject_files:
            result.reject_commit = sink.commit(
                workspace / "warehouse",
                f"{spec.target.table}_rejects",
                reject_files,
                schema_change=spec.policy.schema_change,
            )
    except ParseError as exc:
        result.status = "failed"
        result.errors.append(exc.to_dict())
        ledger.finish(job_id, "failed", result.rows, result.rejects, error=exc.message)
        return result

    result.status = "ok" if not result.failed else "partial"
    ledger.finish(
        job_id, result.status, result.rows, result.rejects,
        snapshot=str(result.commit.get("snapshot_id")),
    )
    return result
