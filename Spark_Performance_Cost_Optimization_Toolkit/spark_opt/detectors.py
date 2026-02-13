from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from spark_opt.metrics import StageMetrics
from spark_opt.config import SparkConf

@dataclass
class Finding:
    code: str
    severity: str  # INFO|WARN|ERROR
    stage_id: Optional[int]
    message: str
    evidence: Dict[str, Any]

def detect_skew(stages: List[StageMetrics], p95_p50_warn: float = 3.0, max_p50_warn: float = 8.0) -> List[Finding]:
    out: List[Finding] = []
    for s in stages:
        if s.num_tasks < 10:
            continue
        if s.skew_ratio_p95_p50 >= p95_p50_warn or s.skew_ratio_max_p50 >= max_p50_warn:
            sev = "ERROR" if s.skew_ratio_max_p50 >= (max_p50_warn * 2) else "WARN"
            out.append(Finding(
                code="SKEW_DETECTED",
                severity=sev,
                stage_id=s.stage_id,
                message=f"Stage {s.stage_id} shows task skew (p95/p50={s.skew_ratio_p95_p50:.2f}, max/p50={s.skew_ratio_max_p50:.2f}).",
                evidence={"p95_p50": s.skew_ratio_p95_p50, "max_p50": s.skew_ratio_max_p50, "p50_ms": s.task_p50_ms, "p95_ms": s.task_p95_ms, "max_ms": s.task_max_ms},
            ))
    return out

def detect_shuffle_heavy(stages: List[StageMetrics], shuffle_mb_warn: float = 1024.0) -> List[Finding]:
    out: List[Finding] = []
    for s in stages:
        total = s.shuffle_read_mb + s.shuffle_write_mb
        if total >= shuffle_mb_warn:
            sev = "ERROR" if total >= shuffle_mb_warn * 5 else "WARN"
            out.append(Finding(
                code="SHUFFLE_HEAVY",
                severity=sev,
                stage_id=s.stage_id,
                message=f"Stage {s.stage_id} is shuffle-heavy (~{total:.0f} MB shuffle I/O).",
                evidence={"shuffle_read_mb": s.shuffle_read_mb, "shuffle_write_mb": s.shuffle_write_mb, "spill_mb": s.spill_mb},
            ))
    return out

def detect_spill_or_gc(stages: List[StageMetrics], spill_mb_warn: float = 512.0, gc_pct_warn: float = 0.10) -> List[Finding]:
    out: List[Finding] = []
    for s in stages:
        if s.spill_mb >= spill_mb_warn:
            out.append(Finding(
                code="SPILL_DETECTED",
                severity="WARN" if s.spill_mb < spill_mb_warn * 5 else "ERROR",
                stage_id=s.stage_id,
                message=f"Stage {s.stage_id} spilled ~{s.spill_mb:.0f} MB, indicating memory pressure.",
                evidence={"spill_mb": s.spill_mb},
            ))
        if s.gc_pct >= gc_pct_warn:
            out.append(Finding(
                code="GC_PRESSURE",
                severity="WARN" if s.gc_pct < gc_pct_warn * 2 else "ERROR",
                stage_id=s.stage_id,
                message=f"Stage {s.stage_id} spent {s.gc_pct*100:.1f}% of task time in GC.",
                evidence={"gc_pct": s.gc_pct},
            ))
    return out

def detect_partitioning_issues(stages: List[StageMetrics], conf: SparkConf, cores_total: Optional[int] = None) -> List[Finding]:
    out: List[Finding] = []
    shuffle_parts = conf.get_int("spark.sql.shuffle.partitions", 200)
    default_par = conf.get_int("spark.default.parallelism", 0)

    if cores_total:
        if shuffle_parts >= cores_total * 20:
            out.append(Finding(
                code="OVER_PARTITIONED",
                severity="WARN",
                stage_id=None,
                message=f"spark.sql.shuffle.partitions={shuffle_parts} is very high vs total cores ({cores_total}).",
                evidence={"shuffle_partitions": shuffle_parts, "cores_total": cores_total},
            ))
        if shuffle_parts <= max(8, cores_total // 4):
            out.append(Finding(
                code="UNDER_PARTITIONED",
                severity="WARN",
                stage_id=None,
                message=f"spark.sql.shuffle.partitions={shuffle_parts} may be low vs total cores ({cores_total}).",
                evidence={"shuffle_partitions": shuffle_parts, "cores_total": cores_total},
            ))
        if default_par and default_par <= max(8, cores_total // 4):
            out.append(Finding(
                code="LOW_DEFAULT_PARALLELISM",
                severity="INFO",
                stage_id=None,
                message=f"spark.default.parallelism={default_par} may be low vs cores ({cores_total}).",
                evidence={"default_parallelism": default_par, "cores_total": cores_total},
            ))
    return out
