from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from spark_opt.detectors import Finding
from spark_opt.config import SparkConf

@dataclass
class Recommendation:
    severity: str
    title: str
    rationale: str
    actions: List[str]
    stage_id: Optional[int] = None
    evidence: Optional[Dict[str, Any]] = None

def recommend(findings: List[Finding], conf: SparkConf) -> List[Recommendation]:
    aqe = conf.get_bool("spark.sql.adaptive.enabled", False)
    skew_join = conf.get_bool("spark.sql.adaptive.skewJoin.enabled", False)

    recs: List[Recommendation] = []
    for f in findings:
        if f.code == "SKEW_DETECTED":
            actions = [
                "Identify skewed keys (join/groupBy). Consider salting, pre-aggregation, or skew-aware partitioning.",
                "Enable AQE: spark.sql.adaptive.enabled=true.",
                "Enable skew join handling: spark.sql.adaptive.skewJoin.enabled=true.",
                "Consider broadcast join for small dimension side (broadcast hint or threshold tuning).",
            ]
            if aqe:
                actions[1] = "AQE is already enabled; validate that coalescing/skew handling is engaged in the plan."
            if skew_join:
                actions[2] = "Skew join handling is already enabled; review thresholds and partition sizing."

            recs.append(Recommendation(
                severity=f.severity,
                title="Mitigate data skew (straggler tasks)",
                rationale="High p95/max vs p50 task duration implies skew; a few tasks dominate stage time.",
                actions=actions,
                stage_id=f.stage_id,
                evidence=f.evidence,
            ))

        elif f.code == "SHUFFLE_HEAVY":
            recs.append(Recommendation(
                severity=f.severity,
                title="Reduce shuffle I/O (joins/aggregations)",
                rationale="Large shuffle read/write often dominates runtime and triggers spill/GC.",
                actions=[
                    "Check join strategy: broadcast small tables where possible.",
                    "Repartition on join keys before a large join if upstream data is poorly distributed.",
                    "Prefer reduceByKey/map-side combines over groupByKey where applicable.",
                    "Enable AQE for shuffle partition coalescing (spark.sql.adaptive.enabled=true).",
                ],
                stage_id=f.stage_id,
                evidence=f.evidence,
            ))

        elif f.code == "SPILL_DETECTED":
            recs.append(Recommendation(
                severity=f.severity,
                title="Reduce spills (memory pressure)",
                rationale="Spills indicate insufficient memory per task or oversized shuffle partitions.",
                actions=[
                    "Increase executor memory or reduce shuffle partition size to fit in memory.",
                    "Address skew: skewed partitions often cause spills.",
                    "Avoid caching large intermediates unless reused; unpersist aggressively.",
                ],
                stage_id=f.stage_id,
                evidence=f.evidence,
            ))

        elif f.code == "GC_PRESSURE":
            recs.append(Recommendation(
                severity=f.severity,
                title="Lower GC overhead",
                rationale="High GC% suggests frequent allocation or large row sizes during shuffle/aggregation.",
                actions=[
                    "Increase executor memory or tune memory settings (platform-specific).",
                    "Reduce wide rows and avoid exploding nested structures unnecessarily.",
                    "Prefer Spark SQL expressions over Python UDFs when possible.",
                ],
                stage_id=f.stage_id,
                evidence=f.evidence,
            ))

        elif f.code in ("OVER_PARTITIONED", "UNDER_PARTITIONED", "LOW_DEFAULT_PARALLELISM"):
            if f.code == "OVER_PARTITIONED":
                recs.append(Recommendation(
                    severity=f.severity,
                    title="Reduce over-partitioning (scheduler overhead)",
                    rationale="Excess partitions create tiny tasks and increase overhead.",
                    actions=[
                        "Lower spark.sql.shuffle.partitions to ~2–4x total cores for many ETL workloads.",
                        "If AQE is enabled, keep shuffle.partitions moderate and rely on coalescing.",
                        "Coalesce before write to avoid many small output files.",
                    ],
                    evidence=f.evidence,
                ))
            elif f.code == "UNDER_PARTITIONED":
                recs.append(Recommendation(
                    severity=f.severity,
                    title="Increase parallelism (under-partitioning)",
                    rationale="Too few partitions can underutilize executors and increase task duration.",
                    actions=[
                        "Increase spark.sql.shuffle.partitions to ~2–4x total cores for shuffle-heavy jobs.",
                        "Repartition by stable keys before wide operations if parallelism remains low.",
                    ],
                    evidence=f.evidence,
                ))
            else:
                recs.append(Recommendation(
                    severity=f.severity,
                    title="Tune default parallelism",
                    rationale="Low default parallelism can limit throughput for RDD-heavy jobs.",
                    actions=[
                        "Set spark.default.parallelism to 2–3x total cores for RDD-heavy workloads.",
                        "For Spark SQL, focus on shuffle partitions + AQE.",
                    ],
                    evidence=f.evidence,
                ))

    order = {"ERROR": 0, "WARN": 1, "INFO": 2}
    recs.sort(key=lambda r: (order.get(r.severity, 9), r.stage_id if r.stage_id is not None else 10**9))
    return recs
