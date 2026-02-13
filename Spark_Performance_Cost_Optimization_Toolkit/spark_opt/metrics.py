from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple
import numpy as np
import pandas as pd
from spark_opt.eventlog_reader import StageCompleted, TaskEnd

@dataclass
class StageMetrics:
    stage_id: int
    attempt: int
    name: str
    num_tasks: int
    stage_duration_ms: int
    task_p50_ms: float
    task_p95_ms: float
    task_max_ms: int
    skew_ratio_p95_p50: float
    skew_ratio_max_p50: float
    gc_pct: float
    shuffle_read_mb: float
    shuffle_write_mb: float
    spill_mb: float

def _percentile(values: List[int], p: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(np.array(values, dtype=np.float64), p))

def build_stage_metrics(stages: List[StageCompleted], tasks: List[TaskEnd]) -> Tuple[pd.DataFrame, List[StageMetrics]]:
    by = {}
    for t in tasks:
        by.setdefault((t.stage_id, t.attempt), []).append(t)

    rows = []
    objs: List[StageMetrics] = []
    for s in stages:
        ts = by.get((s.stage_id, s.attempt), [])
        durations = [int(t.duration_ms) for t in ts if t.duration_ms is not None]
        p50 = _percentile(durations, 50)
        p95 = _percentile(durations, 95)
        mx = int(max(durations) if durations else 0)

        gc = sum(int(t.gc_time_ms) for t in ts)
        run = sum(int(t.duration_ms) for t in ts) or 1
        gc_pct = float(gc) / float(run)

        shuffle_read = sum(int(t.shuffle_read_bytes) for t in ts) / (1024 * 1024)
        shuffle_write = sum(int(t.shuffle_write_bytes) for t in ts) / (1024 * 1024)
        spill = (sum(int(t.spill_mem_bytes) for t in ts) + sum(int(t.spill_disk_bytes) for t in ts)) / (1024 * 1024)

        if s.submission_time_ms is not None and s.completion_time_ms is not None:
            stage_dur = int(max(0, s.completion_time_ms - s.submission_time_ms))
        else:
            stage_dur = mx

        skew_p95_p50 = float(p95 / p50) if p50 > 0 else 0.0
        skew_max_p50 = float(mx / p50) if p50 > 0 else 0.0

        obj = StageMetrics(
            stage_id=s.stage_id, attempt=s.attempt, name=s.name, num_tasks=s.num_tasks,
            stage_duration_ms=stage_dur, task_p50_ms=p50, task_p95_ms=p95, task_max_ms=mx,
            skew_ratio_p95_p50=skew_p95_p50, skew_ratio_max_p50=skew_max_p50,
            gc_pct=gc_pct, shuffle_read_mb=float(shuffle_read), shuffle_write_mb=float(shuffle_write), spill_mb=float(spill),
        )
        objs.append(obj)
        rows.append(obj.__dict__)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["stage_duration_ms"], ascending=False)
    return df, objs
