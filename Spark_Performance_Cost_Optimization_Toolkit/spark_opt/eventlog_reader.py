from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json

@dataclass
class TaskEnd:
    stage_id: int
    attempt: int
    task_id: int
    duration_ms: int
    gc_time_ms: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    spill_mem_bytes: int
    spill_disk_bytes: int

@dataclass
class StageCompleted:
    stage_id: int
    attempt: int
    name: str
    num_tasks: int
    submission_time_ms: Optional[int]
    completion_time_ms: Optional[int]

def read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def parse_eventlog(path: str) -> Tuple[List[StageCompleted], List[TaskEnd], Dict[str, Any]]:
    stages: List[StageCompleted] = []
    tasks: List[TaskEnd] = []
    meta: Dict[str, Any] = {"app_id": None, "app_name": None}

    for evt in read_jsonl(path):
        et = evt.get("Event")
        if et == "SparkListenerApplicationStart":
            meta["app_name"] = evt.get("App Name")
            meta["app_id"] = evt.get("App ID") or evt.get("App Id")
        elif et == "SparkListenerStageCompleted":
            info = evt.get("Stage Info") or {}
            stages.append(StageCompleted(
                stage_id=int(info.get("Stage ID", info.get("Stage Id", -1))),
                attempt=int(info.get("Stage Attempt ID", info.get("Stage Attempt Id", 0))),
                name=str(info.get("Stage Name", "")),
                num_tasks=int(info.get("Number of Tasks", 0)),
                submission_time_ms=info.get("Submission Time"),
                completion_time_ms=info.get("Completion Time"),
            ))
        elif et == "SparkListenerTaskEnd":
            si = evt.get("Stage ID", evt.get("Stage Id", -1))
            sa = evt.get("Stage Attempt ID", evt.get("Stage Attempt Id", 0))
            ti = evt.get("Task Info") or {}
            metrics = evt.get("Task Metrics") or {}
            srm = (metrics.get("Shuffle Read Metrics") or {})
            swm = (metrics.get("Shuffle Write Metrics") or {})
            shuffle_read = int(srm.get("Remote Bytes Read", 0) or 0) + int(srm.get("Local Bytes Read", 0) or 0)
            shuffle_write = int(swm.get("Shuffle Bytes Written", 0) or 0)
            mem_spill = int(metrics.get("Memory Bytes Spilled", 0) or 0)
            disk_spill = int(metrics.get("Disk Bytes Spilled", 0) or 0)

            launch = ti.get("Launch Time")
            finish = ti.get("Finish Time")
            duration = int(finish - launch) if launch is not None and finish is not None else int(metrics.get("Executor Run Time", 0) or 0)

            tasks.append(TaskEnd(
                stage_id=int(si),
                attempt=int(sa),
                task_id=int(ti.get("Task ID", ti.get("Task Id", 0)) or 0),
                duration_ms=max(0, duration),
                gc_time_ms=int(metrics.get("JVM GC Time", 0) or 0),
                shuffle_read_bytes=shuffle_read,
                shuffle_write_bytes=shuffle_write,
                spill_mem_bytes=mem_spill,
                spill_disk_bytes=disk_spill,
            ))
    return stages, tasks, meta
