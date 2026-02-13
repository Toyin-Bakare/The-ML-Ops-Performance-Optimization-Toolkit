from __future__ import annotations
from typing import List, Optional
import json, os
from spark_opt.eventlog_reader import parse_eventlog
from spark_opt.metrics import build_stage_metrics
from spark_opt.detectors import detect_skew, detect_shuffle_heavy, detect_spill_or_gc, detect_partitioning_issues
from spark_opt.recommendations import recommend
from spark_opt.config import SparkConf

def generate_markdown_report(eventlog_path: str, spark_conf: SparkConf, out_path: str,
                             cores_total: Optional[int] = None) -> str:
    stages, tasks, meta = parse_eventlog(eventlog_path)
    df, stage_objs = build_stage_metrics(stages, tasks)

    findings = []
    findings += detect_skew(stage_objs)
    findings += detect_shuffle_heavy(stage_objs)
    findings += detect_spill_or_gc(stage_objs)
    findings += detect_partitioning_issues(stage_objs, spark_conf, cores_total=cores_total)

    recs = recommend(findings, spark_conf)
    top = df.head(10).to_dict(orient="records") if df is not None and not df.empty else []

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    lines: List[str] = []
    lines.append("# Spark Performance + Cost Optimization Report")
    lines.append("")
    lines.append(f"**Eventlog:** `{eventlog_path}`")
    lines.append(f"**App:** `{meta.get('app_name')}`  \\")
    lines.append(f"**App ID:** `{meta.get('app_id')}`")
    lines.append("")
    lines.append("## Top Stages by Duration")
    lines.append("")
    if not top:
        lines.append("_No stage metrics found in event log._")
    else:
        lines.append("| Stage | Duration (ms) | Tasks | p50 (ms) | p95 (ms) | max (ms) | Shuffle (MB) | Spill (MB) | GC % |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for r in top:
            sh = float(r.get("shuffle_read_mb", 0.0)) + float(r.get("shuffle_write_mb", 0.0))
            lines.append(f"| {r['stage_id']} | {r['stage_duration_ms']} | {r['num_tasks']} | {r['task_p50_ms']:.0f} | {r['task_p95_ms']:.0f} | {r['task_max_ms']} | {sh:.0f} | {r['spill_mb']:.0f} | {r['gc_pct']*100:.1f}% |")

    lines.append("")
    lines.append("## Findings")
    lines.append("")
    if not findings:
        lines.append("No significant findings detected.")
    else:
        for f in findings:
            sid = f.stage_id if f.stage_id is not None else "N/A"
            lines.append(f"- **[{f.severity}] {f.code}** (stage: {sid}) — {f.message}")

    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    if not recs:
        lines.append("No recommendations generated.")
    else:
        for r in recs:
            sid = r.stage_id if r.stage_id is not None else "N/A"
            lines.append(f"### {r.title}  \\")
            lines.append(f"**Severity:** {r.severity}  \\")
            lines.append(f"**Stage:** {sid}")
            lines.append("")
            lines.append(f"**Rationale:** {r.rationale}")
            lines.append("")
            lines.append("**Actions:**")
            for a in r.actions:
                lines.append(f"- {a}")
            if r.evidence:
                lines.append("")
                lines.append("<details><summary>Evidence</summary>")
                lines.append("")
                lines.append("```json")
                lines.append(json.dumps(r.evidence, indent=2))
                lines.append("```")
                lines.append("</details>")
            lines.append("")

    content = "\n".join(lines)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(content)
    return out_path
