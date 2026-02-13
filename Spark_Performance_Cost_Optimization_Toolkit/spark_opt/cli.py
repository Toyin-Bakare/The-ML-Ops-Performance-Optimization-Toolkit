from __future__ import annotations
import argparse, json
from spark_opt.eventlog_reader import parse_eventlog
from spark_opt.metrics import build_stage_metrics
from spark_opt.config import SparkConf, ClusterSpec
from spark_opt.detectors import detect_skew, detect_shuffle_heavy, detect_spill_or_gc, detect_partitioning_issues
from spark_opt.recommendations import recommend
from spark_opt.report import generate_markdown_report
from spark_opt.cost_model import estimate_cost

def _load_conf(path: str | None) -> SparkConf:
    if not path:
        return SparkConf(conf={})
    with open(path, "r", encoding="utf-8") as f:
        return SparkConf(conf=json.load(f))

def cmd_analyze_eventlog(args):
    stages, tasks, _ = parse_eventlog(args.eventlog)
    df, _ = build_stage_metrics(stages, tasks)
    if df is None or df.empty:
        print({"message": "no stage/task metrics found"})
        return
    print(df.head(args.top).to_string(index=False))

def cmd_recommend(args):
    spark_conf = _load_conf(args.spark_conf)
    cluster = ClusterSpec(nodes=args.nodes, cores_per_node=args.cores_per_node, memory_gb_per_node=args.memory_gb_per_node)
    cores_total = cluster.nodes * cluster.cores_per_node

    stages, tasks, _ = parse_eventlog(args.eventlog)
    _, stage_objs = build_stage_metrics(stages, tasks)

    findings = []
    findings += detect_skew(stage_objs)
    findings += detect_shuffle_heavy(stage_objs)
    findings += detect_spill_or_gc(stage_objs)
    findings += detect_partitioning_issues(stage_objs, spark_conf, cores_total=cores_total)

    recs = recommend(findings, spark_conf)
    payload = []
    for r in recs:
        payload.append({
            "severity": r.severity,
            "title": r.title,
            "stage_id": r.stage_id,
            "rationale": r.rationale,
            "actions": r.actions,
            "evidence": r.evidence,
        })
    print(json.dumps(payload, indent=2))

def cmd_report(args):
    spark_conf = _load_conf(args.spark_conf)
    cores_total = int(args.nodes) * int(args.cores_per_node) if args.nodes and args.cores_per_node else None
    outp = generate_markdown_report(args.eventlog, spark_conf, args.out, cores_total=cores_total)
    print({"report": outp})

def cmd_cost(args):
    est = estimate_cost(
        runtime_seconds=args.runtime_seconds,
        nodes=args.nodes,
        rate_per_node_hour=args.rate_per_node_hour,
        dbus_per_node=args.dbus_per_node,
        rate_per_dbu_hour=args.rate_per_dbu_hour,
    )
    print(json.dumps(est.__dict__, indent=2))

def main():
    p = argparse.ArgumentParser(prog="spark-opt")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("analyze-eventlog", help="Print top stage metrics from an event log")
    a.add_argument("--eventlog", required=True)
    a.add_argument("--top", type=int, default=10)
    a.set_defaults(fn=cmd_analyze_eventlog)

    r = sub.add_parser("recommend", help="Generate recommendations from event log + spark conf")
    r.add_argument("--eventlog", required=True)
    r.add_argument("--spark-conf")
    r.add_argument("--nodes", type=int, default=10)
    r.add_argument("--cores-per-node", type=int, default=4)
    r.add_argument("--memory-gb-per-node", type=float, default=16.0)
    r.set_defaults(fn=cmd_recommend)

    rep = sub.add_parser("report", help="Generate a Markdown report")
    rep.add_argument("--eventlog", required=True)
    rep.add_argument("--spark-conf")
    rep.add_argument("--nodes", type=int, default=10)
    rep.add_argument("--cores-per-node", type=int, default=4)
    rep.add_argument("--out", required=True)
    rep.set_defaults(fn=cmd_report)

    c = sub.add_parser("cost", help="Estimate cost from runtime + cluster size")
    c.add_argument("--runtime-seconds", type=int, required=True)
    c.add_argument("--nodes", type=int, required=True)
    c.add_argument("--rate-per-node-hour", type=float, default=0.0)
    c.add_argument("--dbus-per-node", type=float, default=0.0)
    c.add_argument("--rate-per-dbu-hour", type=float, default=0.0)
    c.set_defaults(fn=cmd_cost)

    args = p.parse_args()
    args.fn(args)

if __name__ == "__main__":
    main()
