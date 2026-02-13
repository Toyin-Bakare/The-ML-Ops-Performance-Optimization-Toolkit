# Spark Performance + Cost Optimization Toolkit

A project toolkit for **profiling Apache Spark workloads** and generating **actionable performance + cost recommendations** from Spark **event logs** and **Spark configuration**.

This repo is intentionally platform-agnostic: it works for Spark on **Databricks, EMR, Dataproc, on-prem YARN/K8s**, etc., because it uses the same artifacts every Spark platform provides:
- Spark **event logs**
- Spark **conf**
- optional cluster metadata (nodes/cores/memory) for partitioning heuristics and cost estimation

---

## Problem Statement

At scale, Spark jobs become expensive and slow for reasons that are predictable but hard to spot without tooling:

### Common performance failures
- **Data skew**: a few tasks run 10–100× longer → stragglers dominate stage runtime
- **Shuffle explosions**: joins/aggregations trigger heavy shuffle read/write and spill to disk
- **Partitioning mistakes**:
  - too many partitions → scheduler overhead, tiny tasks, lots of small output files
  - too few partitions → poor parallelism, long tasks, underutilized cluster
- **GC pressure & memory spill**: executor memory too small or inefficient transformations
- **Bad join strategy**: missing broadcast opportunities or poor key distribution
- **File layout issues**: many small files → slow I/O + metadata overhead

### Common cost failures
- Scaling clusters to “make it finish” without understanding bottlenecks
- Paying for more executors when skew/shuffle prevents linear speedups
- Over-partitioning and tiny tasks inflate wall-clock time (and therefore spend)

**Goal:** Turn Spark event logs + conf into **clear diagnostics**, **ranked bottlenecks**, and **concrete fixes**.

---

## How this repo solves the problem

1) **Parse event logs** (JSON lines) into structured stage/task events.
2) **Compute stage metrics**:
   - duration, task p50/p95/max
   - skew ratios (p95/p50, max/p50)
   - shuffle read/write MB
   - spill MB, GC %
3) **Run detectors** that flag bottlenecks:
   - skew stages
   - shuffle-heavy stages
   - spill/GC pressure
   - over/under partitioning based on cores + shuffle partitions
4) **Generate recommendations** (severity + rationale + actions)
5) **Estimate cost** using a simple runtime × nodes × rate model for quick what-if analysis
6) **Emit a Markdown report** you can attach to tickets or optimization proposals

---

## Repository Structure (What each file does)

### Core package: `spark_opt/`
- **`spark_opt/config.py`**
  - Typed models for cluster & cost inputs and Spark conf helpers (get_int/get_bool).

- **`spark_opt/eventlog_reader.py`**
  - Reads Spark JSONL event logs and extracts the subset needed for profiling:
    - `SparkListenerStageCompleted`
    - `SparkListenerTaskEnd`
    - `SparkListenerApplicationStart`

- **`spark_opt/metrics.py`**
  - Aggregates per-task metrics into per-stage summaries:
    - p50/p95/max task duration
    - shuffle I/O
    - spill bytes
    - GC percentage
  - Produces a DataFrame for sorting “top bottleneck stages”.

- **`spark_opt/detectors.py`**
  - Converts metrics into normalized `Finding` objects using heuristics:
    - skew detection
    - shuffle-heavy detection
    - spill + GC pressure
    - partitioning issues based on cluster cores and conf

- **`spark_opt/recommendations.py`**
  - Maps findings → recommendations with actions:
    - enable AQE, skew join handling
    - adjust shuffle partitions
    - broadcast joins
    - repartition strategies
    - reduce spills / reduce GC

- **`spark_opt/cost_model.py`**
  - Lightweight cost estimator:
    - node-hours and estimated $ cost
    - optional DBU-style cost

- **`spark_opt/report.py`**
  - Generates a Markdown report:
    - top stages table
    - findings
    - recommendations with evidence blocks

- **`spark_opt/cli.py`**
  - CLI commands:
    - `analyze-eventlog`
    - `recommend`
    - `report`
    - `cost`

### Samples
- **`samples/sample_eventlog.jsonl`**
  - Small synthetic event log with:
    - a normal stage
    - a skewed + shuffle-heavy stage

- **`samples/sample_spark_conf.json`**
  - Example Spark conf used by detectors.

### Tests
- **`tests/test_detectors.py`**
  - Ensures skew/shuffle detectors trigger correctly.

---

## Quickstart

### 1) Install deps
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Analyze the sample event log
```bash
python -m spark_opt.cli analyze-eventlog --eventlog samples/sample_eventlog.jsonl
```

### 3) Generate a report
```bash
python -m spark_opt.cli report \
  --eventlog samples/sample_eventlog.jsonl \
  --spark-conf samples/sample_spark_conf.json \
  --out reports/report.md
```

### 4) Estimate cost
```bash
python -m spark_opt.cli cost --runtime-seconds 1800 --nodes 10 --rate-per-node-hour 0.45
```

---

## Project highlights
- Built a Spark profiling toolkit that parses event logs into stage/task metrics
- Implemented skew/shuffle/partition detectors and generated actionable recommendations
- Added cost modeling to compare runtime optimizations in dollars/DBUs
- Produced a CLI + report generator suitable for platform/data engineering teams
