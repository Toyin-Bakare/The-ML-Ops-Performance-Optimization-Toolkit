from spark_opt.detectors import detect_skew, detect_shuffle_heavy
from spark_opt.metrics import StageMetrics

def test_skew_detector_triggers():
    stages = [StageMetrics(
        stage_id=1, attempt=0, name="x", num_tasks=200, stage_duration_ms=1000,
        task_p50_ms=100.0, task_p95_ms=500.0, task_max_ms=2000,
        skew_ratio_p95_p50=5.0, skew_ratio_max_p50=20.0,
        gc_pct=0.01, shuffle_read_mb=0.0, shuffle_write_mb=0.0, spill_mb=0.0
    )]
    findings = detect_skew(stages)
    assert any(f.code == "SKEW_DETECTED" for f in findings)

def test_shuffle_heavy_detector_triggers():
    stages = [StageMetrics(
        stage_id=2, attempt=0, name="y", num_tasks=200, stage_duration_ms=1000,
        task_p50_ms=100.0, task_p95_ms=120.0, task_max_ms=200,
        skew_ratio_p95_p50=1.2, skew_ratio_max_p50=2.0,
        gc_pct=0.01, shuffle_read_mb=5000.0, shuffle_write_mb=3000.0, spill_mb=0.0
    )]
    findings = detect_shuffle_heavy(stages)
    assert any(f.code == "SHUFFLE_HEAVY" for f in findings)
