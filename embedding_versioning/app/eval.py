import time
from typing import Dict, List, Tuple
import faiss

from app.db import connect
from app.embed_models import Embedder
from app.index_io import load_index
from app.config import settings

def _golden() -> List[Tuple[str, int]]:
    with connect() as conn:
        rows = conn.execute("SELECT query, expected_doc_id FROM golden_queries ORDER BY qid ASC").fetchall()
        return [(r[0], int(r[1])) for r in rows]

def evaluate_version(version: str, top_k: int = 5) -> Dict[str, float]:
    index, doc_ids = load_index(version)
    embedder = Embedder(version)

    gold = _golden()
    if not gold:
        raise RuntimeError("No golden queries found. Run seed first.")

    top1 = 0
    rr_sum = 0.0

    for query, expected_id in gold:
        qv = embedder.embed(query).astype("float32").reshape(1, -1)
        faiss.normalize_L2(qv)

        D, I = index.search(qv, min(top_k, len(doc_ids)))
        ranks = [doc_ids[i] for i in I[0].tolist() if i >= 0]

        if ranks and ranks[0] == expected_id:
            top1 += 1

        rr = 0.0
        for rank_idx, doc_id in enumerate(ranks, start=1):
            if doc_id == expected_id:
                rr = 1.0 / rank_idx
                break
        rr_sum += rr

    n = len(gold)
    out = {
        "top1_accuracy": top1 / n,
        "mrr": rr_sum / n
    }

    with connect() as conn:
        conn.execute(
            """
            INSERT INTO eval_results(version, evaluated_at, top1_accuracy, mrr)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(version) DO UPDATE SET
              evaluated_at=excluded.evaluated_at,
              top1_accuracy=excluded.top1_accuracy,
              mrr=excluded.mrr
            """,
            (version, int(time.time()), out["top1_accuracy"], out["mrr"])
        )
        conn.commit()

    return out

def get_active_version() -> str:
    with connect() as conn:
        return conn.execute("SELECT version FROM active_version WHERE singleton=1").fetchone()[0]

def shadow_compare(candidate_version: str) -> Dict:
    baseline = get_active_version()
    base = evaluate_version(baseline)
    cand = evaluate_version(candidate_version)

    pass_ = (
        cand["top1_accuracy"] >= max(settings.MIN_SCORE, base["top1_accuracy"] - settings.ALLOWED_DROP)
        and cand["mrr"] >= max(settings.MIN_SCORE, base["mrr"] - settings.ALLOWED_DROP)
    )

    with connect() as conn:
        conn.execute(
            """
            INSERT INTO shadow_results(
              baseline_version, candidate_version, evaluated_at,
              baseline_top1, candidate_top1, baseline_mrr, candidate_mrr, pass
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                baseline, candidate_version, int(time.time()),
                base["top1_accuracy"], cand["top1_accuracy"],
                base["mrr"], cand["mrr"],
                1 if pass_ else 0
            )
        )
        conn.commit()

    return {
        "baseline": baseline,
        "candidate": candidate_version,
        "baseline_metrics": base,
        "candidate_metrics": cand,
        "pass": pass_
    }
