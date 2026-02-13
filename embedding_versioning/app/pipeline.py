import time
from typing import Dict, List
import numpy as np
import faiss

from app.db import connect
from app.docs import list_docs
from app.embed_models import Embedder
from app.index_io import save_index
from app.config import settings

def build_version(version: str) -> Dict:
    docs = list_docs()
    embedder = Embedder(version)

    doc_ids: List[int] = []
    vecs: List[np.ndarray] = []

    for doc_id, title, body in docs:
        text = f"{title}\n{body}"
        v = embedder.embed(text)
        doc_ids.append(doc_id)
        vecs.append(v)

    if not vecs:
        raise RuntimeError("No docs to index")

    X = np.vstack(vecs).astype("float32")
    faiss.normalize_L2(X)

    index = faiss.IndexFlatIP(settings.VECTOR_DIM)
    index.add(X)

    meta = {
        "version": version,
        "built_at": int(time.time()),
        "doc_count": len(doc_ids),
        "dim": settings.VECTOR_DIM,
        "type": "IndexFlatIP (cosine via L2 normalize)",
    }
    save_index(version, index, doc_ids, meta)

    with connect() as conn:
        conn.execute(
            """
            INSERT INTO builds(version, built_at, doc_count)
            VALUES (?, ?, ?)
            ON CONFLICT(version) DO UPDATE SET
              built_at=excluded.built_at,
              doc_count=excluded.doc_count
            """,
            (version, meta["built_at"], meta["doc_count"])
        )
        conn.commit()

    return meta
