import os
import json
from typing import List
import numpy as np
import faiss
from app.config import settings

def _version_dir(version: str) -> str:
    return os.path.join(settings.DATA_DIR, version)

def index_paths(version: str):
    vdir = _version_dir(version)
    return {
        "dir": vdir,
        "faiss": os.path.join(vdir, "index.faiss"),
        "ids": os.path.join(vdir, "doc_ids.npy"),
        "meta": os.path.join(vdir, "meta.json"),
    }

def save_index(version: str, index: faiss.Index, doc_ids: List[int], meta: dict):
    os.makedirs(settings.DATA_DIR, exist_ok=True)
    paths = index_paths(version)
    os.makedirs(paths["dir"], exist_ok=True)

    faiss.write_index(index, paths["faiss"])
    np.save(paths["ids"], np.array(doc_ids, dtype=np.int64))
    with open(paths["meta"], "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

def load_index(version: str):
    paths = index_paths(version)
    if not (os.path.exists(paths["faiss"]) and os.path.exists(paths["ids"])):
        raise FileNotFoundError(f"Index artifacts not found for version={version}")
    index = faiss.read_index(paths["faiss"])
    doc_ids = np.load(paths["ids"]).astype("int64").tolist()
    return index, doc_ids
