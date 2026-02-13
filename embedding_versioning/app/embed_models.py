import numpy as np
from app.config import settings

class Embedder:
    """Deterministic embedding model wrapper for versioned indices."""
    def __init__(self, version: str, dim: int = settings.VECTOR_DIM):
        self.version = version
        self.dim = dim

    def embed(self, text: str) -> np.ndarray:
        seed = abs(hash(f"{self.version}::{text}")) % (2**32)
        rng = np.random.default_rng(seed)
        v = rng.normal(size=(self.dim,)).astype("float32")
        return v
