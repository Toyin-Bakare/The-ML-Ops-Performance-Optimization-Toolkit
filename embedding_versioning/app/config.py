import os
from pydantic import BaseModel

class Settings(BaseModel):
    DB_PATH: str = os.getenv("DB_PATH", "embedding_versioning.sqlite3")
    DATA_DIR: str = os.getenv("DATA_DIR", "data")

    VECTOR_DIM: int = int(os.getenv("VECTOR_DIM", "384"))

    # Shadow evaluation thresholds
    ALLOWED_DROP: float = float(os.getenv("ALLOWED_DROP", "0.02"))
    MIN_SCORE: float = float(os.getenv("MIN_SCORE", "0.60"))

settings = Settings()
