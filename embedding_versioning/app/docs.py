import time
from typing import List, Tuple
from app.db import connect

def upsert_doc(doc_id: int, title: str, body: str):
    now = int(time.time())
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO docs(doc_id, title, body, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
              title=excluded.title,
              body=excluded.body,
              updated_at=excluded.updated_at
            """,
            (doc_id, title, body, now)
        )
        conn.commit()

def list_docs() -> List[Tuple[int, str, str]]:
    with connect() as conn:
        rows = conn.execute("SELECT doc_id, title, body FROM docs ORDER BY doc_id ASC").fetchall()
        return [(int(r[0]), r[1], r[2]) for r in rows]
