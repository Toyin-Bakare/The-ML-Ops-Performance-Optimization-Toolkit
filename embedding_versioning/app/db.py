import sqlite3
from contextlib import contextmanager
from app.config import settings

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS docs (
  doc_id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS golden_queries (
  qid INTEGER PRIMARY KEY AUTOINCREMENT,
  query TEXT NOT NULL,
  expected_doc_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS active_version (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS builds (
  version TEXT PRIMARY KEY,
  built_at INTEGER NOT NULL,
  doc_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS eval_results (
  version TEXT PRIMARY KEY,
  evaluated_at INTEGER NOT NULL,
  top1_accuracy REAL NOT NULL,
  mrr REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS shadow_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  baseline_version TEXT NOT NULL,
  candidate_version TEXT NOT NULL,
  evaluated_at INTEGER NOT NULL,
  baseline_top1 REAL NOT NULL,
  candidate_top1 REAL NOT NULL,
  baseline_mrr REAL NOT NULL,
  candidate_mrr REAL NOT NULL,
  pass INTEGER NOT NULL
);
"""

@contextmanager
def connect():
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with connect() as conn:
        conn.executescript(SCHEMA)
        cur = conn.execute("SELECT version FROM active_version WHERE singleton=1")
        row = cur.fetchone()
        if not row:
            conn.execute("INSERT INTO active_version(singleton, version) VALUES (1, 'v1')")
        conn.commit()
