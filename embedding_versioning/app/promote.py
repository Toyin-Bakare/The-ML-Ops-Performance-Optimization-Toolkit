from typing import Dict
from app.db import connect
from app.eval import shadow_compare

def get_active() -> str:
    with connect() as conn:
        return conn.execute("SELECT version FROM active_version WHERE singleton=1").fetchone()[0]

def set_active(version: str) -> None:
    with connect() as conn:
        conn.execute("UPDATE active_version SET version=? WHERE singleton=1", (version,))
        conn.commit()

def promote(version: str, require_shadow_pass: bool = False) -> Dict:
    if require_shadow_pass:
        cmp = shadow_compare(version)
        if not cmp["pass"]:
            return {"promoted": False, "reason": "shadow_eval_failed", "compare": cmp}

    prev = get_active()
    set_active(version)
    return {"promoted": True, "previous": prev, "active": version}

def rollback(previous_version: str) -> Dict:
    prev = get_active()
    set_active(previous_version)
    return {"rolled_back": True, "from": prev, "to": previous_version}
