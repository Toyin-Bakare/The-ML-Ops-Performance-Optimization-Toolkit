from app.db import init_db
from app.seed import main as seed_main
from app.pipeline import build_version
from app.promote import promote
from app.eval import shadow_compare, get_active_version

def test_candidate_blocked_on_regression(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.sqlite3"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("ALLOWED_DROP", "0.0")
    monkeypatch.setenv("MIN_SCORE", "0.99")

    init_db()
    seed_main()

    build_version("v1")
    promote("v1")

    build_version("v2")
    cmp = shadow_compare("v2")
    assert cmp["pass"] is False

    res = promote("v2", require_shadow_pass=True)
    assert res["promoted"] is False
    assert get_active_version() == "v1"
