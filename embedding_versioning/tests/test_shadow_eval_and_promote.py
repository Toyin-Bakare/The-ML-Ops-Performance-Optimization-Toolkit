from app.db import init_db
from app.seed import main as seed_main
from app.pipeline import build_version
from app.eval import shadow_compare, get_active_version
from app.promote import promote

def test_shadow_eval_then_promote(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.sqlite3"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))

    init_db()
    seed_main()

    build_version("v1")
    promote("v1")

    build_version("v2")
    cmp = shadow_compare("v2")
    assert "pass" in cmp

    res = promote("v2", require_shadow_pass=True)
    if cmp["pass"]:
        assert res["promoted"] is True
        assert get_active_version() == "v2"
    else:
        assert res["promoted"] is False
        assert get_active_version() == "v1"
