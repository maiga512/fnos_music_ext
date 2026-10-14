"""/api/v1/sources 合并视图：主源与附加源并成一个列表，一个源一行。"""
from __future__ import annotations

from fastapi.testclient import TestClient

from conftest import FakeRuntime, lxapp


class FakePool:
    def __init__(self, items):
        self._items = items

    async def wait_loaded(self):
        return None

    async def load(self):
        return None

    async def shutdown(self):
        return None

    def configured(self):
        return bool(self._items)

    def describe_all(self):
        return [dict(it) for it in self._items]


def _pool_item(sid, name, version, script_hash, **extra):
    item = {
        "id": sid, "name": name, "url": f"https://src.test/{sid}.js", "enabled": True,
        "auto_update": False, "position": 0, "script_hash": script_hash, "running": True,
        "last_error": "", "update_alert": None,
        "meta": {"name": name, "version": version},
        "source": {"name": name, "version": version, "platforms": {"tx": ["128k"]}, "running": True},
    }
    item.update(extra)
    return item


def test_primary_row_is_first_and_marked(isolated, monkeypatch):
    isolated.active_url = "https://src.test/main.js"
    isolated._runtime = FakeRuntime(platforms={"tx": ["128k"]})
    monkeypatch.setattr(isolated, "describe", lambda: {
        "configured": True, "url": isolated.active_url, "initialized": True,
        "last_error": "", "script_hash": "main-hash",
        "meta": {"name": "付费永久赞助版", "version": "3.2.1"},
        "source": {"name": "付费永久赞助版", "version": "3.2.1",
                   "platforms": {"tx": ["128k"], "kw": ["128k"]}, "running": True},
    })
    monkeypatch.setattr(lxapp, "SOURCE_POOL", FakePool([
        _pool_item("s1", "备源一", "1.0.0", "hash-1"),
    ]))
    with TestClient(lxapp.app) as client:
        items = client.get("/api/v1/sources").json()["data"]["items"]

    assert [it["id"] for it in items] == ["__primary__", "s1"]
    primary = items[0]
    assert primary["primary"] is True
    assert primary["name"] == "付费永久赞助版"
    assert primary["meta"]["version"] == "3.2.1"      # 行内版本来自脚本 @version
    assert primary["running"] is True and primary["enabled"] is True
    assert primary["configured"] is True
    assert "platforms" in primary["source"]
    assert items[1].get("primary") is None


def test_same_script_in_pool_is_not_listed_twice(isolated, monkeypatch):
    """同一份脚本正文（hash 相同）只留一行：主源已在使用，附加行只是重复占用。"""
    monkeypatch.setattr(isolated, "describe", lambda: {
        "configured": True, "url": "https://src.test/main.js", "initialized": False,
        "last_error": "", "script_hash": "same-hash",
        "meta": {"name": "同一脚本", "version": "9.9"},
        "source": {"name": "同一脚本", "version": "9.9", "platforms": {}, "running": False},
    })
    monkeypatch.setattr(lxapp, "SOURCE_POOL", FakePool([
        _pool_item("dup", "同一脚本", "9.9", "same-hash"),
        _pool_item("other", "另一个源", "1.1", "other-hash"),
    ]))
    with TestClient(lxapp.app) as client:
        items = client.get("/api/v1/sources").json()["data"]["items"]
    assert [it["id"] for it in items] == ["__primary__", "other"]


def test_unconfigured_primary_still_listed(isolated, monkeypatch):
    """没装主源也要有一行，管理页才知道主源位是空的（不是列表丢了）。"""
    monkeypatch.setattr(isolated, "describe", lambda: {
        "configured": False, "url": "", "initialized": False, "last_error": "",
        "script_hash": "", "meta": {}, "source": None,
    })
    monkeypatch.setattr(lxapp, "SOURCE_POOL", FakePool([]))
    with TestClient(lxapp.app) as client:
        items = client.get("/api/v1/sources").json()["data"]["items"]
    assert len(items) == 1
    assert items[0]["id"] == "__primary__"
    assert items[0]["configured"] is False
    assert items[0]["running"] is False
    assert items[0]["meta"] == {}


def test_source_info_exposes_primary_hash(isolated, monkeypatch):
    """脚本 hash 也要出现在 /api/v1/source，供上游去重判断（与源池同一算法）。"""
    monkeypatch.setattr(lxapp.SOURCE_MANAGER, "_cached_script", lambda: "console.log(1)\n",
                        raising=False)
    from source_runtime import SourceManager

    described = SourceManager.describe(lxapp.SOURCE_MANAGER)
    assert described["script_hash"] == __import__("hashlib").sha256(
        "console.log(1)\n".encode("utf-8")).hexdigest()
