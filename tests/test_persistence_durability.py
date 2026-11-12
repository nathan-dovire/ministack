"""
Durability of persisted state: fsync before rename, quarantine of unreadable
files, the save lock, and the periodic save.

The live stack once came back with every service file at 0 bytes
after an unclean Docker VM stop that followed a completed shutdown save.
"""

import asyncio
import glob
import json
import os
import stat
import threading

import pytest

from ministack import app
from ministack.core import persistence


@pytest.fixture(autouse=True)
def _enable_persistence(monkeypatch, tmp_path):
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def test_save_state_fsyncs_file_before_rename_and_dir_after(monkeypatch, tmp_path):
    calls = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd):
        calls.append(("fsync", stat.S_ISDIR(os.fstat(fd).st_mode)))
        real_fsync(fd)

    def replace(src, dst):
        calls.append(("replace", os.path.basename(dst)))
        real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)

    persistence.save_state("durability", {"k": "v"})

    assert calls == [("fsync", False), ("replace", "durability.json"), ("fsync", True)]
    with open(tmp_path / "durability.json") as f:
        assert json.load(f) == {"k": "v"}


def test_load_state_quarantines_zero_byte_file(tmp_path):
    (tmp_path / "durability.json").write_bytes(b"")

    assert persistence.load_state("durability") is None

    assert not (tmp_path / "durability.json").exists()
    moved = glob.glob(str(tmp_path / "durability.json.corrupt-*"))
    assert len(moved) == 1
    assert os.path.getsize(moved[0]) == 0


def test_quarantined_file_survives_the_next_save(tmp_path):
    (tmp_path / "durability.json").write_text('{"truncated": ')
    persistence.load_state("durability")

    persistence.save_state("durability", {"fresh": True})

    moved = glob.glob(str(tmp_path / "durability.json.corrupt-*"))
    assert len(moved) == 1
    with open(moved[0]) as f:
        assert f.read() == '{"truncated": '
    with open(tmp_path / "durability.json") as f:
        assert json.load(f) == {"fresh": True}


def test_missing_file_is_not_quarantined(tmp_path):
    assert persistence.load_state("durability") is None
    assert os.listdir(tmp_path) == []


def test_save_all_waits_for_an_in_flight_save(tmp_path):
    entered, release = threading.Event(), threading.Event()
    order = []

    def slow_state():
        entered.set()
        release.wait(5)
        order.append("first")
        return {"n": 1}

    def fast_state():
        order.append("second")
        return {"n": 2}

    first = threading.Thread(target=persistence.save_all, args=({"durability": slow_state},))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=persistence.save_all, args=({"durability": fast_state},))
    second.start()
    second.join(0.2)
    assert second.is_alive(), "second save_all ran while the first held the lock"

    release.set()
    first.join(5)
    second.join(5)
    assert order == ["first", "second"]
    with open(tmp_path / "durability.json") as f:
        assert json.load(f) == {"n": 2}


def test_periodic_save_writes_state_and_stops_on_cancel(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "_build_save_dict", lambda context: {"durability": lambda: {"tick": 1}})

    async def run():
        task = asyncio.create_task(app._periodic_save(0))
        for _ in range(200):
            if (tmp_path / "durability.json").exists():
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    with open(tmp_path / "durability.json") as f:
        assert json.load(f) == {"tick": 1}


def test_periodic_save_survives_a_failing_round(monkeypatch, tmp_path):
    rounds = []

    def build(context):
        rounds.append(context)
        if len(rounds) == 1:
            raise RuntimeError("boom")
        return {"durability": lambda: {"round": len(rounds)}}

    monkeypatch.setattr(app, "_build_save_dict", build)

    async def run():
        task = asyncio.create_task(app._periodic_save(0))
        for _ in range(200):
            if (tmp_path / "durability.json").exists():
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert len(rounds) >= 2
    assert (tmp_path / "durability.json").exists()


@pytest.mark.parametrize("value,expected", [("", 0), ("0", 0), ("300", 300), ("soon", 0)])
def test_persist_interval_parsing(monkeypatch, value, expected):
    if value:
        monkeypatch.setenv("MINISTACK_PERSIST_INTERVAL", value)
    else:
        monkeypatch.delenv("MINISTACK_PERSIST_INTERVAL", raising=False)
    assert app._persist_interval() == expected
