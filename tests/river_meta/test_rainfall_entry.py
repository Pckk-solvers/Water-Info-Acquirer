from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall import entry


def test_entry_runs_gui_without_args(monkeypatch):
    called: dict[str, object] = {}

    def _fake_gui_main(**kwargs) -> int:
        called["gui"] = True
        called["kwargs"] = kwargs
        return 11

    monkeypatch.setattr("river_meta.rainfall.gui.main", _fake_gui_main)

    assert entry.main([]) == 11
    assert called["gui"] is True
    assert "default_parquet_dir_primary" in called["kwargs"]
    assert "default_parquet_dir_secondary" in called["kwargs"]


def test_entry_runs_cli_with_filtered_args(monkeypatch):
    called: dict[str, object] = {}

    def _fake_cli_main(argv):
        called["argv"] = list(argv)
        return 22

    monkeypatch.setattr("river_meta.rainfall.cli.main", _fake_cli_main)

    assert entry.main(["--gui", "--mode", "collect"]) == 22
    assert called["argv"] == ["--mode", "collect"]


def test_entry_sets_runtime_env(monkeypatch):
    monkeypatch.delenv("RIVER_RAINFALL_DISABLE_JMA_CACHE", raising=False)
    monkeypatch.delenv("RIVER_RAINFALL_DISABLE_JMA_LOG_OUTPUT", raising=False)
    monkeypatch.setattr("river_meta.rainfall.gui.main", lambda **kwargs: 0)

    assert entry.main([]) == 0
    assert entry.os.environ["RIVER_RAINFALL_DISABLE_JMA_CACHE"] == "1"
    assert entry.os.environ["RIVER_RAINFALL_DISABLE_JMA_LOG_OUTPUT"] == "1"
