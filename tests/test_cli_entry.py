import builtins
from types import SimpleNamespace

import pytest

import src.__main__ as cli


class StubService:
    def __init__(self):
        self.called_with = None

    def execute(self, options):
        self.called_with = options
        return SimpleNamespace(file_path="output.xlsx")


def test_cli_main_invokes_app_service(monkeypatch):
    service = StubService()
    monkeypatch.setattr(cli, "build_app_service", lambda: service)
    monkeypatch.setattr("builtins.print", lambda *a, **k: None)
    args = [
        "--code",
        "123",
        "--start",
        "2024-01",
        "--end",
        "2024-02",
        "--mode",
        "S",
        "--single-sheet",
    ]

    cli.main(args)

    assert service.called_with.codes == ["123"]
    assert service.called_with.single_sheet is True


def test_cli_falls_back_to_legacy(monkeypatch):
    monkeypatch.setattr(cli, "build_app_service", lambda: None)
    called = {}

    def fake_WWRApp(*args, **kwargs):
        called["value"] = True

    monkeypatch.setattr(cli, "WWRApp", fake_WWRApp)
    cli.main([])
    assert called.get("value") is True

