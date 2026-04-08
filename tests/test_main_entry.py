from __future__ import annotations

from main import _build_parser, _resolve_launch_target


def test_resolve_launch_target_from_option():
    parser = _build_parser()
    args = parser.parse_args(["--app", "water"])

    assert _resolve_launch_target(args) == "water"


def test_resolve_launch_target_from_positional():
    parser = _build_parser()
    args = parser.parse_args(["hydrology_graphs"])

    assert _resolve_launch_target(args) == "hydrology_graphs"


def test_resolve_launch_target_prefers_option_over_positional():
    parser = _build_parser()
    args = parser.parse_args(["--app", "jma", "water"])

    assert _resolve_launch_target(args) == "jma"


def test_resolve_launch_target_none_when_not_specified():
    parser = _build_parser()
    args = parser.parse_args([])

    assert _resolve_launch_target(args) is None
