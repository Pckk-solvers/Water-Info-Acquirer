from __future__ import annotations

from typing import Any


def nested_value(root: dict[str, Any], path: str, default: Any) -> Any:
    """ドット区切りパスで値を取得する。"""

    node: Any = root
    for key in path.split("."):
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    return node


def set_nested_value(root: dict[str, Any], path: str, value: Any) -> None:
    """ドット区切りパスへ値を書き込む。"""

    parts = path.split(".")
    node: dict[str, Any] = root
    for key in parts[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    node[parts[-1]] = value
