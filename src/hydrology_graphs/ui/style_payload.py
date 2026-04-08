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


def delete_nested_value(root: dict[str, Any], path: str) -> None:
    """ドット区切りパスの値を削除する。"""

    parts = path.split(".")
    node: Any = root
    parents: list[tuple[dict[str, Any], str]] = []
    for key in parts[:-1]:
        if not isinstance(node, dict):
            return
        child = node.get(key)
        if not isinstance(child, dict):
            return
        parents.append((node, key))
        node = child
    if not isinstance(node, dict):
        return
    leaf = parts[-1]
    if leaf not in node:
        return
    del node[leaf]
    # 空dictになった親は順に掃除する。
    current = node
    for parent, key in reversed(parents):
        if isinstance(current, dict) and not current:
            del parent[key]
            current = parent
            continue
        break
