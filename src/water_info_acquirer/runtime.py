from __future__ import annotations

import os
from pathlib import Path
import sys


IS_FROZEN = getattr(sys, "frozen", False)
PROJECT_ROOT = Path(sys.executable).resolve().parent if IS_FROZEN else Path(__file__).resolve().parent.parent.parent
SRC_DIR = PROJECT_ROOT / "src"


def ensure_src_on_path() -> None:
    try:
        os.chdir(PROJECT_ROOT)
    except OSError:
        pass
    root_path = str(PROJECT_ROOT)
    src_path = str(SRC_DIR)
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
