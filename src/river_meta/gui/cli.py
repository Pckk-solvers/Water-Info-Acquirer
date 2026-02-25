from __future__ import annotations

from river_meta.gui.app import RiverMetaGuiApp


def main() -> int:
    app = RiverMetaGuiApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
