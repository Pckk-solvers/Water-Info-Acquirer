# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import tomllib


project_root = Path(SPEC).resolve().parent
src_dir = project_root / "src"
pyproject_path = project_root / "pyproject.toml"
pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
project_version = str(pyproject.get("project", {}).get("version", "0.0.0"))
artifact_name = f"Water-Info-Acquirer-v{project_version}-win-f"


a = Analysis(
    ['main.py'],
    pathex=[str(src_dir)],
    binaries=[],
    datas=[
        (str(pyproject_path), '.'),
        (str(src_dir / "river_meta" / "resources" / "jma_station_index.json"), "river_meta/resources"),
        (str(src_dir / "river_meta" / "resources" / "waterinfo_station_index.json"), "river_meta/resources"),
    ],
    hiddenimports=[
        "water_info_acquirer.launcher",
        "water_info_acquirer.app_registry",
        "water_info_acquirer.navigation",
        "water_info.launcher_entry",
        "jma_rainfall_pipeline.launcher_entry",
        "river_meta.rainfall.gui.launcher_entry",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name=artifact_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
