# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

project_root = Path(SPEC).resolve().parent
src_dir = project_root / "src"

a = Analysis(
    ['river_rainfall.py'],
    pathex=[str(src_dir)],
    binaries=[],
    datas=[
        (str(src_dir / "river_meta" / "resources" / "jma_station_index.json"), "river_meta/resources"),
        (str(src_dir / "river_meta" / "resources" / "waterinfo_station_index.json"), "river_meta/resources"),
    ],
    hiddenimports=[
        "river_meta.rainfall.gui",
        "river_meta.rainfall.cli",
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
    [],
    exclude_binaries=True,
    name='RainfallCollector-v0.6.0',
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
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='RainfallCollector-v0.6.0',
)
