# breeze_downloader.spec
# PyInstaller spec — produces a single-file Windows EXE.
#
# Build locally:
#   pip install pyinstaller
#   pyinstaller breeze_downloader.spec
#
# CI/CD: GitHub Actions runs this automatically on tag push.

import os
import sys
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Collect all customtkinter assets (themes, images)
ctk_datas = collect_data_files("customtkinter")

a = Analysis(
    ["main.py"],
    pathex=["."],
    binaries=[],
    datas=ctk_datas,
    hiddenimports=[
        # breeze_connect may import these lazily
        "breeze_connect",
        "socketio",
        "websocket",
        "engineio",
        # pandas internals
        "pandas._libs.tslibs.np_datetime",
        "pandas._libs.tslibs.nattype",
        "pandas._libs.tslibs.timedeltas",
        # PIL / Pillow for customtkinter
        "PIL._tkinter_finder",
        # our packages
        "core",
        "core.downloader",
        "gui",
        "gui.app",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["matplotlib", "scipy", "notebook", "IPython"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="BreezeDownloader",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,          # No terminal window — GUI only
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,              # Set to "assets/icon.ico" if you add one
)
