"""
Balfund · Breeze Data Downloader
Entry point — run this file or the packaged EXE.
"""

import sys
import os

# When running as a PyInstaller bundle, set CWD to the executable's directory
# so config files and output folders are written next to the EXE.
if getattr(sys, "frozen", False):
    os.chdir(os.path.dirname(sys.executable))

from gui.app import BreezeDownloaderApp


def main():
    app = BreezeDownloaderApp()
    app.mainloop()


if __name__ == "__main__":
    main()
