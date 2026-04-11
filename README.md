# Balfund · Breeze Data Downloader

Dark-themed desktop app for downloading NIFTY / BANKNIFTY options historical data via the ICICI Breeze API. Outputs per-strike CSV files organised by expiry date.

---

## Features

| Feature | Detail |
|---|---|
| Instruments | NIFTY, BANKNIFTY |
| Timeframes | 1-minute (full day = 1 API call/strike) · 1-second (chunked, 15-min windows) |
| Strike discovery | Probes ATM ± N pts, finds exact exchange-listed strikes, caches per expiry |
| Resume | Progress persisted to `.progress.json` — safe to re-run after interruption |
| Rate limiting | Configurable calls/min with exponential-backoff retries |
| Spot data | Optional — downloads NSE cash NIFTY/BANKNIFTY alongside options |
| GUI | CustomTkinter dark theme, live log, real-time stats |
| Distribution | Single-file Windows EXE via PyInstaller + GitHub Actions |

---

## Output Structure

```
breeze_data/
├── NIFTY_OPTIONS_1MIN/
│   └── 2024-01-18/          ← expiry date
│       ├── 2024-01-15_21500_CE.csv
│       ├── 2024-01-15_21500_PE.csv
│       └── ...
├── NIFTY_SPOT_1MIN/
│   └── 2024-01-15.csv
├── .progress.json            ← resume tracker
└── .strikes_cache.json       ← cached strike lists per expiry
```

---

## Running from Source

```bash
# 1. Clone
git clone https://github.com/balfundalgo/breeze-data-downloader.git
cd breeze-data-downloader

# 2. Install deps (Python 3.11+)
pip install -r requirements.txt

# 3. Run
python main.py
```

---

## Building the EXE Locally

```bash
pip install pyinstaller
pyinstaller breeze_downloader.spec
# Output: dist/BreezeDownloader.exe
```

---

## CI/CD — Auto-build via GitHub Actions

Every tag push triggers an automatic build and GitHub Release:

```bash
git tag v1.0
git push origin v1.0
```

The workflow (`.github/workflows/build.yml`) will:
1. Spin up a `windows-latest` runner
2. Install Python 3.11 + dependencies
3. Run PyInstaller
4. Attach the EXE to the GitHub Release

You can also trigger a manual build from the **Actions** tab → **Build Windows EXE** → **Run workflow**.

---

## Session Token Refresh

The Breeze Session Token expires daily. Steps to renew:

1. Open the app → **Auth** tab → **Open Login URL**
2. Log in with ICICI credentials + TOTP
3. Copy the `session_token` from the redirect URL
4. Paste it into the Session Token field → **Save Settings**

---

## Config File

Settings are saved automatically to `breeze_downloader_config.json` next to the EXE. Credentials are stored in plain text — keep this file private.

---

## API Limits (Breeze)

| Limit | Value |
|---|---|
| Calls / minute | 100 (set app to 90 for safety) |
| Calls / day | 5,000 |
| Max candles / call | 1,000 |

For 1-second data: 375 min × 60 = 22,500 seconds/day → ~25 API calls/strike/day.
For 1-minute data: 375 candles/day → **1 API call/strike/day**.
