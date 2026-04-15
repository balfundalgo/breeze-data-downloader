"""
Breeze Stock Downloader — Core Engine
Supports NSE EQ stocks:
  - Cash / Spot  (exchange=NSE)
  - Futures      (exchange=NFO, auto expiry)
  - Options      (exchange=NFO, auto expiry + strike discovery from FONSEScripMaster)

All 5 intervals: 1second, 1minute, 5minute, 30minute, 1day
"""

from breeze_connect import BreezeConnect
import pandas as pd
from datetime import datetime, date, timedelta
import os, time, random, threading, json, io, zipfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from core.downloader import RateLimiter, ProgressTracker

SECURITY_MASTER_URL = "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"

# 1sec API limit: max 1000 candles → 15-min chunks
CHUNK_MINUTES_1SEC = 15

# Market hours
MARKET_OPEN  = (9,  15)
MARKET_CLOSE = (15, 30)


def _iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    o = datetime(d.year, d.month, d.day, *MARKET_OPEN,  0)
    c = datetime(d.year, d.month, d.day, *MARKET_CLOSE, 0)
    return o, c


def _daterange(d1: date, d2: date):
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)


def _last_thursday(yr: int, mo: int) -> date:
    """Last Thursday of a given month — NSE stock F&O expiry."""
    # Find last day, walk back to Thursday (weekday=3)
    if mo == 12:
        last = date(yr + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(yr, mo + 1, 1) - timedelta(days=1)
    while last.weekday() != 3:
        last -= timedelta(days=1)
    return last


def _active_monthly_expiry(d: date) -> date:
    """
    Return the active monthly expiry for a given date.
    If today's date is past the current month's last Thursday,
    roll over to next month.
    """
    expiry = _last_thursday(d.year, d.month)
    if d > expiry:
        # Roll to next month
        if d.month == 12:
            expiry = _last_thursday(d.year + 1, 1)
        else:
            expiry = _last_thursday(d.year, d.month + 1)
    return expiry


def _time_chunks(d: date, chunk_min: int = 15) -> list:
    open_, close_ = _day_bounds(d)
    chunks, start = [], open_
    while start < close_:
        end = min(start + timedelta(minutes=chunk_min), close_)
        chunks.append((start, end))
        start = end
    return chunks


# ─────────────────────────────────────────────────────────────
#  Security Master — stock list + F&O strikes
# ─────────────────────────────────────────────────────────────

class SecurityMaster:
    """Downloads and caches NSE Security Master files."""

    def __init__(self, cache_dir: str = "."):
        self.cache_dir  = cache_dir
        self._eq_df     = None   # NSE EQ stocks
        self._fo_df     = None   # FONSEScripMaster (options/futures)
        self._lock      = threading.Lock()

    def _download_zip(self) -> bytes:
        req = urllib.request.Request(
            SECURITY_MASTER_URL, headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read()

    def load(self, log_fn: Callable = print):
        with self._lock:
            if self._eq_df is not None:
                return
            log_fn("📥 Downloading NSE Security Master...")
            zip_bytes = self._download_zip()
            log_fn(f"   ✅ {len(zip_bytes)//1024} KB downloaded")

            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                # NSE EQ stocks
                with z.open("NSEScripMaster.txt") as f:
                    eq = pd.read_csv(f, low_memory=False, encoding="latin-1")
                eq.columns = [c.strip().strip('"') for c in eq.columns]
                for col in eq.columns:
                    if eq[col].dtype == object:
                        eq[col] = eq[col].astype(str).str.strip().str.strip('"')
                self._eq_df = eq

                # F&O master
                with z.open("FONSEScripMaster.txt") as f:
                    fo = pd.read_csv(f, low_memory=False, encoding="latin-1")
                fo.columns = [c.strip().strip('"') for c in fo.columns]
                for col in fo.columns:
                    if fo[col].dtype == object:
                        fo[col] = fo[col].astype(str).str.strip().str.strip('"')
                self._fo_df = fo

            log_fn(f"   📊 EQ stocks: {len(self._eq_df)} | F&O rows: {len(self._fo_df)}")

    def eq_stocks(self) -> pd.DataFrame:
        """Return all EQ series stocks as DataFrame with ShortName, CompanyName."""
        if self._eq_df is None:
            raise RuntimeError("Call load() first")
        df = self._eq_df
        eq = df[df["Series"].astype(str).str.strip().str.upper() == "EQ"].copy()
        eq = eq[eq["ShortName"].str.len() >= 2]
        return eq[["ShortName", "CompanyName"]].drop_duplicates(
            subset="ShortName").reset_index(drop=True)

    def strikes_for_expiry(self, stock_code: str, expiry: date) -> list[int]:
        """
        Return sorted list of available strike prices for a stock+expiry
        from the F&O master. Returns [] if stock not in F&O.
        """
        if self._fo_df is None:
            raise RuntimeError("Call load() first")

        df = self._fo_df
        # Match stock code
        code_col = "ShortName"
        sub = df[df[code_col].str.upper() == stock_code.upper()].copy()
        if sub.empty:
            return []

        # Parse expiry
        sub["_exp"] = pd.to_datetime(sub["ExpiryDate"], errors="coerce", dayfirst=True)
        sub = sub.dropna(subset=["_exp"])

        # Match to target expiry (within ±3 days for tolerance)
        target = pd.Timestamp(expiry)
        sub = sub[abs((sub["_exp"] - target).dt.days) <= 3]
        if sub.empty:
            return []

        # Extract numeric strikes
        strikes = pd.to_numeric(sub["StrikePrice"], errors="coerce").dropna()
        strikes = [int(s) for s in strikes.unique() if s > 0]
        return sorted(strikes)

    def is_fo_stock(self, stock_code: str) -> bool:
        """Return True if stock exists in F&O master."""
        if self._fo_df is None:
            return False
        return not self._fo_df[
            self._fo_df["ShortName"].str.upper() == stock_code.upper()
        ].empty


# Global singleton
_security_master = SecurityMaster()


def get_security_master() -> SecurityMaster:
    return _security_master


# ─────────────────────────────────────────────────────────────
#  Stock Downloader
# ─────────────────────────────────────────────────────────────

class StockDownloader:
    """
    Downloads NSE stock historical data via ICICI Breeze API.

    config keys:
        api_key, api_secret, api_session
        stock_code          : str  e.g. "RELIND"
        interval            : "1second"|"1minute"|"5minute"|"30minute"|"1day"
        from_date           : date
        to_date             : date
        out_dir             : str
        download_spot       : bool  (default True)
        download_futures    : bool  (default False)
        download_options    : bool  (default False)
        strike_range        : int   ATM ± N for options (default 2000)
        max_workers         : int   (default 20)
        calls_per_minute    : float (default 90)
        chunk_minutes       : int   (default 15, 1sec only)
    """

    def __init__(
        self,
        config:     dict,
        log_fn:     Callable[[str], None],
        stats_fn:   Callable[[str, int], None],
        stop_event: threading.Event,
    ):
        self.config     = config
        self.log        = log_fn
        self._stats_fn  = stats_fn
        self.stop_event = stop_event
        self.breeze     = None
        self.rl         = None
        self.progress   = None
        self._plock     = threading.Lock()

    # ── Connection ───────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self.log("🔗 Connecting to Breeze API...")
            self.breeze = BreezeConnect(api_key=self.config["api_key"])
            self.breeze.generate_session(
                api_secret=self.config["api_secret"],
                session_token=self.config["api_session"],
            )
            self.log("✅ Connected")
            return True
        except Exception as e:
            self.log(f"❌ Connection failed: {e}")
            return False

    # ── API call wrapper ─────────────────────────────────────

    def _call(self, **kwargs) -> list:
        self.rl.wait(self.stop_event)
        if self.stop_event.is_set():
            raise InterruptedError("Stopped")
        for attempt in range(8):
            try:
                r = self.breeze.get_historical_data_v2(**kwargs)
                return r.get("Success") or []
            except InterruptedError:
                raise
            except Exception as e:
                msg = str(e).lower()
                transient = any(p in msg for p in [
                    "timeout", "reset", "429", "502", "503", "ssl", "broken"
                ])
                if attempt >= 7 or not transient:
                    return []
                sleep = (2.0 * (2 ** attempt)) + random.uniform(0, 1)
                self.stop_event.wait(timeout=sleep)
        return []

    # ── Helpers ──────────────────────────────────────────────

    def _interval_label(self) -> str:
        return {
            "1second":  "1SEC",
            "1minute":  "1MIN",
            "5minute":  "5MIN",
            "30minute": "30MIN",
            "1day":     "1DAY",
        }.get(self.config["interval"], self.config["interval"].upper())

    def _fetch_day(self, d: date, stock_code: str, exchange: str,
                   product_type: str, expiry_str: str = "",
                   right: str = "", strike: str = "") -> list:
        """Fetch one full day of data, chunked for 1second."""
        interval = self.config["interval"]
        open_, close_ = _day_bounds(d)

        if interval == "1second":
            all_data = []
            for cs, ce in _time_chunks(d, self.config.get("chunk_minutes", 15)):
                if self.stop_event.is_set():
                    raise InterruptedError("Stopped")
                rows = self._call(
                    interval=interval,
                    from_date=_iso_z(cs), to_date=_iso_z(ce),
                    stock_code=stock_code, exchange_code=exchange,
                    product_type=product_type,
                    expiry_date=expiry_str, right=right, strike_price=strike,
                )
                all_data.extend(rows)
            return all_data
        else:
            return self._call(
                interval=interval,
                from_date=_iso_z(open_), to_date=_iso_z(close_),
                stock_code=stock_code, exchange_code=exchange,
                product_type=product_type,
                expiry_date=expiry_str, right=right, strike_price=strike,
            )

    def _save_df(self, rows: list, path: str) -> int:
        if not rows:
            return 0
        df = pd.DataFrame(rows)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(path, index=False)
        return len(df)

    def _update_stats(self, **kw):
        for k, v in kw.items():
            self._stats_fn(k, v)

    # ── Spot download ────────────────────────────────────────

    def _download_spot_day(self, d: date, out_dir: str) -> int:
        os.makedirs(out_dir, exist_ok=True)
        out_csv = os.path.join(out_dir, f"{d.isoformat()}.csv")
        if os.path.exists(out_csv):
            return -1
        rows = self._fetch_day(
            d, self.config["stock_code"], "NSE", "cash"
        )
        return self._save_df(rows, out_csv)

    # ── Futures download ─────────────────────────────────────

    def _download_futures_day(self, d: date, expiry: date, out_dir: str) -> int:
        os.makedirs(out_dir, exist_ok=True)
        exp_key = expiry.isoformat()
        out_csv = os.path.join(out_dir, f"{d.isoformat()}_{exp_key}.csv")
        if os.path.exists(out_csv):
            return -1
        exp_str = _iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        rows = self._fetch_day(
            d, self.config["stock_code"], "NFO", "futures",
            expiry_str=exp_str, right="others", strike="0",
        )
        return self._save_df(rows, out_csv)

    # ── Options download for one strike/right ────────────────

    def _download_option(self, d: date, expiry: date, strike: int,
                         right: str, out_dir: str) -> dict:
        key = (d.isoformat(), expiry.isoformat(), str(strike), right,
               self.config["interval"])
        if self.progress.is_done(key):
            return {"skipped": 1, "files": 0, "rows": 0}

        out_csv = os.path.join(
            out_dir, f"{d.isoformat()}_{strike}_{right[0].upper()}E.csv"
        )
        if os.path.exists(out_csv):
            self.progress.mark_done(key)
            return {"skipped": 1, "files": 0, "rows": 0}

        exp_str = _iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        try:
            rows = self._fetch_day(
                d, self.config["stock_code"], "NFO", "options",
                expiry_str=exp_str, right=right, strike=str(strike),
            )
        except InterruptedError:
            raise
        except Exception as e:
            with self._plock:
                self.log(f"      ⚠️ {strike}{right[0].upper()}E: {str(e)[:50]}")
            return {"skipped": 0, "files": 0, "rows": 0}

        saved = self._save_df(rows, out_csv) if rows else 0
        self.progress.mark_done(key)
        return {"skipped": 0, "files": 1 if saved > 0 else 0, "rows": saved}

    # ── Auto-discover strikes from F&O master ────────────────

    def _get_strikes(self, expiry: date, spot_close: float) -> list[int]:
        sm      = get_security_master()
        strikes = sm.strikes_for_expiry(self.config["stock_code"], expiry)

        if strikes:
            # Filter to ATM ± strike_range
            sr  = self.config.get("strike_range", 9999999)
            atm = round(spot_close / 1) * 1
            strikes = [s for s in strikes if abs(s - atm) <= sr]
            self.log(f"   📂 F&O master: {len(strikes)} strikes for {expiry}")
        else:
            self.log(f"   ⚠️  No strikes in F&O master for {self.config['stock_code']} {expiry}")

        return strikes

    # ── Process one day ──────────────────────────────────────

    def _process_day(self, d: date, spot_close: float,
                     totals: dict) -> None:
        cfg      = self.config
        code     = cfg["stock_code"]
        interval = self._interval_label()
        base_dir = cfg["out_dir"]

        # ── Spot ──────────────────────────────────────────────
        if cfg.get("download_spot", True):
            spot_dir = os.path.join(base_dir, f"{code}_SPOT_{interval}")
            rows = self._download_spot_day(d, spot_dir)
            if rows > 0:
                totals["files"] += 1
                totals["rows"]  += rows
                self.log(f"   💾 Spot: {rows:,} rows")
            elif rows == -1:
                totals["skipped"] += 1
                self.log("   ⏭️  Spot already exists")
            else:
                self.log("   ⚠️  Spot: no data")

        need_fo = cfg.get("download_futures", False) or cfg.get("download_options", False)
        if not need_fo:
            return

        # ── Expiry ────────────────────────────────────────────
        expiry = _active_monthly_expiry(d)
        self.log(f"   📅 Active expiry: {expiry}")

        # ── Futures ───────────────────────────────────────────
        if cfg.get("download_futures", False):
            fut_dir = os.path.join(base_dir, f"{code}_FUTURES_{interval}",
                                   expiry.isoformat())
            os.makedirs(fut_dir, exist_ok=True)
            rows = self._download_futures_day(d, expiry, fut_dir)
            if rows > 0:
                totals["files"] += 1
                totals["rows"]  += rows
                self.log(f"   💾 Futures: {rows:,} rows")
            elif rows == -1:
                totals["skipped"] += 1
                self.log("   ⏭️  Futures already exists")
            else:
                self.log("   ⚠️  Futures: no data")

        # ── Options ───────────────────────────────────────────
        if cfg.get("download_options", False):
            strikes = self._get_strikes(expiry, spot_close)
            if not strikes:
                self.log("   ⚠️  Options: no strikes found — skipping")
                return

            opt_dir = os.path.join(base_dir, f"{code}_OPTIONS_{interval}",
                                   expiry.isoformat())
            os.makedirs(opt_dir, exist_ok=True)

            tasks   = [(s, r) for s in strikes for r in ("call", "put")]
            workers = cfg.get("max_workers", 20)
            done    = 0

            self.log(f"   📊 Options: {len(strikes)} strikes × 2 = {len(tasks)} files")

            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {
                    ex.submit(self._download_option, d, expiry, s, r, opt_dir): (s, r)
                    for s, r in tasks
                }
                for fut in as_completed(futures):
                    if self.stop_event.is_set():
                        break
                    s, r = futures[fut]
                    try:
                        st = fut.result()
                        totals["skipped"] += st["skipped"]
                        totals["files"]   += st["files"]
                        totals["rows"]    += st["rows"]
                        if st["files"] > 0:
                            with self._plock:
                                self.log(f"      💾 {s}{r[0].upper()}E: {st['rows']:,} rows")
                    except InterruptedError:
                        break
                    except Exception as e:
                        with self._plock:
                            self.log(f"      ❌ {s}{r[0].upper()}E: {str(e)[:50]}")
                    done += 1
                    if done % 20 == 0:
                        with self._plock:
                            self.log(f"      ⏳ {done}/{len(tasks)} | API: {self.rl.calls}")

    # ── Main run ─────────────────────────────────────────────

    def run(self):
        cfg = self.config

        self.rl       = RateLimiter(cfg.get("calls_per_minute", 90))
        self.progress = ProgressTracker(
            os.path.join(cfg["out_dir"], f".progress_{cfg['stock_code']}.json")
        )
        os.makedirs(cfg["out_dir"], exist_ok=True)

        # Load security master (needed for options strike discovery)
        sm = get_security_master()
        if cfg.get("download_options", False) and sm._fo_df is None:
            sm.load(log_fn=self.log)

        if self.progress.count() > 0:
            self.log(f"📂 Resuming — {self.progress.count()} items already done")

        from_d = cfg["from_date"]
        to_d   = cfg["to_date"]
        code   = cfg["stock_code"]

        self.log("─" * 60)
        self.log(f"🎯  Stock     : {code}")
        self.log(f"⏱️   Interval  : {cfg['interval']}")
        self.log(f"📅  Date range : {from_d} → {to_d}")
        self.log(f"📦  Products   : "
                 + ", ".join(filter(None, [
                     "Spot"    if cfg.get("download_spot")    else "",
                     "Futures" if cfg.get("download_futures") else "",
                     "Options" if cfg.get("download_options") else "",
                 ])))
        self.log(f"📂  Output    : {cfg['out_dir']}")
        self.log("─" * 60)

        totals = {"days": 0, "files": 0, "skipped": 0, "rows": 0}

        for d in _daterange(from_d, to_d):
            if self.stop_event.is_set():
                self.log("⚠️ Stopped by user")
                break

            self.log(f"\n📅 {d.strftime('%A, %d %b %Y')}")

            # Trading-day check via spot 1-min
            open_, close_ = _day_bounds(d)
            check_rows = self._call(
                interval="1minute",
                from_date=_iso_z(open_),
                to_date=_iso_z(open_ + timedelta(minutes=30)),
                stock_code=code, exchange_code="NSE",
                product_type="cash",
                expiry_date="", right="", strike_price="",
            )
            if not check_rows:
                self.log("   ⏭️  No data (holiday / weekend)")
                continue

            # Get spot close for ATM calculation
            spot_close = float(pd.DataFrame(check_rows)["close"].iloc[-1])

            self._process_day(d, spot_close, totals)
            totals["days"] += 1
            self.progress.save()

            self._update_stats(
                days=totals["days"],
                files=totals["files"],
                rows=totals["rows"],
                api_calls=self.rl.calls,
            )

        self.log(f"\n{'='*60}")
        self.log(f"✅ Done! Days={totals['days']} | "
                 f"Files={totals['files']:,} | "
                 f"Rows={totals['rows']:,} | "
                 f"API calls={self.rl.calls}")
        self.progress.save()
