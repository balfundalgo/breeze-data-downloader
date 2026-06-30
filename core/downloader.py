"""
Breeze Data Downloader — Core Engine v1.6
NIFTY / BANKNIFTY | Options + Futures + Spot + VIX
Auto-detects expiry by probing API — handles all NSE rule changes.
"""

from breeze_connect import BreezeConnect
import pandas as pd
from datetime import datetime, date, timedelta
import os, time, random, threading, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable


# ─────────────────────────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Non-blocking token-bucket — sleeps OUTSIDE lock."""

    def __init__(self, calls_per_minute: float):
        self.min_interval   = 60.0 / calls_per_minute
        self.lock           = threading.Lock()
        self.last_call_time = 0.0
        self.total_calls    = 0

    def wait(self, stop_event=None) -> int:
        while True:
            with self.lock:
                now     = time.time()
                elapsed = now - self.last_call_time
                if elapsed >= self.min_interval:
                    self.last_call_time = now
                    self.total_calls   += 1
                    return self.total_calls
                sleep = self.min_interval - elapsed
            if stop_event:
                stop_event.wait(timeout=sleep)
                if stop_event.is_set():
                    return self.total_calls
            else:
                time.sleep(sleep)

    @property
    def calls(self) -> int:
        with self.lock:
            return self.total_calls


# ─────────────────────────────────────────────────────────────
# Progress Tracker
# ─────────────────────────────────────────────────────────────

class ProgressTracker:
    def __init__(self, filepath: str):
        self.filepath  = filepath
        self.lock      = threading.Lock()
        self.completed = self._load()

    def _load(self) -> set:
        try:
            with open(self.filepath) as f:
                return set(tuple(x) for x in json.load(f))
        except Exception:
            return set()

    def _save(self):
        with open(self.filepath, "w") as f:
            json.dump([list(x) for x in self.completed], f)

    def is_done(self, key: tuple) -> bool:
        with self.lock:
            return key in self.completed

    def mark_done(self, key: tuple):
        with self.lock:
            self.completed.add(key)
            if len(self.completed) % 50 == 0:
                self._save()

    def save(self):
        with self.lock:
            self._save()

    def count(self) -> int:
        with self.lock:
            return len(self.completed)


# ─────────────────────────────────────────────────────────────
# Strikes Cache
# ─────────────────────────────────────────────────────────────

class StrikesCache:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock     = threading.Lock()
        self.cache    = self._load()

    def _load(self) -> dict:
        try:
            with open(self.filepath) as f:
                return json.load(f)
        except Exception:
            return {}

    def _save(self):
        with open(self.filepath, "w") as f:
            json.dump(self.cache, f, indent=2)

    def get(self, expiry: date) -> list | None:
        with self.lock:
            return self.cache.get(expiry.isoformat())

    def set(self, expiry: date, strikes: list):
        with self.lock:
            self.cache[expiry.isoformat()] = sorted(strikes)
            self._save()


# ─────────────────────────────────────────────────────────────
# Main Downloader
# ─────────────────────────────────────────────────────────────

class BreezeDownloader:
    """
    Downloads NIFTY/BANKNIFTY options, futures, spot, and VIX.

    config keys:
        api_key, api_secret, api_session
        instrument          : "NIFTY" | "BANKNIFTY"
        interval            : "1second" | "1minute"
        from_date           : date
        to_date             : date
        out_dir             : str
        strike_discovery_range : int   (default 3000)
        max_workers         : int      (default 20)
        calls_per_minute    : float    (default 90)
        max_retries         : int      (default 8)
        download_spot       : bool     (default True)
        download_futures    : bool     (default False)
        download_vix        : bool     (default False)
        chunk_minutes       : int      (default 15, 1sec only)
    """

    MARKET_OPEN  = (9,  15)
    MARKET_CLOSE = (15, 30)
    STRIKE_STEP  = {
        "NIFTY":     50,
        "BANKNIFTY": 100,
    }
    VIX_STOCK_CODE = "INDVIX"

    # Breeze stock_code for futures — BANKNIFTY uses CNXBAN not BANKNIFTY
    FUTURES_CODE = {
        "NIFTY":     "NIFTY",
        "BANKNIFTY": "CNXBAN",
    }

    # Breeze stock_code for spot/cash — same mapping
    SPOT_CODE = {
        "NIFTY":     "NIFTY",
        "BANKNIFTY": "CNXBAN",
    }

    def _futures_code(self) -> str:
        """Return correct Breeze stock_code for futures (BANKNIFTY→CNXBAN)."""
        return self.FUTURES_CODE.get(self.config["instrument"],
                                     self.config["instrument"])

    def _spot_code(self) -> str:
        """Return correct Breeze stock_code for spot/cash (BANKNIFTY→CNXBAN)."""
        return self.SPOT_CODE.get(self.config["instrument"],
                                  self.config["instrument"])

    def _strike_step(self) -> int:
        """Return strike step: 50 for NIFTY, 100 for BANKNIFTY."""
        return self.STRIKE_STEP.get(self.config["instrument"], 50)

    def __init__(self, config, log_fn, stats_fn, stop_event):
        self.config     = config
        self.log        = log_fn
        self._stats_fn  = stats_fn
        self.stop_event = stop_event
        self.breeze     = None
        self.rate_limiter  = None
        self.progress      = None
        self.strikes_cache = None
        self._print_lock   = threading.Lock()
        self._expiry_cache = {}   # date → expiry (for futures)

    # ── Connection ──────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self.log("🔗 Connecting to Breeze API...")
            self.breeze = BreezeConnect(api_key=self.config["api_key"])
            self.breeze.generate_session(
                api_secret=self.config["api_secret"],
                session_token=self.config["api_session"],
            )
            self.log("✅ Connected to Breeze API")
            return True
        except Exception as e:
            self.log(f"❌ Connection failed: {e}")
            return False

    def reconnect(self) -> bool:
        """
        Recreate the BreezeConnect session to clear leaked SDK connections.
        The breeze-connect SDK leaks socket connections over time, making
        every call progressively slower. Recreating the session periodically
        (every N days) resets this and keeps speed constant.
        """
        try:
            self.log("    🔄 Refreshing session (clears connection leak)...")
            self.breeze = BreezeConnect(api_key=self.config["api_key"])
            self.breeze.generate_session(
                api_secret=self.config["api_secret"],
                session_token=self.config["api_session"],
            )
            return True
        except Exception as e:
            self.log(f"    ⚠️ Reconnect failed: {e}")
            return False

    # ── Helpers ─────────────────────────────────────────────────

    def _iso_z(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _ensure_dir(self, p: str):
        os.makedirs(p, exist_ok=True)

    def _daterange(self, d1: date, d2: date):
        d = d1
        while d <= d2:
            yield d
            d += timedelta(days=1)

    def _round_step(self, x: float, step: int) -> int:
        return int(round(x / step) * step)

    def _day_bounds(self, d: date):
        o = datetime(d.year, d.month, d.day, *self.MARKET_OPEN,  0)
        c = datetime(d.year, d.month, d.day, *self.MARKET_CLOSE, 0)
        return o, c

    def _is_transient(self, msg: str) -> bool:
        m = msg.lower()
        return any(p in m for p in [
            "connection reset", "timeout", "429", "too many",
            "502", "503", "504", "ssl", "broken pipe",
        ])

    def _safe_call(self, fn_name: str, **kwargs):
        fn          = getattr(self.breeze, fn_name)
        max_retries = self.config.get("max_retries", 8)
        for attempt in range(max_retries):
            if self.stop_event.is_set():
                raise InterruptedError("Stopped")
            try:
                self.rate_limiter.wait(self.stop_event)
                if self.stop_event.is_set():
                    raise InterruptedError("Stopped")
                return fn(**kwargs)
            except InterruptedError:
                raise
            except Exception as e:
                err = str(e).lower()
                is_429    = "429" in err or "too many" in err
                transient = is_429 or self._is_transient(err)
                if attempt >= max_retries - 1 or not transient:
                    raise
                sleep = 10.0 if is_429 else (2.0 * (2**attempt)) + random.uniform(0,1)
                if is_429:
                    with self._print_lock:
                        self.log(f"    ⚡ 429 — backing off {sleep:.0f}s")
                self.stop_event.wait(timeout=sleep)

    def _update_stats(self, **kwargs):
        for k, v in kwargs.items():
            self._stats_fn(k, v)

    # ── Chunk helpers ────────────────────────────────────────────

    def _get_time_chunks(self, d: date, chunk_min: int = 15) -> list:
        open_, close_ = self._day_bounds(d)
        chunks, start = [], open_
        while start < close_:
            end = min(start + timedelta(minutes=chunk_min), close_)
            chunks.append((start, end))
            start = end
        return chunks

    # ── Spot (cash) ──────────────────────────────────────────────

    def _get_spot_1min(self, d: date) -> pd.DataFrame | None:
        open_, close_ = self._day_bounds(d)
        try:
            r = self._safe_call("get_historical_data_v2",
                interval="1minute",
                from_date=self._iso_z(open_), to_date=self._iso_z(close_),
                stock_code=self._spot_code(), exchange_code="NSE",
                product_type="cash", expiry_date="", right="", strike_price="",
            )
            rows = r.get("Success") or []
            if not rows: return None
            df = pd.DataFrame(rows)
            if "datetime" in df.columns:
                df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
            return df
        except InterruptedError: raise
        except Exception: return None

    def _get_spot_close(self, df: pd.DataFrame) -> float | None:
        for col in ("close", "Close"):
            if col in df.columns and not df[col].empty:
                try: return float(df[col].iloc[-1])
                except Exception: pass
        return None

    # ── VIX ─────────────────────────────────────────────────────

    def _download_vix_day(self, d: date, out_dir: str) -> int:
        self._ensure_dir(out_dir)
        out_csv = os.path.join(out_dir, f"{d.isoformat()}.csv")
        if os.path.exists(out_csv):
            return -1
        interval      = self.config["interval"]
        open_, close_ = self._day_bounds(d)
        all_data      = []
        chunk_min     = min(self.config.get("chunk_minutes", 15), 15)
        if interval == "1minute":
            try:
                r = self._safe_call("get_historical_data_v2",
                    interval="1minute",
                    from_date=self._iso_z(open_), to_date=self._iso_z(close_),
                    stock_code=self.VIX_STOCK_CODE, exchange_code="NSE",
                    product_type="cash", expiry_date="", right="", strike_price="",
                )
                all_data = r.get("Success") or []
            except InterruptedError: raise
            except Exception as e:
                self.log(f"   ⚠️ VIX error: {str(e)[:60]}")
                return 0
        else:
            for cs, ce in self._get_time_chunks(d, chunk_min):
                if self.stop_event.is_set(): raise InterruptedError("Stopped")
                try:
                    r = self._safe_call("get_historical_data_v2",
                        interval="1second",
                        from_date=self._iso_z(cs), to_date=self._iso_z(ce),
                        stock_code=self.VIX_STOCK_CODE, exchange_code="NSE",
                        product_type="cash", expiry_date="", right="", strike_price="",
                    )
                    all_data.extend(r.get("Success") or [])
                except InterruptedError: raise
                except Exception: pass
        if not all_data: return 0
        df = pd.DataFrame(all_data)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(out_csv, index=False)
        return len(df)

    # ── Futures expiry detection ─────────────────────────────────

    def _candidate_expiries(self, d: date) -> list[date]:
        """
        Generate candidate expiry dates = last weekday of each month
        for the next 4 months. This correctly targets monthly expiries
        (last Thu/Tue/Wed) rather than probing every single weekday.
        Sorted nearest-first so the right expiry is found quickly.
        """
        candidates = set()
        for delta_mo in range(5):  # current + next 4 months
            mo = (d.month - 1 + delta_mo) % 12 + 1
            yr = d.year + ((d.month - 1 + delta_mo) // 12)
            for wd in range(5):   # Mon=0 .. Fri=4
                exp = self._last_weekday_of_month(yr, mo, wd)
                if exp >= d:
                    candidates.add(exp)
        return sorted(candidates)

    def _last_weekday_of_month(self, yr: int, mo: int, wd: int) -> date:
        """Return last occurrence of weekday wd in given month."""
        if mo == 12:
            last = date(yr + 1, 1, 1) - timedelta(days=1)
        else:
            last = date(yr, mo + 1, 1) - timedelta(days=1)
        while last.weekday() != wd:
            last -= timedelta(days=1)
        return last

    def _pick_futures_expiry(self, d: date, instrument: str) -> date | None:
        """
        Probe API to find active futures expiry for a given date.
        Tries nearest weekday candidates first, caches result.
        """
        # Use cache if valid
        cached = self._expiry_cache.get(d.isoformat())
        if cached and isinstance(cached, date) and cached >= d:
            return cached

        open_, _ = self._day_bounds(d)
        probe_to = open_ + timedelta(minutes=30)

        candidates = self._candidate_expiries(d)[:20]

        for expiry in candidates:
            if self.stop_event.is_set():
                return None
            exp_s = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
            try:
                r = self._safe_call("get_historical_data_v2",
                    interval="1minute",
                    from_date=self._iso_z(open_), to_date=self._iso_z(probe_to),
                    stock_code=self._futures_code(), exchange_code="NFO",
                    product_type="futures", expiry_date=exp_s,
                    right="others", strike_price="0",
                )
                if r.get("Success"):
                    self._expiry_cache[d.isoformat()] = expiry
                    return expiry
            except InterruptedError: raise
            except Exception: continue
        return None

    # ── Futures download for one day ─────────────────────────────

    def _download_futures_day(self, d: date, expiry: date,
                               out_dir: str) -> int:
        self._ensure_dir(out_dir)
        out_csv = os.path.join(out_dir, f"{d.isoformat()}.csv")
        if os.path.exists(out_csv):
            return -1

        exp_s         = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        interval      = self.config["interval"]
        open_, close_ = self._day_bounds(d)
        all_data      = []
        chunk_min     = min(self.config.get("chunk_minutes", 15), 15)

        if interval == "1second":
            for cs, ce in self._get_time_chunks(d, chunk_min):
                if self.stop_event.is_set(): raise InterruptedError("Stopped")
                try:
                    r = self._safe_call("get_historical_data_v2",
                        interval="1second",
                        from_date=self._iso_z(cs), to_date=self._iso_z(ce),
                        stock_code=self._futures_code(), exchange_code="NFO",
                        product_type="futures", expiry_date=exp_s,
                        right="others", strike_price="0",
                    )
                    all_data.extend(r.get("Success") or [])
                except InterruptedError: raise
                except Exception: pass
        else:
            try:
                r = self._safe_call("get_historical_data_v2",
                    interval=interval,
                    from_date=self._iso_z(open_), to_date=self._iso_z(close_),
                    stock_code=self._spot_code(), exchange_code="NFO",
                    product_type="futures", expiry_date=exp_s,
                    right="others", strike_price="0",
                )
                all_data = r.get("Success") or []
            except InterruptedError: raise
            except Exception as e:
                self.log(f"   ⚠️ Futures error: {str(e)[:60]}")
                return 0

        if not all_data: return 0
        df = pd.DataFrame(all_data)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(out_csv, index=False)
        return len(df)

    # ── Options expiry detection ─────────────────────────────────

    def _pick_options_expiry(self, d: date, atm: int) -> date | None:
        open_, _ = self._day_bounds(d)
        probe_to  = open_ + timedelta(minutes=30)
        WEEKDAY_PRIORITY = {1: 0, 0: 1, 2: 2, 4: 3, 3: 4}
        candidates = sorted(
            self._candidate_expiries(d)[:10],
            key=lambda x: (x - d).days * 10 + WEEKDAY_PRIORITY.get(x.weekday(), 5)
        )
        for expiry in candidates:
            if self.stop_event.is_set(): return None
            exp_s = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
            try:
                r = self._safe_call("get_historical_data_v2",
                    interval="1minute",
                    from_date=self._iso_z(open_), to_date=self._iso_z(probe_to),
                    stock_code=self._spot_code(), exchange_code="NFO",
                    product_type="options", expiry_date=exp_s,
                    right="call", strike_price=str(atm),
                )
                if r.get("Success"): return expiry
            except InterruptedError: raise
            except Exception: continue
        return None

    # ── Strike discovery ─────────────────────────────────────────

    def _probe_strike(self, d: date, expiry: date, strike: int) -> bool:
        exp_s = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        open_, _ = self._day_bounds(d)
        try:
            r = self._safe_call("get_historical_data_v2",
                interval="1minute",
                from_date=self._iso_z(open_),
                to_date=self._iso_z(open_ + timedelta(minutes=30)),
                stock_code=self._spot_code(), exchange_code="NFO",
                product_type="options", expiry_date=exp_s,
                right="call", strike_price=str(strike),
            )
            return bool(r.get("Success"))
        except InterruptedError: raise
        except Exception: return False

    def _discover_strikes(self, d: date, expiry: date, atm: int) -> list[int]:
        """
        Generate strikes mathematically — NO probing.

        Old approach probed ~97 strikes one-by-one per day (37-106s) and
        leaked SDK connections, causing the downloader to slow to a crawl
        and appear stuck after a day or two.

        New approach: generate ATM ± discovery_range at the correct step.
        Strikes that don't exist simply return empty during download —
        harmless, and the empty-chunk handling already covers it.
        Result: ~0.5s vs 37-106s, and zero discovery API calls.
        """
        disc = self.config.get("strike_discovery_range", 3000)
        step = self._strike_step()
        strikes = list(range(atm - disc, atm + disc + 1, step))
        strikes = [s for s in strikes if s > 0]
        self.log(f"    ⚡ Generated {len(strikes)} strikes instantly "
                 f"({strikes[0]} – {strikes[-1]}, step {step}) — no probing")
        return strikes

    # ── Options: single strike/right (1sec flat pool chunk) ──────

    def _download_single_chunk(self, chunk_start, chunk_end, strike, right, expiry):
        if self.stop_event.is_set(): raise InterruptedError("Stopped")
        exp_s = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        try:
            r = self._safe_call("get_historical_data_v2",
                interval="1second",
                from_date=self._iso_z(chunk_start), to_date=self._iso_z(chunk_end),
                stock_code=self._spot_code(), exchange_code="NFO",
                product_type="options", expiry_date=exp_s,
                right=right, strike_price=str(strike),
            )
            return r.get("Success") or []
        except InterruptedError: raise
        except Exception as e:
            with self._print_lock:
                self.log(f"      ⚠️ chunk {strike}{right[0].upper()}E "
                         f"{chunk_start.strftime('%H:%M')}→{chunk_end.strftime('%H:%M')}: "
                         f"{str(e)[:40]}")
            return []

    def _download_strike_1min(self, d, expiry, strike, right, out_dir, progress):
        key = (d.isoformat(), str(strike), right, "1minute")
        if progress.is_done(key): return {"skipped":1,"files":0,"rows":0}
        out_csv = f"{out_dir}/{d.isoformat()}_{strike}_{right[0].upper()}E.csv"
        if os.path.exists(out_csv):
            progress.mark_done(key); return {"skipped":1,"files":0,"rows":0}
        exp_s = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        open_, close_ = self._day_bounds(d)
        try:
            r = self._safe_call("get_historical_data_v2",
                interval=self.config["interval"],
                from_date=self._iso_z(open_), to_date=self._iso_z(close_),
                stock_code=self._spot_code(), exchange_code="NFO",
                product_type="options", expiry_date=exp_s,
                right=right, strike_price=str(strike),
            )
            rows = r.get("Success") or []
        except InterruptedError: raise
        except Exception: return {"skipped":0,"files":0,"rows":0}
        if not rows: progress.mark_done(key); return {"skipped":0,"files":0,"rows":0}
        df = pd.DataFrame(rows)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(out_csv, index=False)
        progress.mark_done(key)
        return {"skipped":0,"files":1,"rows":len(df)}

    # ── Day processing ───────────────────────────────────────────

    def _interval_label(self) -> str:
        return "1MIN" if self.config["interval"] == "1minute" else "1SEC"

    def _process_options_day(self, d, expiry, strikes, progress) -> dict:
        inst    = self.config["instrument"]
        out_dir = os.path.join(self.config["out_dir"],
                               f"{inst}_OPTIONS_{self._interval_label()}",
                               expiry.isoformat())
        self._ensure_dir(out_dir)
        total = {"skipped":0,"files":0,"rows":0}

        if self.config["interval"] == "1minute":
            tasks   = [(s,r) for s in strikes if s>0 for r in ("call","put")]
            workers = self.config.get("max_workers",20)
            done    = 0
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(self._download_strike_1min, d, expiry, s, r, out_dir, progress):(s,r)
                           for s,r in tasks}
                for fut in as_completed(futures):
                    if self.stop_event.is_set(): break
                    s,r = futures[fut]
                    try:
                        st = fut.result()
                        total["skipped"] += st["skipped"]
                        total["files"]   += st["files"]
                        total["rows"]    += st["rows"]
                        if st["files"] > 0:
                            with self._print_lock:
                                self.log(f"      💾 {s}{r[0].upper()}E: {st['rows']:,} rows")
                    except Exception: pass
                    done += 1
                    if done % 20 == 0:
                        with self._print_lock:
                            self.log(f"      ⏳ {done}/{len(tasks)} | API: {self.rate_limiter.calls}")
        else:
            # 1-second: flat chunk pool
            chunks  = self._get_time_chunks(d, min(self.config.get("chunk_minutes",15), 15))
            pending = {}
            for s in strikes:
                for r in ("call","put"):
                    key     = (d.isoformat(), str(s), r, "1second")
                    out_csv = f"{out_dir}/{d.isoformat()}_{s}_{r[0].upper()}E.csv"
                    if os.path.exists(out_csv):
                        if self._is_file_complete(out_csv, d):
                            progress.mark_done(key); total["skipped"] += 1; continue
                        else:
                            with self._print_lock:
                                self.log(f"    🔄 {s}{r[0].upper()}E: incomplete, re-downloading")
                            try: os.remove(out_csv)
                            except Exception: pass
                    elif progress.is_done(key):
                        total["skipped"] += 1; continue
                    pending[(s,r)] = (key, out_csv)

            if not pending: return total

            n_tasks = len(pending) * len(chunks)
            # Pool size = calls_per_minute / 4
            # This ensures the rate limiter is the bottleneck, not the server.
            # e.g. 300 calls/min → 75 threads max → server never gets burst-flooded
            cpm     = self.config.get("calls_per_minute", 90)
            workers = max(10, min(int(cpm / 4), 200))
            self.log(f"      🚀 Flat pool: {len(pending)} files × {len(chunks)} chunks "
                     f"= {n_tasks} tasks | pool={workers} (capped at CPM/4)")
            chunk_results = {sr: [None]*len(chunks) for sr in pending}
            done_count    = [0]
            lock          = threading.Lock()

            def fetch_with_retry(cs, ce, s, r, expiry, max_retry=1):
                """
                Fetch one chunk with retry on empty response.
                max_retry=1: one retry catches genuine rate-drops without
                the long cascade that happens when Breeze throttles under
                sustained load near the end of a large day.
                """
                for attempt in range(max_retry + 1):
                    if self.stop_event.is_set():
                        raise InterruptedError("Stopped")
                    rows = self._download_single_chunk(cs, ce, s, r, expiry)
                    if rows:
                        return rows
                    if attempt < max_retry:
                        self.stop_event.wait(timeout=0.5)
                return []

            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {}
                for (s,r) in pending:
                    for ci,(cs,ce) in enumerate(chunks):
                        fut = ex.submit(fetch_with_retry, cs, ce, s, r, expiry)
                        futures[fut] = (s,r,ci)
                for fut in as_completed(futures):
                    if self.stop_event.is_set(): break
                    s,r,ci = futures[fut]
                    try: chunk_results[(s,r)][ci] = fut.result() or []
                    except InterruptedError: break
                    except Exception: chunk_results[(s,r)][ci] = []
                    with lock:
                        done_count[0] += 1
                        if done_count[0] % 200 == 0:
                            with self._print_lock:
                                self.log(f"      ⏳ {done_count[0]}/{n_tasks} chunks | "
                                         f"API: {self.rate_limiter.calls}")

            for (s,r),(key,out_csv) in pending.items():
                all_rows = []
                for chunk in chunk_results[(s,r)]:
                    if chunk: all_rows.extend(chunk)
                if all_rows:
                    df = pd.DataFrame(all_rows)
                    if "datetime" in df.columns:
                        df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
                        df.sort_values("datetime", inplace=True)
                    df.to_csv(out_csv, index=False)
                    progress.mark_done(key)
                    total["files"] += 1
                    total["rows"]  += len(df)
                else:
                    progress.mark_done(key)
        return total

    def _is_file_complete(self, csv_path: str, d: date) -> bool:
        try:
            # Fast guard: zero-byte or tiny files (interrupted writes) → incomplete
            size = os.path.getsize(csv_path)
            if size < 50:   # header alone is ~40 bytes; real data is much larger
                return False
            df = pd.read_csv(csv_path, nrows=5, on_bad_lines="skip",
                             engine="c", encoding_errors="ignore")
            if "datetime" not in df.columns or df.empty:
                return False
            first_ts = pd.to_datetime(df["datetime"].iloc[0], errors="coerce")
            if pd.isna(first_ts):
                return False
            expected = datetime(d.year, d.month, d.day, *self.MARKET_OPEN, 0)
            return first_ts <= pd.Timestamp(expected) + pd.Timedelta(minutes=5)
        except Exception:
            return False

    # ── Main run ─────────────────────────────────────────────────

    def run(self):
        cfg     = self.config
        out_dir = cfg["out_dir"]
        self._ensure_dir(out_dir)
        inst    = cfg["instrument"]
        lbl     = self._interval_label()

        self.rate_limiter  = RateLimiter(cfg.get("calls_per_minute", 90))
        self.progress      = ProgressTracker(os.path.join(out_dir, ".progress.json"))
        self.strikes_cache = StrikesCache(os.path.join(out_dir, ".strikes_cache.json"))

        if self.progress.count() > 0:
            self.log(f"📂 Resuming — {self.progress.count()} items already completed")

        from_d = cfg["from_date"]
        to_d   = cfg["to_date"]

        self.log("─" * 60)
        self.log(f"🎯  Instrument : {inst}")
        self.log(f"⏱️   Interval   : {cfg['interval']}")
        self.log(f"📅  Date range  : {from_d} → {to_d}")
        self.log(f"📦  Products    : " + ", ".join(filter(None, [
            "Spot"    if cfg.get("download_spot")    else "",
            "Futures" if cfg.get("download_futures") else "",
            "Options" if cfg.get("download_options", True) else "",
            "VIX"     if cfg.get("download_vix")     else "",
        ])))
        self.log(f"📂  Output      : {out_dir}")
        self.log(f"🔧  Workers     : {cfg.get('max_workers',20)}")
        self.log(f"⚡  API limit   : {cfg.get('calls_per_minute',90)}/min")
        self.log("─" * 60)

        totals = {"days":0,"files":0,"skipped":0,"rows":0}
        days_processed = 0   # for periodic session refresh
        RECONNECT_EVERY = 15  # refresh session every 15 trading days

        for d in self._daterange(from_d, to_d):
            if self.stop_event.is_set():
                self.log("⚠️ Stopped by user"); break

            self.log(f"\n📅 {d.strftime('%A, %d %b %Y')}")

            # ── 1. Spot check (trading day gate) ───────────────
            spot_df = self._get_spot_1min(d)
            if spot_df is None or spot_df.empty:
                self.log("   ⏭️  No spot data (holiday / weekend)")
                continue

            # Periodic session refresh to clear SDK connection leak
            days_processed += 1
            if days_processed % RECONNECT_EVERY == 0:
                self.reconnect()

            spot_close = self._get_spot_close(spot_df)
            if not spot_close or spot_close <= 0:
                self.log("   ⏭️  Invalid spot close — skipping")
                continue

            # ── 2. Save spot ───────────────────────────────────
            if cfg.get("download_spot", True):
                spot_dir = os.path.join(out_dir, f"{inst}_SPOT_{lbl}")
                self._ensure_dir(spot_dir)
                spot_csv = os.path.join(spot_dir, f"{d.isoformat()}.csv")
                if not os.path.exists(spot_csv):
                    if cfg["interval"] == "1minute":
                        spot_df.to_csv(spot_csv, index=False)
                        self.log(f"   💾 Spot saved: {len(spot_df)} rows")
                    else:
                        spot_data = []
                        chunk_min = min(cfg.get("chunk_minutes",15), 15)
                        for cs,ce in self._get_time_chunks(d, chunk_min):
                            if self.stop_event.is_set(): break
                            try:
                                r = self._safe_call("get_historical_data_v2",
                                    interval="1second",
                                    from_date=self._iso_z(cs), to_date=self._iso_z(ce),
                                    stock_code=self._spot_code(), exchange_code="NSE",
                                    product_type="cash", expiry_date="", right="", strike_price="",
                                )
                                spot_data.extend(r.get("Success") or [])
                            except InterruptedError: raise
                            except Exception: pass
                        if spot_data:
                            sdf = pd.DataFrame(spot_data)
                            if "datetime" in sdf.columns:
                                sdf.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
                            sdf.to_csv(spot_csv, index=False)
                            self.log(f"   💾 Spot saved: {len(sdf):,} rows")
                else:
                    self.log("   ⏭️  Spot already exists")

            # ── 3. VIX ─────────────────────────────────────────
            if cfg.get("download_vix", False):
                vix_dir = os.path.join(out_dir, f"INDVIX_{lbl}")
                rows = self._download_vix_day(d, vix_dir)
                if rows > 0:   self.log(f"   📈 VIX saved: {rows:,} rows")
                elif rows ==-1: self.log("   ⏭️  VIX already exists")
                else:           self.log("   ⚠️  VIX — no data returned")

            # ── 4. Futures ─────────────────────────────────────
            if cfg.get("download_futures", False):
                self.log(f"   🔍 Finding futures expiry…")
                fut_expiry = self._pick_futures_expiry(d, inst)
                if fut_expiry:
                    fut_dir = os.path.join(out_dir, f"{inst}_FUTURES_{lbl}",
                                           fut_expiry.isoformat())
                    self._ensure_dir(fut_dir)
                    rows = self._download_futures_day(d, fut_expiry, fut_dir)
                    if rows > 0:
                        totals["files"] += 1; totals["rows"] += rows
                        self.log(f"   💾 Futures [expiry {fut_expiry}]: {rows:,} rows")
                    elif rows == -1:
                        totals["skipped"] += 1
                        self.log(f"   ⏭️  Futures already exists [expiry {fut_expiry}]")
                    else:
                        self.log(f"   ⚠️  Futures — no data (expiry {fut_expiry})")
                else:
                    self.log("   ⚠️  Futures — could not detect expiry")

            # ── 5. Options ─────────────────────────────────────
            if not cfg.get("download_options", True):
                totals["days"] += 1
                self._update_stats(days=totals["days"],
                                   files=totals["files"],
                                   rows=totals["rows"],
                                   api_calls=self.rate_limiter.calls)
                continue

            atm    = self._round_step(spot_close, self._strike_step())
            expiry = self._pick_options_expiry(d, atm)
            if not expiry:
                self.log("   ⏭️  Could not find options expiry — skipping")
                continue

            self.log(f"   📍 Spot={spot_close:.2f} | ATM={atm} | Options expiry={expiry}")

            strikes = self._discover_strikes(d, expiry, atm)
            if not strikes:
                self.log("   ⏭️  No strikes found — skipping")
                continue

            self.log(f"   📊 Downloading {len(strikes)} strikes × 2 sides…")
            day_stats = self._process_options_day(d, expiry, strikes, self.progress)
            totals["days"]    += 1
            totals["files"]   += day_stats["files"]
            totals["skipped"] += day_stats["skipped"]
            totals["rows"]    += day_stats["rows"]
            self.progress.save()

            self._update_stats(
                days=totals["days"], files=totals["files"],
                rows=totals["rows"], api_calls=self.rate_limiter.calls,
            )
            self.log(f"   ✅ {day_stats['files']} new | "
                     f"{day_stats['skipped']} skipped | "
                     f"{day_stats['rows']:,} rows | API: {self.rate_limiter.calls}")

        self.log(f"\n{'='*60}")
        self.log(f"✅ Download complete!")
        self.log(f"   Days processed : {totals['days']}")
        self.log(f"   Files created  : {totals['files']:,}")
        self.log(f"   Files skipped  : {totals['skipped']:,}")
        self.log(f"   Total rows     : {totals['rows']:,}")
        self.log(f"   Total API calls: {self.rate_limiter.calls}")
        self.progress.save()
