"""
Breeze Data Downloader — Core Engine
Supports NIFTY / BANKNIFTY | 1minute / 1second intervals
Multi-threaded with resume, rate limiting, and stop support.
"""

from breeze_connect import BreezeConnect
import pandas as pd
from datetime import datetime, date, timedelta
import os, time, random, threading, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────

class RateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, calls_per_minute: float):
        self.min_interval   = 60.0 / calls_per_minute
        self.lock           = threading.Lock()
        self.last_call_time = 0.0
        self.total_calls    = 0

    def wait(self, stop_event: threading.Event | None = None) -> int:
        with self.lock:
            elapsed = time.time() - self.last_call_time
            sleep   = self.min_interval - elapsed
            if sleep > 0:
                if stop_event:
                    stop_event.wait(timeout=sleep)
                    if stop_event.is_set():
                        return self.total_calls
                else:
                    time.sleep(sleep)
            self.last_call_time = time.time()
            self.total_calls   += 1
            return self.total_calls

    @property
    def calls(self) -> int:
        with self.lock:
            return self.total_calls


class ProgressTracker:
    """Thread-safe progress tracker with disk persistence."""

    def __init__(self, filepath: str):
        self.filepath  = filepath
        self.lock      = threading.Lock()
        self.completed = self._load()

    def _load(self) -> set:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    return set(tuple(x) for x in json.load(f))
            except Exception:
                return set()
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


class StrikesCache:
    """Persist discovered strikes per expiry date."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock     = threading.Lock()
        self.cache    = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r") as f:
                    return json.load(f)
            except Exception:
                return {}
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
#  Main Downloader Class
# ─────────────────────────────────────────────────────────────

class BreezeDownloader:
    """
    Downloads NIFTY/BANKNIFTY options historical data via ICICI Breeze API.

    config keys:
        api_key, api_secret, api_session
        instrument          : "NIFTY" | "BANKNIFTY"
        interval            : "1minute" | "1second"
        from_date           : date
        to_date             : date
        out_dir             : str
        strike_discovery_range : int   (default 3000)
        max_workers         : int      (default 20)
        calls_per_minute    : float    (default 90)
        max_retries         : int      (default 8)
        download_spot       : bool     (default True)
        chunk_minutes       : int      (default 15, 1sec only)
    """

    MARKET_OPEN  = (9,  15)
    MARKET_CLOSE = (15, 30)
    STRIKE_STEP  = 50

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

        self.breeze        = None
        self.rate_limiter  = None
        self.progress      = None
        self.strikes_cache = None
        self._print_lock   = threading.Lock()

    # ── Connection ───────────────────────────────────────────

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

    # ── Internal Helpers ─────────────────────────────────────

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

    def _day_bounds(self, d: date) -> tuple[datetime, datetime]:
        o = datetime(d.year, d.month, d.day, *self.MARKET_OPEN,  0)
        c = datetime(d.year, d.month, d.day, *self.MARKET_CLOSE, 0)
        return o, c

    def _is_transient(self, msg: str) -> bool:
        m = msg.lower()
        return any(p in m for p in [
            "connection reset", "connection aborted", "read timed out", "timeout",
            "temporarily unavailable", "429", "too many requests",
            "502", "503", "504", "bad gateway", "ssl", "handshake",
            "connection refused", "broken pipe", "remote end closed", "connection error",
        ])

    def _safe_call(self, fn_name: str, **kwargs):
        """Rate-limited Breeze API call with exponential-backoff retries."""
        fn          = getattr(self.breeze, fn_name)
        max_retries = self.config.get("max_retries", 8)

        for attempt in range(max_retries):
            if self.stop_event.is_set():
                raise InterruptedError("Stopped by user")
            try:
                self.rate_limiter.wait(self.stop_event)
                if self.stop_event.is_set():
                    raise InterruptedError("Stopped by user")
                return fn(**kwargs)
            except InterruptedError:
                raise
            except Exception as e:
                if attempt >= max_retries - 1 or not self._is_transient(str(e)):
                    raise
                sleep = (2.0 * (2 ** attempt)) + random.uniform(0, 1.0)
                with self._print_lock:
                    self.log(f"    ⚠️ Retry {attempt+1}/{max_retries}: {str(e)[:80]}")
                self.stop_event.wait(timeout=sleep)

    def _get_time_chunks(self, d: date, chunk_min: int = 15) -> list:
        open_, close_ = self._day_bounds(d)
        chunks = []
        start = open_
        while start < close_:
            end = min(start + timedelta(minutes=chunk_min), close_)
            chunks.append((start, end))
            start = end
        return chunks

    def _update_stats(self, **kwargs):
        for k, v in kwargs.items():
            self._stats_fn(k, v)

    # ── Spot Data ────────────────────────────────────────────

    def _get_spot_1min(self, d: date) -> pd.DataFrame | None:
        open_, close_ = self._day_bounds(d)
        try:
            r = self._safe_call(
                "get_historical_data_v2",
                interval="1minute",
                from_date=self._iso_z(open_),
                to_date=self._iso_z(close_),
                stock_code=self.config["instrument"],
                exchange_code="NSE",
                product_type="cash",
                expiry_date="", right="", strike_price="",
            )
            rows = r.get("Success") or []
            if not rows:
                return None
            df = pd.DataFrame(rows)
            if "datetime" in df.columns:
                df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
            return df
        except InterruptedError:
            raise
        except Exception as e:
            with self._print_lock:
                self.log(f"    ⚠️ Spot 1min error: {str(e)[:80]}")
            return None

    def _get_spot_close(self, df: pd.DataFrame) -> float | None:
        for col in ("close", "Close"):
            if col in df.columns and not df[col].empty:
                try:
                    return float(df[col].iloc[-1])
                except Exception:
                    pass
        return None

    # ── VIX Download ─────────────────────────────────────────

    def _download_vix_day(self, d: date, out_dir: str) -> int:
        """
        Download India VIX data for one trading day.
        Returns row count saved, -1 if already exists, 0 if no data.
        """
        self._ensure_dir(out_dir)
        out_csv = os.path.join(out_dir, f"{d.isoformat()}.csv")
        if os.path.exists(out_csv):
            return -1

        interval  = self.config["interval"]
        open_, close_ = self._day_bounds(d)
        all_data  = []

        if interval == "1minute":
            try:
                r = self._safe_call(
                    "get_historical_data_v2",
                    interval="1minute",
                    from_date=self._iso_z(open_),
                    to_date=self._iso_z(close_),
                    stock_code="INDIAVIX",
                    exchange_code="NSE",
                    product_type="cash",
                    expiry_date="", right="", strike_price="",
                )
                all_data = r.get("Success") or []
            except InterruptedError:
                raise
            except Exception as e:
                self.log(f"   ⚠️ VIX error: {str(e)[:60]}")
                return 0
        else:
            # 1-second: chunked
            chunk_min = self.config.get("chunk_minutes", 15)
            for cs, ce in self._get_time_chunks(d, chunk_min):
                if self.stop_event.is_set():
                    raise InterruptedError("Stopped by user")
                try:
                    r = self._safe_call(
                        "get_historical_data_v2",
                        interval="1second",
                        from_date=self._iso_z(cs),
                        to_date=self._iso_z(ce),
                        stock_code="INDIAVIX",
                        exchange_code="NSE",
                        product_type="cash",
                        expiry_date="", right="", strike_price="",
                    )
                    all_data.extend(r.get("Success") or [])
                except InterruptedError:
                    raise
                except Exception:
                    pass

        if not all_data:
            return 0

        df = pd.DataFrame(all_data)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(out_csv, index=False)
        return len(df)

    # ── Expiry Discovery ─────────────────────────────────────

    def _candidate_expiries(self, d: date) -> list[date]:
        """Return candidate expiry dates to probe (next 8 weeks of Thu/Wed/Tue)."""
        candidates = set()
        for delta in range(0, 56):
            candidate = d + timedelta(days=delta)
            if candidate.weekday() in (1, 2, 3):  # Tue, Wed, Thu
                candidates.add(candidate)
        return sorted(candidates)

    def _pick_expiry(self, d: date, atm: int) -> date | None:
        """Find the nearest valid weekly expiry by probing the API."""
        open_, _ = self._day_bounds(d)
        probe_to = open_ + timedelta(minutes=30)

        for expiry in self._candidate_expiries(d):
            if self.stop_event.is_set():
                return None
            exp_str = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
            try:
                r = self._safe_call(
                    "get_historical_data_v2",
                    interval="1minute",
                    from_date=self._iso_z(open_),
                    to_date=self._iso_z(probe_to),
                    stock_code=self.config["instrument"],
                    exchange_code="NFO",
                    product_type="options",
                    expiry_date=exp_str,
                    right="call",
                    strike_price=str(atm),
                )
                if r.get("Success"):
                    return expiry
            except InterruptedError:
                raise
            except Exception:
                continue
        return None

    # ── Strike Discovery ─────────────────────────────────────

    def _probe_strike(self, d: date, expiry: date, strike: int) -> bool:
        exp_str = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        open_, _ = self._day_bounds(d)
        try:
            r = self._safe_call(
                "get_historical_data_v2",
                interval="1minute",
                from_date=self._iso_z(open_),
                to_date=self._iso_z(open_ + timedelta(minutes=30)),
                stock_code=self.config["instrument"],
                exchange_code="NFO",
                product_type="options",
                expiry_date=exp_str,
                right="call",
                strike_price=str(strike),
            )
            return bool(r.get("Success"))
        except InterruptedError:
            raise
        except Exception:
            return False

    def _discover_strikes(
        self, d: date, expiry: date, atm: int, cache: StrikesCache
    ) -> list[int]:
        cached = cache.get(expiry)
        if cached:
            self.log(f"    📂 Cached strikes for {expiry}: {len(cached)} strikes")
            return cached

        self.log(f"    🔍 Discovering strikes for {expiry}...")
        disc  = self.config.get("strike_discovery_range", 3000)
        step  = self.STRIKE_STEP
        avail = set()

        def scan_direction(start, stop, direction):
            bound = start
            for s in range(start, stop, direction * step * 5):
                if self.stop_event.is_set():
                    return bound
                if self._probe_strike(d, expiry, s):
                    bound = s
                    avail.add(s)
                else:
                    found = False
                    for s2 in range(s + direction * step, s + direction * step * 10, direction * step):
                        if (direction > 0 and s2 > atm + disc) or (direction < 0 and s2 < atm - disc):
                            break
                        if self._probe_strike(d, expiry, s2):
                            found = True
                            bound = s2
                            avail.add(s2)
                            break
                    if not found:
                        break
            return bound

        upper = scan_direction(atm, atm + disc + 1, +1)
        lower = scan_direction(atm, atm - disc - 1, -1)
        self.log(f"    📏 Bounds: {lower} – {upper}")

        # Phase 2: fill every strike in range (threaded)
        all_strikes = list(range(lower, upper + 1, step))
        workers     = self.config.get("max_workers", 20)

        with ThreadPoolExecutor(max_workers=min(workers, 20)) as ex:
            futures = {ex.submit(self._probe_strike, d, expiry, s): s
                       for s in all_strikes if s not in avail}
            for fut in as_completed(futures):
                if self.stop_event.is_set():
                    break
                s = futures[fut]
                try:
                    if fut.result():
                        avail.add(s)
                except Exception:
                    pass

        result = sorted(avail)
        if result:
            self.log(f"    ✅ Found {len(result)} strikes: {result[0]} – {result[-1]}")
            cache.set(expiry, result)
        return result

    # ── Per-Strike Download ───────────────────────────────────

    def _download_strike_1min(
        self, d: date, expiry: date, strike: int, right: str,
        out_dir: str, progress: ProgressTracker,
    ) -> dict:
        key = (d.isoformat(), str(strike), right, "1minute")
        if progress.is_done(key):
            return {"skipped": 1, "files": 0, "rows": 0}

        out_csv = f"{out_dir}/{d.isoformat()}_{strike}_{right[0].upper()}E.csv"
        if os.path.exists(out_csv):
            progress.mark_done(key)
            return {"skipped": 1, "files": 0, "rows": 0}

        exp_str       = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        open_, close_ = self._day_bounds(d)

        try:
            r = self._safe_call(
                "get_historical_data_v2",
                interval="1minute",
                from_date=self._iso_z(open_),
                to_date=self._iso_z(close_),
                stock_code=self.config["instrument"],
                exchange_code="NFO",
                product_type="options",
                expiry_date=exp_str,
                right=right,
                strike_price=str(strike),
            )
            rows = r.get("Success") or []
        except InterruptedError:
            raise
        except Exception as e:
            with self._print_lock:
                self.log(f"    ⚠️ {strike}{right[0].upper()}E: {str(e)[:60]}")
            return {"skipped": 0, "files": 0, "rows": 0}

        if not rows:
            progress.mark_done(key)
            return {"skipped": 0, "files": 0, "rows": 0}

        df = pd.DataFrame(rows)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(out_csv, index=False)
        progress.mark_done(key)
        return {"skipped": 0, "files": 1, "rows": len(df)}

    def _download_strike_1sec(
        self, d: date, expiry: date, strike: int, right: str,
        out_dir: str, progress: ProgressTracker,
    ) -> dict:
        key = (d.isoformat(), str(strike), right, "1second")
        if progress.is_done(key):
            return {"skipped": 1, "files": 0, "rows": 0}

        out_csv = f"{out_dir}/{d.isoformat()}_{strike}_{right[0].upper()}E.csv"
        if os.path.exists(out_csv):
            progress.mark_done(key)
            return {"skipped": 1, "files": 0, "rows": 0}

        exp_str   = self._iso_z(datetime(expiry.year, expiry.month, expiry.day, 7, 0, 0))
        chunk_min = self.config.get("chunk_minutes", 15)
        chunks    = self._get_time_chunks(d, chunk_min)
        all_data  = []

        for chunk_start, chunk_end in chunks:
            if self.stop_event.is_set():
                raise InterruptedError("Stopped by user")
            try:
                r = self._safe_call(
                    "get_historical_data_v2",
                    interval="1second",
                    from_date=self._iso_z(chunk_start),
                    to_date=self._iso_z(chunk_end),
                    stock_code=self.config["instrument"],
                    exchange_code="NFO",
                    product_type="options",
                    expiry_date=exp_str,
                    right=right,
                    strike_price=str(strike),
                )
                rows = r.get("Success") or []
                if rows:
                    all_data.extend(rows)
            except InterruptedError:
                raise
            except Exception as e:
                with self._print_lock:
                    self.log(f"    ⚠️ {strike}{right[0].upper()}E chunk: {str(e)[:50]}")

        if not all_data:
            progress.mark_done(key)
            return {"skipped": 0, "files": 0, "rows": 0}

        df = pd.DataFrame(all_data)
        if "datetime" in df.columns:
            df.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
        df.to_csv(out_csv, index=False)
        progress.mark_done(key)
        return {"skipped": 0, "files": 1, "rows": len(df)}

    # ── Day Processing ───────────────────────────────────────

    def _interval_label(self) -> str:
        return "1MIN" if self.config["interval"] == "1minute" else "1SEC"

    def _process_day(
        self, d: date, expiry: date, strikes: list[int], progress: ProgressTracker
    ) -> dict:
        inst    = self.config["instrument"]
        out_dir = os.path.join(
            self.config["out_dir"],
            f"{inst}_OPTIONS_{self._interval_label()}",
            expiry.isoformat(),
        )
        self._ensure_dir(out_dir)

        tasks       = [(s, r) for s in strikes if s > 0 for r in ("call", "put")]
        total       = {"skipped": 0, "files": 0, "rows": 0}
        done        = 0
        workers     = self.config.get("max_workers", 20)
        download_fn = (
            self._download_strike_1min
            if self.config["interval"] == "1minute"
            else self._download_strike_1sec
        )

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(download_fn, d, expiry, s, r, out_dir, progress): (s, r)
                for s, r in tasks
            }
            for fut in as_completed(futures):
                if self.stop_event.is_set():
                    break
                s, r = futures[fut]
                try:
                    stats = fut.result()
                    total["skipped"] += stats["skipped"]
                    total["files"]   += stats["files"]
                    total["rows"]    += stats["rows"]
                    if stats["files"] > 0:
                        with self._print_lock:
                            self.log(f"      💾 {s}{r[0].upper()}E: {stats['rows']:,} rows")
                except InterruptedError:
                    break
                except Exception as e:
                    with self._print_lock:
                        self.log(f"      ❌ {s}{r[0].upper()}E failed: {str(e)[:60]}")

                done += 1
                if done % 20 == 0:
                    with self._print_lock:
                        self.log(f"      ⏳ {done}/{len(tasks)} | API calls: {self.rate_limiter.calls}")

        return total

    # ── Main Run ─────────────────────────────────────────────

    def run(self):
        cfg     = self.config
        out_dir = cfg["out_dir"]
        self._ensure_dir(out_dir)

        self.rate_limiter  = RateLimiter(cfg.get("calls_per_minute", 90))
        self.progress      = ProgressTracker(os.path.join(out_dir, ".progress.json"))
        self.strikes_cache = StrikesCache(os.path.join(out_dir, ".strikes_cache.json"))

        if self.progress.count() > 0:
            self.log(f"📂 Resuming — {self.progress.count()} items already completed")

        from_d = cfg["from_date"]
        to_d   = cfg["to_date"]
        inst   = cfg["instrument"]

        self.log("─" * 60)
        self.log(f"🎯  Instrument : {inst}")
        self.log(f"⏱️   Interval   : {cfg['interval']}")
        self.log(f"📅  Date range  : {from_d} → {to_d}")
        self.log(f"📂  Output      : {out_dir}")
        self.log(f"🔧  Workers     : {cfg.get('max_workers', 20)}")
        self.log(f"⚡  API limit   : {cfg.get('calls_per_minute', 90)}/min")
        self.log(f"📈  VIX         : {'Yes' if cfg.get('download_vix') else 'No'}")
        self.log("─" * 60)

        totals = {"days": 0, "files": 0, "skipped": 0, "rows": 0}

        for d in self._daterange(from_d, to_d):
            if self.stop_event.is_set():
                self.log("⚠️ Stopped by user")
                break

            self.log(f"\n📅 {d.strftime('%A, %d %b %Y')}")

            # 1. Spot 1-min — used as trading-day check + ATM source
            spot_df = self._get_spot_1min(d)
            if spot_df is None or spot_df.empty:
                self.log("   ⏭️  No spot data (holiday / weekend)")
                continue

            spot_close = self._get_spot_close(spot_df)
            if not spot_close or spot_close <= 0:
                self.log("   ⏭️  Invalid spot close — skipping")
                continue

            # 2. Save spot CSV (reuse already-fetched data for 1min)
            if cfg.get("download_spot", True):
                spot_dir = os.path.join(out_dir, f"{inst}_SPOT_{self._interval_label()}")
                self._ensure_dir(spot_dir)
                spot_csv = os.path.join(spot_dir, f"{d.isoformat()}.csv")
                if not os.path.exists(spot_csv):
                    if cfg["interval"] == "1minute":
                        spot_df.to_csv(spot_csv, index=False)
                        self.log(f"   💾 Spot saved: {len(spot_df)} rows")
                    else:
                        # 1sec spot: download separately
                        self.log(f"   📥 Downloading 1sec spot...")
                        spot_data = []
                        for cs, ce in self._get_time_chunks(d, cfg.get("chunk_minutes", 15)):
                            if self.stop_event.is_set(): break
                            try:
                                r = self._safe_call(
                                    "get_historical_data_v2",
                                    interval="1second",
                                    from_date=self._iso_z(cs),
                                    to_date=self._iso_z(ce),
                                    stock_code=inst, exchange_code="NSE",
                                    product_type="cash",
                                    expiry_date="", right="", strike_price="",
                                )
                                spot_data.extend(r.get("Success") or [])
                            except InterruptedError:
                                raise
                            except Exception:
                                pass
                        if spot_data:
                            sdf = pd.DataFrame(spot_data)
                            if "datetime" in sdf.columns:
                                sdf.drop_duplicates(subset=["datetime"], keep="first", inplace=True)
                            sdf.to_csv(spot_csv, index=False)
                            self.log(f"   💾 Spot saved: {len(sdf):,} rows")
                else:
                    self.log("   ⏭️  Spot already exists")

            # 2b. VIX download
            if cfg.get("download_vix", False):
                vix_dir = os.path.join(out_dir, f"INDIAVIX_{self._interval_label()}")
                rows = self._download_vix_day(d, vix_dir)
                if rows > 0:
                    self.log(f"   📈 VIX saved: {rows:,} rows")
                elif rows == -1:
                    self.log("   ⏭️  VIX already exists")
                else:
                    self.log("   ⚠️  VIX — no data returned")

            # 3. ATM + Expiry
            atm    = self._round_step(spot_close, self.STRIKE_STEP)
            expiry = self._pick_expiry(d, atm)
            if not expiry:
                self.log("   ⏭️  Could not find valid expiry — skipping")
                continue

            self.log(f"   📍 Spot={spot_close:.2f} | ATM={atm} | Expiry={expiry}")

            # 4. Strike discovery
            strikes = self._discover_strikes(d, expiry, atm, self.strikes_cache)
            if not strikes:
                self.log("   ⏭️  No strikes found — skipping")
                continue

            self.log(f"   📊 Downloading {len(strikes)} strikes × 2 sides "
                     f"({len(strikes) * 2} files)...")

            # 5. Download
            day_stats = self._process_day(d, expiry, strikes, self.progress)
            totals["days"]    += 1
            totals["files"]   += day_stats["files"]
            totals["skipped"] += day_stats["skipped"]
            totals["rows"]    += day_stats["rows"]
            self.progress.save()

            self._update_stats(
                days=totals["days"],
                files=totals["files"],
                rows=totals["rows"],
                api_calls=self.rate_limiter.calls,
            )

            self.log(
                f"   ✅ {day_stats['files']} new | "
                f"{day_stats['skipped']} skipped | "
                f"{day_stats['rows']:,} rows | "
                f"API: {self.rate_limiter.calls}"
            )

        self.log(f"\n{'=' * 60}")
        self.log(f"✅ Download complete!")
        self.log(f"   Days processed : {totals['days']}")
        self.log(f"   Files created  : {totals['files']:,}")
        self.log(f"   Files skipped  : {totals['skipped']:,}")
        self.log(f"   Total rows     : {totals['rows']:,}")
        self.log(f"   Total API calls: {self.rate_limiter.calls}")
        self.progress.save()
