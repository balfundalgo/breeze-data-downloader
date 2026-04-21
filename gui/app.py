"""
Balfund · Breeze Data Downloader GUI
CustomTkinter dark-themed desktop app.
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import threading
import queue
import json
import os
import webbrowser
import urllib.parse
from datetime import date, datetime

from core.downloader import BreezeDownloader
from core.stock_downloader import StockDownloader, get_security_master

CONFIG_FILE = "breeze_downloader_config.json"

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# ── Colour tokens ──────────────────────────────────────────────────────────────
C_BG_HEADER  = "#0f1117"
C_BG_STATS   = "#111827"
C_BG_LOG     = "#0a0a14"
C_ACCENT     = "#4fc3f7"
C_DIM        = "#78909c"
C_GREEN      = "#66bb6a"
C_ORANGE     = "#ffa726"
C_RED        = "#ef5350"
C_BTN_GREEN  = "#1b5e20"
C_BTN_GHOVER = "#2e7d32"
C_BTN_RED    = "#7f1d1d"
C_BTN_RHOVER = "#991b1b"


class BreezeDownloaderApp(ctk.CTk):
    """Main application window."""

    def __init__(self):
        super().__init__()
        self.title("Balfund · Breeze Data Downloader")
        self.geometry("960x720")
        self.minsize(820, 600)

        self._stop_event      = threading.Event()
        self._stop_stock_event = threading.Event()
        self._log_queue       = queue.Queue()
        self._stock_log_queue = queue.Queue()
        self._download_thread = None
        self._stock_thread    = None
        self._saved_config    = self._load_config()
        self._totals          = {"days": 0, "files": 0, "rows": 0, "api_calls": 0}

        self._build_ui()
        self._populate_fields()
        self._poll_log()
        # Pre-load stock list in background so search is ready immediately
        threading.Thread(target=self._load_stock_list, daemon=True).start()

    # ── Config persistence ────────────────────────────────────────────────────

    def _load_config(self) -> dict:
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_config(self):
        data = {
            "api_key":           self.var_api_key.get(),
            "api_secret":        self.var_api_secret.get(),
            "instrument":        self.var_instrument.get(),
            "interval":          self.var_interval.get(),
            "from_date":         self.var_from_date.get(),
            "to_date":           self.var_to_date.get(),
            "out_dir":           self.var_out_dir.get(),
            "strike_range":      int(self.var_strike_range.get()),
            "max_workers":       int(self.var_workers.get()),
            "calls_per_minute":  int(self.var_cpm.get()),
            "download_spot":     bool(self.var_spot.get()),
            "download_vix":      bool(self.var_vix.get()),
            "chunk_minutes":     int(self.var_chunk_min.get()),
        }
        with open(CONFIG_FILE, "w") as f:
            json.dump(data, f, indent=2)
        self._log("💾 Settings saved")

    def _populate_fields(self):
        c = self._saved_config
        if "api_key"          in c: self.var_api_key.set(c["api_key"])
        if "api_secret"       in c: self.var_api_secret.set(c["api_secret"])
        if "instrument"       in c: self.var_instrument.set(c["instrument"])
        if "interval"         in c: self.var_interval.set(c["interval"])
        if "from_date"        in c: self.var_from_date.set(c["from_date"])
        if "to_date"          in c: self.var_to_date.set(c["to_date"])
        if "out_dir"          in c: self.var_out_dir.set(c["out_dir"])
        if "strike_range"     in c: self.var_strike_range.set(c["strike_range"])
        if "max_workers"      in c: self.var_workers.set(c["max_workers"])
        if "calls_per_minute" in c: self.var_cpm.set(c["calls_per_minute"])
        if "download_spot"    in c: self.var_spot.set(c["download_spot"])
        if "download_vix"     in c: self.var_vix.set(c["download_vix"])
        if "chunk_minutes"    in c: self.var_chunk_min.set(c["chunk_minutes"])
        # Trigger chunk visibility
        self._on_interval_change(self.var_interval.get())

    # ── UI Build ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # ── Header bar ────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=C_BG_HEADER, height=56, corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_propagate(False)

        ctk.CTkLabel(
            hdr,
            text="  🏦  Breeze Data Downloader",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color=C_ACCENT,
        ).place(relx=0, rely=0.5, anchor="w", x=10)

        ctk.CTkLabel(
            hdr,
            text="ICICI Breeze  ·  NIFTY / BANKNIFTY / Stocks",
            font=ctk.CTkFont(size=11),
            text_color=C_DIM,
        ).place(relx=1, rely=0.5, anchor="e", x=-14)

        # ── Tab view ──────────────────────────────────────────
        self.tabs = ctk.CTkTabview(self, corner_radius=8)
        self.tabs.grid(row=1, column=0, sticky="nsew", padx=10, pady=(6, 0))
        for name in ("🔐  Auth", "⚙️  Config", "📥  Download", "📈  Stocks"):
            self.tabs.add(name)

        self._build_auth_tab(self.tabs.tab("🔐  Auth"))
        self._build_config_tab(self.tabs.tab("⚙️  Config"))
        self._build_download_tab(self.tabs.tab("📥  Download"))
        self._build_stocks_tab(self.tabs.tab("📈  Stocks"))

        # ── Status bar ────────────────────────────────────────
        sb = ctk.CTkFrame(self, height=26, corner_radius=0, fg_color=C_BG_HEADER)
        sb.grid(row=2, column=0, sticky="ew")
        sb.grid_propagate(False)
        self._lbl_status = ctk.CTkLabel(
            sb, text="● Ready", text_color=C_GREEN, font=ctk.CTkFont(size=11)
        )
        self._lbl_status.place(relx=0, rely=0.5, anchor="w", x=10)
        self._lbl_api = ctk.CTkLabel(
            sb, text="", text_color=C_DIM, font=ctk.CTkFont(size=11)
        )
        self._lbl_api.place(relx=1, rely=0.5, anchor="e", x=-10)

    # ── Auth Tab ──────────────────────────────────────────────────────────────

    def _build_auth_tab(self, parent):
        parent.grid_columnconfigure(1, weight=1)

        self.var_api_key     = ctk.StringVar()
        self.var_api_secret  = ctk.StringVar()
        self.var_api_session = ctk.StringVar()

        fields = [
            ("API Key",       self.var_api_key,     False, "App key from ICICI developer portal"),
            ("API Secret",    self.var_api_secret,  True,  "Secret key — never share this"),
            ("Session Token", self.var_api_session, False, "Generated daily via login URL below"),
        ]

        for i, (label, var, hide, hint) in enumerate(fields):
            r = i * 2
            ctk.CTkLabel(
                parent, text=label, font=ctk.CTkFont(size=13, weight="bold"), anchor="w"
            ).grid(row=r, column=0, padx=(24, 12), pady=(20, 2), sticky="w")

            ctk.CTkEntry(
                parent, textvariable=var, width=420, height=38,
                show="●" if hide else "",
                font=ctk.CTkFont(size=13),
            ).grid(row=r, column=1, padx=(0, 24), pady=(20, 2), sticky="ew")

            ctk.CTkLabel(
                parent, text=hint, font=ctk.CTkFont(size=10), text_color=C_DIM, anchor="w"
            ).grid(row=r + 1, column=1, padx=(0, 24), pady=(0, 4), sticky="w")

        # Info box
        info = ctk.CTkTextbox(
            parent, height=80, font=ctk.CTkFont(size=11),
            fg_color="#161b27", text_color=C_DIM, corner_radius=6,
        )
        info.grid(row=6, column=0, columnspan=2, padx=24, pady=(14, 6), sticky="ew")
        info.insert(
            "0.0",
            "ℹ️  How to get a Session Token:\n"
            "  1. Click 'Open Login URL' — a browser tab will open.\n"
            "  2. Log in with your ICICI credentials + TOTP.\n"
            "  3. Copy the session_token value from the redirect URL and paste it above.",
        )
        info.configure(state="disabled")

        # Button row
        bf = ctk.CTkFrame(parent, fg_color="transparent")
        bf.grid(row=7, column=0, columnspan=2, padx=24, pady=(6, 20), sticky="w")

        ctk.CTkButton(bf, text="🌐  Open Login URL", width=160,
                       command=self._open_login_url).pack(side="left", padx=(0, 10))
        ctk.CTkButton(bf, text="🔌  Test Connection", width=160,
                       command=self._test_connection).pack(side="left", padx=(0, 10))
        ctk.CTkButton(bf, text="💾  Save Settings", width=140,
                       command=self._save_config).pack(side="left")

    # ── Config Tab ────────────────────────────────────────────────────────────

    def _build_config_tab(self, parent):
        parent.grid_columnconfigure(1, weight=1)

        self.var_instrument  = ctk.StringVar(value="NIFTY")
        self.var_interval    = ctk.StringVar(value="1minute")
        self.var_from_date   = ctk.StringVar(value="2024-01-01")
        self.var_to_date     = ctk.StringVar(value=date.today().isoformat())
        self.var_out_dir     = ctk.StringVar(value="breeze_data")
        self.var_strike_range = ctk.DoubleVar(value=3000)
        self.var_workers     = ctk.DoubleVar(value=100)
        self.var_cpm         = ctk.DoubleVar(value=300)
        self.var_spot        = ctk.BooleanVar(value=True)
        self.var_vix         = ctk.BooleanVar(value=False)
        self.var_chunk_min   = ctk.DoubleVar(value=15)

        def lbl(text, row, col=0, **kw):
            ctk.CTkLabel(
                parent, text=text,
                font=ctk.CTkFont(size=12, weight="bold"), anchor="w", **kw
            ).grid(row=row, column=col, padx=(24, 12), pady=(14, 4), sticky="w")

        def slider_row(parent_widget, var, from_, to, steps, width=220):
            f = ctk.CTkFrame(parent_widget, fg_color="transparent")
            sl = ctk.CTkSlider(f, from_=from_, to=to, variable=var,
                                number_of_steps=steps, width=width)
            sl.pack(side="left")
            ctk.CTkLabel(f, textvariable=var, width=52,
                          font=ctk.CTkFont(size=12)).pack(side="left", padx=(8, 0))
            return f

        # Instrument
        lbl("Instrument", 0)
        ctk.CTkSegmentedButton(
            parent, values=["NIFTY", "BANKNIFTY"],
            variable=self.var_instrument, width=240,
        ).grid(row=0, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # Interval
        lbl("Timeframe", 1)
        ctk.CTkSegmentedButton(
            parent, values=["1minute", "1second"],
            variable=self.var_interval, width=240,
            command=self._on_interval_change,
        ).grid(row=1, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # From date
        lbl("From Date", 2)
        ctk.CTkEntry(
            parent, textvariable=self.var_from_date,
            width=180, placeholder_text="YYYY-MM-DD",
        ).grid(row=2, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # To date
        lbl("To Date", 3)
        ctk.CTkEntry(
            parent, textvariable=self.var_to_date,
            width=180, placeholder_text="YYYY-MM-DD",
        ).grid(row=3, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # Output dir
        lbl("Output Dir", 4)
        drf = ctk.CTkFrame(parent, fg_color="transparent")
        drf.grid(row=4, column=1, padx=(0, 24), pady=(14, 4), sticky="ew")
        ctk.CTkEntry(drf, textvariable=self.var_out_dir, width=300).pack(side="left")
        ctk.CTkButton(drf, text="Browse…", width=80,
                       command=self._browse_dir).pack(side="left", padx=(8, 0))

        # Divider
        sep = ctk.CTkFrame(parent, height=1, fg_color="#2a2a3a")
        sep.grid(row=5, column=0, columnspan=2, padx=24, pady=(16, 4), sticky="ew")
        ctk.CTkLabel(
            parent, text="  Advanced Settings  ", font=ctk.CTkFont(size=10),
            text_color=C_DIM,
        ).grid(row=5, column=0, columnspan=2, padx=24, pady=(16, 4))

        # Strike range
        lbl("Strike Range (±pts)", 6)
        slider_row(parent, self.var_strike_range, 500, 6000, 110).grid(
            row=6, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # Workers
        lbl("Max Workers", 7)
        slider_row(parent, self.var_workers, 1, 200, 199).grid(
            row=7, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # CPM
        lbl("API Calls / Min", 8)
        slider_row(parent, self.var_cpm, 10, 500, 490).grid(
            row=8, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # Chunk minutes (1sec only)
        self._lbl_chunk = ctk.CTkLabel(
            parent, text="Chunk Size (min)\n⚠️ max 15 for 1sec", font=ctk.CTkFont(size=12, weight="bold"),
            anchor="w",
        )
        self._lbl_chunk.grid(row=9, column=0, padx=(24, 12), pady=(14, 4), sticky="w")
        self._frame_chunk = slider_row(parent, self.var_chunk_min, 5, 15, 10)
        self._frame_chunk.grid(row=9, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # Spot toggle
        ctk.CTkCheckBox(
            parent, text="Download Spot Data alongside options",
            variable=self.var_spot, font=ctk.CTkFont(size=12),
        ).grid(row=10, column=0, columnspan=2, padx=24, pady=(16, 4), sticky="w")

        # VIX toggle
        ctk.CTkCheckBox(
            parent,
            text="Download India VIX  (saved to INDIAVIX_1MIN / INDIAVIX_1SEC folder)",
            variable=self.var_vix, font=ctk.CTkFont(size=12),
        ).grid(row=11, column=0, columnspan=2, padx=24, pady=(4, 16), sticky="w")

    # ── Download Tab ──────────────────────────────────────────────────────────

    def _build_download_tab(self, parent):
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(2, weight=1)

        # Stats strip
        stats_frame = ctk.CTkFrame(
            parent, fg_color=C_BG_STATS, corner_radius=8, height=74
        )
        stats_frame.grid(row=0, column=0, padx=8, pady=(8, 4), sticky="ew")
        stats_frame.grid_propagate(False)
        stats_frame.grid_columnconfigure((0, 1, 2, 3), weight=1)

        self._stat_vars = {
            "days":      ctk.StringVar(value="0"),
            "files":     ctk.StringVar(value="0"),
            "rows":      ctk.StringVar(value="0"),
            "api_calls": ctk.StringVar(value="0"),
        }
        stat_defs = [
            ("📅 Days Processed", "days"),
            ("💾 Files Created",  "files"),
            ("📊 Total Rows",     "rows"),
            ("🔌 API Calls",      "api_calls"),
        ]
        for col, (label, key) in enumerate(stat_defs):
            f = ctk.CTkFrame(stats_frame, fg_color="transparent")
            f.grid(row=0, column=col, padx=6, pady=8)
            ctk.CTkLabel(f, text=label, font=ctk.CTkFont(size=10), text_color=C_DIM).pack()
            ctk.CTkLabel(
                f, textvariable=self._stat_vars[key],
                font=ctk.CTkFont(size=20, weight="bold"), text_color=C_ACCENT,
            ).pack()

        # Button row
        bf = ctk.CTkFrame(parent, fg_color="transparent")
        bf.grid(row=1, column=0, padx=8, pady=4, sticky="ew")

        self._btn_start = ctk.CTkButton(
            bf, text="▶  Start Download", width=180, height=42,
            fg_color=C_BTN_GREEN, hover_color=C_BTN_GHOVER,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._start_download,
        )
        self._btn_start.pack(side="left", padx=(0, 8))

        self._btn_stop = ctk.CTkButton(
            bf, text="⏹  Stop", width=110, height=42,
            fg_color=C_BTN_RED, hover_color=C_BTN_RHOVER,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._stop_download, state="disabled",
        )
        self._btn_stop.pack(side="left", padx=(0, 8))

        ctk.CTkButton(
            bf, text="🗑  Clear Log", width=110, height=42,
            command=self._clear_log,
        ).pack(side="left", padx=(0, 8))

        ctk.CTkButton(
            bf, text="📂  Open Folder", width=120, height=42,
            command=self._open_out_dir,
        ).pack(side="left")

        # Log area
        self._log_text = ctk.CTkTextbox(
            parent,
            font=ctk.CTkFont(family="Courier", size=11),
            fg_color=C_BG_LOG, text_color="#d4d4d4",
            corner_radius=8, wrap="word",
        )
        self._log_text.grid(row=2, column=0, padx=8, pady=(4, 8), sticky="nsew")

    # ── Auth Actions ──────────────────────────────────────────────────────────

    def _open_login_url(self):
        key = self.var_api_key.get().strip()
        if not key:
            messagebox.showwarning("Missing", "Enter your API Key first.")
            return
        url = ("https://api.icicidirect.com/apiuser/login?api_key="
               + urllib.parse.quote(key, safe=""))
        webbrowser.open(url)
        self._log("🌐 Login URL opened in browser")
        self._log("   After login, copy the 'session_token' from the redirect URL.")

    def _test_connection(self):
        key     = self.var_api_key.get().strip()
        secret  = self.var_api_secret.get().strip()
        session = self.var_api_session.get().strip()
        if not all([key, secret, session]):
            messagebox.showwarning("Missing Fields", "Fill in API Key, Secret, and Session Token.")
            return
        self._log("🔌 Testing connection...")
        self._set_status("Connecting...", C_ORANGE)

        def _test():
            try:
                from breeze_connect import BreezeConnect
                b = BreezeConnect(api_key=key)
                b.generate_session(api_secret=secret, session_token=session)
                self._log_queue.put("✅ Connection successful!")
                self.after(0, lambda: self._set_status("Connected ✅", C_GREEN))
            except Exception as e:
                self._log_queue.put(f"❌ Connection failed: {e}")
                self.after(0, lambda: self._set_status("Connection failed ❌", C_RED))

        threading.Thread(target=_test, daemon=True).start()

    # ── Config Actions ────────────────────────────────────────────────────────

    def _browse_dir(self):
        d = filedialog.askdirectory(title="Select Output Directory")
        if d:
            self.var_out_dir.set(d)

    def _on_interval_change(self, val: str):
        is_1sec = val == "1second"
        color   = "#e0e0e0" if is_1sec else C_DIM
        try:
            self._lbl_chunk.configure(text_color=color)
        except Exception:
            pass

    def _open_out_dir(self):
        d = self.var_out_dir.get().strip()
        if d and os.path.isdir(d):
            os.startfile(d) if os.name == "nt" else os.system(f'open "{d}"')
        else:
            messagebox.showinfo("Not Found", "Output directory does not exist yet.")

    # ── Download Control ──────────────────────────────────────────────────────

    def _validate(self) -> bool:
        errors = []
        if not self.var_api_key.get().strip():     errors.append("• API Key is required")
        if not self.var_api_secret.get().strip():  errors.append("• API Secret is required")
        if not self.var_api_session.get().strip(): errors.append("• Session Token is required")
        if not self.var_out_dir.get().strip():     errors.append("• Output directory is required")
        for field, name in [(self.var_from_date, "From Date"), (self.var_to_date, "To Date")]:
            try:
                date.fromisoformat(field.get())
            except ValueError:
                errors.append(f"• {name} must be YYYY-MM-DD")
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return False
        return True

    def _start_download(self):
        if not self._validate():
            return
        self._save_config()
        self._stop_event.clear()

        # Reset stats
        for k in self._stat_vars:
            self._stat_vars[k].set("0")
        self._totals = {"days": 0, "files": 0, "rows": 0, "api_calls": 0}

        self._btn_start.configure(state="disabled")
        self._btn_stop.configure(state="normal")
        self._set_status("Downloading…", C_ORANGE)
        self.tabs.set("📥  Download")

        config = {
            "api_key":              self.var_api_key.get().strip(),
            "api_secret":           self.var_api_secret.get().strip(),
            "api_session":          self.var_api_session.get().strip(),
            "instrument":           self.var_instrument.get(),
            "interval":             self.var_interval.get(),
            "from_date":            date.fromisoformat(self.var_from_date.get()),
            "to_date":              date.fromisoformat(self.var_to_date.get()),
            "out_dir":              self.var_out_dir.get().strip(),
            "strike_discovery_range": int(self.var_strike_range.get()),
            "max_workers":          int(self.var_workers.get()),
            "calls_per_minute":     float(self.var_cpm.get()),
            "download_spot":        bool(self.var_spot.get()),
            "download_vix":         bool(self.var_vix.get()),
            "chunk_minutes":        int(self.var_chunk_min.get()),
        }

        def _run():
            dl = BreezeDownloader(
                config,
                log_fn=self._log_queue.put,
                stats_fn=self._update_stat,
                stop_event=self._stop_event,
            )
            if dl.connect():
                dl.run()
            self.after(0, self._on_download_done)

        self._download_thread = threading.Thread(target=_run, daemon=True)
        self._download_thread.start()

    def _stop_download(self):
        self._stop_event.set()
        self._log_queue.put("⚠️ Stop signal sent — waiting for current tasks to finish…")
        self._btn_stop.configure(state="disabled")
        self._set_status("Stopping…", C_ORANGE)

    def _on_download_done(self):
        self._btn_start.configure(state="normal")
        self._btn_stop.configure(state="disabled")
        if self._stop_event.is_set():
            self._set_status("Stopped", C_DIM)
        else:
            self._set_status("Download complete ✅", C_GREEN)

    # ── Log ───────────────────────────────────────────────────────────────────

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self._log_text.configure(state="normal")
        self._log_text.insert("end", f"[{ts}]  {msg}\n")
        self._log_text.see("end")
        self._log_text.configure(state="disabled")

    def _poll_log(self):
        """Drain log queue and schedule next poll."""
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self._log(msg)
        except Exception:
            pass
        # Also drain stock log
        try:
            while True:
                msg = self._stock_log_queue.get_nowait()
                self._stock_log(msg)
        except Exception:
            pass
        self.after(100, self._poll_log)

    def _clear_log(self):
        self._log_text.configure(state="normal")
        self._log_text.delete("0.0", "end")
        self._log_text.configure(state="disabled")

    # ── Stats ─────────────────────────────────────────────────────────────────

    def _update_stat(self, key: str, value: int):
        def _do():
            if key in self._stat_vars:
                self._stat_vars[key].set(f"{value:,}")
            if key == "api_calls":
                self._lbl_api.configure(text=f"API calls: {value:,}  ")
        self.after(0, _do)

    # ── Status bar ────────────────────────────────────────────────────────────

    def _set_status(self, text: str, color: str = "#e0e0e0"):
        self._lbl_status.configure(text=f"● {text}", text_color=color)

    # ═══════════════════════════════════════════════════════════════════════════
    # STOCKS TAB
    # ═══════════════════════════════════════════════════════════════════════════

    def _build_stocks_tab(self, parent):
        """
        Two-panel layout:
          Left  (300px) — live-search stock list
          Right (flex)  — all config, stats, buttons, log
        """
        parent.grid_columnconfigure(0, weight=0)   # left panel fixed
        parent.grid_columnconfigure(1, weight=1)   # right panel expands
        parent.grid_rowconfigure(0, weight=1)

        # ── Variables ─────────────────────────────────────────
        self.var_stock_code     = ctk.StringVar()
        self.var_stock_search   = ctk.StringVar()
        self.var_stock_interval = ctk.StringVar(value="1minute")
        self.var_stock_from     = ctk.StringVar(value="2024-01-01")
        self.var_stock_to       = ctk.StringVar(value=date.today().isoformat())
        self.var_stock_out_dir  = ctk.StringVar(value="breeze_data/stocks")
        self.var_stock_spot     = ctk.BooleanVar(value=True)
        self.var_stock_futures  = ctk.BooleanVar(value=False)
        self.var_stock_options  = ctk.BooleanVar(value=False)
        self.var_stock_workers  = ctk.DoubleVar(value=20)
        self.var_stock_cpm      = ctk.DoubleVar(value=90)
        self.var_stock_chunk    = ctk.DoubleVar(value=15)
        self._stock_list        = []
        self._stock_btns        = []

        # ═══════════════════════════════════════════════════════
        # LEFT PANEL — stock search
        # ═══════════════════════════════════════════════════════
        left = ctk.CTkFrame(parent, fg_color="#111827", corner_radius=8, width=290)
        left.grid(row=0, column=0, padx=(8, 4), pady=8, sticky="nsew")
        left.grid_propagate(False)
        left.grid_columnconfigure(0, weight=1)
        left.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(
            left, text="🔍  Select Stock",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=C_ACCENT,
        ).grid(row=0, column=0, padx=12, pady=(12, 6), sticky="w")

        # Search entry
        self._stock_search_entry = ctk.CTkEntry(
            left, textvariable=self.var_stock_search,
            placeholder_text="Type name or code…",
            font=ctk.CTkFont(size=12), height=34,
        )
        self._stock_search_entry.grid(row=1, column=0, padx=10, pady=(0, 6), sticky="ew")

        # Live results list
        self._stock_results_frame = ctk.CTkScrollableFrame(
            left, fg_color="#0d1117", corner_radius=6,
            scrollbar_button_color="#1e293b",
        )
        self._stock_results_frame.grid(row=2, column=0, padx=10, pady=(0, 6), sticky="nsew")
        self._stock_results_frame.grid_columnconfigure(0, weight=1)

        # Selected stock display
        self._lbl_selected = ctk.CTkLabel(
            left, text="No stock selected",
            font=ctk.CTkFont(size=11), text_color=C_DIM,
            wraplength=260, justify="left",
        )
        self._lbl_selected.grid(row=3, column=0, padx=12, pady=(4, 12), sticky="w")

        # Wire up live search
        self.var_stock_search.trace_add("write", lambda *_: self._refresh_stock_list())

        # ═══════════════════════════════════════════════════════
        # RIGHT PANEL — config + stats + log
        # ═══════════════════════════════════════════════════════
        right = ctk.CTkFrame(parent, fg_color="transparent")
        right.grid(row=0, column=1, padx=(0, 8), pady=8, sticky="nsew")
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(3, weight=1)

        # ── Config card ───────────────────────────────────────
        cfg = ctk.CTkFrame(right, fg_color="#111827", corner_radius=8)
        cfg.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        cfg.grid_columnconfigure(1, weight=1)

        def lbl(text, row):
            ctk.CTkLabel(cfg, text=text,
                          font=ctk.CTkFont(size=12, weight="bold"), anchor="w"
                          ).grid(row=row, column=0, padx=(14, 8), pady=(10, 4), sticky="w")

        # Interval
        lbl("Interval", 0)
        ctk.CTkSegmentedButton(
            cfg,
            values=["1second", "1minute", "5minute", "30minute", "1day"],
            variable=self.var_stock_interval,
        ).grid(row=0, column=1, padx=(0, 14), pady=(10, 4), sticky="ew")

        # From date
        lbl("From Date", 1)
        ctk.CTkEntry(cfg, textvariable=self.var_stock_from,
                      width=160, placeholder_text="YYYY-MM-DD",
                      ).grid(row=1, column=1, padx=(0, 14), pady=(6, 4), sticky="w")

        # To date
        lbl("To Date", 2)
        ctk.CTkEntry(cfg, textvariable=self.var_stock_to,
                      width=160, placeholder_text="YYYY-MM-DD",
                      ).grid(row=2, column=1, padx=(0, 14), pady=(6, 4), sticky="w")

        # Output dir
        lbl("Output Dir", 3)
        od = ctk.CTkFrame(cfg, fg_color="transparent")
        od.grid(row=3, column=1, padx=(0, 14), pady=(6, 4), sticky="ew")
        ctk.CTkEntry(od, textvariable=self.var_stock_out_dir).pack(side="left", fill="x", expand=True)
        ctk.CTkButton(od, text="Browse…", width=76,
                       command=self._stock_browse_dir).pack(side="left", padx=(6, 0))

        # Products
        lbl("Products", 4)
        pf = ctk.CTkFrame(cfg, fg_color="transparent")
        pf.grid(row=4, column=1, padx=(0, 14), pady=(6, 4), sticky="w")
        ctk.CTkCheckBox(pf, text="Spot", variable=self.var_stock_spot,
                         font=ctk.CTkFont(size=12)).pack(side="left", padx=(0, 16))
        ctk.CTkCheckBox(pf, text="Futures (auto expiry)",
                         variable=self.var_stock_futures,
                         font=ctk.CTkFont(size=12)).pack(side="left", padx=(0, 16))
        ctk.CTkCheckBox(pf, text="Options (auto expiry + all strikes)",
                         variable=self.var_stock_options,
                         font=ctk.CTkFont(size=12)).pack(side="left")

        # Advanced sliders
        sf = ctk.CTkFrame(cfg, fg_color="transparent")
        sf.grid(row=5, column=0, columnspan=2, padx=14, pady=(4, 12), sticky="ew")

        def mini_slider(parent, label, var, lo, hi, steps):
            f = ctk.CTkFrame(parent, fg_color="transparent")
            ctk.CTkLabel(f, text=label, font=ctk.CTkFont(size=10),
                          text_color=C_DIM).pack(anchor="w")
            r = ctk.CTkFrame(f, fg_color="transparent")
            r.pack(fill="x")
            ctk.CTkSlider(r, from_=lo, to=hi, variable=var,
                           number_of_steps=steps, width=150).pack(side="left")
            ctk.CTkLabel(r, textvariable=var, width=42,
                          font=ctk.CTkFont(size=10)).pack(side="left", padx=(5, 0))
            return f

        mini_slider(sf, "Workers", self.var_stock_workers, 1, 50, 49
                    ).pack(side="left", padx=(0, 20))
        mini_slider(sf, "API Calls/Min", self.var_stock_cpm, 10, 200, 190
                    ).pack(side="left", padx=(0, 20))
        mini_slider(sf, "Chunk Min (1sec)", self.var_stock_chunk, 5, 60, 11
                    ).pack(side="left")

        # ── Stats strip ───────────────────────────────────────
        self._stock_stat_vars = {
            "days":      ctk.StringVar(value="0"),
            "files":     ctk.StringVar(value="0"),
            "rows":      ctk.StringVar(value="0"),
            "api_calls": ctk.StringVar(value="0"),
        }
        sf2 = ctk.CTkFrame(right, fg_color=C_BG_STATS, corner_radius=8, height=58)
        sf2.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        sf2.grid_propagate(False)
        sf2.grid_columnconfigure((0,1,2,3), weight=1)
        for col, (label, key) in enumerate([
            ("📅 Days","days"),("💾 Files","files"),
            ("📊 Rows","rows"),("🔌 API","api_calls"),
        ]):
            f = ctk.CTkFrame(sf2, fg_color="transparent")
            f.grid(row=0, column=col, padx=4, pady=4)
            ctk.CTkLabel(f, text=label, font=ctk.CTkFont(size=9),
                          text_color=C_DIM).pack()
            ctk.CTkLabel(f, textvariable=self._stock_stat_vars[key],
                          font=ctk.CTkFont(size=16, weight="bold"),
                          text_color=C_ACCENT).pack()

        # ── Buttons ───────────────────────────────────────────
        bf = ctk.CTkFrame(right, fg_color="transparent")
        bf.grid(row=2, column=0, sticky="ew", pady=(0, 4))

        self._btn_stock_start = ctk.CTkButton(
            bf, text="▶  Start Download", width=170, height=40,
            fg_color=C_BTN_GREEN, hover_color=C_BTN_GHOVER,
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self._start_stock_download,
        )
        self._btn_stock_start.pack(side="left", padx=(0, 6))

        self._btn_stock_stop = ctk.CTkButton(
            bf, text="⏹  Stop", width=100, height=40,
            fg_color=C_BTN_RED, hover_color=C_BTN_RHOVER,
            font=ctk.CTkFont(size=13, weight="bold"),
            command=self._stop_stock_download, state="disabled",
        )
        self._btn_stock_stop.pack(side="left", padx=(0, 6))

        ctk.CTkButton(bf, text="🗑 Clear Log", width=100, height=40,
                       command=self._clear_stock_log).pack(side="left", padx=(0, 6))
        ctk.CTkButton(bf, text="📂 Open Folder", width=110, height=40,
                       command=self._open_stock_dir).pack(side="left")

        # ── Log ───────────────────────────────────────────────
        self._stock_log_text = ctk.CTkTextbox(
            right, font=ctk.CTkFont(family="Courier", size=11),
            fg_color=C_BG_LOG, text_color="#d4d4d4",
            corner_radius=8, wrap="word",
        )
        self._stock_log_text.grid(row=3, column=0, sticky="nsew")

    # ── Live stock search ─────────────────────────────────────────────────────

    def _refresh_stock_list(self):
        """Re-render the stock results list based on current search query."""
        # Destroy ALL children of the scrollable frame — not just tracked ones.
        # This fixes the "Loading…" label staying behind and blocking results.
        try:
            # CTkScrollableFrame keeps user widgets in _scrollable_frame
            inner = getattr(self._stock_results_frame, "_scrollable_frame",
                            self._stock_results_frame)
            for w in inner.winfo_children():
                w.destroy()
        except Exception:
            # Fallback: destroy tracked buttons only
            for b in self._stock_btns:
                try:
                    b.destroy()
                except Exception:
                    pass
        self._stock_btns.clear()

        q = self.var_stock_search.get().strip().upper()

        if not self._stock_list:
            lbl = ctk.CTkLabel(
                self._stock_results_frame,
                text="Loading stocks…", font=ctk.CTkFont(size=11),
                text_color=C_DIM,
            )
            lbl.grid(row=0, column=0, padx=8, pady=8)
            self._stock_btns.append(lbl)   # track it so next refresh clears it
            return

        if not q:
            # Show first 60 when empty
            results = self._stock_list[:60]
        else:
            words = q.split()   # support "TATA CONS" → ["TATA", "CONS"]

            def matches(code, name):
                code_u = code.upper()
                name_u = name.upper()
                # All words must appear somewhere in code OR name
                return all(
                    w in code_u or w in name_u
                    for w in words
                )

            # Exact code prefix first
            exact   = [(c, n) for c, n in self._stock_list
                       if c.upper().startswith(q)]
            # Then full-word matches
            partial = [(c, n) for c, n in self._stock_list
                       if not c.upper().startswith(q) and matches(c, n)]
            results = (exact + partial)[:80]

        for i, (code, name) in enumerate(results):
            is_selected = (code == self.var_stock_code.get())
            b = ctk.CTkButton(
                self._stock_results_frame,
                text=f"{code:<10} {name[:28]}",
                font=ctk.CTkFont(family="Courier", size=11),
                anchor="w", height=26,
                fg_color="#1e3a5f" if is_selected else "transparent",
                hover_color="#1e293b",
                command=lambda c=code, n=name: self._select_stock(c, n),
            )
            b.grid(row=i, column=0, sticky="ew", pady=1, padx=2)
            self._stock_btns.append(b)

        if not results and q:
            lbl = ctk.CTkLabel(
                self._stock_results_frame,
                text=f'No results for "{q}"',
                font=ctk.CTkFont(size=11), text_color=C_DIM,
            )
            lbl.grid(row=0, column=0, padx=8, pady=12)
            self._stock_btns.append(lbl)

        # Update count in the selected label if nothing selected yet
        if not self.var_stock_code.get():
            count = len(results)
            hint = f"showing {count}" if q else f"{len(self._stock_list):,} stocks — type to filter"
            self._lbl_selected.configure(text=hint, text_color=C_DIM)

    def _select_stock(self, code: str, name: str):
        self.var_stock_code.set(code)
        self._lbl_selected.configure(
            text=f"✅  {code}  —  {name}", text_color=C_GREEN
        )
        self._refresh_stock_list()  # re-render to highlight selected

        # ── Load stock list from Security Master ──────────────────────────────────

    def _load_stock_list(self):
        try:
            sm = get_security_master()
            sm.load(log_fn=self._stock_log_queue.put)
            df = sm.eq_stocks()
            # Sort: code alphabetically
            self._stock_list = list(
                zip(df["ShortName"].tolist(), df["CompanyName"].tolist())
            )
            self._stock_log_queue.put(
                f"✅ {len(self._stock_list)} NSE stocks loaded — type to search"
            )
            # Refresh the results list on the main thread
            self.after(0, self._refresh_stock_list)
        except Exception as e:
            self._stock_log_queue.put(f"❌ Failed to load stocks: {e}")

    # ── Stock download control ────────────────────────────────────────────────

    def _validate_stock(self) -> bool:
        errors = []
        if not self.var_api_key.get().strip():     errors.append("• API Key missing (Auth tab)")
        if not self.var_api_secret.get().strip():  errors.append("• API Secret missing (Auth tab)")
        if not self.var_api_session.get().strip(): errors.append("• Session Token missing (Auth tab)")
        if not self.var_stock_code.get().strip():  errors.append("• Stock code is required")
        if not self.var_stock_out_dir.get().strip(): errors.append("• Output directory required")
        if not any([self.var_stock_spot.get(),
                    self.var_stock_futures.get(),
                    self.var_stock_options.get()]):
            errors.append("• Select at least one product (Spot / Futures / Options)")
        for field, name in [(self.var_stock_from, "From Date"),
                            (self.var_stock_to, "To Date")]:
            try:
                date.fromisoformat(field.get())
            except ValueError:
                errors.append(f"• {name} must be YYYY-MM-DD")
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return False
        return True

    def _start_stock_download(self):
        if not self._validate_stock():
            return

        self._stop_stock_event.clear()

        for k in self._stock_stat_vars:
            self._stock_stat_vars[k].set("0")

        self._btn_stock_start.configure(state="disabled")
        self._btn_stock_stop.configure(state="normal")
        self._set_status("Downloading stocks…", C_ORANGE)
        self.tabs.set("📈  Stocks")

        config = {
            "api_key":           self.var_api_key.get().strip(),
            "api_secret":        self.var_api_secret.get().strip(),
            "api_session":       self.var_api_session.get().strip(),
            "stock_code":        self.var_stock_code.get().strip().upper(),
            "interval":          self.var_stock_interval.get(),
            "from_date":         date.fromisoformat(self.var_stock_from.get()),
            "to_date":           date.fromisoformat(self.var_stock_to.get()),
            "out_dir":           self.var_stock_out_dir.get().strip(),
            "download_spot":     bool(self.var_stock_spot.get()),
            "download_futures":  bool(self.var_stock_futures.get()),
            "download_options":  bool(self.var_stock_options.get()),
            "max_workers":       int(self.var_stock_workers.get()),
            "calls_per_minute":  float(self.var_stock_cpm.get()),
            "chunk_minutes":     int(self.var_stock_chunk.get()),
        }

        def _run():
            dl = StockDownloader(
                config,
                log_fn=self._stock_log_queue.put,
                stats_fn=self._update_stock_stat,
                stop_event=self._stop_stock_event,
            )
            if dl.connect():
                dl.run()
            self.after(0, self._on_stock_download_done)

        self._stock_thread = threading.Thread(target=_run, daemon=True)
        self._stock_thread.start()

    def _stop_stock_download(self):
        self._stop_stock_event.set()
        self._stock_log_queue.put("⚠️ Stop signal sent…")
        self._btn_stock_stop.configure(state="disabled")
        self._set_status("Stopping…", C_ORANGE)

    def _on_stock_download_done(self):
        self._btn_stock_start.configure(state="normal")
        self._btn_stock_stop.configure(state="disabled")
        if self._stop_stock_event.is_set():
            self._set_status("Stopped", C_DIM)
        else:
            self._set_status("Stock download complete ✅", C_GREEN)

    # ── Stock log ─────────────────────────────────────────────────────────────

    def _stock_log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self._stock_log_text.configure(state="normal")
        self._stock_log_text.insert("end", f"[{ts}]  {msg}\n")
        self._stock_log_text.see("end")
        self._stock_log_text.configure(state="disabled")

    def _clear_stock_log(self):
        self._stock_log_text.configure(state="normal")
        self._stock_log_text.delete("0.0", "end")
        self._stock_log_text.configure(state="disabled")

    # ── Stock helpers ─────────────────────────────────────────────────────────

    def _stock_browse_dir(self):
        d = filedialog.askdirectory(title="Select Output Directory")
        if d:
            self.var_stock_out_dir.set(d)

    def _open_stock_dir(self):
        d = self.var_stock_out_dir.get().strip()
        if d and os.path.isdir(d):
            os.startfile(d) if os.name == "nt" else os.system(f'open "{d}"')
        else:
            messagebox.showinfo("Not Found", "Output directory does not exist yet.")

    def _update_stock_stat(self, key: str, value: int):
        def _do():
            if key in self._stock_stat_vars:
                self._stock_stat_vars[key].set(f"{value:,}")
            if key == "api_calls":
                self._lbl_api.configure(text=f"API calls: {value:,}  ")
        self.after(0, _do)
