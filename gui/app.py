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
        self._log_queue       = queue.Queue()
        self._download_thread = None
        self._saved_config    = self._load_config()
        self._totals          = {"days": 0, "files": 0, "rows": 0, "api_calls": 0}

        self._build_ui()
        self._populate_fields()
        self._poll_log()

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
        for name in ("🔐  Auth", "⚙️  Config", "📥  Download"):
            self.tabs.add(name)

        self._build_auth_tab(self.tabs.tab("🔐  Auth"))
        self._build_config_tab(self.tabs.tab("⚙️  Config"))
        self._build_download_tab(self.tabs.tab("📥  Download"))

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
        self.var_workers     = ctk.DoubleVar(value=20)
        self.var_cpm         = ctk.DoubleVar(value=90)
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
        slider_row(parent, self.var_workers, 1, 50, 49).grid(
            row=7, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # CPM
        lbl("API Calls / Min", 8)
        slider_row(parent, self.var_cpm, 10, 200, 190).grid(
            row=8, column=1, padx=(0, 24), pady=(14, 4), sticky="w")

        # Chunk minutes (1sec only)
        self._lbl_chunk = ctk.CTkLabel(
            parent, text="Chunk Size (min)", font=ctk.CTkFont(size=12, weight="bold"),
            anchor="w",
        )
        self._lbl_chunk.grid(row=9, column=0, padx=(24, 12), pady=(14, 4), sticky="w")
        self._frame_chunk = slider_row(parent, self.var_chunk_min, 5, 60, 11)
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
