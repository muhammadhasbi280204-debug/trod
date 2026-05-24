"""
Bot Scalping v14 — ADAPTIVE MOMENTUM ENGINE 🎯🧠
==================================================

PERUBAHAN dari v14 original → v14-ADAPTIVE:
─────────────────────────────────────────────
✅ Self-Learning Engine terintegrasi penuh
✅ AdaptiveParamEngine — auto-adjust parameter setiap 10 trade
✅ SignalQualityLearner — blacklist sinyal combo WR < 30%
✅ ForceCloseAnalyzer — detect pola force-close per jam/BTC/symbol
✅ DynamicScoreThreshold — naikkan min_score saat losing streak
✅ HoldingTimeOptimizer — cari sweet spot durasi hold dari distribusi PnL
✅ calc_atr_levels_adaptive — gunakan multiplier yang sudah diadaptasi
✅ manage_positions pakai max_holding dari adaptive params
✅ record_trade otomatis dipanggil di close_trade & partial_close_tp1

Root cause dari stats jelek (WR:55%, 101 Force, TP2:0, Exp:-0.0043U):
  → Force close 74%  : bot masuk tapi market tidak bergerak cukup 5m
  → TP2 = 0          : trail terlalu ketat, cut profit terlalu awal
  → SL:26 vs TP1:6   : SL terlalu sempit / entry direction sering salah
  Semua diperbaiki secara otomatis oleh Adaptive Engine.
"""

import os, time, math, json, threading, queue, re
import requests
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# ══════════════════════════════════════════════════════════
# TESTNET / REAL — pilih salah satu
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
# Untuk real: comment baris atas
# ══════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════
#  CONFIG v14-ADAPTIVE
# ════════════════════════════════════════════════════

# ── CORE ─────────────────────────────────────────────────
LEVERAGE              = 20
ORDER_USDT            = 2
MAX_POSITIONS         = 3

# ── ATR MULTIPLIER (base — akan di-override adaptive) ────
ATR_SL_MULT           = 1.2
ATR_TP1_MULT          = 2.0
ATR_TP2_MULT          = 3.5
ATR_TRAIL_MULT        = 0.8
ATR_TRAIL_TIGHT_MULT  = 0.5

# Batas atas/bawah ATR-based SL
MIN_SL_PCT            = 0.0010
MAX_SL_PCT            = 0.0060
MIN_TP1_PCT           = 0.0020
MAX_TP2_PCT           = 0.0200

# ── DELAYED TRAIL ────────────────────────────────────────
TRAIL_ACTIVATE_PCT    = 0.0020
TRAIL_BE_PCT          = 0.0005
TRAIL_TIGHT_PCT       = 0.0040

# Partial close
TP1_CLOSE_RATIO       = 0.60
TP2_CLOSE_RATIO       = 0.40

# ── INSTANT CUT ──────────────────────────────────────────
INSTANT_CUT_MULT      = 0.5
INSTANT_CUT_WINDOW    = 3

# ── CHOP / REGIME FILTER ─────────────────────────────────
CHOP_INDEX_THRESHOLD  = 58.0
MIN_BB_WIDTH_PCT      = 0.005
MAX_EMA_CROSS_FREQ    = 3
MIN_ADX               = 20

# ── CONTINUATION CONFIRMATION ────────────────────────────
CONFIRM_CANDLES       = 1

# ── SPREAD FILTER ────────────────────────────────────────
MAX_SPREAD_RATIO      = 0.30

# ── MOMENTUM FILTER (base — akan di-override adaptive) ───
MIN_MOMENTUM_PCT      = 0.0018
MIN_VOL_SURGE         = 1.6
MIN_TREND_CANDLES     = 3

# ── KECEPATAN ────────────────────────────────────────────
SCAN_INTERVAL         = 3
POSITION_MONITOR_SEC  = 1
SCAN_DELAY_MS         = 0.050
BATCH_SIZE            = 15
MAX_HOLDING_MIN       = 5
SYMBOL_COOLDOWN_SEC   = 10
RE_SCAN_DELAY_SEC     = 0.3

# ── SESSION FILTER ───────────────────────────────────────
BAD_HOURS_UTC         = {4, 5, 6}
BAD_HOURS_MIN_SCORE   = 60

# ── KILL SWITCH ──────────────────────────────────────────
DAILY_LOSS_LIMIT      = -5.0
CONSEC_LOSS_MAX       = 5
CONSEC_LOSS_PAUSE_MIN = 30
MAX_API_LAG_SEC       = 3.0

# ── CACHE ────────────────────────────────────────────────
OHLCV_CACHE_TTL_1M    = 2
OHLCV_CACHE_TTL_3M    = 4
OHLCV_CACHE_TTL_5M    = 5
OHLCV_CACHE_TTL_15M   = 30
OHLCV_CACHE_TTL_1H    = 1800
TICKER24H_TTL         = 8
FUNDING_TTL           = 30
TOP_MOVERS_TTL        = 8

# ── FILTER (base — akan di-override adaptive) ────────────
MIN_SCORE             = 45
MIN_ENTRY_SIGNALS     = 2
MIN_FNG               = 15
MAX_FNG_LONG          = 92
MIN_BREADTH           = 0.0
MAX_SL_ATR_PCT        = 0.010

# ── SYMBOLS ──────────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    "MATICUSDT","LTCUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT",
    "SUIUSDT","TIAUSDT","AAVEUSDT","RUNEUSDT","FILUSDT",
    "1000PEPEUSDT","WIFUSDT","JUPUSDT","SEIUSDT","PYTHUSDT",
    "FETUSDT","RENDERUSDT","WLDUSDT","STRKUSDT","ALTUSDT",
    "DYMUSDT","PIXELUSDT","ACEUSDT","MANTAUSDT","ZETAUSDT",
    "RONINUSDT","NOTUSDT","DOGSUSDT","EIGENUSDT","CATIUSDT",
    "1000BONKUSDT","PORTALUSDT",
    "CRVUSDT","MKRUSDT","COMPUSDT","SUSHIUSDT",
    "SNXUSDT","1INCHUSDT","BALUSDT","DYDXUSDT",
    "GMXUSDT","PENDLEUSDT","JTOUSDT","RAYUSDT",
    "ALGOUSDT","ICPUSDT","FTMUSDT","HBARUSDT","FLOWUSDT",
    "EGLDUSDT","THETAUSDT","KAVAUSDT","BANDUSDT",
    "SKLUSDT","CELRUSDT","CTSIUSDT",
    "AXSUSDT","SANDUSDT","MANAUSDT","ENJUSDT","GALAUSDT",
    "IMXUSDT","BLURUSDT","MASKUSDT","HIGHUSDT",
    "BEAMXUSDT","MEMEUSDT","ORDIUSDT",
    "ARUSDT","OCEANUSDT","TRUUSDT","POLYXUSDT","BLZUSDT",
    "SHIBUSDT","FLOKIUSDT","BONKUSDT","JASMYUSDT",
    "LUNCUSDT","CFXUSDT","COMBOUSDT","AGLDUSDT","IDUSDT","GASUSDT",
    "STXUSDT","KASUSDT","TONUSDT","TAOUSDT","ONDOUSDT",
    "ENARUSDT","WUSDT","BOMEUSDT","SAFEUSDT",
    "VANRYUSDT","XAIUSDT","ATAUSDT",
    "MOVRUSDT","CKBUSDT","NMRUSDT","HOOKUSDT",
    "GLMRUSDT","AMBUSDT","RENUSDT","CVCUSDT","VOXELUSDT",
    "PERPUSDT","LITUSDT","UNFIUSDT","DENTUSDT",
    "HOTUSDT","IOSTUSDT","OGNUSDT","LINAUSDT","SFPUSDT",
    "1000XECUSDT","BNTUSDT","FLMUSDT","TLMUSDT",
]


# ════════════════════════════════════════════════════
#  STATE GLOBAL
# ════════════════════════════════════════════════════
open_positions      = {}
trade_log           = []
_ohlcv_cache        = {}
_sym_info           = {}
_sym_cooldown       = {}
_btc_price_history  = deque(maxlen=300)
_scan_batch_idx     = 0
_lock               = threading.Lock()
_executor           = ThreadPoolExecutor(max_workers=15)
_rescan_queue       = queue.Queue()
_hot_symbols        = deque(maxlen=30)
_ticker24h_cache    = {}
_ticker24h_ts       = 0
_funding_cache      = {}
_funding_ts         = 0
_top_movers         = []
_top_movers_ts      = 0

_kill_switch = {
    "active":           False,
    "reason":           "",
    "resume_time":      0,
    "consec_losses":    0,
    "daily_pnl":        0.0,
    "daily_reset_ts":   0,
    "last_api_check":   0,
    "api_lag":          0.0,
}

_perf        = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "trades": 0})
_perf_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})

_macro = {
    "fng": 50, "fng_label": "Neutral",
    "btc_trend_1m":  "UNKNOWN",
    "btc_trend_5m":  "UNKNOWN",
    "btc_trend_15m": "UNKNOWN",
    "btc_trend_1h":  "UNKNOWN",
    "market_breadth": 0.5,
    "news": "neutral",
    "scalp_mode": "TREND",
    "last_fng": 0, "last_btc": 0, "last_breadth": 0, "last_news": 0,
}

_stats = {
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl": 0.0,
    "best_trade": 0.0,
    "worst_trade": 0.0,
    "tp1_hits": 0,
    "tp2_hits": 0,
    "sl_hits": 0,
    "instant_cuts": 0,
    "force_closes": 0,
    "rescans": 0,
    "skipped_no_momentum": 0,
    "skipped_chop": 0,
    "skipped_spread": 0,
    "skipped_session": 0,
    "skipped_mean_rev": 0,
    "skipped_adaptive": 0,
    "pnl_history": deque(maxlen=200),
    "session_start": time.time(),
}

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}


# ════════════════════════════════════════════════════
#  ███████  ADAPTIVE ENGINE  ███████
# ════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:      str
    side:        str
    entry:       float
    exit_price:  float
    pnl:         float
    hold_sec:    int
    reason:      str
    signals:     list
    score:       int
    scalp_mode:  str
    btc_trend:   str
    mom_pct:     float
    atr_pct:     float
    fng:         int
    hour_utc:    int
    ts:          float = field(default_factory=time.time)

    @property
    def win(self):          return self.pnl > 0
    @property
    def force_closed(self): return "Force" in self.reason
    @property
    def sl_hit(self):       return "SL" in self.reason or "Stop" in self.reason


@dataclass
class AdaptedParams:
    min_score:           int   = 45
    min_entry_signals:   int   = 2
    min_momentum_pct:    float = 0.0018
    min_vol_surge:       float = 1.6
    min_trend_candles:   int   = 3
    atr_sl_mult:         float = 1.2
    atr_tp1_mult:        float = 2.0
    atr_tp2_mult:        float = 3.5
    atr_trail_mult:      float = 0.8
    atr_trail_tight:     float = 0.5
    max_holding_min:     float = 5.0
    trail_activate_pct:  float = 0.0020
    trend_min_score:     int   = 45
    mean_rev_enabled:    bool  = False
    blacklisted_signals: list  = field(default_factory=list)
    symbol_cooldowns:    dict  = field(default_factory=dict)


class PerformanceTracker:
    WINDOW = 50

    def __init__(self):
        self.all_trades: deque = deque(maxlen=500)
        self.recent:     deque = deque(maxlen=self.WINDOW)
        self._lock = threading.Lock()

    def add(self, trade: TradeRecord):
        with self._lock:
            self.all_trades.append(trade)
            self.recent.append(trade)

    def rolling_stats(self) -> dict:
        with self._lock:
            trades = list(self.recent)
        if not trades:
            return {"n": 0, "wr": 0.5, "expectancy": 0, "avg_hold": 0,
                    "force_rate": 0, "sl_rate": 0, "avg_pnl": 0}
        n        = len(trades)
        wins     = [t for t in trades if t.win]
        losses   = [t for t in trades if not t.win]
        forces   = [t for t in trades if t.force_closed]
        sl_hits  = [t for t in trades if t.sl_hit]
        wr       = len(wins) / n
        avg_w    = sum(t.pnl for t in wins)   / len(wins)   if wins   else 0
        avg_l    = abs(sum(t.pnl for t in losses) / len(losses)) if losses else 0
        exp      = (wr * avg_w) - ((1 - wr) * avg_l)
        return {
            "n":          n,
            "wr":         round(wr, 3),
            "expectancy": round(exp, 5),
            "avg_hold":   round(sum(t.hold_sec for t in trades) / n, 1),
            "force_rate": round(len(forces) / n, 3),
            "sl_rate":    round(len(sl_hits) / n, 3),
            "avg_pnl":    round(sum(t.pnl for t in trades) / n, 5),
        }

    def stats_by_signal_combo(self) -> dict:
        with self._lock:
            trades = list(self.all_trades)
        by_combo = defaultdict(list)
        for t in trades:
            if not t.signals: continue
            sig_types = frozenset(_normalize_signal(s) for s in t.signals)
            key       = "|".join(sorted(sig_types))
            by_combo[key].append(t.pnl)
        result = {}
        for combo, pnls in by_combo.items():
            n  = len(pnls)
            if n < 3: continue
            wr = sum(1 for p in pnls if p > 0) / n
            result[combo] = {"n": n, "wr": round(wr, 2), "avg": round(sum(pnls)/n, 5)}
        return result

    def stats_by_regime(self) -> dict:
        with self._lock:
            trades = list(self.all_trades)
        by_regime = defaultdict(list)
        for t in trades:
            by_regime[t.scalp_mode].append(t.pnl)
        result = {}
        for regime, pnls in by_regime.items():
            n  = len(pnls)
            wr = sum(1 for p in pnls if p > 0) / n if n else 0
            result[regime] = {"n": n, "wr": round(wr, 2), "avg": round(sum(pnls)/n, 5)}
        return result

    def holding_time_distribution(self) -> dict:
        with self._lock:
            trades = list(self.all_trades)
        buckets = {"0-60s": [], "60-120s": [], "120-180s": [],
                   "180-240s": [], "240-300s": [], "300s+": []}
        for t in trades:
            s = t.hold_sec
            if s < 60:    buckets["0-60s"].append(t.pnl)
            elif s < 120: buckets["60-120s"].append(t.pnl)
            elif s < 180: buckets["120-180s"].append(t.pnl)
            elif s < 240: buckets["180-240s"].append(t.pnl)
            elif s < 300: buckets["240-300s"].append(t.pnl)
            else:         buckets["300s+"].append(t.pnl)
        result = {}
        for bucket, pnls in buckets.items():
            if not pnls: continue
            n  = len(pnls)
            wr = sum(1 for p in pnls if p > 0) / n
            result[bucket] = {"n": n, "wr": round(wr, 2), "avg": round(sum(pnls)/n, 5)}
        return result

    def force_close_patterns(self) -> dict:
        with self._lock:
            trades = list(self.all_trades)
        force_trades = [t for t in trades if t.force_closed]
        if len(force_trades) < 5:
            return {}
        patterns = {}
        by_hour = defaultdict(list)
        for t in force_trades:
            by_hour[t.hour_utc].append(t.pnl)
        patterns["bad_hours"] = [
            h for h, pnls in by_hour.items()
            if len(pnls) >= 3 and sum(1 for p in pnls if p > 0) / len(pnls) < 0.35
        ]
        by_btc = defaultdict(list)
        for t in force_trades:
            by_btc[t.btc_trend].append(t.pnl)
        patterns["bad_btc_trends"] = [
            b for b, pnls in by_btc.items()
            if len(pnls) >= 3 and sum(1 for p in pnls if p > 0) / len(pnls) < 0.35
        ]
        by_sym = defaultdict(list)
        for t in force_trades:
            by_sym[t.symbol].append(t.pnl)
        patterns["bad_symbols_force"] = [
            s for s, pnls in by_sym.items()
            if len(pnls) >= 3 and sum(1 for p in pnls if p > 0) / len(pnls) < 0.30
        ]
        return patterns


def _normalize_signal(s: str) -> str:
    return re.sub(r'[\d\.\+\-\%\:x]', '', s).strip()


class AdaptiveParamEngine:
    LIMITS = {
        "min_score":          (40, 72),
        "min_momentum_pct":   (0.0010, 0.0040),
        "min_vol_surge":      (1.2, 2.5),
        "atr_sl_mult":        (0.8, 2.2),
        "atr_tp1_mult":       (1.5, 3.0),
        "atr_tp2_mult":       (2.5, 5.0),
        "atr_trail_mult":     (0.4, 1.2),
        "max_holding_min":    (3.0, 8.0),
        "trail_activate_pct": (0.0010, 0.0045),
    }

    def __init__(self, tracker: PerformanceTracker):
        self.tracker      = tracker
        self.params       = AdaptedParams()
        self._trade_count = 0
        self._adapt_every = 10
        self._lock        = threading.Lock()
        self._adapt_log   = deque(maxlen=100)

    def notify_trade(self, trade: TradeRecord):
        self.tracker.add(trade)
        self._trade_count += 1
        if self._trade_count % self._adapt_every == 0:
            self._run_adaptation()

    def _run_adaptation(self):
        with self._lock:
            stats   = self.tracker.rolling_stats()
            changes = []
            if stats["n"] < 5:
                return

            wr         = stats["wr"]
            force_rate = stats["force_rate"]
            sl_rate    = stats["sl_rate"]
            expectancy = stats["expectancy"]

            # ── Fix 1: force_rate tinggi ─────────────────────────────
            if force_rate > 0.50:
                force_trades = [t for t in self.tracker.recent if t.force_closed]
                force_win    = sum(1 for t in force_trades if t.win) / max(len(force_trades), 1)
                if force_win > 0.5:
                    old = self.params.max_holding_min
                    self.params.max_holding_min = self._clamp("max_holding_min", old + 0.5)
                    changes.append(f"⏰ MaxHold {old:.1f}→{self.params.max_holding_min:.1f}m (force profitable)")
                else:
                    old = self.params.min_momentum_pct
                    self.params.min_momentum_pct = self._clamp("min_momentum_pct", old + 0.0003)
                    changes.append(f"📈 MinMom ↑{old*100:.2f}%→{self.params.min_momentum_pct*100:.2f}% (force_loss={1-force_win:.0%})")

            # ── Fix 2: SL rate tinggi ────────────────────────────────
            if sl_rate > 0.25:
                old_score = self.params.min_score
                old_sl    = self.params.atr_sl_mult
                self.params.min_score    = self._clamp("min_score",    old_score + 3)
                self.params.atr_sl_mult  = self._clamp("atr_sl_mult",  old_sl + 0.1)
                changes.append(f"🛑 Score ↑{old_score}→{self.params.min_score} + SL×{old_sl:.1f}→{self.params.atr_sl_mult:.1f}")

            # ── Fix 3: losing streak ─────────────────────────────────
            if wr < 0.40:
                streak = self._count_losing_streak()
                if streak >= 3:
                    old = self.params.min_score
                    self.params.min_score         = self._clamp("min_score", old + min(5, streak))
                    self.params.min_entry_signals = min(self.params.min_entry_signals + 1, 4)
                    changes.append(f"🎯 Score ↑{old}→{self.params.min_score} + Signals→{self.params.min_entry_signals} (streak={streak})")

            # ── Fix 4: WR ok tapi expectancy negatif (situasi kamu) ──
            # Trail terlalu ketat → TP2 tidak pernah hit → ratio RR jelek
            if wr > 0.50 and expectancy < 0:
                old_trail = self.params.atr_trail_mult
                old_act   = self.params.trail_activate_pct
                self.params.atr_trail_mult     = self._clamp("atr_trail_mult",     old_trail - 0.05)
                self.params.trail_activate_pct = self._clamp("trail_activate_pct", old_act   + 0.0005)
                changes.append(f"✨ Trail×{old_trail:.2f}→{self.params.atr_trail_mult:.2f} + Activate↑{old_act*100:.2f}%→{self.params.trail_activate_pct*100:.2f}% (fix RR)")

            # ── Fix 5: perform bagus → relax sedikit ─────────────────
            if wr >= 0.60 and expectancy > 0.002:
                if self.params.min_score > 45:
                    old = self.params.min_score
                    self.params.min_score = self._clamp("min_score", old - 2)
                    changes.append(f"😊 Score ↓{old}→{self.params.min_score} (perform bagus)")

            # ── Fix 6: holding time optimizer ────────────────────────
            self._optimize_holding_time(changes)

            # ── Fix 7: blacklist sinyal jelek ─────────────────────────
            self._update_signal_blacklist(changes)

            # ── Fix 8: regime analysis ────────────────────────────────
            self._update_regime_params(changes)

            if changes:
                self._adapt_log.append({
                    "ts": time.strftime("%H:%M:%S"),
                    "stats": {k: v for k, v in stats.items() if k != "n"},
                    "changes": changes,
                })
                print(f"\n  🧠 ADAPTIVE ENGINE — Trade #{self._trade_count}")
                print(f"     Stats: WR:{wr:.0%} | Force:{force_rate:.0%} | SL:{sl_rate:.0%} | Exp:{expectancy:+.5f}U")
                for c in changes:
                    print(f"     → {c}")
                print(f"     Now: Score≥{self.params.min_score} | Mom≥{self.params.min_momentum_pct*100:.2f}% | "
                      f"Hold≤{self.params.max_holding_min:.1f}m | SL×{self.params.atr_sl_mult:.1f} | Trail×{self.params.atr_trail_mult:.2f}")

    def _optimize_holding_time(self, changes):
        dist = self.tracker.holding_time_distribution()
        if not dist: return
        best = max(dist.items(), key=lambda x: x[1]["avg"], default=None)
        if best is None or best[1]["n"] < 3: return
        bucket_to_min = {"0-60s": 1.5, "60-120s": 2.5, "120-180s": 3.5,
                         "180-240s": 4.5, "240-300s": 5.5, "300s+": 7.0}
        optimal = bucket_to_min.get(best[0], self.params.max_holding_min)
        cur     = self.params.max_holding_min
        if abs(optimal - cur) > 0.5:
            direction = 1 if optimal > cur else -1
            self.params.max_holding_min = self._clamp("max_holding_min", cur + direction * 0.3)
            changes.append(f"⏱️  MaxHold {cur:.1f}→{self.params.max_holding_min:.1f}m (best={best[0]} avg={best[1]['avg']:+.4f}U)")

    def _update_signal_blacklist(self, changes):
        combo_stats  = self.tracker.stats_by_signal_combo()
        new_bl       = [c for c, s in combo_stats.items() if s["n"] >= 5 and s["wr"] < 0.30]
        if new_bl != self.params.blacklisted_signals:
            added   = [c for c in new_bl if c not in self.params.blacklisted_signals]
            removed = [c for c in self.params.blacklisted_signals if c not in new_bl]
            self.params.blacklisted_signals = new_bl
            if added:   changes.append(f"🚫 Blacklist: {added[:2]}")
            if removed: changes.append(f"✅ Unblacklist: {removed[:2]}")

    def _update_regime_params(self, changes):
        regime_stats = self.tracker.stats_by_regime()
        for regime, stats in regime_stats.items():
            if stats["n"] < 5: continue
            if regime == "TREND" and stats["wr"] < 0.45:
                old = self.params.trend_min_score
                self.params.trend_min_score = min(old + 2, 72)
                if old != self.params.trend_min_score:
                    changes.append(f"📊 TREND score ↑{old}→{self.params.trend_min_score} (wr={stats['wr']:.0%})")
            elif regime == "MEAN_REV":
                if stats["wr"] >= 0.60 and not self.params.mean_rev_enabled:
                    self.params.mean_rev_enabled = True
                    changes.append(f"📊 MEAN_REV ON (wr={stats['wr']:.0%})")
                elif stats["wr"] < 0.40 and self.params.mean_rev_enabled:
                    self.params.mean_rev_enabled = False
                    changes.append(f"📊 MEAN_REV OFF (wr={stats['wr']:.0%})")

    def _count_losing_streak(self) -> int:
        streak = 0
        for t in reversed(list(self.tracker.recent)):
            if not t.win: streak += 1
            else: break
        return streak

    def _clamp(self, name: str, value: float) -> float:
        lo, hi = self.LIMITS.get(name, (-999, 999))
        return max(lo, min(hi, value))

    def get_params(self) -> AdaptedParams:
        with self._lock:
            return self.params

    def get_adapt_log(self) -> list:
        return list(self._adapt_log)


class DynamicScoreThreshold:
    def __init__(self, base: int = 45):
        self.base  = base
        self._streak = 0
        self._extra  = 0

    def record(self, win: bool):
        if win: self._streak = max(0, self._streak) + 1
        else:   self._streak = min(0, self._streak) - 1
        s = self._streak
        if s <= -3:   self._extra = min(abs(s) * 3, 20)
        elif s >= 3:  self._extra = max(-min(s, 5), -5)
        else:         self._extra = 0

    @property
    def current(self) -> int:
        return max(40, min(75, self.base + self._extra))

    @property
    def info(self) -> str:
        if self._streak <= -3: return f"❌LS:{abs(self._streak)} +{self._extra}pts"
        if self._streak >= 3:  return f"✅WS:{self._streak} {self._extra:+d}pts"
        return f"Streak:{self._streak:+d}"


class AdaptiveController:
    """Single interface semua komponen adaptive. Dipanggil dari seluruh bot."""

    def __init__(self):
        self.tracker   = PerformanceTracker()
        self.engine    = AdaptiveParamEngine(self.tracker)
        self.dyn_score = DynamicScoreThreshold(MIN_SCORE)
        self._fc_patterns: dict = {}
        self._fc_last_update: float = 0
        self._sig_scores: dict = {}
        self._sig_last_update: float = 0

    def record_trade(self, symbol, side, entry, exit_price, pnl, hold_sec,
                     reason, signals, score, scalp_mode, btc_trend,
                     mom_pct, atr_pct, fng):
        trade = TradeRecord(
            symbol=symbol, side=side, entry=entry, exit_price=exit_price,
            pnl=pnl, hold_sec=hold_sec, reason=reason, signals=signals,
            score=score, scalp_mode=scalp_mode, btc_trend=btc_trend,
            mom_pct=mom_pct, atr_pct=atr_pct, fng=fng,
            hour_utc=time.gmtime().tm_hour,
        )
        self.engine.notify_trade(trade)
        self.dyn_score.record(pnl > 0)
        now = time.time()
        if now - self._fc_last_update > 300:
            self._fc_patterns     = self.tracker.force_close_patterns()
            self._fc_last_update  = now
        if now - self._sig_last_update > 300:
            self._update_sig_scores()
            self._sig_last_update = now

    def _update_sig_scores(self):
        combo_stats = self.tracker.stats_by_signal_combo()
        new_scores  = defaultdict(list)
        for combo_str, stats in combo_stats.items():
            if stats["n"] < 3: continue
            score = stats["wr"] * (1 + max(0, stats["avg"]) * 10)
            for sig in combo_str.split("|"):
                new_scores[sig].append(score)
        self._sig_scores = {sig: sum(v)/len(v) for sig, v in new_scores.items() if v}

    def get_params(self) -> AdaptedParams:
        return self.engine.get_params()

    def get_effective_min_score(self) -> int:
        p = self.engine.get_params()
        return max(p.min_score, p.trend_min_score, self.dyn_score.current)

    def adjust_score(self, raw: int, signals: list) -> int:
        if not self._sig_scores or not signals:
            return raw
        weights = [self._sig_scores[_normalize_signal(s)]
                   for s in signals if _normalize_signal(s) in self._sig_scores]
        if not weights:
            return raw
        q = max(0.7, min(1.3, sum(weights) / len(weights)))
        return max(0, min(100, int(raw * q)))

    def should_enter_signal(self, signals, symbol, btc_trend) -> tuple:
        p    = self.engine.get_params()
        hour = time.gmtime().tm_hour

        # Blacklist check
        if p.blacklisted_signals:
            normalized  = frozenset(_normalize_signal(s) for s in signals)
            combo_key   = "|".join(sorted(normalized))
            if combo_key in p.blacklisted_signals:
                return False, "blacklist_signal"

        # Force-close pattern check
        pats = self._fc_patterns
        if pats.get("bad_hours") and hour in pats["bad_hours"]:
            return False, f"fc_hour:{hour}"
        if pats.get("bad_btc_trends") and btc_trend in pats["bad_btc_trends"]:
            return False, f"fc_btc:{btc_trend}"
        if pats.get("bad_symbols_force") and symbol in pats["bad_symbols_force"]:
            return False, f"fc_sym:{symbol}"

        return True, ""

    def get_max_holding(self) -> float:
        return self.engine.get_params().max_holding_min

    def print_status(self):
        stats  = self.tracker.rolling_stats()
        params = self.engine.get_params()
        print(f"\n  {'─'*62}")
        print(f"  🧠 ADAPTIVE ENGINE STATUS")
        print(f"     Rolling({self.tracker.WINDOW}): WR:{stats['wr']:.0%} | "
              f"Force:{stats['force_rate']:.0%} | SL:{stats['sl_rate']:.0%} | "
              f"Exp:{stats['expectancy']:+.5f}U | n={stats['n']}")
        print(f"     Score effective: {self.get_effective_min_score()} | {self.dyn_score.info}")
        print(f"     Params: Score≥{params.min_score} | Mom≥{params.min_momentum_pct*100:.2f}% | "
              f"Vol≥{params.min_vol_surge:.1f}x | Hold≤{params.max_holding_min:.1f}m | "
              f"SL×{params.atr_sl_mult:.1f} | TP1×{params.atr_tp1_mult:.1f} | "
              f"Trail×{params.atr_trail_mult:.2f} | Act@{params.trail_activate_pct*100:.2f}%")
        if params.blacklisted_signals:
            print(f"     🚫 Blacklisted combos: {params.blacklisted_signals[:3]}")
        fc = self._fc_patterns
        if fc.get("bad_hours"):
            print(f"     ⏰ FC bad hours: UTC{fc['bad_hours']}")
        if fc.get("bad_btc_trends"):
            print(f"     ₿  FC bad BTC: {fc['bad_btc_trends']}")
        if fc.get("bad_symbols_force"):
            print(f"     📉 FC bad syms: {fc['bad_symbols_force'][:5]}")
        log = self.engine.get_adapt_log()
        if log:
            last = log[-1]
            print(f"     Last adapt [{last['ts']}]: {last['changes'][0] if last['changes'] else '—'}")
        print(f"  {'─'*62}")


# Singleton — digunakan seluruh bot
adaptive = AdaptiveController()


# ════════════════════════════════════════════════════
#  KILL SWITCH ENGINE
# ════════════════════════════════════════════════════
def check_kill_switch():
    ks  = _kill_switch
    now = time.time()
    if ks["active"] and now >= ks["resume_time"]:
        ks["active"] = False; ks["reason"] = ""; ks["consec_losses"] = 0
        print(f"\n  ✅ Kill switch CLEARED — bot aktif kembali")
    if ks["active"]:
        return True, ks["reason"]
    day_start = now - (now % 86400)
    if day_start > ks["daily_reset_ts"]:
        ks["daily_pnl"] = 0.0; ks["daily_reset_ts"] = day_start
    if ks["daily_pnl"] <= DAILY_LOSS_LIMIT:
        ks["active"] = True; ks["reason"] = f"daily_loss({ks['daily_pnl']:.2f}U)"
        ks["resume_time"] = day_start + 86400
        print(f"\n  🚨 KILL SWITCH: daily loss limit ({ks['daily_pnl']:.2f}U)")
        return True, ks["reason"]
    if ks["consec_losses"] >= CONSEC_LOSS_MAX:
        ks["active"] = True; ks["reason"] = f"consec_loss({ks['consec_losses']})"
        ks["resume_time"] = now + (CONSEC_LOSS_PAUSE_MIN * 60)
        print(f"\n  🚨 KILL SWITCH: {ks['consec_losses']} loss beruntun — pause {CONSEC_LOSS_PAUSE_MIN}m")
        return True, ks["reason"]
    return False, ""


def update_kill_switch_after_trade(pnl):
    ks = _kill_switch
    ks["daily_pnl"] += pnl
    if pnl < 0: ks["consec_losses"] += 1
    else:       ks["consec_losses"]  = 0


def check_api_latency():
    try:
        t0  = time.time()
        client.futures_ping()
        lag = time.time() - t0
        _kill_switch["api_lag"] = lag
        if lag > MAX_API_LAG_SEC:
            print(f"  ⚠️ API lag: {lag:.2f}s — skip entry")
            return False
        return True
    except:
        return False


# ════════════════════════════════════════════════════
#  CHOP / REGIME FILTER
# ════════════════════════════════════════════════════
def calc_choppiness_index(df, period=14):
    if df is None or len(df) < period + 2: return 50.0
    try:
        high  = df["high"].values
        low   = df["low"].values
        close = df["close"].values
        tr_sum = sum(max(high[i]-low[i], abs(high[i]-close[i-1]),
                        abs(low[i]-close[i-1])) for i in range(-period, 0))
        pr = max(high[-period:]) - min(low[-period:])
        if pr == 0 or tr_sum == 0: return 50.0
        return round(100 * math.log10(tr_sum / pr) / math.log10(period), 2)
    except: return 50.0


def calc_ema_cross_frequency(df, period=20):
    if df is None or len(df) < period + 10: return 0
    try:
        e3 = df["ema3"].values[-period:]
        e9 = df["ema9"].values[-period:]
        return sum(1 for i in range(1, len(e3))
                   if (e3[i-1]>e9[i-1] and e3[i]<=e9[i]) or
                      (e3[i-1]<e9[i-1] and e3[i]>=e9[i]))
    except: return 0


def is_chop_market(df_5m, direction):
    if df_5m is None or len(df_5m) < 20: return False, "no_data"
    reasons = []
    ci = calc_choppiness_index(df_5m, 14)
    if ci > CHOP_INDEX_THRESHOLD: reasons.append(f"CI={ci:.1f}")
    last = df_5m.iloc[-1]
    if last.get("bb_width", 0.01) < MIN_BB_WIDTH_PCT:
        reasons.append(f"BB_narrow")
    if calc_ema_cross_frequency(df_5m, 20) > MAX_EMA_CROSS_FREQ:
        reasons.append(f"EMA_choppy")
    hist_std = float(np.std(df_5m["macd_hist"].values[-10:])) \
               if len(df_5m) >= 10 else 0
    if hist_std < 0.00001: reasons.append("MACD_flat")
    is_chop = len(reasons) >= 2
    return is_chop, "|".join(reasons) if reasons else "ok"


# ════════════════════════════════════════════════════
#  SPREAD / SESSION FILTER
# ════════════════════════════════════════════════════
def get_spread_ratio(symbol, tp1_price, entry_price):
    try:
        ob       = client.futures_order_book(symbol=symbol, limit=5)
        spread   = float(ob["asks"][0][0]) - float(ob["bids"][0][0])
        tp1_dist = abs(tp1_price - entry_price)
        return round(spread / tp1_dist, 3) if tp1_dist else 1.0
    except: return 0.0


def get_session_min_score():
    if time.gmtime().tm_hour in BAD_HOURS_UTC: return BAD_HOURS_MIN_SCORE
    return MIN_SCORE


# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info: return _sym_info[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        _sym_info[symbol] = {"step": float(f["stepSize"]),
                                             "minQty": float(f["minQty"])}
                        return _sym_info[symbol]
    except: pass
    return {"step": 1.0, "minQty": 1.0}


def round_step(qty, step):
    p = max(0, int(round(-math.log(step, 10), 0))) if step < 1 else 0
    return round(math.floor(qty / step) * step, p)


def calc_qty(symbol, price):
    info = get_sym_info(symbol)
    raw  = (ORDER_USDT * LEVERAGE) / price
    return max(round_step(raw, info["step"]), info["minQty"])


def set_leverage(symbol):
    try: client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except: pass


def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0


def get_exchange_amt(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0: return amt
        return 0
    except: return None


def is_symbol_cooling_down(symbol):
    if symbol not in _sym_cooldown: return False
    return (time.time() - _sym_cooldown[symbol]) < SYMBOL_COOLDOWN_SEC


def set_symbol_cooldown(symbol):
    _sym_cooldown[symbol] = time.time()


def validate_symbols():
    try:
        valid  = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                  if s["status"] == "TRADING"}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        print(f"  ✅ {len(result)}/{len(SYMBOLS)} symbols valid")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))


# ════════════════════════════════════════════════════
#  DATA SOURCES
# ════════════════════════════════════════════════════
def fetch_ticker24h_all():
    global _ticker24h_cache, _ticker24h_ts
    now = time.time()
    if now - _ticker24h_ts < TICKER24H_TTL and _ticker24h_cache:
        return _ticker24h_cache
    try:
        tickers   = client.futures_ticker()
        new_cache = {}
        for t in tickers:
            sym = t["symbol"]
            new_cache[sym] = {
                "pct":    float(t["priceChangePercent"]),
                "price":  float(t["lastPrice"]),
                "vol24h": float(t["quoteVolume"]),
                "high24": float(t["highPrice"]),
                "low24":  float(t["lowPrice"]),
                "count":  int(t["count"]),
            }
        _ticker24h_cache = new_cache
        _ticker24h_ts    = now
        return new_cache
    except: return _ticker24h_cache


def fetch_funding_rates():
    global _funding_cache, _funding_ts
    now = time.time()
    if now - _funding_ts < FUNDING_TTL and _funding_cache:
        return _funding_cache
    try:
        premium   = client.futures_mark_price()
        new_cache = {p["symbol"]: float(p.get("lastFundingRate", 0)) for p in premium}
        _funding_cache = new_cache
        _funding_ts    = now
        return new_cache
    except: return _funding_cache


def get_top_movers(symbols_active, n=30):
    global _top_movers, _top_movers_ts
    now = time.time()
    if now - _top_movers_ts < TOP_MOVERS_TTL and _top_movers:
        return _top_movers
    try:
        tickers    = fetch_ticker24h_all()
        active_set = set(symbols_active)
        movers     = [(sym, d["pct"], d["vol24h"]) for sym, d in tickers.items()
                      if sym in active_set and d["vol24h"] >= 1_000_000]
        movers.sort(key=lambda x: abs(x[1]), reverse=True)
        result = [(s, p, "LONG" if p > 0 else "SHORT") for s, p, _ in movers[:n]]
        _top_movers    = result
        _top_movers_ts = now
        return result
    except: return _top_movers


def get_funding_bias(symbol):
    rates = fetch_funding_rates()
    fr = rates.get(symbol, 0)
    if fr > 0.0005:  return "bearish_bias", fr
    if fr < -0.0005: return "bullish_bias", fr
    return "neutral", fr


# ════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=100):
    cache_key = (symbol, interval)
    now = time.time()
    ttl_map = {
        Client.KLINE_INTERVAL_1MINUTE:  OHLCV_CACHE_TTL_1M,
        Client.KLINE_INTERVAL_3MINUTE:  OHLCV_CACHE_TTL_3M,
        Client.KLINE_INTERVAL_5MINUTE:  OHLCV_CACHE_TTL_5M,
        Client.KLINE_INTERVAL_15MINUTE: OHLCV_CACHE_TTL_15M,
        Client.KLINE_INTERVAL_1HOUR:    OHLCV_CACHE_TTL_1H,
    }
    ttl = ttl_map.get(interval, 30)
    if cache_key in _ohlcv_cache:
        ts, df_cached = _ohlcv_cache[cache_key]
        if now - ts < ttl: return df_cached
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df     = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[cache_key] = (now, df)
        return df
    except:
        if cache_key in _ohlcv_cache: return _ohlcv_cache[cache_key][1]
        return None


# ════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ════════════════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]        = ta.momentum.RSIIndicator(c, 14).rsi()
    df["rsi_fast"]   = ta.momentum.RSIIndicator(c, 7).rsi()
    macd             = ta.trend.MACD(c, 12, 26, 9)
    df["macd"]       = macd.macd()
    df["macd_sig"]   = macd.macd_signal()
    df["macd_hist"]  = macd.macd_diff()
    df["ema3"]       = ta.trend.EMAIndicator(c, 3).ema_indicator()
    df["ema5"]       = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["ema9"]       = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"]      = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"]      = ta.trend.EMAIndicator(c, 50).ema_indicator()
    bb               = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_hi"]      = bb.bollinger_hband()
    df["bb_lo"]      = bb.bollinger_lband()
    df["bb_mid"]     = bb.bollinger_mavg()
    df["bb_width"]   = (df["bb_hi"] - df["bb_lo"]) / df["bb_mid"]
    stoch            = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
    df["stk"]        = stoch.stoch()
    df["std"]        = stoch.stoch_signal()
    df["atr"]        = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["vol_ma"]     = v.rolling(20).mean()
    df["vol_ratio"]  = v / df["vol_ma"].replace(0, 1)
    df["buy_ratio"]  = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"]       = abs(df["close"] - df["open"])
    df["range_"]     = df["high"] - df["low"]
    df["body_ratio"] = df["body"] / df["range_"].replace(0, 1)
    df["bb_squeeze"] = df["bb_width"] < df["bb_width"].rolling(20).mean() * 0.85
    df["mom5"]       = (c - c.shift(5)) / c.shift(5)
    df["mom3"]       = (c - c.shift(3)) / c.shift(3)
    return df


def _calc_trend(df):
    if df is None or len(df) < 25: return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    chg   = (price - c.iloc[-4]) / c.iloc[-4] * 100
    if price > ema9 > ema21 > ema50 and chg > 0:   return "BULL"
    elif price < ema9 < ema21 < ema50 and chg < 0: return "BEAR"
    elif price > ema21 and chg > -0.2:             return "MILD_BULL"
    elif price < ema21 and chg < 0.2:              return "MILD_BEAR"
    return "SIDEWAYS"


# ════════════════════════════════════════════════════
#  ATR-BASED LEVELS — versi adaptive
# ════════════════════════════════════════════════════
def calc_atr_levels(entry, atr, direction):
    """Gunakan params dari adaptive engine."""
    p = adaptive.get_params()
    return _calc_atr_levels_with(entry, atr, direction,
                                 p.atr_sl_mult, p.atr_tp1_mult,
                                 p.atr_tp2_mult, INSTANT_CUT_MULT)


def _calc_atr_levels_with(entry, atr, direction, sl_m, tp1_m, tp2_m, ic_m):
    raw_sl  = atr * sl_m
    raw_tp1 = atr * tp1_m
    raw_tp2 = atr * tp2_m
    raw_ic  = atr * ic_m
    sl_d    = max(entry * MIN_SL_PCT, min(raw_sl,  entry * MAX_SL_PCT))
    tp1_d   = max(entry * MIN_TP1_PCT, raw_tp1)
    tp2_d   = min(entry * MAX_TP2_PCT, raw_tp2)
    tp2_d   = max(tp2_d, tp1_d * 1.5)
    if direction == "LONG":
        sl  = round(entry - sl_d,  8)
        tp1 = round(entry + tp1_d, 8)
        tp2 = round(entry + tp2_d, 8)
        ic  = round(entry - raw_ic, 8)
    else:
        sl  = round(entry + sl_d,  8)
        tp1 = round(entry - tp1_d, 8)
        tp2 = round(entry - tp2_d, 8)
        ic  = round(entry + raw_ic, 8)
    return {"sl": sl, "tp1": tp1, "tp2": tp2, "instant_cut": ic,
            "sl_pct": sl_d/entry, "tp1_pct": tp1_d/entry, "tp2_pct": tp2_d/entry,
            "atr": atr, "atr_pct": atr/entry}


# ════════════════════════════════════════════════════
#  MOMENTUM CHECK
# ════════════════════════════════════════════════════
def check_momentum_strength(df, direction):
    if df is None or len(df) < 10: return False, 0, "no_data"
    p      = adaptive.get_params()
    last   = df.iloc[-1]
    recent = df.iloc[-6:-1]
    price_now  = last["close"]
    price_5ago = df.iloc[-6]["close"]
    mom_pct    = (price_now - price_5ago) / price_5ago
    min_mom    = p.min_momentum_pct
    min_vol    = p.min_vol_surge
    min_can    = p.min_trend_candles
    if direction == "LONG"  and mom_pct < min_mom:   return False, mom_pct, f"mom_weak({mom_pct*100:.2f}%)"
    if direction == "SHORT" and mom_pct > -min_mom:  return False, mom_pct, f"mom_weak({mom_pct*100:.2f}%)"
    if last["vol_ratio"] < min_vol:                  return False, mom_pct, f"vol_low({last['vol_ratio']:.1f}x)"
    if direction == "LONG":
        if sum(1 for _, r in recent.iterrows() if r["close"] > r["open"]) < min_can:
            return False, mom_pct, "candles_weak"
    else:
        if sum(1 for _, r in recent.iterrows() if r["close"] < r["open"]) < min_can:
            return False, mom_pct, "candles_weak"
    if last["body_ratio"] < 0.4: return False, mom_pct, f"weak_candle(body:{last['body_ratio']:.2f})"
    return True, mom_pct, f"mom={mom_pct*100:+.2f}% vol={last['vol_ratio']:.1f}x"


# ════════════════════════════════════════════════════
#  CONTINUATION CONFIRMATION
# ════════════════════════════════════════════════════
def check_continuation(df, direction):
    if df is None or len(df) < 5: return False, "no_data"
    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    if direction == "LONG":
        if last["close"] <= last["open"]:                                       return False, "last_bearish"
        if last["high"] <= prev["high"] and prev["high"] <= prev2["high"]:      return False, "no_hh"
        if prev["close"] < prev["open"] and prev["body_ratio"] > 0.7:           return False, "engulf_bear_prev"
    else:
        if last["close"] >= last["open"]:                                       return False, "last_bullish"
        if last["low"] >= prev["low"] and prev["low"] >= prev2["low"]:          return False, "no_ll"
        if prev["close"] > prev["open"] and prev["body_ratio"] > 0.7:           return False, "engulf_bull_prev"
    return True, "ok"


# ════════════════════════════════════════════════════
#  MACRO REFRESH
# ════════════════════════════════════════════════════
def refresh_macro():
    now = time.time()
    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
            _macro["fng"]       = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"]  = now
        except: pass

    if now - _macro["last_btc"] > 5:
        try:
            df1m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, 30)
            df5m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
            df15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 60)
            _macro["btc_trend_1m"]  = _calc_trend(df1m)
            _macro["btc_trend_5m"]  = _calc_trend(df5m)
            _macro["btc_trend_15m"] = _calc_trend(df15m)
            _macro["btc_trend_1h"]  = _calc_trend(df1h)
            _macro["last_btc"]      = now
            t5m  = _macro["btc_trend_5m"]
            t15m = _macro["btc_trend_15m"]
            if t15m in ("BULL","BEAR") or t5m in ("BULL","BEAR"):
                _macro["scalp_mode"] = "TREND"
            else:
                # MEAN_REV hanya aktif kalau adaptive engine mengizinkan
                _macro["scalp_mode"] = "MEAN_REV"
        except: pass

    if now - _macro["last_breadth"] > 30:
        try:
            bullish = 0
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 10)
                if df is not None and len(df) >= 5:
                    e9 = ta.trend.EMAIndicator(df["close"], 9).ema_indicator().iloc[-1]
                    if df["close"].iloc[-1] > e9: bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except: pass

    if now - _macro.get("last_news", 0) > 120:
        try:
            data    = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg_kw  = ["crash","hack","ban","fraud","collapse","seized","scam","plunge"]
            pos_kw  = ["institutional","ath","approved","record","bullish","rally","surge"]
            neg = pos = 0
            for post in data.get("results", [])[:8]:
                tl = post.get("title","").lower()
                if any(w in tl for w in neg_kw): neg += 1
                if any(w in tl for w in pos_kw): pos += 1
            sc = pos - neg
            _macro["news"] = ("strong_negative" if sc <= -3 else
                              "negative"        if sc <= -1 else
                              "strong_positive" if sc >=  3 else "neutral")
            _macro["last_news"] = now
        except: pass


def update_btc_price():
    try:
        px = get_price("BTCUSDT")
        if px > 0: _btc_price_history.append((time.time(), px))
    except: pass


def detect_flash_move():
    if len(_btc_price_history) < 2: return "none", 0.0
    cutoff  = time.time() - 120
    oldest  = next((px for ts, px in _btc_price_history if ts >= cutoff), None)
    if oldest is None: return "none", 0.0
    current = _btc_price_history[-1][1]
    pct     = (current - oldest) / oldest * 100
    if pct <= -1.0: return "crash", abs(pct)
    if pct >= 1.0:  return "pump",  abs(pct)
    return "none", 0.0


# ════════════════════════════════════════════════════
#  ORDER BOOK IMBALANCE
# ════════════════════════════════════════════════════
def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=50)
        bid_w = sum(float(b[1]) * (1/(i+1)) for i, b in enumerate(ob["bids"][:20]))
        ask_w = sum(float(a[1]) * (1/(i+1)) for i, a in enumerate(ob["asks"][:20]))
        total = bid_w + ask_w
        return round((bid_w - ask_w) / total, 3) if total else 0.0
    except: return 0.0


# ════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE v14
# ════════════════════════════════════════════════════
def get_entry_score(symbol, df_5m, direction):
    if df_5m is None or len(df_5m) < 30: return 0, []
    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    prev2 = df_5m.iloc[-3]
    sigs  = []
    score = 0

    # ── A: TREND (max 25) ────────────────────────────────
    e3, e5, e9, e21 = last["ema3"], last["ema5"], last["ema9"], last["ema21"]
    p = last["close"]
    if direction == "LONG":
        if p > e3 > e5 > e9 > e21:        score += 25; sigs.append("📐EMA_STACK↑")
        elif p > e5 > e9 > e21:           score += 18; sigs.append("📐EMA↑")
        elif p > e9 > e21:                score += 12; sigs.append("📐EMA_align↑")
    else:
        if p < e3 < e5 < e9 < e21:        score += 25; sigs.append("📐EMA_STACK↓")
        elif p < e5 < e9 < e21:           score += 18; sigs.append("📐EMA↓")
        elif p < e9 < e21:                score += 12; sigs.append("📐EMA_align↓")

    # ── B: VOLATILITY/MOMENTUM (max 25) ──────────────────
    mom5    = abs(last.get("mom5", 0))
    vol_rat = last["vol_ratio"]
    atr_now = last["atr"]
    atr_prv = df_5m.iloc[-6]["atr"] if len(df_5m) > 6 else atr_now
    atr_exp = atr_now > atr_prv * 1.2
    if mom5 >= 0.008 and atr_exp:       score += 25; sigs.append(f"🚀Mom{mom5*100:.1f}%+ATRexp")
    elif mom5 >= 0.005 and vol_rat >= 2: score += 20; sigs.append(f"📈Mom{mom5*100:.1f}%+Vol{vol_rat:.1f}x")
    elif mom5 >= 0.003:                  score += 13; sigs.append(f"📈Mom{mom5*100:.1f}%")
    elif vol_rat >= 3.0:                 score += 13; sigs.append(f"🔥VolSurge{vol_rat:.1f}x")
    elif vol_rat >= 2.0:                 score += 8

    # ── C: ORDER FLOW (max 25) ────────────────────────────
    h_now  = last["macd_hist"]
    h_prev = prev["macd_hist"]
    h_p2   = prev2["macd_hist"]
    br     = last["buy_ratio"]
    if direction == "LONG":
        if h_now > 0 and h_now > h_prev > h_p2 and br > 0.55:  score += 25; sigs.append(f"✅MACD↑↑+Buy{br:.0%}")
        elif h_now > 0 and h_now > h_prev:                      score += 17; sigs.append("✅MACD↑")
        elif h_prev < 0 and h_now >= 0:                         score += 20; sigs.append("⚡MACD_X0↑")
        elif br > 0.60:                                          score += 10; sigs.append(f"💧Buy{br:.0%}")
    else:
        if h_now < 0 and h_now < h_prev < h_p2 and br < 0.45:  score += 25; sigs.append(f"✅MACD↓↓+Sell{1-br:.0%}")
        elif h_now < 0 and h_now < h_prev:                      score += 17; sigs.append("✅MACD↓")
        elif h_prev > 0 and h_now <= 0:                         score += 20; sigs.append("⚡MACD_X0↓")
        elif br < 0.40:                                          score += 10; sigs.append(f"💧Sell{1-br:.0%}")

    # ── D: MARKET STRUCTURE (max 25) ─────────────────────
    rec_hi = df_5m.iloc[-6:-1]["high"].max()
    rec_lo = df_5m.iloc[-6:-1]["low"].min()
    if direction == "LONG":
        if p > rec_hi and last["body_ratio"] > 0.6 and last["vol_ratio"] > 1.5:    score += 25; sigs.append("🚀BreakoutBull")
        elif last["close"] > last["open"] and last["close"] > prev["high"] and last["body_ratio"] > 0.6: score += 20; sigs.append("🕯️Engulf↑")
        elif p > rec_hi:                                                             score += 12; sigs.append("📈Breakout↑")
    else:
        if p < rec_lo and last["body_ratio"] > 0.6 and last["vol_ratio"] > 1.5:    score += 25; sigs.append("💥BreakoutBear")
        elif last["close"] < last["open"] and last["close"] < prev["low"] and last["body_ratio"] > 0.6: score += 20; sigs.append("🕯️Engulf↓")
        elif p < rec_lo:                                                             score += 12; sigs.append("📈Breakout↓")

    return max(0, min(score, 100)), sigs


def determine_direction(df_5m, df_15m=None):
    if df_5m is None or len(df_5m) < 20: return None
    last   = df_5m.iloc[-1]
    prev   = df_5m.iloc[-2]
    price  = last["close"]
    e3, e5, e9 = last["ema3"], last["ema5"], last["ema9"]
    lp = sp = 0
    if price > e3 > e5 > e9:     lp += 4
    elif price < e3 < e5 < e9:   sp += 4
    elif price > e5 > e9:        lp += 2
    elif price < e5 < e9:        sp += 2
    mom5 = last.get("mom5", 0)
    if mom5 > 0.002:   lp += 3
    elif mom5 < -0.002: sp += 3
    if last["macd_hist"] > prev["macd_hist"]: lp += 2
    else:                                      sp += 2
    if last["buy_ratio"] > 0.55 and last["close"] > last["open"]:   lp += 2
    elif last["buy_ratio"] < 0.45 and last["close"] < last["open"]: sp += 2
    if df_15m is not None and len(df_15m) >= 20:
        l15 = df_15m.iloc[-1]
        if l15["ema9"] > l15["ema21"]: lp += 2
        else:                          sp += 2
    btc_t = _macro.get("btc_trend_5m", "UNKNOWN")
    if btc_t in BULL_TRENDS: lp += 2
    elif btc_t in BEAR_TRENDS: sp += 2
    if lp > sp and lp >= 6:  return "LONG"
    if sp > lp and sp >= 6:  return "SHORT"
    return None


# ════════════════════════════════════════════════════
#  ENTRY FILTER v14-ADAPTIVE
# ════════════════════════════════════════════════════
def should_enter(symbol):
    killed, kill_reason = check_kill_switch()
    if killed: return None, f"kill:{kill_reason}"
    if is_symbol_cooling_down(symbol): return None, "cooldown"

    fng  = _macro["fng"]
    news = _macro["news"]
    if fng < MIN_FNG:             return None, f"F&G={fng}"
    if news == "strong_negative": return None, "bad_news"

    flash_dir, _ = detect_flash_move()
    if flash_dir != "none": return None, f"flash_{flash_dir}"

    tickers = fetch_ticker24h_all()
    pct_24h = 0.0
    if symbol in tickers:
        t24 = tickers[symbol]
        if t24["vol24h"] < 500_000: return None, f"illiquid(${t24['vol24h']/1e6:.2f}M)"
        pct_24h = t24["pct"]

    df_5m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 80)
    df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60)
    if df_5m is None or len(df_5m) < 30: return None, "no_data"

    df_5m  = run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m) >= 20:
        df_15m = run_ta(df_15m.copy())

    direction = determine_direction(df_5m, df_15m)
    if direction is None: return None, "no_direction"

    is_chop, chop_desc = is_chop_market(df_5m, direction)
    if is_chop:
        _stats["skipped_chop"] += 1
        return None, f"chop:{chop_desc}"

    mom_pass, mom_pct, mom_desc = check_momentum_strength(df_5m, direction)
    if not mom_pass:
        _stats["skipped_no_momentum"] += 1
        return None, f"no_mom:{mom_desc}"

    cont_pass, cont_desc = check_continuation(df_5m, direction)
    if not cont_pass: return None, f"no_cont:{cont_desc}"

    funding_bias, fr = get_funding_bias(symbol)
    if direction == "LONG"  and funding_bias == "bearish_bias" and fr > 0.001:
        return None, f"funding_bearish({fr*100:.3f}%)"
    if direction == "SHORT" and funding_bias == "bullish_bias" and fr < -0.001:
        return None, f"funding_bullish({fr*100:.3f}%)"

    # ── MEAN_REV gate via adaptive engine ────────────────
    scalp_mode = _macro.get("scalp_mode", "TREND")
    p_adapt    = adaptive.get_params()
    if scalp_mode == "MEAN_REV" and not p_adapt.mean_rev_enabled:
        _stats["skipped_mean_rev"] += 1
        return None, "skip_MEAN_REV(regime)"

    btc_5m  = _macro["btc_trend_5m"]
    btc_15m = _macro["btc_trend_15m"]
    if direction == "LONG"  and btc_5m in BEAR_TRENDS and btc_15m in BEAR_TRENDS:
        return None, f"skip_LONG:BTC_{btc_5m}"
    if direction == "SHORT" and btc_5m in BULL_TRENDS and btc_15m in BULL_TRENDS:
        return None, f"skip_SHORT:BTC_{btc_5m}"
    if direction == "LONG"  and fng > MAX_FNG_LONG:
        return None, f"overbought:F&G={fng}"

    # ── Score dengan adaptive quality adjustment ──────────
    score, sigs = get_entry_score(symbol, df_5m, direction)
    score       = adaptive.adjust_score(score, sigs)          # ← ADAPTIVE

    # ── Adaptive signal & force-close pattern filter ──────
    sig_ok, sig_reason = adaptive.should_enter_signal(
        sigs, symbol, btc_5m)                                 # ← ADAPTIVE
    if not sig_ok:
        _stats["skipped_adaptive"] += 1
        return None, f"adaptive:{sig_reason}"

    # ── Score threshold: max dari session + adaptive + streak
    min_score_now = max(get_session_min_score(),
                        adaptive.get_effective_min_score())    # ← ADAPTIVE

    if score < min_score_now:
        if time.gmtime().tm_hour in BAD_HOURS_UTC:
            _stats["skipped_session"] += 1
        return None, f"score={score:.0f}<{min_score_now}"

    if len(sigs) < p_adapt.min_entry_signals:
        return None, f"signals={len(sigs)}<{p_adapt.min_entry_signals}"

    atr   = df_5m["atr"].iloc[-1]
    price = df_5m["close"].iloc[-1]
    if atr / price > MAX_SL_ATR_PCT:
        return None, f"ATR_terlalu_besar({atr/price*100:.2f}%)"

    levels = calc_atr_levels(price, atr, direction)            # ← uses adaptive params

    spread_ratio = get_spread_ratio(symbol, levels["tp1"], price)
    if spread_ratio > MAX_SPREAD_RATIO:
        _stats["skipped_spread"] += 1
        return None, f"spread_lebar({spread_ratio:.2f}x_TP1)"

    ob_imb = get_ob_imbalance(symbol)
    if direction == "LONG"  and ob_imb < -0.20: return None, f"OB_SHORT({ob_imb:.2f})"
    if direction == "SHORT" and ob_imb > 0.20:  return None, f"OB_LONG({ob_imb:.2f})"

    return direction, {
        "score":       score,
        "signals":     sigs,
        "direction":   direction,
        "sl":          levels["sl"],
        "tp1":         levels["tp1"],
        "tp2":         levels["tp2"],
        "sl_pct":      levels["sl_pct"],
        "tp1_pct":     levels["tp1_pct"],
        "ob_imb":      ob_imb,
        "atr":         atr,
        "atr_pct":     levels["atr_pct"],
        "mom_pct":     mom_pct,
        "pct_24h":     pct_24h,
        "funding":     fr,
        "scalp_mode":  scalp_mode,
        "btc_trend":   btc_5m,
        "instant_cut": levels["instant_cut"],
    }


# ════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction, info = should_enter(symbol)
        if direction: return symbol, direction, info
    except: pass
    return None


def scan_batch_parallel(symbols):
    candidates       = []
    symbols_to_scan  = symbols[:20]
    futures          = {_executor.submit(scan_symbol_safe, sym): sym for sym in symbols_to_scan}
    try:
        for future in as_completed(futures, timeout=12):
            try:
                result = future.result(timeout=2)
                if result: candidates.append(result)
            except: pass
    except TimeoutError:
        done_count    = sum(1 for f in futures if f.done())
        pending_count = len(futures) - done_count
        for future in futures:
            if future.done():
                try:
                    result = future.result(timeout=0)
                    if result: candidates.append(result)
                except: pass
            else: future.cancel()
        if pending_count > 0:
            print(f"  ⚠️  Scan partial timeout: {done_count}/{len(futures)} selesai ({len(candidates)} kandidat)")
    except Exception as e:
        print(f"  ❌ Scan error: {e}")
    return candidates


# ════════════════════════════════════════════════════
#  INSTANT RE-SCAN
# ════════════════════════════════════════════════════
def trigger_rescan(reason="", priority_symbol=None):
    if priority_symbol: _hot_symbols.appendleft(priority_symbol)
    _rescan_queue.put({"reason": reason, "ts": time.time()})


def instant_rescan_worker(symbols_active):
    while True:
        try:
            event  = _rescan_queue.get(timeout=60)
            reason = event.get("reason", "")
            time.sleep(RE_SCAN_DELAY_SEC)
            slots_free = MAX_POSITIONS - len(open_positions)
            if slots_free <= 0: continue
            killed, _ = check_kill_switch()
            if killed: continue
            flash_dir, _ = detect_flash_move()
            if flash_dir != "none": continue
            if _macro["news"] == "strong_negative": continue
            hot      = [s for s in list(_hot_symbols) if s not in open_positions]
            rest     = [s for s in symbols_active    if s not in open_positions and s not in hot]
            scan_list = hot + rest
            _stats["rescans"] += 1
            print(f"\n  ⚡ RESCAN [{reason}] — {len(scan_list)} symbols, {slots_free} slot")
            try:
                candidates = scan_batch_parallel(scan_list[:40])
            except Exception as e:
                print(f"  ❌ Rescan error: {e}")
                candidates = []
            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS: break
                    sig_str = " | ".join(info.get("signals", [])[:3])
                    print(f"     ⭐ {sym} {direction} Score:{info['score']:.0f} | {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ Rescan: no setup")
        except queue.Empty: pass
        except Exception as e:
            print(f"  ❌ Rescan worker error: {e}")


# ════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    with _lock:
        if symbol in open_positions: return
        if len(open_positions) >= MAX_POSITIONS: return
        open_positions[symbol] = {"_reserved": True}
        if len(open_positions) > MAX_POSITIONS:
            open_positions.pop(symbol, None); return
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if price == 0:
            with _lock: open_positions.pop(symbol, None)
            return
        qty    = calc_qty(symbol, price)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET, quantity=qty)
        entry  = get_price(symbol)
        atr    = info.get("atr", entry * 0.002)
        levels = calc_atr_levels(entry, atr, direction)   # adaptive params
        sl     = levels["sl"]
        tp1    = levels["tp1"]
        tp2    = levels["tp2"]
        ic     = levels["instant_cut"]
        p      = adaptive.get_params()
        if direction == "LONG":
            trail_sl = max(entry * (1 - atr * p.atr_trail_mult / entry), sl)
        else:
            trail_sl = min(entry * (1 + atr * p.atr_trail_mult / entry), sl)
        # trail_activate_pct от adaptive
        trail_act = p.trail_activate_pct
        open_positions[symbol] = {
            "side":             direction,
            "entry":            entry,
            "qty":              qty,
            "qty_remain":       qty,
            "sl":               sl,
            "tp1":              tp1,
            "tp2":              tp2,
            "peak":             entry,
            "trail_sl":         trail_sl,
            "trail_phase":      1,
            "trail_active":     False,
            "trail_activate":   trail_act,   # ← dari adaptive
            "tp1_hit":          False,
            "be_active":        False,
            "open_time":        time.time(),
            "score":            info.get("score", 0),
            "signals":          info.get("signals", []),
            "instant_cut":      ic,
            "instant_cut_done": False,
            "mom_pct":          info.get("mom_pct", 0),
            "entry_candle":     0,
            "atr":              atr,
            "scalp_mode":       info.get("scalp_mode", "TREND"),
            "btc_trend":        info.get("btc_trend", "UNKNOWN"),
            "fng":              _macro.get("fng", 50),
        }
        print(f"\n  {'🟢' if direction=='LONG' else '🔴'} [{symbol}] {direction} @{entry:.5g}")
        print(f"     ATR:{atr:.5g}({levels['atr_pct']*100:.2f}%) | SL:{levels['sl_pct']*100:.2f}% | "
              f"TP1:{levels['tp1_pct']*100:.2f}% | TP2:{levels['tp2_pct']*100:.2f}%")
        print(f"     Trail DELAYED (aktif > {trail_act*100:.2f}%) | Score:{info['score']:.0f} | "
              f"Hold≤{adaptive.get_max_holding():.1f}m")
        print(f"     {' | '.join(info.get('signals', [])[:3])}")
        _stats["total_trades"] += 1
    except Exception as e:
        with _lock: open_positions.pop(symbol, None)
        print(f"  ❌ [{symbol}] Entry error: {e}")


def partial_close_tp1(symbol):
    pos = open_positions.get(symbol)
    if pos is None or pos.get("tp1_hit"): return
    try:
        amt = get_exchange_amt(symbol)
        if amt is None or amt == 0:
            pos["tp1_hit"] = True; return
        close_qty = round_step(abs(amt) * TP1_CLOSE_RATIO, get_sym_info(symbol)["step"])
        close_qty = max(close_qty, get_sym_info(symbol)["minQty"])
        if close_qty > abs(amt): close_qty = abs(amt)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=close_qty, reduceOnly=True)
        exit_p = get_price(symbol)
        side   = pos["side"]
        pnl    = ((exit_p - pos["entry"]) if side == "LONG" else
                  (pos["entry"] - exit_p)) * close_qty
        hold_s = time.time() - pos["open_time"]
        print(f"  🎯 [{symbol}] TP1 ({hold_s:.0f}s) PnL:{pnl:+.4f}U")
        pos["tp1_hit"]    = True
        pos["qty_remain"] = abs(amt) - close_qty
        pos["be_active"]  = True
        pos["sl"]         = round(pos["entry"] * (1 + TRAIL_BE_PCT if side=="LONG" else 1 - TRAIL_BE_PCT), 8)
        pos["trail_phase"]  = 2
        pos["trail_active"] = True
        pos["peak"]         = exit_p
        p_adp = adaptive.get_params()
        if side == "LONG":
            pos["trail_sl"] = exit_p * (1 - pos["atr"] * p_adp.atr_trail_mult / exit_p)
        else:
            pos["trail_sl"] = exit_p * (1 + pos["atr"] * p_adp.atr_trail_mult / exit_p)
        _stats["tp1_hits"]  += 1
        _stats["wins"]      += 1
        _stats["total_pnl"] += pnl
        _stats["pnl_history"].append(pnl)
        update_kill_switch_after_trade(pnl)
        _perf[symbol]["wins"] += 1; _perf[symbol]["pnl"] += pnl; _perf[symbol]["trades"] += 1
        if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl
        trade_log.append({"symbol": symbol, "side": side, "pnl": round(pnl,4),
                          "reason": "TP1 Partial", "hold_sec": int(hold_s)})

        # ── ADAPTIVE record ──────────────────────────────
        adaptive.record_trade(
            symbol=symbol, side=side, entry=pos["entry"], exit_price=exit_p,
            pnl=pnl, hold_sec=int(hold_s), reason="TP1 Partial",
            signals=pos.get("signals",[]), score=pos.get("score",0),
            scalp_mode=pos.get("scalp_mode","TREND"),
            btc_trend=pos.get("btc_trend","UNKNOWN"),
            mom_pct=pos.get("mom_pct",0),
            atr_pct=pos.get("atr",0)/pos["entry"] if pos["entry"]>0 else 0,
            fng=pos.get("fng",50))

        _hot_symbols.appendleft(symbol)
        print_stats_inline()
    except Exception as e:
        print(f"  ❌ [{symbol}] TP1 error: {e}")
        pos["tp1_hit"] = True


def close_trade(symbol, reason=""):
    try:
        amt = get_exchange_amt(symbol)
        if amt is None: return False
        if amt == 0:
            with _lock: open_positions.pop(symbol, None)
            set_symbol_cooldown(symbol)
            trigger_rescan(f"close@{symbol}", priority_symbol=symbol)
            return True
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=abs(amt), reduceOnly=True)
        with _lock:
            pos = open_positions.pop(symbol, None)
        if pos:
            exit_p  = get_price(symbol)
            qty_r   = pos.get("qty_remain", pos["qty"])
            side    = pos["side"]
            pnl     = ((exit_p - pos["entry"]) if side=="LONG" else
                       (pos["entry"] - exit_p)) * qty_r
            pct     = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
            hold_s  = time.time() - pos["open_time"]
            emoji   = "🟢" if pnl >= 0 else "🔴"
            be_tag  = "[BE]" if pos.get("be_active") else ""
            print(f"  {emoji} [{symbol}] CLOSE — {reason}{be_tag} | {hold_s:.0f}s")
            print(f"     PnL: {pnl:+.4f}U ({pct:+.2f}%)")
            trade_log.append({"symbol": symbol, "side": side, "pnl": round(pnl,4),
                               "reason": reason, "hold_sec": int(hold_s)})

            # ── ADAPTIVE record ──────────────────────────────────
            adaptive.record_trade(
                symbol=symbol, side=side, entry=pos["entry"], exit_price=exit_p,
                pnl=pnl, hold_sec=int(hold_s), reason=reason,
                signals=pos.get("signals",[]), score=pos.get("score",0),
                scalp_mode=pos.get("scalp_mode","TREND"),
                btc_trend=pos.get("btc_trend","UNKNOWN"),
                mom_pct=pos.get("mom_pct",0),
                atr_pct=pos.get("atr",0)/pos["entry"] if pos["entry"]>0 else 0,
                fng=pos.get("fng",50))

            _stats["total_pnl"]  += pnl
            _stats["pnl_history"].append(pnl)
            update_kill_switch_after_trade(pnl)
            _perf[symbol]["trades"] += 1; _perf[symbol]["pnl"] += pnl
            if pnl >= 0:
                _stats["wins"] += 1; _perf[symbol]["wins"] += 1
                if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl
            else:
                _stats["losses"] += 1; _perf[symbol]["losses"] += 1
                if pnl < _stats["worst_trade"]: _stats["worst_trade"] = pnl
            regime = pos.get("scalp_mode", "UNKNOWN")
            _perf_regime[regime]["pnl"] += pnl
            if pnl >= 0: _perf_regime[regime]["wins"] += 1
            else:        _perf_regime[regime]["losses"] += 1
            if "TP2"   in reason: _stats["tp2_hits"]    += 1
            if "SL"    in reason or "Stop" in reason: _stats["sl_hits"] += 1
            if "Force" in reason: _stats["force_closes"] += 1
            if "Inst"  in reason: _stats["instant_cuts"] += 1
            print_stats_inline()
            set_symbol_cooldown(symbol)
            trigger_rescan(f"close@{symbol}({reason})", priority_symbol=symbol)
        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Close error: {e}")
        return False


# ════════════════════════════════════════════════════
#  POSITION MONITOR v14-ADAPTIVE
# ════════════════════════════════════════════════════
def manage_positions():
    if not open_positions: return
    flash_dir, flash_pct = detect_flash_move()

    for symbol in list(open_positions.keys()):
        pos = open_positions.get(symbol)
        if pos is None or pos.get("_reserved"): continue
        price = get_price(symbol)
        if price == 0: continue
        side  = pos["side"]
        entry = pos["entry"]
        atr   = pos.get("atr", entry * 0.002)
        pos["entry_candle"] = pos.get("entry_candle", 0) + 1

        # ── Holding time dari adaptive ────────────────────
        hold_min   = (time.time() - pos["open_time"]) / 60
        hold_limit = adaptive.get_max_holding()                # ← ADAPTIVE
        if hold_min >= hold_limit * 0.95:
            close_trade(symbol, f"⏰Force({hold_min:.1f}m)")
            continue

        if flash_dir == "crash" and side == "LONG":
            close_trade(symbol, f"⚡FlashCrash-{flash_pct:.1f}%"); continue
        elif flash_dir == "pump" and side == "SHORT":
            close_trade(symbol, f"⚡FlashPump+{flash_pct:.1f}%"); continue

        # ── Instant cut ───────────────────────────────────
        within_window = pos.get("entry_candle", 0) <= (INSTANT_CUT_WINDOW * 5)
        if not pos.get("instant_cut_done") and not pos.get("tp1_hit") and within_window:
            ic = pos["instant_cut"]
            if (side == "LONG" and price <= ic) or (side == "SHORT" and price >= ic):
                pos["instant_cut_done"] = True
                close_trade(symbol, "⚡InstCut"); continue
        elif not within_window:
            pos["instant_cut_done"] = True

        # ── Trail activate pct dari adaptive ──────────────
        trail_act = pos.get("trail_activate", adaptive.get_params().trail_activate_pct)
        p_adp     = adaptive.get_params()

        if side == "LONG":
            profit_pct = (price - entry) / entry
            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close_tp1(symbol); continue
            if not pos["trail_active"] and profit_pct >= trail_act:
                pos["trail_active"] = True
                pos["sl"]       = round(entry * (1 + TRAIL_BE_PCT), 8)
                pos["trail_sl"] = price * (1 - atr * p_adp.atr_trail_mult / price)
                pos["peak"]     = price
                print(f"     🔓 [{symbol}] Trail AKTIF @ {profit_pct*100:+.2f}% → BE")
            if profit_pct >= TRAIL_TIGHT_PCT and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3
            if pos["trail_active"] and price > pos["peak"]:
                pos["peak"] = price
                tm   = p_adp.atr_trail_tight if pos["trail_phase"] >= 3 else p_adp.atr_trail_mult
                pos["trail_sl"] = max(pos["trail_sl"], price * (1 - atr * tm / price))
            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨TP2"); continue
            if pos["trail_active"] and price <= pos["trail_sl"]:
                close_trade(symbol, "🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"); continue
            if price <= pos["sl"]:
                close_trade(symbol, "🛑SL"); continue
            pnl  = (price - entry) * pos.get("qty_remain", pos["qty"])
            tp_s = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            act  = "✅" if pos["trail_active"] else "⏸️ "
            print(f"  📌 [{symbol}] L@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) | "
                  f"{pnl:+.3f}U | {hold_min:.1f}m | TSL[{act}P{pos['trail_phase']}]:{pos['trail_sl']:.5g} {tp_s}")
        else:
            profit_pct = (entry - price) / entry
            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close_tp1(symbol); continue
            if not pos["trail_active"] and profit_pct >= trail_act:
                pos["trail_active"] = True
                pos["sl"]       = round(entry * (1 - TRAIL_BE_PCT), 8)
                pos["trail_sl"] = price * (1 + atr * p_adp.atr_trail_mult / price)
                pos["peak"]     = price
                print(f"     🔓 [{symbol}] Trail AKTIF @ {profit_pct*100:+.2f}% → BE")
            if profit_pct >= TRAIL_TIGHT_PCT and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3
            if pos["trail_active"] and price < pos["peak"]:
                pos["peak"] = price
                tm   = p_adp.atr_trail_tight if pos["trail_phase"] >= 3 else p_adp.atr_trail_mult
                pos["trail_sl"] = min(pos["trail_sl"], price * (1 + atr * tm / price))
            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨TP2"); continue
            if pos["trail_active"] and price >= pos["trail_sl"]:
                close_trade(symbol, "🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"); continue
            if price >= pos["sl"]:
                close_trade(symbol, "🛑SL"); continue
            pnl  = (entry - price) * pos.get("qty_remain", pos["qty"])
            tp_s = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            act  = "✅" if pos["trail_active"] else "⏸️ "
            print(f"  📌 [{symbol}] S@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) | "
                  f"{pnl:+.3f}U | {hold_min:.1f}m | TSL[{act}P{pos['trail_phase']}]:{pos['trail_sl']:.5g} {tp_s}")


# ════════════════════════════════════════════════════
#  ANALYTICS
# ════════════════════════════════════════════════════
def calc_expectancy():
    wins   = [t["pnl"] for t in trade_log if t["pnl"] > 0]
    losses = [t["pnl"] for t in trade_log if t["pnl"] < 0]
    if not wins and not losses: return 0.0
    wr    = len(wins) / (len(wins) + len(losses))
    avg_w = sum(wins) / len(wins) if wins else 0
    avg_l = abs(sum(losses) / len(losses)) if losses else 0
    return round((wr * avg_w) - ((1 - wr) * avg_l), 5)


def calc_sharpe():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 5: return 0.0
    arr  = np.array(pnls)
    std  = float(np.std(arr))
    return round(float(np.mean(arr)) / std, 3) if std else 0.0


def calc_max_drawdown():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 2: return 0.0
    eq   = np.cumsum(pnls)
    return round(float(np.min(eq - np.maximum.accumulate(eq))), 4)


def print_stats_inline():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["total_pnl"]
    exp  = calc_expectancy()
    bar  = ("█" * _stats["wins"] + "░" * _stats["losses"])[-20:]
    emoji = "💚" if pnl >= 0 else "🔴"
    print(f"     ┌─ 📊 {n}T | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']} | {emoji}PnL:{pnl:+.4f}U | Exp:{exp:+.4f}U")
    print(f"     └─ TP1:{_stats['tp1_hits']} TP2:{_stats['tp2_hits']} SL:{_stats['sl_hits']} "
          f"⚡Cut:{_stats['instant_cuts']} Force:{_stats['force_closes']} [{bar}]")
    # Mini adaptive status
    p = adaptive.get_params()
    print(f"     🧠 Score≥{adaptive.get_effective_min_score()} Mom≥{p.min_momentum_pct*100:.2f}% "
          f"Hold≤{p.max_holding_min:.1f}m SL×{p.atr_sl_mult:.1f} Trail×{p.atr_trail_mult:.2f} "
          f"| {adaptive.dyn_score.info}")


def print_stats():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["session_start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    pnl  = _stats["total_pnl"]
    emoji = "💚" if pnl >= 0 else "🔴"
    exp  = calc_expectancy()
    sr   = calc_sharpe()
    mdd  = calc_max_drawdown()
    ks   = _kill_switch
    print(f"\n  {'─'*64}")
    print(f"  📊 SESSION {sess*60:.0f}m | {tph:.0f} T/jam | Rescans:{_stats['rescans']}")
    print(f"  🎯 {n} trades | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {emoji} Total P&L:  {pnl:+.4f} USDT")
    print(f"  📐 Expectancy: {exp:+.5f}U | Sharpe: {sr:.2f} | MaxDD: {mdd:.4f}U")
    print(f"  📈 Best:{_stats['best_trade']:+.4f}U │ 📉 Worst:{_stats['worst_trade']:+.4f}U")
    print(f"  🎯TP1:{_stats['tp1_hits']} │ ✨TP2:{_stats['tp2_hits']} │ 🛑SL:{_stats['sl_hits']} "
          f"│ ⚡Cut:{_stats['instant_cuts']} │ ⏰Force:{_stats['force_closes']}")
    print(f"  🚫 Skip: Chop:{_stats['skipped_chop']} NoMom:{_stats['skipped_no_momentum']} "
          f"Spread:{_stats['skipped_spread']} Session:{_stats.get('skipped_session',0)} "
          f"MeanRev:{_stats.get('skipped_mean_rev',0)} Adaptive:{_stats.get('skipped_adaptive',0)}")
    print(f"  🛡️  KS: {'ACTIVE('+ks['reason']+')' if ks['active'] else 'OK'} | "
          f"CL:{ks['consec_losses']} | DailyPnL:{ks['daily_pnl']:+.2f}U | Lag:{ks['api_lag']*1000:.0f}ms")
    sym_sorted = sorted(_perf.items(), key=lambda x: x[1]["pnl"], reverse=True)
    if sym_sorted:
        print(f"  🏆 Top symbols:")
        for sym, data in sym_sorted[:5]:
            wr_s = data["wins"] / data["trades"] * 100 if data["trades"] else 0
            print(f"     {sym:<14} {data['trades']}T WR:{wr_s:.0f}% PnL:{data['pnl']:+.4f}U")
    if _perf_regime:
        print(f"  📊 By regime:")
        for regime, data in _perf_regime.items():
            total_r = data["wins"] + data["losses"]
            wr_r    = data["wins"] / total_r * 100 if total_r else 0
            print(f"     {regime:<12} WR:{wr_r:.0f}% PnL:{data['pnl']:+.4f}U")
    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            e    = "🟢" if t["pnl"] > 0 else "🔴"
            hold = f"{t.get('hold_sec',0)//60}m{t.get('hold_sec',0)%60}s"
            print(f"     {e} {t['symbol']:<14} {t['side']} {t['pnl']:+.4f}U ({hold}) — {t['reason'][:30]}")
    # ── ADAPTIVE STATUS ──────────────────────────────────
    adaptive.print_status()
    print(f"  {'─'*64}")


# ════════════════════════════════════════════════════
#  THREADS
# ════════════════════════════════════════════════════
def position_monitor_thread():
    while True:
        try:
            if open_positions:
                manage_positions()
        except Exception as e:
            print(f"  ❌ Monitor error: {e}")
        time.sleep(POSITION_MONITOR_SEC)


# ════════════════════════════════════════════════════
#  MAIN LOOP
# ════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  🎯🧠 BOT SCALPING v14-ADAPTIVE — SELF-LEARNING ENGINE       ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Leverage:{LEVERAGE}x │ Per trade:${ORDER_USDT} │ Max posisi:{MAX_POSITIONS}              ║")
    print(f"║  SL: ATR×{ATR_SL_MULT} (adaptive) │ TP1: ATR×{ATR_TP1_MULT} │ TP2: ATR×{ATR_TP2_MULT}  ║")
    print(f"║  Trail: DELAYED (adaptive activate %) │ Width: adaptive     ║")
    print(f"║  🧠 Adaptive: adjust tiap 10 trade | window 50 trade        ║")
    print(f"║  🧠 Fixes: ForceClose, SL rate, RR ratio, signal blacklist  ║")
    print(f"║  💀 KillSwitch: DailyLoss${abs(DAILY_LOSS_LIMIT)} | ConsecLoss×{CONSEC_LOSS_MAX}         ║")
    print(f"║  ⏰ Session filter: jam {BAD_HOURS_UTC} UTC → score+{BAD_HOURS_MIN_SCORE-MIN_SCORE}         ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    print("\n  ⏳ Validasi symbols...")
    symbols_active = validate_symbols()
    print(f"  📊 {len(symbols_active)} symbols aktif")
    print(f"  📦 Pre-load symbol info...")
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(get_sym_info, symbols_active[:60]))
    print(f"  🌐 Refresh macro...")
    refresh_macro(); update_btc_price()
    print(f"\n  ✅ BTC:{_macro['btc_trend_5m']} | Mode:{_macro['scalp_mode']} | F&G:{_macro['fng']}")
    print(f"  🧠 Adaptive engine: ACTIVE | Base score:{MIN_SCORE} | Window:50 trades")
    print(f"  🚀 Start dalam 3 detik...\n")
    time.sleep(3)

    pm_thread = threading.Thread(target=position_monitor_thread, daemon=True)
    pm_thread.start()
    print("  🔧 Position monitor: START ✅")
    rs_thread = threading.Thread(target=instant_rescan_worker, args=(symbols_active,), daemon=True)
    rs_thread.start()
    print("  🔧 Re-scan thread: START ✅\n")

    global _scan_batch_idx
    cycle         = 0
    total_batches = math.ceil(len(symbols_active) / BATCH_SIZE)

    while True:
        cycle += 1
        refresh_macro(); update_btc_price()
        if cycle % 30 == 0: check_api_latency()

        flash_dir, flash_pct = detect_flash_move()
        flash_info = f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir != "none" else ""
        utc_h  = time.gmtime().tm_hour
        sess_t = f"⚠️ JAM_JELEK(UTC{utc_h})" if utc_h in BAD_HOURS_UTC else ""

        p_adp  = adaptive.get_params()
        print(f"\n{'═'*67}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} | F&G:{_macro['fng']} | "
              f"BTC1m:{_macro['btc_trend_1m']} 5m:{_macro['btc_trend_5m']} {flash_info} {sess_t}")
        print(f"  Mode:{_macro['scalp_mode']} | Breadth:{_macro['market_breadth']*100:.0f}% | "
              f"News:{_macro['news']} | Posisi({len(open_positions)}/{MAX_POSITIONS}): "
              f"{list(open_positions.keys()) or '—'}")
        print(f"  🧠 Score≥{adaptive.get_effective_min_score()} | Mom≥{p_adp.min_momentum_pct*100:.2f}% | "
              f"Hold≤{p_adp.max_holding_min:.1f}m | SL×{p_adp.atr_sl_mult:.1f} | "
              f"Trail×{p_adp.atr_trail_mult:.2f} | {adaptive.dyn_score.info}")

        slots_free = MAX_POSITIONS - len(open_positions)
        ks_active, ks_reason = check_kill_switch()
        if ks_active:
            resume_in = max(0, _kill_switch["resume_time"] - time.time())
            print(f"  🚨 KILL SWITCH: {ks_reason} | Resume in: {resume_in/60:.1f}m")

        skip_reason = None
        if slots_free == 0:                          skip_reason = "posisi_penuh"
        elif _macro["news"] == "strong_negative":    skip_reason = "bad_news"
        elif flash_dir != "none":                    skip_reason = f"flash_{flash_dir}"
        elif ks_active:                              skip_reason = f"kill:{ks_reason}"

        if not skip_reason:
            top_mv      = get_top_movers(symbols_active, n=40)
            top_mv_syms = [s for s, _, _ in top_mv if s not in open_positions]
            batch_start   = _scan_batch_idx * BATCH_SIZE
            batch_regular = [s for s in symbols_active[batch_start:batch_start + BATCH_SIZE]
                             if s not in open_positions and s not in top_mv_syms]
            _scan_batch_idx = (_scan_batch_idx + 1) % total_batches
            scan_list   = top_mv_syms[:20] + batch_regular[:15]
            top_str     = " | ".join(f"{s}({p:+.1f}%)" for s, p, _ in top_mv[:5])
            print(f"  📊 TopMovers: {top_str}")
            print(f"  🔍 Scan {len(scan_list)} syms | Chop:{_stats['skipped_chop']} "
                  f"Spread:{_stats['skipped_spread']} Adaptive:{_stats['skipped_adaptive']}")
            try:
                candidates = scan_batch_parallel(scan_list)
            except Exception as e:
                print(f"  ❌ Scan loop error: {e}")
                candidates = []
            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
                print(f"  🎯 {len(candidates)} setup! Ambil top {min(len(candidates), slots_free)}")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS: break
                    sig_str = " | ".join(info.get("signals",[])[:3])
                    print(f"     ⭐ {sym} {direction} Mom:{info.get('mom_pct',0)*100:+.2f}% "
                          f"24h:{info.get('pct_24h',0):+.1f}% Score:{info['score']:.0f}")
                    print(f"        {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ No setup found")
        else:
            print(f"  ⏸️  Skip: {skip_reason}")

        if cycle % 30 == 0:
            print_stats()

        print(f"  ⏱️  Next:{SCAN_INTERVAL}s | KS:{_kill_switch['consec_losses']}CL/"
              f"{_kill_switch['daily_pnl']:+.2f}U | Rescans:{_stats['rescans']} | "
              f"Lag:{_kill_switch['api_lag']*1000:.0f}ms")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
