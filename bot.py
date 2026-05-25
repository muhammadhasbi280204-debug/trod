"""
Bot Scalping v15 — SMART ADAPTIVE ENGINE 🎯🧠
===============================================

ROOT CAUSE ANALYSIS dari v14-adaptive yang gagal:
─────────────────────────────────────────────────
✗ Adaptive engine malah MEMPERBURUK: LS +15pts → score 60 → hampir tidak entry
✗ NoMom:127K skip — momentum filter terlalu ketat (adaptive naikkan terus)
✗ Chop:22K skip — chop filter terlalu agresif
✗ BTC BULL diblacklist FC pattern → skip saat market justru bagus
✗ Force:70% — masalah bukan di entry filter tapi di trade management
✗ Adaptive tidak punya batas aman — bisa spiral jadi terlalu ketat

PERBAIKAN v15:
──────────────
✅ ADAPTIVE TERBATAS: setiap parameter punya corridor min/max yang masuk akal
✅ ADAPTIVE HANYA ADJUST ±SMALL STEP, tidak bisa spiral
✅ FC PATTERN: tidak blacklist BTC trend global (terlalu broad), 
   hanya skip per-symbol yang konsisten force-close loss
✅ MOMENTUM FILTER: lebih relaxed default, adaptive hanya naik kalau
   force_rate DAN force_loss_rate KEDUANYA tinggi
✅ CHOP FILTER: relaxed threshold, butuh 3 kondisi bukan 2
✅ SCORE STREAK: cap +10pts max (bukan +20), reset lebih cepat
✅ ENTRY FREQUENCY FIX: target 5-8 trades/jam bukan 2/jam
✅ TRADE MANAGEMENT FIX: 
   - Partial close TP1 dulu, sisanya trail ke TP2
   - BE stop setelah TP1 hit, bukan langsung cut
   - Holding time optimal berdasarkan data (target 3-4 menit)
✅ MEAN_REV: disable default kecuali adaptive buktikan profitable
✅ DIAGNOSTIC: print kenapa setiap scan tidak ada setup
"""

import os, time, math, json, threading, queue, re
import requests
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# ══════════════════════════════════════════════════════════
# TESTNET / REAL
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
# ══════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════
#  CONFIG v15
# ════════════════════════════════════════════════════

LEVERAGE              = 20
ORDER_USDT            = 2
MAX_POSITIONS         = 3

# ── ATR MULTIPLIER ───────────────────────────────────────
ATR_SL_MULT           = 1.5       # v15: sedikit lebih lebar dari 1.2
ATR_TP1_MULT          = 1.8       # v15: lebih dekat, lebih sering hit
ATR_TP2_MULT          = 3.5
ATR_TRAIL_MULT        = 1.0       # v15: lebih longgar, TP2 lebih sering hit
ATR_TRAIL_TIGHT_MULT  = 0.6

MIN_SL_PCT            = 0.0008
MAX_SL_PCT            = 0.0070
MIN_TP1_PCT           = 0.0015    # v15: lebih kecil, TP1 lebih mudah hit
MAX_TP2_PCT           = 0.0250

# ── TRAIL ────────────────────────────────────────────────
TRAIL_ACTIVATE_PCT    = 0.0015    # v15: lebih cepat aktif
TRAIL_BE_PCT          = 0.0003
TRAIL_TIGHT_PCT       = 0.0035

TP1_CLOSE_RATIO       = 0.50      # v15: tutup 50% di TP1, biar TP2 hit lebih sering
TP2_CLOSE_RATIO       = 0.50

# ── INSTANT CUT ──────────────────────────────────────────
INSTANT_CUT_MULT      = 0.6       # v15: sedikit lebih longgar
INSTANT_CUT_WINDOW    = 4

# ── CHOP FILTER — RELAXED ────────────────────────────────
CHOP_INDEX_THRESHOLD  = 61.0      # v15: naik dari 58 → lebih sedikit yang di-skip
MIN_BB_WIDTH_PCT      = 0.003     # v15: turun dari 0.005
MAX_EMA_CROSS_FREQ    = 5         # v15: naik dari 3
CHOP_MIN_CONDITIONS   = 3         # v15: butuh 3 kondisi (bukan 2) untuk dianggap chop

# ── MOMENTUM FILTER — RELAXED ────────────────────────────
MIN_MOMENTUM_PCT      = 0.0010    # v15: turun dari 0.0018 (ini penyebab 127K skip!)
MIN_VOL_SURGE         = 1.3       # v15: turun dari 1.6
MIN_TREND_CANDLES     = 2         # v15: turun dari 3

# ── SCORE FILTER ─────────────────────────────────────────
MIN_SCORE             = 42        # v15: turun dari 45
MIN_ENTRY_SIGNALS     = 2

# ── KECEPATAN ────────────────────────────────────────────
SCAN_INTERVAL         = 3
POSITION_MONITOR_SEC  = 1
SCAN_DELAY_MS         = 0.040
BATCH_SIZE            = 20        # v15: naik dari 15
MAX_HOLDING_MIN       = 4.5       # v15: turun dari 5 — force close lebih cepat
SYMBOL_COOLDOWN_SEC   = 8         # v15: turun dari 10
RE_SCAN_DELAY_SEC     = 0.2

# ── SESSION FILTER ───────────────────────────────────────
BAD_HOURS_UTC         = {4, 5, 6}
BAD_HOURS_MIN_SCORE   = 55        # v15: turun dari 60

# ── KILL SWITCH ──────────────────────────────────────────
DAILY_LOSS_LIMIT      = -6.0
CONSEC_LOSS_MAX       = 6         # v15: naik dari 5 (beri lebih banyak kesempatan)
CONSEC_LOSS_PAUSE_MIN = 20        # v15: turun dari 30

# ── CACHE ────────────────────────────────────────────────
OHLCV_CACHE_TTL_1M    = 2
OHLCV_CACHE_TTL_3M    = 4
OHLCV_CACHE_TTL_5M    = 5
OHLCV_CACHE_TTL_15M   = 30
OHLCV_CACHE_TTL_1H    = 1800
TICKER24H_TTL         = 8
FUNDING_TTL           = 30
TOP_MOVERS_TTL        = 8

# ── FILTER LAIN ──────────────────────────────────────────
MIN_FNG               = 12        # v15: turun dari 15
MAX_FNG_LONG          = 93
MIN_BREADTH           = 0.0
MAX_SL_ATR_PCT        = 0.012     # v15: naik dari 0.010
MAX_SPREAD_RATIO      = 0.35      # v15: naik dari 0.30

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

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}


# ════════════════════════════════════════════════════
#  STATE GLOBAL
# ════════════════════════════════════════════════════
open_positions     = {}
trade_log          = []
_ohlcv_cache       = {}
_sym_info          = {}
_sym_cooldown      = {}
_btc_price_history = deque(maxlen=300)
_scan_batch_idx    = 0
_lock              = threading.Lock()
_executor          = ThreadPoolExecutor(max_workers=15)
_rescan_queue      = queue.Queue()
_hot_symbols       = deque(maxlen=30)
_ticker24h_cache   = {}
_ticker24h_ts      = 0
_funding_cache     = {}
_funding_ts        = 0
_top_movers        = []
_top_movers_ts     = 0

_kill_switch = {
    "active": False, "reason": "", "resume_time": 0,
    "consec_losses": 0, "daily_pnl": 0.0, "daily_reset_ts": 0,
    "last_api_check": 0, "api_lag": 0.0,
}

_perf        = defaultdict(lambda: {"wins":0,"losses":0,"pnl":0.0,"trades":0})
_perf_regime = defaultdict(lambda: {"wins":0,"losses":0,"pnl":0.0})

_macro = {
    "fng": 50, "fng_label": "Neutral",
    "btc_trend_1m": "UNKNOWN", "btc_trend_5m": "UNKNOWN",
    "btc_trend_15m": "UNKNOWN", "btc_trend_1h": "UNKNOWN",
    "market_breadth": 0.5, "news": "neutral", "scalp_mode": "TREND",
    "last_fng": 0, "last_btc": 0, "last_breadth": 0, "last_news": 0,
}

_stats = {
    "total_trades":0,"wins":0,"losses":0,"total_pnl":0.0,
    "best_trade":0.0,"worst_trade":0.0,"tp1_hits":0,"tp2_hits":0,
    "sl_hits":0,"instant_cuts":0,"force_closes":0,"rescans":0,
    "skipped_no_momentum":0,"skipped_chop":0,"skipped_spread":0,
    "skipped_session":0,"skipped_mean_rev":0,"skipped_adaptive":0,
    "pnl_history":deque(maxlen=200),"session_start":time.time(),
    # diagnostic counters
    "skip_no_dir":0,"skip_funding":0,"skip_btc":0,"skip_cont":0,
    "skip_illiquid":0,"skip_ob":0,"skip_atr_big":0,
}


# ════════════════════════════════════════════════════
#  ███  ADAPTIVE ENGINE v2 — BOUNDED & SAFE  ███
# ════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:str; side:str; entry:float; exit_price:float; pnl:float
    hold_sec:int; reason:str; signals:list; score:int
    scalp_mode:str; btc_trend:str; mom_pct:float; atr_pct:float
    fng:int; hour_utc:int; ts:float = field(default_factory=time.time)

    @property
    def win(self):          return self.pnl > 0
    @property
    def force_closed(self): return "Force" in self.reason
    @property
    def sl_hit(self):       return "SL" in self.reason or "Stop" in self.reason


# Parameter adaptive — semua punya default = nilai v15 yang sudah relaxed
@dataclass
class AdaptedParams:
    # Bounded corridors — tidak bisa keluar dari range ini (fix spiral)
    min_score:          int   = 42    # range [38, 55]  (bukan sampai 72!)
    min_entry_signals:  int   = 2     # range [2, 3]
    min_momentum_pct:   float = 0.0010  # range [0.0008, 0.0022]
    min_vol_surge:      float = 1.3   # range [1.1, 1.8]
    min_trend_candles:  int   = 2     # range [1, 3]
    atr_sl_mult:        float = 1.5   # range [1.0, 2.0]
    atr_tp1_mult:       float = 1.8   # range [1.4, 2.5]
    atr_tp2_mult:       float = 3.5   # range [2.5, 5.0]
    atr_trail_mult:     float = 1.0   # range [0.6, 1.4]
    atr_trail_tight:    float = 0.6   # range [0.4, 0.9]
    max_holding_min:    float = 4.5   # range [3.0, 7.0]
    trail_activate_pct: float = 0.0015  # range [0.0010, 0.0035]
    mean_rev_enabled:   bool  = False
    # Hanya blacklist per-symbol (bukan BTC trend global!)
    bad_symbols:        list  = field(default_factory=list)
    # Extra score untuk streak (terbatas ±10)
    streak_score_adj:   int   = 0     # range [-5, +10]


def _normalize_signal(s: str) -> str:
    return re.sub(r'[\d\.\+\-\%\:x↑↓]', '', s).strip()


class AdaptiveEngineV2:
    """
    Adaptive engine yang BOUNDED dan SAFE.
    
    Perubahan dari v1:
    - Setiap parameter punya hard corridor, tidak bisa spiral
    - Streak adjustment max +10 (bukan +20)
    - FC pattern: hanya blacklist per-symbol (tidak BTC trend global)
    - Momentum hanya diperketat kalau force_loss_rate SANGAT tinggi (>60%)
    - Rolling window 30 (bukan 50) untuk lebih cepat belajar
    - Adapt setiap 5 trade (bukan 10) tapi step lebih kecil
    """

    WINDOW      = 30
    ADAPT_EVERY = 5

    # Hard boundaries — parameter tidak boleh keluar dari sini
    BOUNDS = {
        "min_score":         (38, 55),
        "min_momentum_pct":  (0.0008, 0.0022),
        "min_vol_surge":     (1.1, 1.8),
        "atr_sl_mult":       (1.0, 2.0),
        "atr_tp1_mult":      (1.4, 2.5),
        "atr_tp2_mult":      (2.5, 5.0),
        "atr_trail_mult":    (0.6, 1.4),
        "max_holding_min":   (3.0, 7.0),
        "trail_activate_pct":(0.0010, 0.0035),
        "streak_score_adj":  (-5, 10),
    }

    def __init__(self):
        self.params      = AdaptedParams()
        self._trades:deque[TradeRecord] = deque(maxlen=200)
        self._recent:deque[TradeRecord] = deque(maxlen=self.WINDOW)
        self._count      = 0
        self._streak     = 0
        self._lock       = threading.Lock()
        self._adapt_log  = deque(maxlen=50)
        self._sym_fc_count:dict = defaultdict(int)   # symbol → force-loss count

    def record(self, trade: TradeRecord):
        with self._lock:
            self._trades.append(trade)
            self._recent.append(trade)
            self._count += 1

            # Streak tracking
            if trade.win: self._streak = max(0, self._streak) + 1
            else:         self._streak = min(0, self._streak) - 1

            # Per-symbol force-loss tracking
            if trade.force_closed and not trade.win:
                self._sym_fc_count[trade.symbol] += 1

            # Update streak score adjustment (bounded ±)
            s = self._streak
            if s <= -4:   adj = min(10, abs(s) * 2)    # max +10
            elif s >= 3:  adj = max(-5, -s)             # min -5 (relax saat winning)
            else:         adj = 0
            self.params.streak_score_adj = self._clamp("streak_score_adj", adj)

            if self._count % self.ADAPT_EVERY == 0:
                self._adapt()

    def _adapt(self):
        recent = list(self._recent)
        if len(recent) < 5: return
        n      = len(recent)
        wins   = [t for t in recent if t.win]
        forces = [t for t in recent if t.force_closed]
        sls    = [t for t in recent if t.sl_hit]
        wr     = len(wins) / n
        fr     = len(forces) / n
        sr     = len(sls)   / n
        force_loss_rate = sum(1 for t in forces if not t.win) / max(len(forces), 1)
        avg_pnl = sum(t.pnl for t in recent) / n
        changes = []

        # ── Fix force-close (tapi HANYA jika losses, bukan wins) ──
        # Kalau force_rate tinggi TAPI banyak yang profit → extend holding
        # Kalau force_rate tinggi DAN mostly loss → cari entry yang lebih kuat
        if fr > 0.60:
            if force_loss_rate < 0.50:
                # Force tapi profit → extend holding time
                old = self.params.max_holding_min
                new = self._clamp("max_holding_min", old + 0.3)
                if new != old:
                    self.params.max_holding_min = new
                    changes.append(f"⏰ Hold {old:.1f}→{new:.1f}m (force profitable)")
            elif force_loss_rate > 0.65:
                # Force DAN loss → perketat momentum sedikit (step kecil!)
                old = self.params.min_momentum_pct
                new = self._clamp("min_momentum_pct", old + 0.0001)  # step KECIL: 0.01%
                if new != old:
                    self.params.min_momentum_pct = new
                    changes.append(f"📈 Mom ↑{old*100:.2f}%→{new*100:.2f}% (force_loss)")

        # ── Fix SL rate tinggi ─────────────────────────────────
        if sr > 0.25:
            # Perlebar SL (beri napas), naikkan score sedikit
            old_sl = self.params.atr_sl_mult
            new_sl = self._clamp("atr_sl_mult", old_sl + 0.1)
            if new_sl != old_sl:
                self.params.atr_sl_mult = new_sl
                changes.append(f"🛑 SL×{old_sl:.1f}→{new_sl:.1f} (sl_rate={sr:.0%})")

        # ── Fix expectancy negatif (WR ok tapi masih rugi) ───
        # Biasanya karena TP hits kecil vs SL hits besar
        # Solusi: perlebar TP1 jarak dan longgarkan trail
        if wr > 0.45 and avg_pnl < -0.001:
            old_tp1   = self.params.atr_tp1_mult
            old_trail = self.params.atr_trail_mult
            new_tp1   = self._clamp("atr_tp1_mult",   old_tp1   + 0.05)
            new_trail = self._clamp("atr_trail_mult",  old_trail + 0.05)
            changed   = False
            if new_tp1 != old_tp1:
                self.params.atr_tp1_mult  = new_tp1;  changed = True
            if new_trail != old_trail:
                self.params.atr_trail_mult = new_trail; changed = True
            if changed:
                changes.append(f"✨ TP1×{old_tp1:.2f}→{new_tp1:.2f} Trail×{old_trail:.2f}→{new_trail:.2f}")

        # ── Perform bagus: relax sedikit ─────────────────────
        if wr >= 0.60 and avg_pnl > 0.002:
            if self.params.min_score > 40:
                old = self.params.min_score
                self.params.min_score = self._clamp("min_score", old - 1)
                changes.append(f"😊 Score ↓{old}→{self.params.min_score}")
            if self.params.min_momentum_pct > 0.0009:
                old = self.params.min_momentum_pct
                self.params.min_momentum_pct = self._clamp("min_momentum_pct", old - 0.0001)
                changes.append(f"📉 Mom ↓{old*100:.2f}%→{self.params.min_momentum_pct*100:.2f}%")

        # ── Blacklist symbols yang konsisten force-loss ───────
        new_bad = [s for s, cnt in self._sym_fc_count.items() if cnt >= 4]
        if set(new_bad) != set(self.params.bad_symbols):
            self.params.bad_symbols = new_bad
            changes.append(f"🚫 BadSyms: {new_bad[:3]}")

        # ── Regime: aktifkan MEAN_REV hanya jika terbukti ────
        regime_pnl = defaultdict(list)
        for t in self._trades:
            regime_pnl[t.scalp_mode].append(t.pnl)
        mr_pnls = regime_pnl.get("MEAN_REV", [])
        if len(mr_pnls) >= 8:
            mr_wr = sum(1 for p in mr_pnls if p > 0) / len(mr_pnls)
            if mr_wr >= 0.60 and not self.params.mean_rev_enabled:
                self.params.mean_rev_enabled = True
                changes.append(f"📊 MEAN_REV ON (wr={mr_wr:.0%})")
            elif mr_wr < 0.40 and self.params.mean_rev_enabled:
                self.params.mean_rev_enabled = False
                changes.append(f"📊 MEAN_REV OFF (wr={mr_wr:.0%})")

        # ── Optimize holding dari distribusi PnL ─────────────
        buckets = {"<90s":[],"90-180s":[],"180-270s":[],"270s+":[]},
        bucket_d = {"<90s":[],"90-180s":[],"180-270s":[],"270s+":[]}
        for t in list(self._trades)[-60:]:
            s = t.hold_sec
            if s < 90:    bucket_d["<90s"].append(t.pnl)
            elif s < 180: bucket_d["90-180s"].append(t.pnl)
            elif s < 270: bucket_d["180-270s"].append(t.pnl)
            else:         bucket_d["270s+"].append(t.pnl)
        best_b = max(
            ((k,v) for k,v in bucket_d.items() if len(v)>=3),
            key=lambda x: sum(x[1])/len(x[1]),
            default=None)
        if best_b:
            bmap = {"<90s":2.0,"90-180s":3.5,"180-270s":5.0,"270s+":6.5}
            opt  = bmap.get(best_b[0], self.params.max_holding_min)
            cur  = self.params.max_holding_min
            if abs(opt - cur) > 0.4:
                d   = 0.3 if opt > cur else -0.3
                new = self._clamp("max_holding_min", cur + d)
                if new != cur:
                    self.params.max_holding_min = new
                    changes.append(f"⏱️  Hold {cur:.1f}→{new:.1f}m (best={best_b[0]})")

        if changes:
            log = {
                "ts":n_str(time.time()), "n":n, "wr":f"{wr:.0%}",
                "force":f"{fr:.0%}", "changes":changes
            }
            self._adapt_log.append(log)
            print(f"\n  🧠 ADAPT #{self._count}: WR:{wr:.0%} Force:{fr:.0%} SL:{sr:.0%} AvgPnL:{avg_pnl:+.4f}U")
            for c in changes: print(f"     → {c}")

    def _clamp(self, name, val):
        lo, hi = self.BOUNDS.get(name, (-999,999))
        return type(val)(max(lo, min(hi, val)))

    def effective_min_score(self) -> int:
        return max(38, min(55, self.params.min_score + self.params.streak_score_adj))

    def is_bad_symbol(self, symbol) -> bool:
        return symbol in self.params.bad_symbols

    def rolling_stats(self) -> dict:
        r = list(self._recent)
        if not r: return {"n":0,"wr":0,"fr":0,"sr":0,"avg":0}
        n  = len(r)
        wr = sum(1 for t in r if t.win) / n
        fr = sum(1 for t in r if t.force_closed) / n
        sr = sum(1 for t in r if t.sl_hit) / n
        return {"n":n,"wr":round(wr,3),"fr":round(fr,3),"sr":round(sr,3),
                "avg":round(sum(t.pnl for t in r)/n,5)}

    def print_status(self):
        s  = self.rolling_stats()
        p  = self.params
        st = self._streak
        streak_str = f"❌LS:{abs(st)}" if st<=-3 else f"✅WS:{st}" if st>=3 else f"Streak:{st:+d}"
        print(f"\n  {'─'*62}")
        print(f"  🧠 ADAPTIVE v2 | n={s['n']} | WR:{s['wr']:.0%} | Force:{s['fr']:.0%} | "
              f"SL:{s['sr']:.0%} | Avg:{s['avg']:+.4f}U")
        print(f"     Score: base={p.min_score} adj={p.streak_score_adj:+d} → eff={self.effective_min_score()} | {streak_str}")
        print(f"     Mom≥{p.min_momentum_pct*100:.2f}% Vol≥{p.min_vol_surge:.1f}x "
              f"Hold≤{p.max_holding_min:.1f}m SL×{p.atr_sl_mult:.1f} "
              f"TP1×{p.atr_tp1_mult:.1f} Trail×{p.atr_trail_mult:.2f} Act@{p.trail_activate_pct*100:.2f}%")
        if p.bad_symbols:
            print(f"     🚫 BadSyms: {p.bad_symbols[:5]}")
        log = list(self._adapt_log)
        if log:
            last = log[-1]
            print(f"     Last adapt [{last['ts']}]: {last['changes'][0] if last['changes'] else '—'}")
        print(f"  {'─'*62}")


def n_str(ts): return time.strftime("%H:%M:%S", time.localtime(ts))

# Singleton
adaptive = AdaptiveEngineV2()


# ════════════════════════════════════════════════════
#  KILL SWITCH
# ════════════════════════════════════════════════════
def check_kill_switch():
    ks  = _kill_switch
    now = time.time()
    if ks["active"] and now >= ks["resume_time"]:
        ks["active"]=False; ks["reason"]=""; ks["consec_losses"]=0
        print(f"  ✅ Kill switch CLEARED")
    if ks["active"]: return True, ks["reason"]
    day_start = now - (now % 86400)
    if day_start > ks["daily_reset_ts"]:
        ks["daily_pnl"]=0.0; ks["daily_reset_ts"]=day_start
    if ks["daily_pnl"] <= DAILY_LOSS_LIMIT:
        ks["active"]=True; ks["reason"]=f"daily_loss({ks['daily_pnl']:.2f}U)"
        ks["resume_time"]=day_start+86400
        print(f"  🚨 KS: daily loss {ks['daily_pnl']:.2f}U")
        return True, ks["reason"]
    if ks["consec_losses"] >= CONSEC_LOSS_MAX:
        ks["active"]=True; ks["reason"]=f"consec_loss({ks['consec_losses']})"
        ks["resume_time"]=now+(CONSEC_LOSS_PAUSE_MIN*60)
        print(f"  🚨 KS: {ks['consec_losses']} loss beruntun — pause {CONSEC_LOSS_PAUSE_MIN}m")
        return True, ks["reason"]
    return False, ""

def update_ks(pnl):
    _kill_switch["daily_pnl"] += pnl
    if pnl < 0: _kill_switch["consec_losses"] += 1
    else:        _kill_switch["consec_losses"]  = 0

def check_api_latency():
    try:
        t0=time.time(); client.futures_ping()
        lag=time.time()-t0; _kill_switch["api_lag"]=lag
        if lag>3.0: print(f"  ⚠️ Lag {lag:.2f}s"); return False
        return True
    except: return False


# ════════════════════════════════════════════════════
#  CHOP FILTER — RELAXED (butuh 3 kondisi)
# ════════════════════════════════════════════════════
def calc_choppiness_index(df, period=14):
    if df is None or len(df)<period+2: return 50.0
    try:
        hi=df["high"].values; lo=df["low"].values; cl=df["close"].values
        tr_sum = sum(max(hi[i]-lo[i],abs(hi[i]-cl[i-1]),abs(lo[i]-cl[i-1]))
                     for i in range(-period,0))
        pr = max(hi[-period:])-min(lo[-period:])
        if pr==0 or tr_sum==0: return 50.0
        return round(100*math.log10(tr_sum/pr)/math.log10(period),2)
    except: return 50.0

def is_chop_market(df_5m, direction):
    if df_5m is None or len(df_5m)<20: return False,"no_data"
    reasons=[]
    ci = calc_choppiness_index(df_5m,14)
    if ci > CHOP_INDEX_THRESHOLD: reasons.append(f"CI={ci:.1f}")
    bw = df_5m.iloc[-1].get("bb_width",0.01)
    if bw < MIN_BB_WIDTH_PCT: reasons.append(f"BB<{MIN_BB_WIDTH_PCT*100:.1f}%")
    # EMA cross frequency
    if len(df_5m)>=25:
        e3=df_5m["ema3"].values[-20:]; e9=df_5m["ema9"].values[-20:]
        xf=sum(1 for i in range(1,len(e3)) if
               (e3[i-1]>e9[i-1] and e3[i]<=e9[i]) or
               (e3[i-1]<e9[i-1] and e3[i]>=e9[i]))
        if xf > MAX_EMA_CROSS_FREQ: reasons.append(f"EMAx{xf}")
    # MACD flat
    if len(df_5m)>=10:
        hstd=float(np.std(df_5m["macd_hist"].values[-10:]))
        if hstd<0.00001: reasons.append("MACD_flat")
    # v15: butuh CHOP_MIN_CONDITIONS = 3
    is_chop = len(reasons) >= CHOP_MIN_CONDITIONS
    return is_chop, "|".join(reasons) if reasons else "ok"


# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info: return _sym_info[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"]==symbol:
                for f in s["filters"]:
                    if f["filterType"]=="LOT_SIZE":
                        _sym_info[symbol]={"step":float(f["stepSize"]),"minQty":float(f["minQty"])}
                        return _sym_info[symbol]
    except: pass
    return {"step":1.0,"minQty":1.0}

def round_step(qty,step):
    p=max(0,int(round(-math.log(step,10),0))) if step<1 else 0
    return round(math.floor(qty/step)*step,p)

def calc_qty(symbol,price):
    info=get_sym_info(symbol)
    raw=(ORDER_USDT*LEVERAGE)/price
    return max(round_step(raw,info["step"]),info["minQty"])

def set_leverage(symbol):
    try: client.futures_change_leverage(symbol=symbol,leverage=LEVERAGE)
    except: pass

def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def get_exchange_amt(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt=float(p["positionAmt"])
            if amt!=0: return amt
        return 0
    except: return None

def is_cooling(symbol):
    if symbol not in _sym_cooldown: return False
    return (time.time()-_sym_cooldown[symbol])<SYMBOL_COOLDOWN_SEC

def set_cooldown(symbol): _sym_cooldown[symbol]=time.time()

def validate_symbols():
    try:
        valid={s["symbol"] for s in client.futures_exchange_info()["symbols"]
               if s["status"]=="TRADING"}
        result=list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        print(f"  ✅ {len(result)}/{len(SYMBOLS)} valid")
        return result
    except: return list(dict.fromkeys(SYMBOLS))


# ════════════════════════════════════════════════════
#  DATA SOURCES
# ════════════════════════════════════════════════════
def fetch_ticker24h_all():
    global _ticker24h_cache,_ticker24h_ts
    now=time.time()
    if now-_ticker24h_ts<TICKER24H_TTL and _ticker24h_cache: return _ticker24h_cache
    try:
        tickers=client.futures_ticker()
        _ticker24h_cache={t["symbol"]:{"pct":float(t["priceChangePercent"]),
            "price":float(t["lastPrice"]),"vol24h":float(t["quoteVolume"]),
            "high24":float(t["highPrice"]),"low24":float(t["lowPrice"]),
            "count":int(t["count"])} for t in tickers}
        _ticker24h_ts=now; return _ticker24h_cache
    except: return _ticker24h_cache

def fetch_funding_rates():
    global _funding_cache,_funding_ts
    now=time.time()
    if now-_funding_ts<FUNDING_TTL and _funding_cache: return _funding_cache
    try:
        p=client.futures_mark_price()
        _funding_cache={x["symbol"]:float(x.get("lastFundingRate",0)) for x in p}
        _funding_ts=now; return _funding_cache
    except: return _funding_cache

def get_top_movers(symbols_active,n=30):
    global _top_movers,_top_movers_ts
    now=time.time()
    if now-_top_movers_ts<TOP_MOVERS_TTL and _top_movers: return _top_movers
    try:
        tickers=fetch_ticker24h_all(); aset=set(symbols_active)
        mv=[(s,d["pct"],d["vol24h"]) for s,d in tickers.items()
            if s in aset and d["vol24h"]>=1_000_000]
        mv.sort(key=lambda x:abs(x[1]),reverse=True)
        _top_movers=[(s,p,"LONG" if p>0 else "SHORT") for s,p,_ in mv[:n]]
        _top_movers_ts=now; return _top_movers
    except: return _top_movers

def get_funding_bias(symbol):
    rates=fetch_funding_rates(); fr=rates.get(symbol,0)
    if fr>0.0005:  return "bearish_bias",fr
    if fr<-0.0005: return "bullish_bias",fr
    return "neutral",fr


# ════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════
def get_ohlcv(symbol,interval,limit=100):
    key=(symbol,interval); now=time.time()
    ttl={Client.KLINE_INTERVAL_1MINUTE:OHLCV_CACHE_TTL_1M,
         Client.KLINE_INTERVAL_3MINUTE:OHLCV_CACHE_TTL_3M,
         Client.KLINE_INTERVAL_5MINUTE:OHLCV_CACHE_TTL_5M,
         Client.KLINE_INTERVAL_15MINUTE:OHLCV_CACHE_TTL_15M,
         Client.KLINE_INTERVAL_1HOUR:OHLCV_CACHE_TTL_1H}.get(interval,30)
    if key in _ohlcv_cache:
        ts,df=_ohlcv_cache[key]
        if now-ts<ttl: return df
    try:
        klines=client.futures_klines(symbol=symbol,interval=interval,limit=limit)
        df=pd.DataFrame(klines,columns=["time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c]=df[c].astype(float)
        df["time"]=pd.to_numeric(df["time"])
        _ohlcv_cache[key]=(now,df); return df
    except:
        if key in _ohlcv_cache: return _ohlcv_cache[key][1]
        return None


# ════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ════════════════════════════════════════════════════
def run_ta(df):
    c,h,l,v=df["close"],df["high"],df["low"],df["volume"]
    df["rsi"]      =ta.momentum.RSIIndicator(c,14).rsi()
    df["rsi_fast"] =ta.momentum.RSIIndicator(c,7).rsi()
    macd           =ta.trend.MACD(c,12,26,9)
    df["macd"]     =macd.macd()
    df["macd_sig"] =macd.macd_signal()
    df["macd_hist"]=macd.macd_diff()
    df["ema3"]     =ta.trend.EMAIndicator(c,3).ema_indicator()
    df["ema5"]     =ta.trend.EMAIndicator(c,5).ema_indicator()
    df["ema9"]     =ta.trend.EMAIndicator(c,9).ema_indicator()
    df["ema21"]    =ta.trend.EMAIndicator(c,21).ema_indicator()
    df["ema50"]    =ta.trend.EMAIndicator(c,50).ema_indicator()
    bb             =ta.volatility.BollingerBands(c,20,2)
    df["bb_hi"]    =bb.bollinger_hband()
    df["bb_lo"]    =bb.bollinger_lband()
    df["bb_mid"]   =bb.bollinger_mavg()
    df["bb_width"] =(df["bb_hi"]-df["bb_lo"])/df["bb_mid"]
    stoch          =ta.momentum.StochasticOscillator(h,l,c,14,3)
    df["stk"]      =stoch.stoch()
    df["std"]      =stoch.stoch_signal()
    df["atr"]      =ta.volatility.AverageTrueRange(h,l,c,14).average_true_range()
    df["vol_ma"]   =v.rolling(20).mean()
    df["vol_ratio"]=v/df["vol_ma"].replace(0,1)
    df["buy_ratio"]=df["tbbase"]/df["volume"].replace(0,1)
    df["body"]     =abs(df["close"]-df["open"])
    df["range_"]   =df["high"]-df["low"]
    df["body_ratio"]=df["body"]/df["range_"].replace(0,1)
    df["bb_squeeze"]=df["bb_width"]<df["bb_width"].rolling(20).mean()*0.85
    df["mom5"]     =(c-c.shift(5))/c.shift(5)
    df["mom3"]     =(c-c.shift(3))/c.shift(3)
    return df

def _calc_trend(df):
    if df is None or len(df)<25: return "UNKNOWN"
    c=df["close"]; price=c.iloc[-1]
    ema9 =ta.trend.EMAIndicator(c,9).ema_indicator().iloc[-1]
    ema21=ta.trend.EMAIndicator(c,21).ema_indicator().iloc[-1]
    ema50=ta.trend.EMAIndicator(c,50).ema_indicator().iloc[-1]
    chg  =(price-c.iloc[-4])/c.iloc[-4]*100
    if price>ema9>ema21>ema50 and chg>0:   return "BULL"
    elif price<ema9<ema21<ema50 and chg<0: return "BEAR"
    elif price>ema21 and chg>-0.2:         return "MILD_BULL"
    elif price<ema21 and chg<0.2:          return "MILD_BEAR"
    return "SIDEWAYS"


# ════════════════════════════════════════════════════
#  ATR LEVELS — gunakan adaptive params
# ════════════════════════════════════════════════════
def calc_atr_levels(entry, atr, direction):
    p   = adaptive.params
    sl_d  = max(entry*MIN_SL_PCT, min(atr*p.atr_sl_mult, entry*MAX_SL_PCT))
    tp1_d = max(entry*MIN_TP1_PCT, atr*p.atr_tp1_mult)
    tp2_d = min(entry*MAX_TP2_PCT, atr*p.atr_tp2_mult)
    tp2_d = max(tp2_d, tp1_d*1.5)
    ic_d  = atr*INSTANT_CUT_MULT
    if direction=="LONG":
        return {"sl":round(entry-sl_d,8),"tp1":round(entry+tp1_d,8),
                "tp2":round(entry+tp2_d,8),"instant_cut":round(entry-ic_d,8),
                "sl_pct":sl_d/entry,"tp1_pct":tp1_d/entry,"tp2_pct":tp2_d/entry,
                "atr":atr,"atr_pct":atr/entry}
    else:
        return {"sl":round(entry+sl_d,8),"tp1":round(entry-tp1_d,8),
                "tp2":round(entry-tp2_d,8),"instant_cut":round(entry+ic_d,8),
                "sl_pct":sl_d/entry,"tp1_pct":tp1_d/entry,"tp2_pct":tp2_d/entry,
                "atr":atr,"atr_pct":atr/entry}


# ════════════════════════════════════════════════════
#  MOMENTUM CHECK — RELAXED
# ════════════════════════════════════════════════════
def check_momentum_strength(df, direction):
    if df is None or len(df)<10: return False,0,"no_data"
    p      = adaptive.params
    last   = df.iloc[-1]
    recent = df.iloc[-6:-1]
    price_now  = last["close"]
    price_5ago = df.iloc[-6]["close"]
    mom_pct    = (price_now-price_5ago)/price_5ago
    min_mom = p.min_momentum_pct
    min_vol = p.min_vol_surge
    min_can = p.min_trend_candles

    if direction=="LONG"  and mom_pct < min_mom:  return False,mom_pct,f"mom_weak({mom_pct*100:.2f}%<{min_mom*100:.2f}%)"
    if direction=="SHORT" and mom_pct > -min_mom: return False,mom_pct,f"mom_weak({mom_pct*100:.2f}%)"

    if last["vol_ratio"] < min_vol:
        return False,mom_pct,f"vol_low({last['vol_ratio']:.1f}x<{min_vol:.1f}x)"

    # Body ratio check — relaxed
    if last["body_ratio"] < 0.30:
        return False,mom_pct,f"doji(body:{last['body_ratio']:.2f})"

    # Trend candle check — OPTIONAL, hanya kalau min_can > 1
    if min_can > 1:
        if direction=="LONG":
            bc=sum(1 for _,r in recent.iterrows() if r["close"]>r["open"])
            if bc < min_can: return False,mom_pct,f"candles({bc}/{min_can})"
        else:
            bc=sum(1 for _,r in recent.iterrows() if r["close"]<r["open"])
            if bc < min_can: return False,mom_pct,f"candles({bc}/{min_can})"

    return True,mom_pct,f"mom={mom_pct*100:+.2f}% vol={last['vol_ratio']:.1f}x"


# ════════════════════════════════════════════════════
#  CONTINUATION CONFIRMATION — RELAXED
# ════════════════════════════════════════════════════
def check_continuation(df, direction):
    if df is None or len(df)<5: return False,"no_data"
    last=df.iloc[-1]; prev=df.iloc[-2]; prev2=df.iloc[-3]
    if direction=="LONG":
        # v15: cukup salah satu dari tiga kondisi passing
        if last["close"]<=last["open"] and last["body_ratio"]>0.7:
            return False,"strong_bearish_last"
        # Kalau engulf bear kuat di prev, skip
        if prev["close"]<prev["open"] and prev["body_ratio"]>0.8:
            return False,"engulf_bear_prev"
        return True,"ok"
    else:
        if last["close"]>=last["open"] and last["body_ratio"]>0.7:
            return False,"strong_bullish_last"
        if prev["close"]>prev["open"] and prev["body_ratio"]>0.8:
            return False,"engulf_bull_prev"
        return True,"ok"


# ════════════════════════════════════════════════════
#  MACRO REFRESH
# ════════════════════════════════════════════════════
def refresh_macro():
    now=time.time()
    if now-_macro["last_fng"]>300:
        try:
            d=requests.get("https://api.alternative.me/fng/?limit=1",timeout=5).json()["data"][0]
            _macro["fng"]=int(d["value"]); _macro["fng_label"]=d["value_classification"]
            _macro["last_fng"]=now
        except: pass

    if now-_macro["last_btc"]>5:
        try:
            for iv,key in [(Client.KLINE_INTERVAL_1MINUTE,"btc_trend_1m"),
                           (Client.KLINE_INTERVAL_5MINUTE,"btc_trend_5m"),
                           (Client.KLINE_INTERVAL_15MINUTE,"btc_trend_15m"),
                           (Client.KLINE_INTERVAL_1HOUR,"btc_trend_1h")]:
                df=get_ohlcv("BTCUSDT",iv,60)
                _macro[key]=_calc_trend(df)
            _macro["last_btc"]=now
            t5=_macro["btc_trend_5m"]; t15=_macro["btc_trend_15m"]
            _macro["scalp_mode"]="TREND" if (t15 in("BULL","BEAR") or t5 in("BULL","BEAR")) else "MEAN_REV"
        except: pass

    if now-_macro["last_breadth"]>30:
        try:
            bullish=0; sample=SYMBOLS[:20]
            for sym in sample:
                df=get_ohlcv(sym,Client.KLINE_INTERVAL_5MINUTE,10)
                if df is not None and len(df)>=5:
                    e9=ta.trend.EMAIndicator(df["close"],9).ema_indicator().iloc[-1]
                    if df["close"].iloc[-1]>e9: bullish+=1
            _macro["market_breadth"]=bullish/len(sample)
            _macro["last_breadth"]=now
        except: pass

    if now-_macro.get("last_news",0)>120:
        try:
            data=requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg=["crash","hack","ban","fraud","collapse","seized","scam","plunge"]
            pos=["institutional","ath","approved","record","bullish","rally","surge"]
            n=p=0
            for post in data.get("results",[])[:8]:
                tl=post.get("title","").lower()
                if any(w in tl for w in neg): n+=1
                if any(w in tl for w in pos): p+=1
            sc=p-n
            _macro["news"]=("strong_negative" if sc<=-3 else "negative" if sc<=-1
                            else "strong_positive" if sc>=3 else "neutral")
            _macro["last_news"]=now
        except: pass

def update_btc_price():
    try:
        px=get_price("BTCUSDT")
        if px>0: _btc_price_history.append((time.time(),px))
    except: pass

def detect_flash_move():
    if len(_btc_price_history)<2: return "none",0.0
    cutoff=time.time()-120
    oldest=next((px for ts,px in _btc_price_history if ts>=cutoff),None)
    if oldest is None: return "none",0.0
    current=_btc_price_history[-1][1]
    pct=(current-oldest)/oldest*100
    if pct<=-1.0: return "crash",abs(pct)
    if pct>=1.0:  return "pump",abs(pct)
    return "none",0.0


# ════════════════════════════════════════════════════
#  ORDER BOOK
# ════════════════════════════════════════════════════
def get_ob_imbalance(symbol):
    try:
        ob=client.futures_order_book(symbol=symbol,limit=50)
        bw=sum(float(b[1])*(1/(i+1)) for i,b in enumerate(ob["bids"][:20]))
        aw=sum(float(a[1])*(1/(i+1)) for i,a in enumerate(ob["asks"][:20]))
        tot=bw+aw
        return round((bw-aw)/tot,3) if tot else 0.0
    except: return 0.0

def get_spread_ratio(symbol,tp1_price,entry_price):
    try:
        ob=client.futures_order_book(symbol=symbol,limit=5)
        spread=float(ob["asks"][0][0])-float(ob["bids"][0][0])
        tp1d=abs(tp1_price-entry_price)
        return round(spread/tp1d,3) if tp1d else 1.0
    except: return 0.0


# ════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE v15
# ════════════════════════════════════════════════════
def get_entry_score(symbol, df_5m, direction):
    if df_5m is None or len(df_5m)<30: return 0,[]
    last=df_5m.iloc[-1]; prev=df_5m.iloc[-2]; prev2=df_5m.iloc[-3]
    sigs=[]; score=0
    e3,e5,e9,e21=last["ema3"],last["ema5"],last["ema9"],last["ema21"]
    p=last["close"]

    # ── A: TREND (max 25) ────────────────────────────────
    if direction=="LONG":
        if p>e3>e5>e9>e21:    score+=25; sigs.append("📐EMA_STACK↑")
        elif p>e5>e9>e21:     score+=18; sigs.append("📐EMA↑")
        elif p>e9>e21:        score+=12; sigs.append("📐EMA_align↑")
        elif p>e9:            score+=6;  sigs.append("📐EMA9↑")
    else:
        if p<e3<e5<e9<e21:    score+=25; sigs.append("📐EMA_STACK↓")
        elif p<e5<e9<e21:     score+=18; sigs.append("📐EMA↓")
        elif p<e9<e21:        score+=12; sigs.append("📐EMA_align↓")
        elif p<e9:            score+=6;  sigs.append("📐EMA9↓")

    # ── B: VOLATILITY/MOMENTUM (max 25) ──────────────────
    mom5=abs(last.get("mom5",0)); vr=last["vol_ratio"]
    atr_now=last["atr"]
    atr_prv=df_5m.iloc[-6]["atr"] if len(df_5m)>6 else atr_now
    atr_exp=atr_now>atr_prv*1.15   # v15: threshold lebih rendah

    if mom5>=0.007 and atr_exp:     score+=25; sigs.append(f"🚀Mom{mom5*100:.1f}%+ATRexp")
    elif mom5>=0.004 and vr>=1.8:   score+=20; sigs.append(f"📈Mom{mom5*100:.1f}%+Vol{vr:.1f}x")
    elif mom5>=0.002:               score+=14; sigs.append(f"📈Mom{mom5*100:.1f}%")
    elif mom5>=0.001:               score+=8;  sigs.append(f"📈Mom{mom5*100:.1f}%")
    elif vr>=2.5:                   score+=12; sigs.append(f"🔥Vol{vr:.1f}x")
    elif vr>=1.5:                   score+=7;  sigs.append(f"Vol{vr:.1f}x")

    # ── C: ORDER FLOW (max 25) ────────────────────────────
    hn=last["macd_hist"]; hp=prev["macd_hist"]; hp2=prev2["macd_hist"]
    br=last["buy_ratio"]
    if direction=="LONG":
        if hn>0 and hn>hp>hp2 and br>0.55:    score+=25; sigs.append(f"✅MACD↑↑+Buy{br:.0%}")
        elif hn>0 and hn>hp:                   score+=17; sigs.append("✅MACD↑")
        elif hp<0 and hn>=0:                   score+=20; sigs.append("⚡MACD_X0↑")
        elif hn>hp and br>0.52:                score+=10; sigs.append(f"💧MACD+Buy{br:.0%}")
        elif br>0.60:                          score+=8;  sigs.append(f"💧Buy{br:.0%}")
    else:
        if hn<0 and hn<hp<hp2 and br<0.45:    score+=25; sigs.append(f"✅MACD↓↓+Sell{1-br:.0%}")
        elif hn<0 and hn<hp:                   score+=17; sigs.append("✅MACD↓")
        elif hp>0 and hn<=0:                   score+=20; sigs.append("⚡MACD_X0↓")
        elif hn<hp and br<0.48:                score+=10; sigs.append(f"💧MACD+Sell{1-br:.0%}")
        elif br<0.40:                          score+=8;  sigs.append(f"💧Sell{1-br:.0%}")

    # ── D: MARKET STRUCTURE (max 25) ─────────────────────
    rhi=df_5m.iloc[-6:-1]["high"].max(); rlo=df_5m.iloc[-6:-1]["low"].min()
    if direction=="LONG":
        if p>rhi and last["body_ratio"]>0.55 and vr>1.4: score+=25; sigs.append("🚀BreakoutBull")
        elif last["close"]>last["open"] and last["close"]>prev["high"] and last["body_ratio"]>0.55: score+=20; sigs.append("🕯️Engulf↑")
        elif p>rhi:                                        score+=12; sigs.append("📈Breakout↑")
        elif p>prev["high"]:                              score+=6;  sigs.append("📈HH↑")
    else:
        if p<rlo and last["body_ratio"]>0.55 and vr>1.4: score+=25; sigs.append("💥BreakoutBear")
        elif last["close"]<last["open"] and last["close"]<prev["low"] and last["body_ratio"]>0.55: score+=20; sigs.append("🕯️Engulf↓")
        elif p<rlo:                                        score+=12; sigs.append("📈Breakout↓")
        elif p<prev["low"]:                               score+=6;  sigs.append("📈LL↓")

    return max(0,min(score,100)),sigs


def determine_direction(df_5m,df_15m=None):
    if df_5m is None or len(df_5m)<20: return None
    last=df_5m.iloc[-1]; prev=df_5m.iloc[-2]
    price=last["close"]; e3,e5,e9=last["ema3"],last["ema5"],last["ema9"]
    lp=sp=0
    if price>e3>e5>e9:    lp+=4
    elif price<e3<e5<e9:  sp+=4
    elif price>e5>e9:     lp+=2
    elif price<e5<e9:     sp+=2
    mom5=last.get("mom5",0)
    if mom5>0.001:   lp+=3
    elif mom5<-0.001: sp+=3
    if last["macd_hist"]>prev["macd_hist"]: lp+=2
    else:                                    sp+=2
    if last["buy_ratio"]>0.55 and last["close"]>last["open"]:   lp+=2
    elif last["buy_ratio"]<0.45 and last["close"]<last["open"]: sp+=2
    if df_15m is not None and len(df_15m)>=20:
        l15=df_15m.iloc[-1]
        if l15["ema9"]>l15["ema21"]: lp+=2
        else:                        sp+=2
    btc=_macro.get("btc_trend_5m","UNKNOWN")
    if btc in BULL_TRENDS: lp+=2
    elif btc in BEAR_TRENDS: sp+=2
    # v15: threshold lebih rendah (5 bukan 6)
    if lp>sp and lp>=5:  return "LONG"
    if sp>lp and sp>=5:  return "SHORT"
    return None


# ════════════════════════════════════════════════════
#  ENTRY FILTER v15
# ════════════════════════════════════════════════════
def should_enter(symbol):
    killed,kill_reason=check_kill_switch()
    if killed: return None,f"kill:{kill_reason}"
    if is_cooling(symbol): return None,"cooldown"
    if adaptive.is_bad_symbol(symbol): return None,"adaptive_bad_sym"

    fng=_macro["fng"]; news=_macro["news"]
    if fng<MIN_FNG:            return None,f"F&G={fng}"
    if news=="strong_negative": return None,"bad_news"

    flash_dir,_=detect_flash_move()
    if flash_dir!="none": return None,f"flash_{flash_dir}"

    tickers=fetch_ticker24h_all(); pct_24h=0.0
    if symbol in tickers:
        t24=tickers[symbol]
        if t24["vol24h"]<500_000:
            _stats["skip_illiquid"]+=1
            return None,f"illiquid"
        pct_24h=t24["pct"]

    df_5m =get_ohlcv(symbol,Client.KLINE_INTERVAL_5MINUTE,80)
    df_15m=get_ohlcv(symbol,Client.KLINE_INTERVAL_15MINUTE,60)
    if df_5m is None or len(df_5m)<30: return None,"no_data"

    df_5m=run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m)>=20:
        df_15m=run_ta(df_15m.copy())

    direction=determine_direction(df_5m,df_15m)
    if direction is None:
        _stats["skip_no_dir"]+=1
        return None,"no_direction"

    is_chop,chop_desc=is_chop_market(df_5m,direction)
    if is_chop:
        _stats["skipped_chop"]+=1
        return None,f"chop:{chop_desc}"

    mom_pass,mom_pct,mom_desc=check_momentum_strength(df_5m,direction)
    if not mom_pass:
        _stats["skipped_no_momentum"]+=1
        return None,f"no_mom:{mom_desc}"

    cont_pass,cont_desc=check_continuation(df_5m,direction)
    if not cont_pass:
        _stats["skip_cont"]+=1
        return None,f"no_cont:{cont_desc}"

    funding_bias,fr=get_funding_bias(symbol)
    if direction=="LONG"  and funding_bias=="bearish_bias" and fr>0.001:
        _stats["skip_funding"]+=1
        return None,f"funding_bearish"
    if direction=="SHORT" and funding_bias=="bullish_bias" and fr<-0.001:
        _stats["skip_funding"]+=1
        return None,f"funding_bullish"

    # MEAN_REV gate
    scalp_mode=_macro.get("scalp_mode","TREND")
    if scalp_mode=="MEAN_REV" and not adaptive.params.mean_rev_enabled:
        _stats["skipped_mean_rev"]+=1
        return None,"skip_MEAN_REV"

    # BTC trend filter — RELAXED (hanya skip kalau SANGAT berlawanan)
    btc_5m=_macro["btc_trend_5m"]; btc_15m=_macro["btc_trend_15m"]
    if direction=="LONG"  and btc_5m=="BEAR" and btc_15m=="BEAR":
        _stats["skip_btc"]+=1
        return None,f"btc_BEAR"
    if direction=="SHORT" and btc_5m=="BULL" and btc_15m=="BULL":
        _stats["skip_btc"]+=1
        return None,f"btc_BULL"
    if direction=="LONG"  and _macro["fng"]>MAX_FNG_LONG:
        return None,f"overbought:F&G={fng}"

    score,sigs=get_entry_score(symbol,df_5m,direction)

    # Score threshold: session + adaptive effective
    min_score_now=max(
        MIN_SCORE if time.gmtime().tm_hour not in BAD_HOURS_UTC else BAD_HOURS_MIN_SCORE,
        adaptive.effective_min_score()
    )

    if score<min_score_now:
        return None,f"score={score}<{min_score_now}"

    if len(sigs)<adaptive.params.min_entry_signals:
        return None,f"sigs={len(sigs)}<{adaptive.params.min_entry_signals}"

    atr=df_5m["atr"].iloc[-1]; price=df_5m["close"].iloc[-1]
    if atr/price>MAX_SL_ATR_PCT:
        _stats["skip_atr_big"]+=1
        return None,f"ATR_big({atr/price*100:.2f}%)"

    levels=calc_atr_levels(price,atr,direction)

    spread_ratio=get_spread_ratio(symbol,levels["tp1"],price)
    if spread_ratio>MAX_SPREAD_RATIO:
        _stats["skipped_spread"]+=1
        return None,f"spread({spread_ratio:.2f})"

    ob_imb=get_ob_imbalance(symbol)
    if direction=="LONG"  and ob_imb<-0.25:
        _stats["skip_ob"]+=1; return None,f"OB_SHORT({ob_imb:.2f})"
    if direction=="SHORT" and ob_imb>0.25:
        _stats["skip_ob"]+=1; return None,f"OB_LONG({ob_imb:.2f})"

    return direction,{
        "score":score,"signals":sigs,"direction":direction,
        "sl":levels["sl"],"tp1":levels["tp1"],"tp2":levels["tp2"],
        "sl_pct":levels["sl_pct"],"tp1_pct":levels["tp1_pct"],
        "ob_imb":ob_imb,"atr":atr,"atr_pct":levels["atr_pct"],
        "mom_pct":mom_pct,"pct_24h":pct_24h,"funding":fr,
        "scalp_mode":scalp_mode,"btc_trend":btc_5m,
        "instant_cut":levels["instant_cut"],
    }


# ════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction,info=should_enter(symbol)
        if direction: return symbol,direction,info
    except: pass
    return None

def scan_batch_parallel(symbols):
    candidates=[]; syms=symbols[:20]
    futures={_executor.submit(scan_symbol_safe,sym):sym for sym in syms}
    try:
        for future in as_completed(futures,timeout=12):
            try:
                r=future.result(timeout=2)
                if r: candidates.append(r)
            except: pass
    except TimeoutError:
        done=[f for f in futures if f.done()]
        pend=len(futures)-len(done)
        for f in done:
            try:
                r=f.result(timeout=0)
                if r: candidates.append(r)
            except: pass
        for f in futures:
            if not f.done(): f.cancel()
        if pend>0:
            print(f"  ⚠️  Timeout: {len(done)}/{len(futures)} done ({len(candidates)} kandidat)")
    except Exception as e:
        print(f"  ❌ Scan error: {e}")
    return candidates


# ════════════════════════════════════════════════════
#  RE-SCAN
# ════════════════════════════════════════════════════
def trigger_rescan(reason="",priority_symbol=None):
    if priority_symbol: _hot_symbols.appendleft(priority_symbol)
    _rescan_queue.put({"reason":reason,"ts":time.time()})

def instant_rescan_worker(symbols_active):
    while True:
        try:
            event=_rescan_queue.get(timeout=60)
            time.sleep(RE_SCAN_DELAY_SEC)
            slots_free=MAX_POSITIONS-len(open_positions)
            if slots_free<=0: continue
            killed,_=check_kill_switch()
            if killed: continue
            flash_dir,_=detect_flash_move()
            if flash_dir!="none": continue
            if _macro["news"]=="strong_negative": continue
            hot=[s for s in list(_hot_symbols) if s not in open_positions]
            rest=[s for s in symbols_active if s not in open_positions and s not in hot]
            _stats["rescans"]+=1
            reason=event.get("reason","")
            print(f"\n  ⚡ RESCAN [{reason}] — {slots_free} slot")
            try:
                candidates=scan_batch_parallel((hot+rest)[:40])
            except Exception as e:
                print(f"  ❌ Rescan error: {e}"); candidates=[]
            if candidates:
                candidates.sort(key=lambda x:x[2].get("score",0),reverse=True)
                for sym,direction,info in candidates[:slots_free]:
                    if len(open_positions)>=MAX_POSITIONS: break
                    print(f"     ⭐ {sym} {direction} Score:{info['score']:.0f}")
                    open_trade(sym,direction,info)
            else:
                print(f"  ⏳ Rescan: no setup")
        except queue.Empty: pass
        except Exception as e:
            print(f"  ❌ Rescan worker: {e}")


# ════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    with _lock:
        if symbol in open_positions: return
        if len(open_positions)>=MAX_POSITIONS: return
        open_positions[symbol]={"_reserved":True}
        if len(open_positions)>MAX_POSITIONS:
            open_positions.pop(symbol,None); return
    try:
        set_leverage(symbol)
        price=get_price(symbol)
        if price==0:
            with _lock: open_positions.pop(symbol,None)
            return
        qty=calc_qty(symbol,price)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if direction=="LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET, quantity=qty)
        entry=get_price(symbol)
        atr=info.get("atr",entry*0.002)
        levels=calc_atr_levels(entry,atr,direction)
        p=adaptive.params
        trail_act=p.trail_activate_pct
        if direction=="LONG":
            trail_sl=max(entry*(1-atr*p.atr_trail_mult/entry),levels["sl"])
        else:
            trail_sl=min(entry*(1+atr*p.atr_trail_mult/entry),levels["sl"])
        open_positions[symbol]={
            "side":direction,"entry":entry,"qty":qty,"qty_remain":qty,
            "sl":levels["sl"],"tp1":levels["tp1"],"tp2":levels["tp2"],
            "peak":entry,"trail_sl":trail_sl,"trail_phase":1,
            "trail_active":False,"trail_activate":trail_act,
            "tp1_hit":False,"be_active":False,
            "open_time":time.time(),"score":info.get("score",0),
            "signals":info.get("signals",[]),"instant_cut":levels["instant_cut"],
            "instant_cut_done":False,"mom_pct":info.get("mom_pct",0),
            "entry_candle":0,"atr":atr,"scalp_mode":info.get("scalp_mode","TREND"),
            "btc_trend":info.get("btc_trend","UNKNOWN"),"fng":_macro.get("fng",50),
        }
        print(f"\n  {'🟢' if direction=='LONG' else '🔴'} [{symbol}] {direction} @{entry:.5g}")
        print(f"     ATR:{atr:.4g}({levels['atr_pct']*100:.2f}%) SL:{levels['sl_pct']*100:.2f}% "
              f"TP1:{levels['tp1_pct']*100:.2f}% TP2:{levels['tp2_pct']*100:.2f}%")
        print(f"     Score:{info['score']} Trail@{trail_act*100:.2f}% Hold≤{p.max_holding_min:.1f}m")
        print(f"     {' | '.join(info.get('signals',[])[:3])}")
        _stats["total_trades"]+=1
    except Exception as e:
        with _lock: open_positions.pop(symbol,None)
        print(f"  ❌ [{symbol}] Entry: {e}")


def _record_trade_adaptive(pos, symbol, exit_p, pnl, hold_s, reason):
    """Helper — record ke adaptive engine."""
    adaptive.record(TradeRecord(
        symbol=symbol, side=pos["side"], entry=pos["entry"], exit_price=exit_p,
        pnl=pnl, hold_sec=int(hold_s), reason=reason,
        signals=pos.get("signals",[]), score=pos.get("score",0),
        scalp_mode=pos.get("scalp_mode","TREND"),
        btc_trend=pos.get("btc_trend","UNKNOWN"),
        mom_pct=pos.get("mom_pct",0),
        atr_pct=pos.get("atr",0)/pos["entry"] if pos["entry"]>0 else 0,
        fng=pos.get("fng",50), hour_utc=time.gmtime().tm_hour))


def partial_close_tp1(symbol):
    pos=open_positions.get(symbol)
    if pos is None or pos.get("tp1_hit"): return
    try:
        amt=get_exchange_amt(symbol)
        if amt is None or amt==0:
            pos["tp1_hit"]=True; return
        info=get_sym_info(symbol)
        close_qty=round_step(abs(amt)*TP1_CLOSE_RATIO,info["step"])
        close_qty=max(close_qty,info["minQty"])
        if close_qty>abs(amt): close_qty=abs(amt)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt>0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=close_qty, reduceOnly=True)
        exit_p=get_price(symbol)
        side=pos["side"]
        pnl=((exit_p-pos["entry"]) if side=="LONG" else (pos["entry"]-exit_p))*close_qty
        hold_s=time.time()-pos["open_time"]
        print(f"  🎯 [{symbol}] TP1 ({hold_s:.0f}s) PnL:{pnl:+.4f}U")
        pos["tp1_hit"]=True; pos["qty_remain"]=abs(amt)-close_qty
        pos["be_active"]=True
        # BE stop: entry + small buffer (tidak langsung break even)
        pos["sl"]=round(pos["entry"]*(1+TRAIL_BE_PCT if side=="LONG" else 1-TRAIL_BE_PCT),8)
        pos["trail_phase"]=2; pos["trail_active"]=True; pos["peak"]=exit_p
        p=adaptive.params
        if side=="LONG":
            pos["trail_sl"]=exit_p*(1-pos["atr"]*p.atr_trail_mult/exit_p)
        else:
            pos["trail_sl"]=exit_p*(1+pos["atr"]*p.atr_trail_mult/exit_p)
        _stats["tp1_hits"]+=1; _stats["wins"]+=1; _stats["total_pnl"]+=pnl
        _stats["pnl_history"].append(pnl); update_ks(pnl)
        _perf[symbol]["wins"]+=1; _perf[symbol]["pnl"]+=pnl; _perf[symbol]["trades"]+=1
        if pnl>_stats["best_trade"]: _stats["best_trade"]=pnl
        trade_log.append({"symbol":symbol,"side":side,"pnl":round(pnl,4),
                          "reason":"TP1 Partial","hold_sec":int(hold_s)})
        _record_trade_adaptive(pos,symbol,exit_p,pnl,hold_s,"TP1 Partial")
        _hot_symbols.appendleft(symbol)
        print_stats_inline()
    except Exception as e:
        print(f"  ❌ [{symbol}] TP1: {e}"); pos["tp1_hit"]=True


def close_trade(symbol, reason=""):
    try:
        amt=get_exchange_amt(symbol)
        if amt is None: return False
        if amt==0:
            with _lock: open_positions.pop(symbol,None)
            set_cooldown(symbol)
            trigger_rescan(f"close@{symbol}",priority_symbol=symbol)
            return True
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt>0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=abs(amt), reduceOnly=True)
        with _lock:
            pos=open_positions.pop(symbol,None)
        if pos:
            exit_p=get_price(symbol)
            qty_r=pos.get("qty_remain",pos["qty"]); side=pos["side"]
            pnl=((exit_p-pos["entry"]) if side=="LONG" else (pos["entry"]-exit_p))*qty_r
            pct=pnl/(pos["entry"]*qty_r)*100 if qty_r>0 else 0
            hold_s=time.time()-pos["open_time"]
            emoji="🟢" if pnl>=0 else "🔴"
            be_tag="[BE]" if pos.get("be_active") else ""
            print(f"  {emoji} [{symbol}] CLOSE {reason}{be_tag} | {hold_s:.0f}s PnL:{pnl:+.4f}U ({pct:+.2f}%)")
            trade_log.append({"symbol":symbol,"side":side,"pnl":round(pnl,4),
                               "reason":reason,"hold_sec":int(hold_s)})
            _record_trade_adaptive(pos,symbol,exit_p,pnl,hold_s,reason)
            _stats["total_pnl"]+=pnl; _stats["pnl_history"].append(pnl); update_ks(pnl)
            _perf[symbol]["trades"]+=1; _perf[symbol]["pnl"]+=pnl
            if pnl>=0:
                _stats["wins"]+=1; _perf[symbol]["wins"]+=1
                if pnl>_stats["best_trade"]: _stats["best_trade"]=pnl
            else:
                _stats["losses"]+=1; _perf[symbol]["losses"]+=1
                if pnl<_stats["worst_trade"]: _stats["worst_trade"]=pnl
            regime=pos.get("scalp_mode","UNKNOWN")
            _perf_regime[regime]["pnl"]+=pnl
            if pnl>=0: _perf_regime[regime]["wins"]+=1
            else:      _perf_regime[regime]["losses"]+=1
            if "TP2"  in reason: _stats["tp2_hits"]+=1
            if "SL"   in reason or "Stop" in reason: _stats["sl_hits"]+=1
            if "Force"in reason: _stats["force_closes"]+=1
            if "Inst" in reason: _stats["instant_cuts"]+=1
            print_stats_inline()
            set_cooldown(symbol)
            trigger_rescan(f"close@{symbol}({reason})",priority_symbol=symbol)
        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Close: {e}"); return False


# ════════════════════════════════════════════════════
#  POSITION MONITOR v15
# ════════════════════════════════════════════════════
def manage_positions():
    if not open_positions: return
    flash_dir,flash_pct=detect_flash_move()
    for symbol in list(open_positions.keys()):
        pos=open_positions.get(symbol)
        if pos is None or pos.get("_reserved"): continue
        price=get_price(symbol)
        if price==0: continue
        side=pos["side"]; entry=pos["entry"]; atr=pos.get("atr",entry*0.002)
        pos["entry_candle"]=pos.get("entry_candle",0)+1

        hold_min=(time.time()-pos["open_time"])/60
        hold_limit=adaptive.params.max_holding_min
        if hold_min>=hold_limit*0.95:
            close_trade(symbol,f"⏰Force({hold_min:.1f}m)"); continue

        if flash_dir=="crash" and side=="LONG":
            close_trade(symbol,f"⚡FlashCrash-{flash_pct:.1f}%"); continue
        elif flash_dir=="pump" and side=="SHORT":
            close_trade(symbol,f"⚡FlashPump+{flash_pct:.1f}%"); continue

        within_window=pos.get("entry_candle",0)<=(INSTANT_CUT_WINDOW*5)
        if not pos.get("instant_cut_done") and not pos.get("tp1_hit") and within_window:
            ic=pos["instant_cut"]
            if (side=="LONG" and price<=ic) or (side=="SHORT" and price>=ic):
                pos["instant_cut_done"]=True
                close_trade(symbol,"⚡InstCut"); continue
        elif not within_window:
            pos["instant_cut_done"]=True

        trail_act=pos.get("trail_activate",TRAIL_ACTIVATE_PCT)
        p=adaptive.params

        if side=="LONG":
            profit_pct=(price-entry)/entry
            if not pos["tp1_hit"] and price>=pos["tp1"]:
                partial_close_tp1(symbol); continue
            if not pos["trail_active"] and profit_pct>=trail_act:
                pos["trail_active"]=True
                pos["sl"]=round(entry*(1+TRAIL_BE_PCT),8)
                pos["trail_sl"]=price*(1-atr*p.atr_trail_mult/price)
                pos["peak"]=price
                print(f"     🔓 [{symbol}] Trail AKTIF @ {profit_pct*100:+.2f}%")
            if profit_pct>=TRAIL_TIGHT_PCT and pos["trail_phase"]<3:
                pos["trail_phase"]=3
            if pos["trail_active"] and price>pos["peak"]:
                pos["peak"]=price
                tm=p.atr_trail_tight if pos["trail_phase"]>=3 else p.atr_trail_mult
                pos["trail_sl"]=max(pos["trail_sl"],price*(1-atr*tm/price))
            if pos["tp1_hit"] and price>=pos["tp2"]:
                close_trade(symbol,"✨TP2"); continue
            if pos["trail_active"] and price<=pos["trail_sl"]:
                close_trade(symbol,"🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"); continue
            if price<=pos["sl"]:
                close_trade(symbol,"🛑SL"); continue
            pnl=(price-entry)*pos.get("qty_remain",pos["qty"])
            act="✅" if pos["trail_active"] else "⏸️"
            tp_s=f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 [{symbol}] L@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) "
                  f"{pnl:+.3f}U {hold_min:.1f}m TSL[{act}]:{pos['trail_sl']:.5g} {tp_s}")
        else:
            profit_pct=(entry-price)/entry
            if not pos["tp1_hit"] and price<=pos["tp1"]:
                partial_close_tp1(symbol); continue
            if not pos["trail_active"] and profit_pct>=trail_act:
                pos["trail_active"]=True
                pos["sl"]=round(entry*(1-TRAIL_BE_PCT),8)
                pos["trail_sl"]=price*(1+atr*p.atr_trail_mult/price)
                pos["peak"]=price
                print(f"     🔓 [{symbol}] Trail AKTIF @ {profit_pct*100:+.2f}%")
            if profit_pct>=TRAIL_TIGHT_PCT and pos["trail_phase"]<3:
                pos["trail_phase"]=3
            if pos["trail_active"] and price<pos["peak"]:
                pos["peak"]=price
                tm=p.atr_trail_tight if pos["trail_phase"]>=3 else p.atr_trail_mult
                pos["trail_sl"]=min(pos["trail_sl"],price*(1+atr*tm/price))
            if pos["tp1_hit"] and price<=pos["tp2"]:
                close_trade(symbol,"✨TP2"); continue
            if pos["trail_active"] and price>=pos["trail_sl"]:
                close_trade(symbol,"🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"); continue
            if price>=pos["sl"]:
                close_trade(symbol,"🛑SL"); continue
            pnl=(entry-price)*pos.get("qty_remain",pos["qty"])
            act="✅" if pos["trail_active"] else "⏸️"
            tp_s=f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 [{symbol}] S@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) "
                  f"{pnl:+.3f}U {hold_min:.1f}m TSL[{act}]:{pos['trail_sl']:.5g} {tp_s}")


# ════════════════════════════════════════════════════
#  ANALYTICS
# ════════════════════════════════════════════════════
def calc_expectancy():
    wins=[t["pnl"] for t in trade_log if t["pnl"]>0]
    losses=[t["pnl"] for t in trade_log if t["pnl"]<0]
    if not wins and not losses: return 0.0
    wr=len(wins)/(len(wins)+len(losses))
    avg_w=sum(wins)/len(wins) if wins else 0
    avg_l=abs(sum(losses)/len(losses)) if losses else 0
    return round((wr*avg_w)-((1-wr)*avg_l),5)

def calc_sharpe():
    pnls=list(_stats["pnl_history"])
    if len(pnls)<5: return 0.0
    arr=np.array(pnls); std=float(np.std(arr))
    return round(float(np.mean(arr))/std,3) if std else 0.0

def calc_max_drawdown():
    pnls=list(_stats["pnl_history"])
    if len(pnls)<2: return 0.0
    eq=np.cumsum(pnls)
    return round(float(np.min(eq-np.maximum.accumulate(eq))),4)

def print_stats_inline():
    n=_stats["wins"]+_stats["losses"]
    wr=_stats["wins"]/n*100 if n else 0
    pnl=_stats["total_pnl"]; exp=calc_expectancy()
    bar=("█"*_stats["wins"]+"░"*_stats["losses"])[-20:]
    emoji="💚" if pnl>=0 else "🔴"
    p=adaptive.params
    print(f"     ┌─ {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {emoji}PnL:{pnl:+.4f}U Exp:{exp:+.4f}U [{bar}]")
    print(f"     └─ TP1:{_stats['tp1_hits']} TP2:{_stats['tp2_hits']} SL:{_stats['sl_hits']} "
          f"⚡{_stats['instant_cuts']} ⏰{_stats['force_closes']} | "
          f"🧠 Score≥{adaptive.effective_min_score()} Mom≥{p.min_momentum_pct*100:.2f}% "
          f"Hold≤{p.max_holding_min:.1f}m SL×{p.atr_sl_mult:.1f} Trail×{p.atr_trail_mult:.2f}")

def print_stats():
    n=_stats["wins"]+_stats["losses"]
    wr=_stats["wins"]/n*100 if n else 0
    sess=(time.time()-_stats["session_start"])/3600
    pnl=_stats["total_pnl"]; emoji="💚" if pnl>=0 else "🔴"
    print(f"\n  {'─'*64}")
    print(f"  📊 SESSION {sess*60:.0f}m | {n/sess if sess>0 else 0:.1f} T/jam | Rescans:{_stats['rescans']}")
    print(f"  🎯 {n}T | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {emoji} PnL:{pnl:+.4f}U | Exp:{calc_expectancy():+.5f}U | Sharpe:{calc_sharpe():.2f} | MDD:{calc_max_drawdown():.4f}U")
    print(f"  📈 Best:{_stats['best_trade']:+.4f}U 📉 Worst:{_stats['worst_trade']:+.4f}U")
    print(f"  🎯TP1:{_stats['tp1_hits']} ✨TP2:{_stats['tp2_hits']} 🛑SL:{_stats['sl_hits']} "
          f"⚡Cut:{_stats['instant_cuts']} ⏰Force:{_stats['force_closes']}")
    print(f"  🚫 Chop:{_stats['skipped_chop']} NoMom:{_stats['skipped_no_momentum']} "
          f"NoDir:{_stats['skip_no_dir']} Spread:{_stats['skipped_spread']} "
          f"MeanRev:{_stats['skipped_mean_rev']} BTC:{_stats['skip_btc']} "
          f"Cont:{_stats['skip_cont']} Adaptive:{_stats['skipped_adaptive']}")
    print(f"  🛡️  KS: {'ACTIVE('+_kill_switch['reason']+')' if _kill_switch['active'] else 'OK'} | "
          f"CL:{_kill_switch['consec_losses']} | DailyPnL:{_kill_switch['daily_pnl']:+.2f}U | "
          f"Lag:{_kill_switch['api_lag']*1000:.0f}ms")
    sym_s=sorted(_perf.items(),key=lambda x:x[1]["pnl"],reverse=True)
    if sym_s:
        print(f"  🏆 Top symbols:")
        for sym,d in sym_s[:5]:
            wr_s=d["wins"]/d["trades"]*100 if d["trades"] else 0
            print(f"     {sym:<14} {d['trades']}T WR:{wr_s:.0f}% PnL:{d['pnl']:+.4f}U")
    if _perf_regime:
        print(f"  📊 By regime:")
        for regime,d in _perf_regime.items():
            tot=d["wins"]+d["losses"]
            wr_r=d["wins"]/tot*100 if tot else 0
            print(f"     {regime:<12} WR:{wr_r:.0f}% PnL:{d['pnl']:+.4f}U")
    if trade_log:
        print(f"  📋 Last 5:")
        for t in trade_log[-5:]:
            e="🟢" if t["pnl"]>0 else "🔴"
            hold=f"{t.get('hold_sec',0)//60}m{t.get('hold_sec',0)%60}s"
            print(f"     {e} {t['symbol']:<14} {t['side']} {t['pnl']:+.4f}U ({hold}) — {t['reason'][:30]}")
    adaptive.print_status()
    print(f"  {'─'*64}")


# ════════════════════════════════════════════════════
#  THREADS
# ════════════════════════════════════════════════════
def position_monitor_thread():
    while True:
        try:
            if open_positions: manage_positions()
        except Exception as e:
            print(f"  ❌ Monitor: {e}")
        time.sleep(POSITION_MONITOR_SEC)


# ════════════════════════════════════════════════════
#  MAIN LOOP
# ════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  🎯🧠 BOT SCALPING v15 — SMART ADAPTIVE (BOUNDED)           ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Lev:{LEVERAGE}x $:{ORDER_USDT} Max:{MAX_POSITIONS}pos                             ║")
    print(f"║  SL×{ATR_SL_MULT} TP1×{ATR_TP1_MULT} TP2×{ATR_TP2_MULT} Trail×{ATR_TRAIL_MULT}           ║")
    print(f"║  Mom≥{MIN_MOMENTUM_PCT*100:.2f}% Vol≥{MIN_VOL_SURGE}x Score≥{MIN_SCORE} Hold≤{MAX_HOLDING_MIN}m          ║")
    print(f"║  Chop: butuh {CHOP_MIN_CONDITIONS} kondisi (lebih relaxed dari v14)           ║")
    print(f"║  Adaptive: bounded corridor, step kecil, window 30T        ║")
    print(f"║  Streak adj: max +10pts (bukan +20 seperti v14)            ║")
    print(f"║  FC blacklist: per-symbol saja, bukan BTC trend global     ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    print("\n  ⏳ Validasi symbols...")
    symbols_active=validate_symbols()
    print(f"  📦 Pre-load sym info...")
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(get_sym_info,symbols_active[:60]))
    print(f"  🌐 Refresh macro...")
    refresh_macro(); update_btc_price()
    print(f"  ✅ BTC:{_macro['btc_trend_5m']} Mode:{_macro['scalp_mode']} F&G:{_macro['fng']}")
    print(f"  🧠 Adaptive engine v2 ACTIVE — bounded, safe, fast-learn")
    print(f"  🚀 Start dalam 3 detik...\n")
    time.sleep(3)

    threading.Thread(target=position_monitor_thread,daemon=True).start()
    print("  🔧 Position monitor: ✅")
    threading.Thread(target=instant_rescan_worker,args=(symbols_active,),daemon=True).start()
    print("  🔧 Re-scan thread: ✅\n")

    global _scan_batch_idx
    cycle=0; total_batches=math.ceil(len(symbols_active)/BATCH_SIZE)

    while True:
        cycle+=1
        refresh_macro(); update_btc_price()
        if cycle%30==0: check_api_latency()

        flash_dir,flash_pct=detect_flash_move()
        flash_info=f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir!="none" else ""
        utc_h=time.gmtime().tm_hour
        sess_t=f"⚠️JAM_JELEK(UTC{utc_h})" if utc_h in BAD_HOURS_UTC else ""
        p=adaptive.params

        print(f"\n{'═'*67}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} F&G:{_macro['fng']} "
              f"BTC:{_macro['btc_trend_5m']}/{_macro['btc_trend_15m']} {flash_info} {sess_t}")
        print(f"  Mode:{_macro['scalp_mode']} Breadth:{_macro['market_breadth']*100:.0f}% "
              f"News:{_macro['news']} Pos({len(open_positions)}/{MAX_POSITIONS}):"
              f"{list(open_positions.keys()) or '—'}")
        print(f"  🧠 Score≥{adaptive.effective_min_score()} Mom≥{p.min_momentum_pct*100:.2f}% "
              f"Vol≥{p.min_vol_surge:.1f}x Hold≤{p.max_holding_min:.1f}m "
              f"SL×{p.atr_sl_mult:.1f} Trail×{p.atr_trail_mult:.2f} | {adaptive._streak:+d}streak")

        slots_free=MAX_POSITIONS-len(open_positions)
        ks_active,ks_reason=check_kill_switch()
        if ks_active:
            resume_in=max(0,_kill_switch["resume_time"]-time.time())
            print(f"  🚨 KS: {ks_reason} | Resume:{resume_in/60:.1f}m")

        skip_reason=None
        if slots_free==0:                       skip_reason="posisi_penuh"
        elif _macro["news"]=="strong_negative": skip_reason="bad_news"
        elif flash_dir!="none":                 skip_reason=f"flash_{flash_dir}"
        elif ks_active:                         skip_reason=f"kill:{ks_reason}"

        if not skip_reason:
            top_mv=get_top_movers(symbols_active,n=40)
            top_mv_syms=[s for s,_,_ in top_mv if s not in open_positions]
            batch_start=_scan_batch_idx*BATCH_SIZE
            batch_reg=[s for s in symbols_active[batch_start:batch_start+BATCH_SIZE]
                       if s not in open_positions and s not in top_mv_syms]
            _scan_batch_idx=(_scan_batch_idx+1)%total_batches
            scan_list=top_mv_syms[:20]+batch_reg[:15]
            top_str=" | ".join(f"{s}({pc:+.1f}%)" for s,pc,_ in top_mv[:5])
            print(f"  📊 TopMov: {top_str}")
            print(f"  🔍 Scan {len(scan_list)} | Chop:{_stats['skipped_chop']} "
                  f"NoMom:{_stats['skipped_no_momentum']} NoDir:{_stats['skip_no_dir']} "
                  f"Cont:{_stats['skip_cont']} BTC:{_stats['skip_btc']}")
            try:
                candidates=scan_batch_parallel(scan_list)
            except Exception as e:
                print(f"  ❌ Scan: {e}"); candidates=[]
            if candidates:
                candidates.sort(key=lambda x:x[2].get("score",0),reverse=True)
                print(f"  🎯 {len(candidates)} setup! Ambil {min(len(candidates),slots_free)}")
                for sym,direction,info in candidates[:slots_free]:
                    if len(open_positions)>=MAX_POSITIONS: break
                    sig_str=" | ".join(info.get("signals",[])[:3])
                    print(f"     ⭐ {sym} {direction} Mom:{info.get('mom_pct',0)*100:+.2f}% "
                          f"Score:{info['score']:.0f}")
                    print(f"        {sig_str}")
                    open_trade(sym,direction,info)
            else:
                print(f"  ⏳ No setup")
        else:
            print(f"  ⏸️  Skip: {skip_reason}")

        if cycle%30==0:
            print_stats()

        print(f"  ⏱️  Next:{SCAN_INTERVAL}s KS:{_kill_switch['consec_losses']}CL/"
              f"{_kill_switch['daily_pnl']:+.2f}U Rescans:{_stats['rescans']} "
              f"Lag:{_kill_switch['api_lag']*1000:.0f}ms")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
