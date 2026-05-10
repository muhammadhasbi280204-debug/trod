"""
Bot Scalping v13 — ULTRA LIGHTNING ⚡⚡⚡
==========================================

FILOSOFI v13:
  🎯 "Entry presisi, exit INSTAN kalau minus, trail CERDAS kalau profit"

  PERUBAHAN UTAMA dari v12:
  ─────────────────────────
  ✅ ZERO-DELAY LOSS CUT  : Minus dari entry → close di tick BERIKUTNYA (< 2 detik)
  ✅ SMART DYNAMIC TRAIL  : Trail naik SETIAP TICK, jarak otomatis menyempit makin profit
  ✅ ATR-BASED TRAIL DIST : Jarak trail pakai ATR realtime, bukan % flat → lebih adaptif
  ✅ BREAKEVEN LOCK CEPAT : Setelah +0.15% langsung geser SL ke entry (lock modal)
  ✅ PRIORITY RE-ENTRY    : Score sistem canggih → pilih coin paling "panas" saat itu
  ✅ MOMENTUM RANK        : Setiap close → ranking ulang semua kandidat by momentum score
  ✅ PARALLEL PRICE FEED  : Ambil harga semua open positions sekaligus (bukan satu-satu)
  ✅ TICK-SPEED MONITOR   : Monitor posisi tiap 1 detik (lebih cepat dari v12's 2 detik)
  ✅ ADAPTIVE SCAN        : Scan lebih sering kalau ada slot kosong
  ✅ VOLUME SURGE DETECT  : Deteksi volume spike dalam 10 detik terakhir
  ✅ MULTI-TIMEFRAME CONF : Konfirmasi entry dari 1m + 5m + 15m sekaligus

ARSITEKTUR v13:
  ┌─────────────────────────────────────────────────────────────┐
  │  PRICE FEED (parallel bulk fetch, per 1 detik)             │
  │  → Harga semua posisi + BTC diambil BERSAMAAN              │
  └──────────────────┬──────────────────────────────────────────┘
                     │
  ┌──────────────────▼──────────────────────────────────────────┐
  │  POSITION ENGINE (1 detik tick)                            │
  │  - Instant cut: price < entry × (1 - CUT_PCT) → EXIT NOW  │
  │  - Breakeven lock: profit +0.15% → SL = entry             │
  │  - ATR-based dynamic trail: makin profit → trail makin     │
  │    ketat (bukan % flat, tapi proporsional ATR realtime)    │
  │  - Trail naik setiap tick tanpa delay                      │
  └──────────────────┬──────────────────────────────────────────┘
                     │
  ┌──────────────────▼──────────────────────────────────────────┐
  │  MOMENTUM SCANNER (event-driven + time-based)              │
  │  - Setelah close → LANGSUNG scan + rank semua kandidat     │
  │  - Rank by: momentum_score = volume_spike × RSI × MACD    │
  │  - Entry coin paling tinggi momentum_score-nya             │
  └─────────────────────────────────────────────────────────────┘

TRAIL LOGIC (v13):
  Entry @100
    ↓ harga naik ke 100.15 → SL geser ke 100.00 (breakeven lock)
    ↓ harga naik ke 100.30 → trail SL = 100.30 × (1 - atr_trail_pct)
    ↓ harga naik ke 100.50 → trail SL makin ketat (karena atr_trail_pct makin kecil)
    ↓ harga naik ke 100.60 → partial TP: 60% close, sisanya trail super ketat
    ↓ harga drop ke trail_SL → CLOSE, profit terkunci

INSTANT CUT LOGIC (v13):
  Entry @100
    ↓ harga turun ke 99.90 (0.10%) → CLOSE SEKARANG, sebelum makin dalam
    (tidak tunggu SL 0.25% — cut lebih awal, lindungi modal)
"""

import os, time, math, json, threading, queue, asyncio
import requests
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Untuk real: comment baris atas
# ══════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════
#  SETTINGS — v13
# ════════════════════════════════════════════════════

# ── CORE ─────────────────────────────────────────────
LEVERAGE              = 20
ORDER_USDT            = 10
MAX_POSITIONS         = 5

# ── INSTANT LOSS CUT ─────────────────────────────────
# Kalau harga langsung turun X% dari entry → cut SEKARANG
# Tidak menunggu SL normal. Perlindungan modal maksimal.
INSTANT_CUT_PCT       = 0.0010    # 0.10% — lebih agresif dari v12

# ── BREAKEVEN LOCK ───────────────────────────────────
# Setelah profit X% → SL otomatis digeser ke entry (modal aman)
BE_LOCK_PCT           = 0.0015    # 0.15% profit → breakeven lock

# ── ATR-BASED DYNAMIC TRAILING ───────────────────────
# Trail distance = ATR × multiplier (bukan % flat)
# Makin profit → multiplier makin kecil → trail makin ketat
TRAIL_ATR_MULT_PHASE1 = 1.5       # Awal: trail = 1.5 × ATR (longgar)
TRAIL_ATR_MULT_PHASE2 = 1.0       # Setelah profit 0.3%: trail = 1.0 × ATR
TRAIL_ATR_MULT_PHASE3 = 0.6       # Setelah profit 0.5%: trail = 0.6 × ATR (ketat)

# Fallback kalau ATR tidak tersedia (flat %)
TRAIL_FALLBACK_P1     = 0.0018
TRAIL_FALLBACK_P2     = 0.0010
TRAIL_FALLBACK_P3     = 0.0006

# Threshold upgrade trail phase
TRAIL_P2_TRIGGER      = 0.0030    # profit 0.30% → phase 2
TRAIL_P3_TRIGGER      = 0.0050    # profit 0.50% → phase 3

# ── PROFIT TARGETS ───────────────────────────────────
TP1_PCT               = 0.0035    # 0.35% → partial close 60%
TP2_PCT               = 0.0060    # 0.60% → full close (kalau trail belum kena)
SL_PCT                = 0.0025    # 0.25% — hard SL backup
TP1_CLOSE_RATIO       = 0.60      # 60% tutup di TP1

# ── KECEPATAN v13 ────────────────────────────────────
SCAN_INTERVAL         = 10        # scan normal tiap 10 detik
POSITION_MONITOR_SEC  = 1         # 1 DETIK — lebih cepat dari v12 (2 detik)!
SCAN_DELAY_MS         = 0.020     # 20ms antar API call (lebih cepat)
BATCH_SIZE            = 25        # 25 symbol per batch
MAX_HOLDING_MIN       = 8         # force close setelah 8 menit
SYMBOL_COOLDOWN_SEC   = 12        # cooldown 12 detik (lebih cepat dari v12)
RE_SCAN_DELAY_SEC     = 0.3       # delay 0.3 detik setelah close (lebih cepat dari 0.5)
PRICE_FETCH_PARALLEL  = 30        # fetch harga N simbol sekaligus

# ── CACHE ─────────────────────────────────────────────
OHLCV_CACHE_TTL_1M    = 5         # 5 detik (ultra fresh untuk 1m)
OHLCV_CACHE_TTL_5M    = 6
OHLCV_CACHE_TTL_15M   = 40
OHLCV_CACHE_TTL_1H    = 1800

# ── FILTER ENTRY ─────────────────────────────────────
MIN_SCORE_TREND       = 48
MIN_SCORE_MEANREV     = 58
MIN_ENTRY_SIGNALS     = 2
MIN_VOL_SPIKE         = 1.2
MIN_FNG               = 20
MAX_FNG_LONG          = 90
MIN_BREADTH           = 0.30
MAX_SL_PCT            = 0.004

# ── SYMBOLS (100+) ───────────────────────────────────
SYMBOLS = [
    # Tier 1 - Mega Cap
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    # Tier 2 - Large Cap
    "MATICUSDT","LTCUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT",
    "SUIUSDT","TIAUSDT","AAVEUSDT","RUNEUSDT","FILUSDT",
    "1000PEPEUSDT","WIFUSDT","JUPUSDT","SEIUSDT","PYTHUSDT",
    # Tier 3 - Mid Cap
    "FETUSDT","RENDERUSDT","WLDUSDT","STRKUSDT","ALTUSDT",
    "DYMUSDT","PIXELUSDT","PORTALUSDT","ACEUSDT",
    "MANTAUSDT","ZETAUSDT","RONINUSDT","NOTUSDT","DOGSUSDT",
    "EIGENUSDT","CATIUSDT","1000BONKUSDT",
    # DeFi
    "CRVUSDT","MKRUSDT","COMPUSDT","SUSHIUSDT",
    "SNXUSDT","1INCHUSDT","BALUSDT","DYDXUSDT",
    "GMXUSDT","RDNTUSDT","PENDLEUSDT","JTOUSDT","RAYUSDT",
    # L1/L2
    "ALGOUSDT","ICPUSDT","FTMUSDT","HBARUSDT","FLOWUSDT",
    "EGLDUSDT","THETAUSDT","KSMUSDT","KAVAUSDT","BANDUSDT",
    "COTIUSDT","SKLUSDT","CELRUSDT","CTSIUSDT",
    # Gaming/NFT
    "AXSUSDT","SANDUSDT","MANAUSDT","ENJUSDT","GALAUSDT",
    "IMXUSDT","BLURUSDT","MASKUSDT","HIGHUSDT",
    "BEAMXUSDT","MEMEUSDT","ORDIUSDT","1000RATSUSDT",
    # Infrastructure
    "ARUSDT","OCEANUSDT","TRUUSDT","SXPUSDT","BLZUSDT","POLYXUSDT",
    # Meme/Viral
    "SHIBUSDT","FLOKIUSDT","BONKUSDT","JASMYUSDT",
    "LUNCUSDT","CFXUSDT","COMBOUSDT","AGLDUSDT","IDUSDT","GASUSDT",
]

# ════════════════════════════════════════════════════
#  STATE GLOBAL
# ════════════════════════════════════════════════════
open_positions      = {}
trade_log           = []
_ohlcv_cache        = {}
_price_cache        = {}          # v13: cache harga bulk
_sym_info           = {}
_sym_cooldown       = {}
_atr_cache          = {}          # v13: cache ATR per symbol
_btc_price_history  = deque(maxlen=300)
_scan_batch_idx     = 0
_lock               = threading.Lock()
_executor           = ThreadPoolExecutor(max_workers=30)
_rescan_queue       = queue.Queue()
_hot_symbols        = deque(maxlen=30)
_momentum_scores    = {}          # v13: cache momentum score hasil ranking

_macro = {
    "fng": 50, "fng_label": "Neutral",
    "btc_trend_1m": "UNKNOWN",
    "btc_trend_5m": "UNKNOWN",
    "btc_trend_15m": "UNKNOWN",
    "btc_trend_1h": "UNKNOWN",
    "market_breadth": 0.5,
    "news": "neutral",
    "scalp_mode": "TREND",
    "last_fng": 0, "last_btc": 0, "last_breadth": 0, "last_news": 0,
}

_stats = {
    "total_trades":  0,
    "wins":          0,
    "losses":        0,
    "total_pnl":     0.0,
    "best_trade":    0.0,
    "worst_trade":   0.0,
    "tp1_hits":      0,
    "tp2_hits":      0,
    "sl_hits":       0,
    "instant_cuts":  0,
    "be_locks":      0,    # v13: berapa kali BE lock aktif
    "force_closes":  0,
    "rescans":       0,
    "momentum_entries": 0, # v13: entry berdasarkan momentum rank
    "session_start": time.time(),
}

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}


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
                        _sym_info[symbol] = {
                            "step":   float(f["stepSize"]),
                            "minQty": float(f["minQty"])
                        }
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
    """Single price fetch — pakai cache kalau fresh (< 1 detik)"""
    now = time.time()
    if symbol in _price_cache:
        ts, px = _price_cache[symbol]
        if now - ts < 1.0: return px
    try:
        px = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        _price_cache[symbol] = (now, px)
        return px
    except: return 0.0

def bulk_get_prices(symbols):
    """
    v13: Ambil harga banyak symbol SEKALIGUS (parallel).
    Jauh lebih cepat daripada satu-satu.
    """
    def _fetch(sym):
        try:
            px = float(client.futures_symbol_ticker(symbol=sym)["price"])
            _price_cache[sym] = (time.time(), px)
            return sym, px
        except: return sym, 0.0

    results = {}
    futures_map = {_executor.submit(_fetch, s): s for s in symbols}
    for f in as_completed(futures_map, timeout=3):
        sym, px = f.result()
        if px > 0: results[sym] = px
    return results

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
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        print(f"  ✅ {len(result)}/{len(SYMBOLS)} symbols valid")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))


# ════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=100):
    cache_key = (symbol, interval)
    now = time.time()
    ttl_map = {
        Client.KLINE_INTERVAL_1MINUTE:  OHLCV_CACHE_TTL_1M,
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
        df = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[cache_key] = (now, df)
        return df
    except:
        if cache_key in _ohlcv_cache:
            return _ohlcv_cache[cache_key][1]
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
    # v13: volume surge dalam 3 candle terakhir
    df["vol_surge"]  = (v > df["vol_ma"] * 2.0) & (v.shift(1) > df["vol_ma"].shift(1) * 1.5)
    return df

def _calc_trend(df):
    if df is None or len(df) < 25: return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    chg   = (price - c.iloc[-4]) / c.iloc[-4] * 100
    if price > ema9 > ema21 > ema50 and chg > 0:    return "BULL"
    elif price < ema9 < ema21 < ema50 and chg < 0:  return "BEAR"
    elif price > ema21 and chg > -0.2:              return "MILD_BULL"
    elif price < ema21 and chg < 0.2:               return "MILD_BEAR"
    return "SIDEWAYS"

def get_atr_pct(symbol, df_5m=None):
    """
    v13: Ambil ATR sebagai persentase harga.
    Dipakai untuk dynamic trail distance.
    """
    now = time.time()
    if symbol in _atr_cache:
        ts, val = _atr_cache[symbol]
        if now - ts < 30: return val  # cache 30 detik
    try:
        if df_5m is None:
            df_5m = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 30)
        if df_5m is not None and len(df_5m) >= 15:
            atr   = ta.volatility.AverageTrueRange(
                df_5m["high"], df_5m["low"], df_5m["close"], 14
            ).average_true_range().iloc[-1]
            price = df_5m["close"].iloc[-1]
            val   = atr / price if price > 0 else 0.002
            _atr_cache[symbol] = (now, val)
            return val
    except: pass
    return 0.002  # fallback 0.2%


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

    if now - _macro["last_btc"] > 10:
        try:
            df_1m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, 30)  # v13: tambah 1m
            df_5m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df_1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 60)
            _macro["btc_trend_1m"]  = _calc_trend(df_1m)
            _macro["btc_trend_5m"]  = _calc_trend(df_5m)
            _macro["btc_trend_15m"] = _calc_trend(df_15m)
            _macro["btc_trend_1h"]  = _calc_trend(df_1h)
            _macro["last_btc"]      = now
            t5m  = _macro["btc_trend_5m"]
            t15m = _macro["btc_trend_15m"]
            if t15m in ("BULL","BEAR") or t5m in ("BULL","BEAR"):
                _macro["scalp_mode"] = "TREND"
            else:
                _macro["scalp_mode"] = "MEAN_REV"
        except: pass

    if now - _macro["last_breadth"] > 60:
        try:
            bullish = 0
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 10)
                if df is not None and len(df) >= 5:
                    c  = df["close"]
                    e9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    if c.iloc[-1] > e9: bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except: pass

    if now - _macro.get("last_news", 0) > 120:
        try:
            data = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg_kw = ["crash","hack","ban","fraud","collapse","seized","scam","plunge"]
            pos_kw = ["institutional","ath","approved","record","bullish","rally","surge"]
            neg = pos = 0
            for post in data.get("results", [])[:8]:
                tl = post.get("title","").lower()
                if any(w in tl for w in neg_kw): neg += 1
                if any(w in tl for w in pos_kw): pos += 1
            score = pos - neg
            if score <= -3:   _macro["news"] = "strong_negative"
            elif score <= -1: _macro["news"] = "negative"
            elif score >= 3:  _macro["news"] = "strong_positive"
            else:             _macro["news"] = "neutral"
            _macro["last_news"] = now
        except: pass

def update_btc_price():
    try:
        px = get_price("BTCUSDT")
        if px > 0: _btc_price_history.append((time.time(), px))
    except: pass

def detect_flash_move():
    if len(_btc_price_history) < 2: return "none", 0.0
    cutoff  = time.time() - 180
    oldest  = next((px for ts, px in _btc_price_history if ts >= cutoff), None)
    if oldest is None: return "none", 0.0
    current = _btc_price_history[-1][1]
    pct = (current - oldest) / oldest * 100
    if pct <= -0.8: return "crash", abs(pct)
    if pct >= 0.8:  return "pump",  abs(pct)
    return "none", 0.0


# ════════════════════════════════════════════════════
#  ORDER BOOK IMBALANCE
# ════════════════════════════════════════════════════
def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=20)
        bids  = sum(float(b[1]) for b in ob["bids"][:10])
        asks  = sum(float(a[1]) for a in ob["asks"][:10])
        total = bids + asks
        return round((bids - asks) / total, 3) if total else 0.0
    except: return 0.0


# ════════════════════════════════════════════════════
#  v13: MOMENTUM SCORE — Ranking Kandidat Entry
# ════════════════════════════════════════════════════
def calc_momentum_score(symbol, df_5m, df_1m=None):
    """
    v13: Hitung momentum score untuk ranking entry.
    Lebih tinggi = lebih berpotensi profit sekarang.

    Komponen:
    1. Volume Surge Score (0-40): volume spike besar = momentum kuat
    2. RSI Momentum (0-25): RSI bergerak ke arah yang benar
    3. MACD Acceleration (0-20): MACD histogram semakin besar
    4. Price Velocity (0-15): seberapa cepat harga bergerak
    """
    if df_5m is None or len(df_5m) < 20: return 0.0

    score   = 0.0
    last    = df_5m.iloc[-1]
    prev    = df_5m.iloc[-2]

    # 1. Volume Surge (0-40)
    vr = last.get("vol_ratio", 1.0)
    if vr >= 3.0:   score += 40
    elif vr >= 2.0: score += 30
    elif vr >= 1.5: score += 20
    elif vr >= 1.2: score += 10

    # Bonus: buy ratio (order flow)
    br = last.get("buy_ratio", 0.5)
    if br > 0.60: score += 5
    elif br < 0.40: score += 5  # sell pressure juga momentum (untuk SHORT)

    # 2. RSI Momentum (0-25)
    rsi_now  = last.get("rsi_fast", 50)
    rsi_prev = prev.get("rsi_fast", 50)
    rsi_delta = abs(rsi_now - rsi_prev)
    if rsi_delta >= 5:   score += 25
    elif rsi_delta >= 3: score += 15
    elif rsi_delta >= 1: score += 8

    # 3. MACD Acceleration (0-20)
    h_now  = abs(last.get("macd_hist", 0))
    h_prev = abs(prev.get("macd_hist", 0))
    if h_prev > 0:
        macd_accel = (h_now - h_prev) / h_prev
        if macd_accel >= 0.5:   score += 20
        elif macd_accel >= 0.2: score += 12
        elif macd_accel >= 0.1: score += 6

    # 4. Price Velocity (0-15)
    if len(df_5m) >= 4:
        p_now  = df_5m["close"].iloc[-1]
        p_old  = df_5m["close"].iloc[-4]
        vel    = abs(p_now - p_old) / p_old * 100
        if vel >= 0.5:   score += 15
        elif vel >= 0.3: score += 10
        elif vel >= 0.1: score += 5

    # Bonus: 1m confirmation
    if df_1m is not None and len(df_1m) >= 5:
        l1  = df_1m.iloc[-1]
        l1p = df_1m.iloc[-2]
        vr1 = l1.get("vol_ratio", 1.0) if "vol_ratio" in df_1m.columns else 1.0
        if vr1 >= 2.0: score += 5  # spike di 1m juga = fresh momentum

    return min(score, 100.0)


def rank_candidates_by_momentum(candidates):
    """
    v13: Urutkan kandidat entry berdasarkan momentum score.
    Pastikan kita selalu entry ke coin yang PALING PANAS saat ini.
    """
    ranked = []
    for sym, direction, info in candidates:
        mom_score = _momentum_scores.get(sym, info.get("score", 0))
        combined  = info.get("score", 0) * 0.6 + mom_score * 0.4
        ranked.append((sym, direction, info, combined))
    ranked.sort(key=lambda x: x[3], reverse=True)
    return [(s, d, i) for s, d, i, _ in ranked]


# ════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE
# ════════════════════════════════════════════════════
def get_hft_entry_score(symbol, df_5m, direction, df_1m=None):
    if df_5m is None or len(df_5m) < 30:
        return 0, []
    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    prev2 = df_5m.iloc[-3]
    sigs  = []
    score = 0

    # ── 1. RSI MOMENTUM (max 20) ──────────────────────
    rsi      = last["rsi"]
    rsi_fast = last["rsi_fast"]
    rsi_prev = prev["rsi_fast"]
    if direction == "LONG":
        if rsi < 45 and rsi_fast > rsi_prev:
            score += 20; sigs.append(f"📈RSI bounce({rsi:.0f}↑)")
        elif rsi < 55 and rsi_fast > rsi_prev:
            score += 10; sigs.append(f"📈RSI ok({rsi:.0f})")
        elif rsi > 70: score -= 10
    else:
        if rsi > 55 and rsi_fast < rsi_prev:
            score += 20; sigs.append(f"📉RSI reject({rsi:.0f}↓)")
        elif rsi > 45 and rsi_fast < rsi_prev:
            score += 10; sigs.append(f"📉RSI ok({rsi:.0f})")
        elif rsi < 30: score -= 10

    # ── 2. MACD (max 20) ──────────────────────────────
    h_now   = last["macd_hist"]
    h_prev  = prev["macd_hist"]
    h_prev2 = prev2["macd_hist"]
    if direction == "LONG":
        if h_now > 0 and h_now > h_prev > h_prev2:
            score += 20; sigs.append("✅MACD hist naik")
        elif h_now > h_prev and h_now > 0:
            score += 12; sigs.append("✅MACD pos")
        elif h_prev < 0 and h_now >= 0:
            score += 15; sigs.append("⚡MACD cross 0")
    else:
        if h_now < 0 and h_now < h_prev < h_prev2:
            score += 20; sigs.append("✅MACD hist turun")
        elif h_now < h_prev and h_now < 0:
            score += 12; sigs.append("✅MACD neg")
        elif h_prev > 0 and h_now <= 0:
            score += 15; sigs.append("⚡MACD cross 0")

    # ── 3. VOLUME + ORDER FLOW (max 20) ───────────────
    vr = last["vol_ratio"]
    br = last["buy_ratio"]
    if vr >= MIN_VOL_SPIKE:
        if direction == "LONG" and br > 0.55 and last["close"] > last["open"]:
            pts = min(20, int(vr * 8)); score += pts
            sigs.append(f"🔥Vol{vr:.1f}x(buy{br:.0%})")
        elif direction == "SHORT" and br < 0.45 and last["close"] < last["open"]:
            pts = min(20, int(vr * 8)); score += pts
            sigs.append(f"🔥Vol{vr:.1f}x(sell{1-br:.0%})")
        else: score += 5
    elif vr > 1.0: score += 3

    # v13: volume surge bonus
    if last.get("vol_surge", False):
        score += 8; sigs.append("🌊VolSurge!")

    # ── 4. MICRO BREAKOUT (max 15) ────────────────────
    recent_5 = df_5m.iloc[-6:-1]
    micro_hi = recent_5["high"].max()
    micro_lo = recent_5["low"].min()
    price    = last["close"]
    if direction == "LONG" and price > micro_hi and last["body_ratio"] > 0.5:
        score += 15; sigs.append(f"🚀Break>{micro_hi:.5g}")
    elif direction == "SHORT" and price < micro_lo and last["body_ratio"] > 0.5:
        score += 15; sigs.append(f"💥Break<{micro_lo:.5g}")
    elif direction == "LONG" and price > (micro_hi + micro_lo) / 2:
        score += 7
    elif direction == "SHORT" and price < (micro_hi + micro_lo) / 2:
        score += 7

    # ── 5. EMA ALIGNMENT (max 15) ─────────────────────
    e5  = last["ema5"]
    e9  = last["ema9"]
    e21 = last["ema21"]
    p5  = prev["ema5"]
    p9  = prev["ema9"]
    if direction == "LONG":
        if price > e5 > e9 > e21:
            score += 15; sigs.append("📐EMA bull stack")
        elif price > e5 > e9:
            score += 10; sigs.append("📐EMA5>9")
        elif e5 > e9 and p5 <= p9:
            score += 8; sigs.append("📐EMA cross↑")
    else:
        if price < e5 < e9 < e21:
            score += 15; sigs.append("📐EMA bear stack")
        elif price < e5 < e9:
            score += 10; sigs.append("📐EMA5<9")
        elif e5 < e9 and p5 >= p9:
            score += 8; sigs.append("📐EMA cross↓")

    # ── 6. BB SQUEEZE / BOUNCE (max 10) ───────────────
    squeeze = bool(last["bb_squeeze"])
    if direction == "LONG":
        if price <= last["bb_lo"] * 1.002:
            score += 10; sigs.append("🎯BB bounce lo")
        elif squeeze and last["close"] > last["open"] and last["close"] > prev["high"]:
            score += 10; sigs.append("💥BB squeeze↑")
        elif price < last["bb_mid"]: score += 3
    else:
        if price >= last["bb_hi"] * 0.998:
            score += 10; sigs.append("🎯BB bounce hi")
        elif squeeze and last["close"] < last["open"] and last["close"] < prev["low"]:
            score += 10; sigs.append("💥BB squeeze↓")
        elif price > last["bb_mid"]: score += 3

    # Engulfing bonus
    if direction == "LONG" and last["close"] > last["open"] and \
       last["close"] > prev["high"] and last["body_ratio"] > 0.65:
        score += 8; sigs.append("🕯️Engulf↑")
    elif direction == "SHORT" and last["close"] < last["open"] and \
         last["close"] < prev["low"] and last["body_ratio"] > 0.65:
        score += 8; sigs.append("🕯️Engulf↓")

    # Stoch cross
    k  = last["stk"]; d_ = last["std"]
    pk = prev["stk"]; pd_ = prev["std"]
    if direction == "LONG" and k > d_ and pk <= pd_ and k < 75:
        score += 5; sigs.append(f"⚡Stoch GX({k:.0f})")
    elif direction == "SHORT" and k < d_ and pk >= pd_ and k > 25:
        score += 5; sigs.append(f"⚡Stoch DX({k:.0f})")

    # v13: 1m confirmation bonus
    if df_1m is not None and len(df_1m) >= 5:
        l1m = df_1m.iloc[-1]
        if direction == "LONG" and l1m["close"] > l1m["open"]: score += 5
        elif direction == "SHORT" and l1m["close"] < l1m["open"]: score += 5

    return max(0, min(score, 100)), sigs


def determine_direction(df_5m, df_15m=None, df_1m=None):
    if df_5m is None or len(df_5m) < 20: return None
    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    price = last["close"]
    e5, e9 = last["ema5"], last["ema9"]
    long_pts = short_pts = 0

    if price > e5 > e9:   long_pts  += 3
    elif price < e5 < e9: short_pts += 3
    if last["macd_hist"] > prev["macd_hist"]: long_pts  += 2
    else:                                     short_pts += 2
    rsi = last["rsi"]
    if rsi < 50:   long_pts  += 1
    elif rsi > 50: short_pts += 1
    if last["buy_ratio"] > 0.52 and last["close"] > last["open"]:  long_pts  += 2
    elif last["buy_ratio"] < 0.48 and last["close"] < last["open"]: short_pts += 2

    if df_15m is not None and len(df_15m) >= 20:
        l15 = df_15m.iloc[-1]
        if l15["ema9"] > l15["ema21"]: long_pts  += 2
        else:                          short_pts += 2

    # v13: 1m tiebreaker
    if df_1m is not None and len(df_1m) >= 3:
        l1 = df_1m.iloc[-1]
        if l1["close"] > l1["open"]: long_pts  += 1
        else:                        short_pts += 1

    btc_t = _macro.get("btc_trend_5m", "UNKNOWN")
    if btc_t in BULL_TRENDS:  long_pts  += 2
    elif btc_t in BEAR_TRENDS: short_pts += 2

    if long_pts > short_pts and long_pts >= 5:  return "LONG"
    if short_pts > long_pts and short_pts >= 5: return "SHORT"
    return None


# ════════════════════════════════════════════════════
#  ENTRY FILTER
# ════════════════════════════════════════════════════
def should_enter_hft(symbol):
    if is_symbol_cooling_down(symbol): return None, "cooldown"
    fng  = _macro["fng"]
    news = _macro["news"]
    if fng < MIN_FNG:              return None, f"F&G={fng}"
    if news == "strong_negative":  return None, "bad_news"
    flash_dir, _ = detect_flash_move()
    if flash_dir != "none":        return None, f"flash_{flash_dir}"

    df_1m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_1MINUTE, 30)   # v13: tambah 1m
    df_5m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 80)
    df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60)
    if df_5m is None or len(df_5m) < 30: return None, "no_data"

    df_5m  = run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m) >= 20:
        df_15m = run_ta(df_15m.copy())
    if df_1m is not None and len(df_1m) >= 10:
        df_1m  = run_ta(df_1m.copy())

    direction = determine_direction(df_5m, df_15m, df_1m)
    if direction is None: return None, "no_direction"

    btc_5m  = _macro["btc_trend_5m"]
    btc_15m = _macro["btc_trend_15m"]
    if direction == "LONG"  and btc_5m in BEAR_TRENDS and btc_15m in BEAR_TRENDS:
        return None, f"skip LONG — BTC {btc_5m}"
    if direction == "SHORT" and btc_5m in BULL_TRENDS and btc_15m in BULL_TRENDS:
        return None, f"skip SHORT — BTC {btc_5m}"
    if direction == "LONG"  and fng > MAX_FNG_LONG:
        return None, f"overbought F&G={fng}"

    score, sigs = get_hft_entry_score(symbol, df_5m, direction, df_1m)
    min_score   = MIN_SCORE_MEANREV if _macro["scalp_mode"] == "MEAN_REV" else MIN_SCORE_TREND
    if score < min_score: return None, f"score={score:.0f}<{min_score}"
    if len(sigs) < MIN_ENTRY_SIGNALS: return None, f"signals={len(sigs)}"

    atr     = df_5m["atr"].iloc[-1]
    price   = df_5m["close"].iloc[-1]
    atr_pct = atr / price
    if atr_pct > MAX_SL_PCT * 2: return None, f"ATR besar({atr_pct*100:.2f}%)"

    ob_imb = get_ob_imbalance(symbol)
    if direction == "LONG"  and ob_imb < -0.25: return None, f"OB imbal SHORT({ob_imb:.2f})"
    if direction == "SHORT" and ob_imb > 0.25:  return None, f"OB imbal LONG({ob_imb:.2f})"

    # Simpan momentum score
    mom_score = calc_momentum_score(symbol, df_5m, df_1m)
    _momentum_scores[symbol] = mom_score

    entry = price
    if direction == "LONG":
        sl  = round(entry * (1 - SL_PCT), 8)
        tp1 = round(entry * (1 + TP1_PCT), 8)
        tp2 = round(entry * (1 + TP2_PCT), 8)
    else:
        sl  = round(entry * (1 + SL_PCT), 8)
        tp1 = round(entry * (1 - TP1_PCT), 8)
        tp2 = round(entry * (1 - TP2_PCT), 8)

    return direction, {
        "score":       score,
        "mom_score":   mom_score,
        "signals":     sigs,
        "direction":   direction,
        "sl":          sl,
        "tp1":         tp1,
        "tp2":         tp2,
        "ob_imb":      ob_imb,
        "atr_pct":     atr_pct,
        "atr_abs":     atr,
        "scalp_mode":  _macro["scalp_mode"],
        "btc_trend":   _macro["btc_trend_5m"],
    }


# ════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction, info = should_enter_hft(symbol)
        if direction: return symbol, direction, info
    except: pass
    return None

def scan_batch_parallel(symbols):
    candidates = []
    futures = {_executor.submit(scan_symbol_safe, sym): sym for sym in symbols}
    for future in as_completed(futures, timeout=8):
        result = future.result()
        if result: candidates.append(result)
    # v13: rank by momentum score setelah collect
    return rank_candidates_by_momentum(candidates)


# ════════════════════════════════════════════════════
#  ⚡ INSTANT RE-SCAN (v13: lebih cepat + momentum rank)
# ════════════════════════════════════════════════════
def trigger_rescan(reason="", priority_symbol=None):
    if priority_symbol:
        _hot_symbols.appendleft(priority_symbol)
    _rescan_queue.put({"reason": reason, "ts": time.time()})

def instant_rescan_worker(symbols_active):
    while True:
        try:
            event = _rescan_queue.get(timeout=60)
            reason = event.get("reason", "")
            time.sleep(RE_SCAN_DELAY_SEC)  # 0.3 detik saja (v13)

            slots_free = MAX_POSITIONS - len(open_positions)
            if slots_free <= 0: continue

            flash_dir, _ = detect_flash_move()
            if flash_dir != "none": continue
            if _macro["news"] == "strong_negative": continue

            # Priority: hot symbols (baru TP) → lalu semua
            hot  = [s for s in list(_hot_symbols) if s not in open_positions]
            rest = [s for s in symbols_active if s not in open_positions and s not in hot]
            scan_list = hot + rest

            _stats["rescans"] += 1
            print(f"\n  ⚡ INSTANT RESCAN [{reason}] — {len(scan_list)} syms, {slots_free} slot")

            # Scan lebih banyak symbol sekaligus (v13: 80 vs v12: 60)
            candidates = scan_batch_parallel(scan_list[:80])

            if candidates:
                print(f"  🎯 Rescan: {len(candidates)} kandidat! (ranked by momentum)")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS: break
                    sig_str  = " | ".join(info.get("signals", [])[:3])
                    mom_str  = f"Mom:{info.get('mom_score',0):.0f}"
                    print(f"     ⭐ {sym} {direction} Score:{info['score']:.0f} {mom_str} | {sig_str}")
                    open_trade(sym, direction, info)
                    _stats["momentum_entries"] += 1
            else:
                print(f"  ⏳ Rescan: belum ada setup bagus")
        except queue.Empty:
            pass
        except Exception as e:
            print(f"  ❌ Rescan error: {e}")


# ════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    with _lock:
        if symbol in open_positions: return
        if len(open_positions) >= MAX_POSITIONS: return
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if price == 0: return
        qty = calc_qty(symbol, price)

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty)

        entry   = get_price(symbol)
        atr_pct = info.get("atr_pct", get_atr_pct(symbol))

        # ═══════════════════════════════════════════════════
        # v13 TRAIL SETUP: ATR-based, aktif dari entry
        # ═══════════════════════════════════════════════════
        trail_dist = max(atr_pct * TRAIL_ATR_MULT_PHASE1, TRAIL_FALLBACK_P1)

        if direction == "LONG":
            instant_cut = round(entry * (1 - INSTANT_CUT_PCT), 8)
            be_trigger  = round(entry * (1 + BE_LOCK_PCT), 8)
            trail_sl    = entry * (1 - trail_dist)
            sl_hard     = round(entry * (1 - SL_PCT), 8)
            tp1         = round(entry * (1 + TP1_PCT), 8)
            tp2         = round(entry * (1 + TP2_PCT), 8)
        else:
            instant_cut = round(entry * (1 + INSTANT_CUT_PCT), 8)
            be_trigger  = round(entry * (1 - BE_LOCK_PCT), 8)
            trail_sl    = entry * (1 + trail_dist)
            sl_hard     = round(entry * (1 + SL_PCT), 8)
            tp1         = round(entry * (1 - TP1_PCT), 8)
            tp2         = round(entry * (1 - TP2_PCT), 8)

        with _lock:
            open_positions[symbol] = {
                "side":             direction,
                "entry":            entry,
                "qty":              qty,
                "qty_remain":       qty,
                "sl":               sl_hard,
                "tp1":              tp1,
                "tp2":              tp2,
                "peak":             entry,
                "trail_sl":         trail_sl,
                "trail_phase":      1,
                "trailing_active":  True,       # aktif dari entry ✅
                "tp1_hit":          False,
                "be_active":        False,
                "be_trigger":       be_trigger, # v13: threshold BE lock
                "open_time":        time.time(),
                "score":            info.get("score", 0),
                "mom_score":        info.get("mom_score", 0),
                "signals":          info.get("signals", []),
                "atr_pct":          atr_pct,    # v13: ATR untuk dynamic trail
                "instant_cut":      instant_cut,
                "instant_cut_done": False,
            }

        sl_p  = abs(entry - sl_hard) / entry * 100
        tp1_p = abs(tp1 - entry) / entry * 100
        tp2_p = abs(tp2 - entry) / entry * 100
        sig_str = " | ".join(info.get("signals", [])[:3])

        print(f"\n  {'🟢' if direction=='LONG' else '🔴'} [{symbol}] {direction} @{entry:.5g}")
        print(f"     SL:{sl_p:.2f}% | TP1:{tp1_p:.2f}% | TP2:{tp2_p:.2f}% | ATR:{atr_pct*100:.3f}%")
        print(f"     InstantCut:{INSTANT_CUT_PCT*100:.2f}% | BE@+{BE_LOCK_PCT*100:.2f}% | Trail ATR×{TRAIL_ATR_MULT_PHASE1}")
        print(f"     Score:{info['score']:.0f} | Mom:{info.get('mom_score',0):.0f} | {sig_str}")
        _stats["total_trades"] += 1
    except Exception as e:
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
            type=ORDER_TYPE_MARKET,
            quantity=close_qty,
            reduceOnly=True)

        exit_p   = get_price(symbol)
        side     = pos["side"]
        pnl      = (exit_p - pos["entry"]) * close_qty if side == "LONG" \
                   else (pos["entry"] - exit_p) * close_qty
        hold_s   = time.time() - pos["open_time"]
        print(f"  🎯 [{symbol}] TP1 ({hold_s:.0f}s) | PnL: {pnl:+.4f}U ({TP1_PCT*100:.2f}%)")

        pos["tp1_hit"]    = True
        pos["qty_remain"] = abs(amt) - close_qty
        pos["be_active"]  = True
        pos["sl"]         = pos["entry"]
        pos["trail_phase"]= 2

        # v13: ATR-based trail phase 2
        atr_p = pos.get("atr_pct", 0.002)
        trail_dist = max(atr_p * TRAIL_ATR_MULT_PHASE2, TRAIL_FALLBACK_P2)
        if side == "LONG":
            pos["trail_sl"] = exit_p * (1 - trail_dist)
        else:
            pos["trail_sl"] = exit_p * (1 + trail_dist)
        pos["peak"] = exit_p

        _stats["tp1_hits"]  += 1
        _stats["wins"]      += 1
        _stats["total_pnl"] += pnl
        if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl

        trade_log.append({
            "symbol": symbol, "side": side,
            "pnl": round(pnl, 4), "reason": "TP1 Partial",
            "hold_sec": int(hold_s)
        })
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
            type=ORDER_TYPE_MARKET,
            quantity=abs(amt),
            reduceOnly=True)

        with _lock:
            pos = open_positions.pop(symbol, None)

        if pos:
            exit_p  = get_price(symbol)
            qty_r   = pos.get("qty_remain", pos["qty"])
            side    = pos["side"]
            pnl     = (exit_p - pos["entry"]) * qty_r if side == "LONG" \
                      else (pos["entry"] - exit_p) * qty_r
            pct     = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
            hold_s  = time.time() - pos["open_time"]
            emoji   = "🟢" if pnl >= 0 else "🔴"
            be_tag  = "[BE]" if pos.get("be_active") else ""
            print(f"  {emoji} [{symbol}] CLOSE — {reason}{be_tag} | {hold_s:.0f}s")
            print(f"     PnL: {pnl:+.4f}U ({pct:+.2f}%)")

            trade_log.append({
                "symbol": symbol, "side": side,
                "pnl": round(pnl, 4), "reason": reason,
                "hold_sec": int(hold_s)
            })
            _stats["total_pnl"] += pnl
            if pnl >= 0:
                _stats["wins"] += 1
                if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl
            else:
                _stats["losses"] += 1
                if pnl < _stats["worst_trade"]: _stats["worst_trade"] = pnl

            if "TP2"     in reason: _stats["tp2_hits"]     += 1
            if "SL"      in reason or "Stop" in reason: _stats["sl_hits"] += 1
            if "Force"   in reason: _stats["force_closes"]  += 1
            if "Instant" in reason: _stats["instant_cuts"]  += 1
            if "BE"      in reason: _stats["be_locks"]      += 1

            print_stats_inline()
            set_symbol_cooldown(symbol)
            trigger_rescan(f"close@{symbol}({reason})", priority_symbol=symbol)
        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Close error: {e}")
        return False


# ════════════════════════════════════════════════════
#  ⚡ POSITION MONITOR v13
#  — tick 1 detik, bulk price fetch, ATR trail dinamis
# ════════════════════════════════════════════════════
def _get_trail_dist(pos, profit_pct):
    """
    v13: Trail distance berbasis ATR (bukan % flat).
    Makin profit → multiplier makin kecil → trail makin ketat.
    """
    atr_p = pos.get("atr_pct", 0.002)
    phase = pos.get("trail_phase", 1)

    if phase >= 3 or profit_pct >= TRAIL_P3_TRIGGER:
        return max(atr_p * TRAIL_ATR_MULT_PHASE3, TRAIL_FALLBACK_P3)
    if phase >= 2 or profit_pct >= TRAIL_P2_TRIGGER:
        return max(atr_p * TRAIL_ATR_MULT_PHASE2, TRAIL_FALLBACK_P2)
    return max(atr_p * TRAIL_ATR_MULT_PHASE1, TRAIL_FALLBACK_P1)


def manage_positions(price_map=None):
    """
    v13: Terima price_map (bulk fetch) supaya tidak re-fetch per symbol.
    """
    if not open_positions: return
    flash_dir, flash_pct = detect_flash_move()

    for symbol in list(open_positions.keys()):
        pos = open_positions.get(symbol)
        if pos is None: continue

        # Pakai harga dari bulk fetch, atau fallback ke single fetch
        price = (price_map or {}).get(symbol) or get_price(symbol)
        if price == 0: continue

        side  = pos["side"]
        entry = pos["entry"]

        # ── FORCE CLOSE: timeout ─────────────────────────
        hold_min = (time.time() - pos["open_time"]) / 60
        if hold_min >= MAX_HOLDING_MIN:
            close_trade(symbol, f"⏰Force({hold_min:.0f}m)")
            continue

        # ── FLASH CRASH EXIT ──────────────────────────────
        if flash_dir == "crash" and side == "LONG":
            close_trade(symbol, f"⚡FlashCrash-{flash_pct:.1f}%")
            continue
        elif flash_dir == "pump" and side == "SHORT":
            close_trade(symbol, f"⚡FlashPump+{flash_pct:.1f}%")
            continue

        # ════════════════════════════════════════════════
        # ⚡ INSTANT CUT — prioritas tertinggi
        # Kalau harga langsung turun dari entry → EXIT NOW
        # ════════════════════════════════════════════════
        if not pos.get("instant_cut_done") and not pos.get("tp1_hit"):
            ic = pos["instant_cut"]
            if (side == "LONG"  and price <= ic) or \
               (side == "SHORT" and price >= ic):
                pos["instant_cut_done"] = True
                close_trade(symbol, f"⚡InstantCut(-{INSTANT_CUT_PCT*100:.2f}%)")
                continue

        # ════════════════════════════════════════════════
        # 🔒 BREAKEVEN LOCK — v13 fitur baru
        # Setelah profit BE_LOCK_PCT → SL naik ke entry
        # ════════════════════════════════════════════════
        if not pos.get("be_active") and not pos.get("be_locked"):
            be_trig = pos.get("be_trigger", entry)
            if (side == "LONG"  and price >= be_trig) or \
               (side == "SHORT" and price <= be_trig):
                pos["sl"]       = entry   # SL ke entry = modal aman
                pos["be_locked"] = True   # flag sudah di-lock
                _stats["be_locks"] += 1
                print(f"     🔒 [{symbol}] BE Lock! SL → entry@{entry:.5g}")

        if side == "LONG":
            profit_pct = (price - entry) / entry

            # ── TP1 partial close ───────────────────────
            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            # ── Upgrade trail phase ─────────────────────
            if profit_pct >= TRAIL_P3_TRIGGER and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3
                print(f"     ⬆️  [{symbol}] Trail→Phase3 (ATR×{TRAIL_ATR_MULT_PHASE3})")
            elif profit_pct >= TRAIL_P2_TRIGGER and pos["trail_phase"] < 2:
                pos["trail_phase"] = 2
                print(f"     ⬆️  [{symbol}] Trail→Phase2 (ATR×{TRAIL_ATR_MULT_PHASE2})")

            # ── Update ATR trailing SL setiap tick ──────
            # Trail naik SETIAP kali harga baru lebih tinggi
            if price > pos["peak"]:
                trail_dist      = _get_trail_dist(pos, profit_pct)
                new_trail_sl    = price * (1 - trail_dist)
                pos["peak"]     = price
                pos["trail_sl"] = new_trail_sl  # trail naik mengikuti harga ✅

            # ── TP2 full close ──────────────────────────
            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨TP2")
                continue

            # ── Trail stop hit ──────────────────────────
            if price <= pos["trail_sl"]:
                be_str = "BE" if pos.get("be_locked") else ""
                tp_str = "TP1" if pos.get("tp1_hit") else ""
                tag    = f"🔒Trail{be_str}{tp_str}" if (be_str or tp_str) else "🔄TrailStop"
                close_trade(symbol, tag)
                continue

            # ── Hard SL (backup akhir) ──────────────────
            if price <= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            # ── Status ──────────────────────────────────
            pnl     = (price - entry) * pos.get("qty_remain", pos["qty"])
            phase   = pos.get("trail_phase", 1)
            tsl     = f"TSL[P{phase}]:{pos['trail_sl']:.5g}"
            tp      = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            pnl_pct = profit_pct * 100
            be_icon = "🔒" if pos.get("be_locked") else "  "
            print(f"  📌{be_icon}[{symbol}] L@{entry:.5g}→{price:.5g} ({pnl_pct:+.2f}%) | {pnl:+.3f}U | {hold_min:.1f}m | {tsl} {tp}")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            if profit_pct >= TRAIL_P3_TRIGGER and pos["trail_phase"] < 3:
                pos["trail_phase"] = 3
            elif profit_pct >= TRAIL_P2_TRIGGER and pos["trail_phase"] < 2:
                pos["trail_phase"] = 2

            if price < pos["peak"]:
                trail_dist      = _get_trail_dist(pos, profit_pct)
                pos["peak"]     = price
                pos["trail_sl"] = price * (1 + trail_dist)

            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨TP2")
                continue

            if price >= pos["trail_sl"]:
                be_str = "BE" if pos.get("be_locked") else ""
                tp_str = "TP1" if pos.get("tp1_hit") else ""
                tag    = f"🔒Trail{be_str}{tp_str}" if (be_str or tp_str) else "🔄TrailStop"
                close_trade(symbol, tag)
                continue

            if price >= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            pnl     = (entry - price) * pos.get("qty_remain", pos["qty"])
            phase   = pos.get("trail_phase", 1)
            tsl     = f"TSL[P{phase}]:{pos['trail_sl']:.5g}"
            tp      = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            pnl_pct = profit_pct * 100
            be_icon = "🔒" if pos.get("be_locked") else "  "
            print(f"  📌{be_icon}[{symbol}] S@{entry:.5g}→{price:.5g} ({pnl_pct:+.2f}%) | {pnl:+.3f}U | {hold_min:.1f}m | {tsl} {tp}")


# ════════════════════════════════════════════════════
#  STATS
# ════════════════════════════════════════════════════
def print_stats_inline():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["total_pnl"]
    sess = (time.time() - _stats["session_start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    bar  = ("█" * _stats["wins"] + "░" * _stats["losses"])[-20:]
    emoji = "💚" if pnl >= 0 else "🔴"
    print(f"     ┌─ 📊 {n}T | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']} | {emoji}PnL:{pnl:+.4f}U | {tph:.0f}T/h")
    print(f"     └─ TP1:{_stats['tp1_hits']} TP2:{_stats['tp2_hits']} SL:{_stats['sl_hits']} ⚡Cut:{_stats['instant_cuts']} 🔒BE:{_stats['be_locks']} Force:{_stats['force_closes']} | [{bar}]")

def print_stats():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["session_start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    pnl  = _stats["total_pnl"]
    emoji = "💚" if pnl >= 0 else "🔴"
    print(f"\n  {'─'*67}")
    print(f"  📊 SESSION {sess*60:.0f}m | {tph:.0f} trades/jam | Rescans: {_stats['rescans']} | MomEntry:{_stats['momentum_entries']}")
    print(f"  🎯 {n} trades | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {emoji} Total P&L : {pnl:+.4f} USDT")
    print(f"  📈 Best:{_stats['best_trade']:+.4f}U │ 📉 Worst:{_stats['worst_trade']:+.4f}U")
    print(f"  🎯TP1:{_stats['tp1_hits']} ✨TP2:{_stats['tp2_hits']} 🛑SL:{_stats['sl_hits']} ⚡Cut:{_stats['instant_cuts']} 🔒BE:{_stats['be_locks']} ⏰Force:{_stats['force_closes']}")
    if trade_log:
        print(f"  📋 Last 5 trades:")
        for t in trade_log[-5:]:
            e    = "🟢" if t["pnl"] > 0 else "🔴"
            secs = t.get("hold_sec", 0)
            hold = f"{secs//60}m{secs%60}s"
            print(f"     {e} {t['symbol']:<14} {t['side']} {t['pnl']:+.4f}U ({hold}) — {t['reason'][:30]}")
    print(f"  {'─'*67}")


# ════════════════════════════════════════════════════
#  POSITION MONITOR THREAD v13
#  — 1 detik interval + bulk price fetch
# ════════════════════════════════════════════════════
def position_monitor_thread():
    """
    v13: Monitor tiap 1 detik (bukan 2 detik).
    Bulk fetch harga semua posisi sekaligus → lebih efisien.
    """
    while True:
        try:
            if open_positions:
                syms_to_watch = list(open_positions.keys())
                # v13: ambil semua harga sekaligus dalam satu batch
                price_map = bulk_get_prices(syms_to_watch)
                manage_positions(price_map)
        except Exception as e:
            print(f"  ❌ Monitor thread error: {e}")
        time.sleep(POSITION_MONITOR_SEC)  # 1 detik


# ════════════════════════════════════════════════════
#  MAIN LOOP — ULTRA LIGHTNING v13
# ════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║  ⚡⚡ BOT SCALPING v13 — ULTRA LIGHTNING                         ║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  Leverage:{LEVERAGE}x │ Per trade:${ORDER_USDT} │ Max posisi:{MAX_POSITIONS}                ║")
    print(f"║  TP1:{TP1_PCT*100:.2f}%(60%) │ TP2:{TP2_PCT*100:.2f}% │ SL:{SL_PCT*100:.2f}%                 ║")
    print(f"║  ⚡ InstantCut: -{INSTANT_CUT_PCT*100:.2f}% dari entry → CUT LANGSUNG       ║")
    print(f"║  🔒 BE Lock: +{BE_LOCK_PCT*100:.2f}% profit → SL naik ke entry             ║")
    print(f"║  📐 Trail: ATR×{TRAIL_ATR_MULT_PHASE1}→{TRAIL_ATR_MULT_PHASE2}→{TRAIL_ATR_MULT_PHASE3} (dinamis, naik setiap tick)     ║")
    print(f"║  🏆 Momentum Rank: entry ke coin PALING PANAS dulu              ║")
    print(f"║  🔍 Monitor: tiap {POSITION_MONITOR_SEC} detik | Bulk price fetch (parallel) ║")
    print("╚══════════════════════════════════════════════════════════════════╝")

    print("\n  ⏳ Validasi symbols...")
    symbols_active = validate_symbols()

    print(f"  📦 Pre-load symbol info (parallel)...")
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(get_sym_info, symbols_active[:50]))

    print(f"  🌐 Refresh macro...")
    refresh_macro()
    update_btc_price()

    print(f"\n  ✅ {len(symbols_active)} symbols aktif")
    print(f"  📊 BTC: 1m={_macro['btc_trend_1m']} 5m={_macro['btc_trend_5m']} 15m={_macro['btc_trend_15m']}")
    print(f"  🎯 Mode:{_macro['scalp_mode']} | F&G:{_macro['fng']} | Breadth:{_macro['market_breadth']*100:.0f}%")
    print(f"\n  🚀 Start dalam 3 detik...\n")
    time.sleep(3)

    # ── Dedicated threads ──────────────────────────────────────
    # 1. Position monitor (1 detik, bulk price fetch)
    pm_thread = threading.Thread(target=position_monitor_thread, daemon=True)
    pm_thread.start()
    print("  🔧 Position monitor thread (1s tick, bulk price): START ✅")

    # 2. Instant re-scan worker
    rs_thread = threading.Thread(
        target=instant_rescan_worker,
        args=(symbols_active,), daemon=True)
    rs_thread.start()
    print("  🔧 Instant re-scan thread (momentum ranked): START ✅")

    global _scan_batch_idx
    cycle = 0
    total_batches = math.ceil(len(symbols_active) / BATCH_SIZE)

    while True:
        cycle += 1
        refresh_macro()
        update_btc_price()

        flash_dir, flash_pct = detect_flash_move()
        flash_info = f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir != "none" else ""
        mode_e = "📈" if _macro["scalp_mode"] == "TREND" else "↩️"

        print(f"\n{'═'*70}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} | "
              f"F&G:{_macro['fng']}({_macro['fng_label']}) | "
              f"BTC1m:{_macro['btc_trend_1m']} 5m:{_macro['btc_trend_5m']} {flash_info}")
        print(f"  {mode_e} Mode:{_macro['scalp_mode']} | "
              f"Breadth:{_macro['market_breadth']*100:.0f}% | "
              f"News:{_macro['news']}")
        print(f"  📂 Posisi({len(open_positions)}/{MAX_POSITIONS}): "
              f"{list(open_positions.keys()) or '—'}")

        slots_free = MAX_POSITIONS - len(open_positions)

        if slots_free > 0 and \
           _macro["news"] != "strong_negative" and \
           flash_dir == "none" and \
           _macro["market_breadth"] >= MIN_BREADTH:

            batch_start = _scan_batch_idx * BATCH_SIZE
            batch = [s for s in symbols_active[batch_start:batch_start + BATCH_SIZE]
                     if s not in open_positions]
            _scan_batch_idx = (_scan_batch_idx + 1) % total_batches

            print(f"  🔍 Batch {_scan_batch_idx}/{total_batches} ({len(batch)} symbols) [parallel+momentum]")

            candidates = scan_batch_parallel(batch)

            if candidates:
                print(f"  🎯 {len(candidates)} setup! (sorted by momentum score)")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS: break
                    sig_str = " | ".join(info.get("signals", [])[:3])
                    mom_str = f"Mom:{info.get('mom_score',0):.0f}"
                    print(f"     ⭐ {sym} {direction} Score:{info['score']:.0f} {mom_str} | {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ Belum ada setup di batch ini")
        else:
            if slots_free == 0:
                print(f"  ⏸️  Posisi penuh ({MAX_POSITIONS}/{MAX_POSITIONS})")
            elif flash_dir != "none":
                print(f"  ⚡ Flash {flash_dir} — skip entry")
            elif _macro["market_breadth"] < MIN_BREADTH:
                print(f"  ⚠️  Breadth rendah — skip")
            else:
                print(f"  🚫 Bad news — skip entry")

        if cycle % 20 == 0:
            print_stats()

        print(f"\n  ⏱️  Scan berikutnya: {SCAN_INTERVAL}s | "
              f"Rescans: {_stats['rescans']} | "
              f"⚡Cuts: {_stats['instant_cuts']} | "
              f"🔒BE: {_stats['be_locks']} | "
              f"🏆MomEntry: {_stats['momentum_entries']}")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()