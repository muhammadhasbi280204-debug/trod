"""
Bot Scalping v15 — QUANTUM SPEED ENGINE ⚡
==========================================

PERUBAHAN DARI v14 → v15
─────────────────────────
✅ INSTANT CUT dari entry — kalau minus -0.08% langsung close tanpa nunggu
✅ TRAILING STOP AKTIF DARI ENTRY — bukan delayed, langsung jalan dari open
✅ ADAPTIVE TRAIL — makin profit → trail makin ketat (phase 1→2→3→4)
✅ ULTRA FAST SCAN — multi-thread 20 workers, scan paralel terus-menerus
✅ NOISE FILTER v2 — deteksi noise vs trend real pakai ATR/BB/orderflow combo
✅ VOLUME QUALITY FILTER — hanya coin dengan volume gede + spread kecil
✅ SIGNAL STRENGTH RANKING — entry hanya kalau 3+ signal konfirmasi
✅ DYNAMIC POSITION SIZING — posisi lebih gede kalau sinyal super kuat
✅ ANTI-CHURN LOGIC — skip coin yang baru kena SL dalam 60 detik
✅ MOMENTUM DECAY DETECTION — kalau momentum mulai lemah, trail dipercepat
✅ ORDER BOOK PRESSURE SCORE — 20-level OB analysis buat konfirmasi arah
✅ RESCAN AGRESIF — setiap posisi close → langsung scan ulang dalam 0.2s
"""

import os, time, math, json, threading, queue
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
# TESTNET / REAL — pilih salah satu
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
# Untuk real: comment baris atas
# ══════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════
#  CONFIG v15 — QUANTUM SPEED ENGINE
# ════════════════════════════════════════════════════

# ── CORE ─────────────────────────────────────────────────
LEVERAGE              = 20
ORDER_USDT            = 2            # $2 per trade × 20 = $40 exposure
MAX_POSITIONS         = 3

# ── INSTANT CUT — KECEPATAN CAHAYA ───────────────────────
INSTANT_CUT_PCT       = 0.0008      # -0.08% → langsung kabur (tanpa basa-basi)
INSTANT_CUT_ENABLED   = True        # selalu aktif dari detik pertama

# ── ATR TRAILING STOP (aktif dari entry) ──────────────────
# Phase 1: Baru entry, trail lebar (kasih napas dikit)
# Phase 2: Profit > 0.15%, trail mulai ketat
# Phase 3: Profit > 0.30%, trail sangat ketat
# Phase 4: Profit > 0.50%, trail super ketat (lindungi profit)
ATR_TRAIL_PHASE1      = 1.0         # ATR × 1.0 dari entry
ATR_TRAIL_PHASE2      = 0.7         # ATR × 0.7 setelah profit 0.15%
ATR_TRAIL_PHASE3      = 0.5         # ATR × 0.5 setelah profit 0.30%
ATR_TRAIL_PHASE4      = 0.3         # ATR × 0.3 setelah profit 0.50%

TRAIL_PHASE2_PCT      = 0.0015      # aktif kalau profit > 0.15%
TRAIL_PHASE3_PCT      = 0.0030      # aktif kalau profit > 0.30%
TRAIL_PHASE4_PCT      = 0.0050      # aktif kalau profit > 0.50%

# ── ATR SL/TP ─────────────────────────────────────────────
ATR_SL_MULT           = 1.0         # SL lebih ketat dari v14
ATR_TP1_MULT          = 1.8
ATR_TP2_MULT          = 3.0

MIN_SL_PCT            = 0.0008
MAX_SL_PCT            = 0.0050
MIN_TP1_PCT           = 0.0015
MAX_TP2_PCT           = 0.0180

# Partial close TP1
TP1_CLOSE_RATIO       = 0.55

# ── SPEED CONFIG ──────────────────────────────────────────
SCAN_INTERVAL         = 2           # scan tiap 2 detik (lebih cepat dari v14)
POSITION_MONITOR_SEC  = 0.5         # monitor posisi tiap 0.5 detik (2× lebih cepat)
SCAN_DELAY_MS         = 0.030       # delay antar API call
BATCH_SIZE            = 20
MAX_WORKERS           = 20          # lebih banyak worker
MAX_HOLDING_MIN       = 4.5         # paksa close lebih cepat
SYMBOL_COOLDOWN_SEC   = 30          # cooldown lebih lama setelah close
RE_SCAN_DELAY_SEC     = 0.2         # rescan lebih cepat

# ── NOISE FILTER v2 ───────────────────────────────────────
CHOP_INDEX_THRESHOLD  = 55.0        # lebih ketat dari v14 (58)
MIN_BB_WIDTH_PCT      = 0.006       # BB harus cukup lebar
MIN_ADX               = 22          # ADX minimum untuk masuk
MIN_VOLUME_USDT       = 2_000_000   # minimum volume 24h = $2M
MIN_VOL_SURGE_ENTRY   = 1.8         # volume harus surge 1.8× dari rata-rata
MAX_SPREAD_RATIO      = 0.25        # spread ketat
MIN_OB_PRESSURE       = 0.15        # minimum OB imbalance untuk entry

# ── MOMENTUM FILTER ──────────────────────────────────────
MIN_MOMENTUM_PCT      = 0.0020      # minimum momentum 0.20%
MIN_TREND_CANDLES     = 3           # minimal 3 candle searah

# ── SIGNAL QUALITY ────────────────────────────────────────
MIN_SCORE             = 50          # lebih tinggi dari v14
MIN_ENTRY_SIGNALS     = 3           # butuh 3 signal minimum
MAX_SL_ATR_PCT        = 0.008       # skip kalau ATR terlalu gede

# ── SESSION FILTER ────────────────────────────────────────
BAD_HOURS_UTC         = {4, 5, 6}
BAD_HOURS_MIN_SCORE   = 65

# ── KILL SWITCH ───────────────────────────────────────────
DAILY_LOSS_LIMIT      = -5.0
CONSEC_LOSS_MAX       = 4           # lebih ketat (v14=5)
CONSEC_LOSS_PAUSE_MIN = 20          # pause lebih pendek supaya bisa balik cepat
MAX_API_LAG_SEC       = 2.5

# ── CACHE TTL ─────────────────────────────────────────────
OHLCV_CACHE_TTL_1M    = 1.5         # lebih fresh
OHLCV_CACHE_TTL_3M    = 3
OHLCV_CACHE_TTL_5M    = 4
OHLCV_CACHE_TTL_15M   = 25
OHLCV_CACHE_TTL_1H    = 1800
TICKER24H_TTL         = 6
FUNDING_TTL           = 25
TOP_MOVERS_TTL        = 6

# ── FILTER MACRO ──────────────────────────────────────────
MIN_FNG               = 15
MAX_FNG_LONG          = 92
MIN_BREADTH           = 0.0

# ── SYMBOLS — fokus ke high volume liquid coins ───────────
SYMBOLS = [
    # Tier 1 — paling liquid, spread kecil, noise rendah
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    "MATICUSDT","ATOMUSDT","UNIUSDT","NEARUSDT","APTUSDT",
    "ARBUSDT","OPUSDT","INJUSDT","SUIUSDT","AAVEUSDT",

    # Tier 2 — medium cap, masih liquid
    "TIAUSDT","FILUSDT","1000PEPEUSDT","WIFUSDT","JUPUSDT",
    "SEIUSDT","PYTHUSDT","FETUSDT","RENDERUSDT","WLDUSDT",
    "STRKUSDT","RONINUSDT","EIGENUSDT","CATIUSDT","1000BONKUSDT",
    "CRVUSDT","MKRUSDT","DYDXUSDT","GMXUSDT","PENDLEUSDT",
    "JTOUSDT","RAYUSDT","ALGOUSDT","ICPUSDT","FTMUSDT",
    "HBARUSDT","THETAUSDT","AXSUSDT","IMXUSDT","ORDIUSDT",
    "ARUSDT","STXUSDT","TONUSDT","TAOUSDT","ONDOUSDT",
    "ENARUSDT","BOMEUSDT","SAFEUSDT","NOTUSDT","KASUSDT",

    # Tier 3 — aktif tapi butuh filter lebih ketat
    "LTCUSDT","ETCUSDT","RUNEUSDT","ALTUSDT","DYMUSDT",
    "PIXELUSDT","DOGSUSDT","SANDUSDT","MANAUSDT","GALAUSDT",
    "BLURUSDT","MASKUSDT","BEAMXUSDT","MEMEUSDT",
    "OCEANUSDT","SHIBUSDT","FLOKIUSDT","LUNCUSDT",
    "WUSDT","VANRYUSDT","XAIUSDT","SNXUSDT","COMPUSDT",
    "BANDUSDT","SKLUSDT","HIGHUSDT","EGLDUSDT","KAVAUSDT",
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
_executor           = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_queue       = queue.Queue()
_hot_symbols        = deque(maxlen=50)
_ticker24h_cache    = {}
_ticker24h_ts       = 0
_funding_cache      = {}
_funding_ts         = 0
_top_movers         = []
_top_movers_ts      = 0

# ── Kill switch ───────────────────────────────────────────
_kill_switch = {
    "active":         False,
    "reason":         "",
    "resume_time":    0,
    "consec_losses":  0,
    "daily_pnl":      0.0,
    "daily_reset_ts": 0,
    "last_api_check": 0,
    "api_lag":        0.0,
}

# ── Performance analytics ─────────────────────────────────
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
    "skipped_noise": 0,
    "pnl_history": deque(maxlen=200),
    "session_start": time.time(),
}

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}


# ════════════════════════════════════════════════════
#  KILL SWITCH ENGINE
# ════════════════════════════════════════════════════
def check_kill_switch():
    ks  = _kill_switch
    now = time.time()

    if ks["active"] and now >= ks["resume_time"]:
        ks["active"]       = False
        ks["reason"]       = ""
        ks["consec_losses"] = 0
        print(f"\n  ✅ Kill switch CLEARED — bot aktif kembali")

    if ks["active"]:
        return True, ks["reason"]

    day_start = now - (now % 86400)
    if day_start > ks["daily_reset_ts"]:
        ks["daily_pnl"]      = 0.0
        ks["daily_reset_ts"] = day_start

    if ks["daily_pnl"] <= DAILY_LOSS_LIMIT:
        ks["active"]      = True
        ks["reason"]      = f"daily_loss({ks['daily_pnl']:.2f}U)"
        ks["resume_time"] = day_start + 86400
        print(f"\n  🚨 KILL SWITCH: daily loss ({ks['daily_pnl']:.2f}U)")
        return True, ks["reason"]

    if ks["consec_losses"] >= CONSEC_LOSS_MAX:
        ks["active"]      = True
        ks["reason"]      = f"consec_loss({ks['consec_losses']})"
        ks["resume_time"] = now + (CONSEC_LOSS_PAUSE_MIN * 60)
        print(f"\n  🚨 KILL SWITCH: {ks['consec_losses']} loss beruntun — pause {CONSEC_LOSS_PAUSE_MIN}m")
        return True, ks["reason"]

    return False, ""


def update_kill_switch_after_trade(pnl):
    ks = _kill_switch
    ks["daily_pnl"] += pnl
    if pnl < 0:
        ks["consec_losses"] += 1
    else:
        ks["consec_losses"] = 0


def check_api_latency():
    try:
        t0  = time.time()
        client.futures_ping()
        lag = time.time() - t0
        _kill_switch["api_lag"] = lag
        return lag <= MAX_API_LAG_SEC
    except:
        return False


# ════════════════════════════════════════════════════
#  NOISE DETECTOR v2 — Bedain noise vs real move
# ════════════════════════════════════════════════════
def calc_choppiness_index(df, period=14):
    """CI < 38 = strong trend | CI > 58 = chop"""
    if df is None or len(df) < period + 2:
        return 50.0
    try:
        high  = df["high"].values
        low   = df["low"].values
        close = df["close"].values
        tr_sum = 0.0
        for i in range(-period, 0):
            tr = max(
                high[i] - low[i],
                abs(high[i] - close[i-1]),
                abs(low[i]  - close[i-1])
            )
            tr_sum += tr
        highest_high = max(high[-period:])
        lowest_low   = min(low[-period:])
        price_range  = highest_high - lowest_low
        if price_range == 0 or tr_sum == 0:
            return 50.0
        ci = 100 * math.log10(tr_sum / price_range) / math.log10(period)
        return round(ci, 2)
    except:
        return 50.0


def calc_noise_ratio(df, period=10):
    """
    Noise ratio = (sum of |candle body| / total price range)
    Semakin kecil = choppy/noisy
    Semakin besar = directional/clean
    """
    if df is None or len(df) < period:
        return 0.5
    try:
        recent = df.iloc[-period:]
        total_body = (recent["close"] - recent["open"]).abs().sum()
        total_range = recent["high"].max() - recent["low"].min()
        if total_range == 0:
            return 0.0
        return min(total_body / total_range, 1.0)
    except:
        return 0.5


def calc_trend_consistency(df, direction, period=8):
    """
    Hitung berapa % candle searah dengan direction.
    LONG: bullish candles / total
    SHORT: bearish candles / total
    """
    if df is None or len(df) < period:
        return 0.0
    try:
        recent = df.iloc[-period:]
        if direction == "LONG":
            aligned = (recent["close"] > recent["open"]).sum()
        else:
            aligned = (recent["close"] < recent["open"]).sum()
        return aligned / period
    except:
        return 0.0


def is_noise_market(df_5m, direction):
    """
    v2 Noise Detector — multi-factor.
    Return (is_noisy, score, reasons)
    """
    if df_5m is None or len(df_5m) < 20:
        return True, 0.0, ["no_data"]

    reasons = []
    noise_score = 0

    # 1. Choppiness Index
    ci = calc_choppiness_index(df_5m, 14)
    if ci > CHOP_INDEX_THRESHOLD:
        noise_score += 30
        reasons.append(f"CI={ci:.1f}")

    # 2. Noise ratio — body vs range
    nr = calc_noise_ratio(df_5m, 10)
    if nr < 0.25:
        noise_score += 25
        reasons.append(f"NR={nr:.2f}")

    # 3. BB width
    last = df_5m.iloc[-1]
    bb_width = last.get("bb_width", 0.01)
    if bb_width < MIN_BB_WIDTH_PCT:
        noise_score += 20
        reasons.append(f"BB_narrow({bb_width*100:.2f}%)")

    # 4. EMA cross frequency (banyak cross = sideways)
    try:
        e3 = df_5m["ema3"].values[-20:]
        e9 = df_5m["ema9"].values[-20:]
        cross_count = sum(
            1 for i in range(1, len(e3))
            if (e3[i-1] > e9[i-1]) != (e3[i] > e9[i])
        )
        if cross_count > 3:
            noise_score += 20
            reasons.append(f"EMAx{cross_count}")
    except:
        pass

    # 5. Trend consistency
    tc = calc_trend_consistency(df_5m, direction, 8)
    if tc < 0.50:
        noise_score += 20
        reasons.append(f"TC={tc:.0%}")

    # 6. MACD histogram stability
    try:
        hist_recent = df_5m["macd_hist"].values[-8:]
        sign_changes = sum(
            1 for i in range(1, len(hist_recent))
            if (hist_recent[i-1] > 0) != (hist_recent[i] > 0)
        )
        if sign_changes >= 3:
            noise_score += 15
            reasons.append(f"MACD_flip{sign_changes}")
    except:
        pass

    # Threshold: noise_score >= 40 = noisy market
    is_noisy = noise_score >= 40
    return is_noisy, noise_score, reasons


# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info:
        return _sym_info[symbol]
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
    except:
        pass
    return {"step": 1.0, "minQty": 1.0}


def round_step(qty, step):
    p = max(0, int(round(-math.log(step, 10), 0))) if step < 1 else 0
    return round(math.floor(qty / step) * step, p)


def calc_qty(symbol, price):
    info = get_sym_info(symbol)
    raw  = (ORDER_USDT * LEVERAGE) / price
    return max(round_step(raw, info["step"]), info["minQty"])


def set_leverage(symbol):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except:
        pass


def get_price(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0


def get_exchange_amt(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0:
                return amt
        return 0
    except:
        return None


def is_symbol_cooling_down(symbol):
    if symbol not in _sym_cooldown:
        return False
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
#  MARKET DATA
# ════════════════════════════════════════════════════
def fetch_ticker24h_all():
    global _ticker24h_cache, _ticker24h_ts
    now = time.time()
    if now - _ticker24h_ts < TICKER24H_TTL and _ticker24h_cache:
        return _ticker24h_cache
    try:
        tickers    = client.futures_ticker()
        new_cache  = {}
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
    except:
        return _ticker24h_cache


def fetch_funding_rates():
    global _funding_cache, _funding_ts
    now = time.time()
    if now - _funding_ts < FUNDING_TTL and _funding_cache:
        return _funding_cache
    try:
        premium   = client.futures_mark_price()
        new_cache = {}
        for p in premium:
            sym = p["symbol"]
            fr  = float(p.get("lastFundingRate", 0))
            new_cache[sym] = fr
        _funding_cache = new_cache
        _funding_ts    = now
        return new_cache
    except:
        return _funding_cache


def get_top_movers(symbols_active, n=40):
    """
    Ambil top movers berdasarkan:
    1. % perubahan 24h terbesar (momentum)
    2. Volume gede ($2M+)
    3. Bukan sideways
    """
    global _top_movers, _top_movers_ts
    now = time.time()
    if now - _top_movers_ts < TOP_MOVERS_TTL and _top_movers:
        return _top_movers
    try:
        tickers    = fetch_ticker24h_all()
        active_set = set(symbols_active)
        movers     = []
        for sym, data in tickers.items():
            if sym not in active_set:
                continue
            pct = data["pct"]
            vol = data["vol24h"]
            if vol < MIN_VOLUME_USDT:
                continue
            # Score = momentum × log(volume)
            score = abs(pct) * math.log10(max(vol, 1))
            movers.append((sym, pct, vol, score))
        movers.sort(key=lambda x: x[3], reverse=True)
        result = []
        for sym, pct, vol, score in movers[:n]:
            direction = "LONG" if pct > 0 else "SHORT"
            result.append((sym, pct, direction))
        _top_movers    = result
        _top_movers_ts = now
        return result
    except:
        return _top_movers


def get_funding_bias(symbol):
    rates = fetch_funding_rates()
    fr    = rates.get(symbol, 0)
    if fr > 0.0005:
        return "bearish_bias", fr
    if fr < -0.0005:
        return "bullish_bias", fr
    return "neutral", fr


def get_ob_pressure(symbol):
    """
    Analisis order book 20 level.
    Return: (imbalance_score, bid_depth, ask_depth, wall_detected)
    """
    try:
        ob        = client.futures_order_book(symbol=symbol, limit=20)
        bids      = [(float(b[0]), float(b[1])) for b in ob["bids"][:20]]
        asks      = [(float(a[0]), float(a[1])) for a in ob["asks"][:20]]

        # Weight-adjusted (level lebih dekat = lebih berat)
        bid_w = sum(qty * (1 / (i + 1)) for i, (_, qty) in enumerate(bids))
        ask_w = sum(qty * (1 / (i + 1)) for i, (_, qty) in enumerate(asks))
        total = bid_w + ask_w

        imbalance = round((bid_w - ask_w) / total, 3) if total else 0.0

        # Deteksi big wall (level dengan qty > 5× rata-rata)
        bid_qtys   = [qty for _, qty in bids[:5]]
        ask_qtys   = [qty for _, qty in asks[:5]]
        avg_bid    = sum(bid_qtys) / len(bid_qtys) if bid_qtys else 0
        avg_ask    = sum(ask_qtys) / len(ask_qtys) if ask_qtys else 0
        bid_wall   = max(bid_qtys) > avg_bid * 5 if bid_qtys else False
        ask_wall   = max(ask_qtys) > avg_ask * 5 if ask_qtys else False

        return imbalance, bid_w, ask_w, bid_wall, ask_wall
    except:
        return 0.0, 0.0, 0.0, False, False


# ════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=100):
    cache_key = (symbol, interval)
    now       = time.time()
    ttl_map   = {
        Client.KLINE_INTERVAL_1MINUTE:  OHLCV_CACHE_TTL_1M,
        Client.KLINE_INTERVAL_3MINUTE:  OHLCV_CACHE_TTL_3M,
        Client.KLINE_INTERVAL_5MINUTE:  OHLCV_CACHE_TTL_5M,
        Client.KLINE_INTERVAL_15MINUTE: OHLCV_CACHE_TTL_15M,
        Client.KLINE_INTERVAL_1HOUR:    OHLCV_CACHE_TTL_1H,
    }
    ttl = ttl_map.get(interval, 30)
    if cache_key in _ohlcv_cache:
        ts, df_cached = _ohlcv_cache[cache_key]
        if now - ts < ttl:
            return df_cached
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

    df["rsi"]       = ta.momentum.RSIIndicator(c, 14).rsi()
    df["rsi_fast"]  = ta.momentum.RSIIndicator(c, 7).rsi()

    macd            = ta.trend.MACD(c, 12, 26, 9)
    df["macd"]      = macd.macd()
    df["macd_sig"]  = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()

    df["ema3"]      = ta.trend.EMAIndicator(c, 3).ema_indicator()
    df["ema5"]      = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["ema9"]      = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"]     = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"]     = ta.trend.EMAIndicator(c, 50).ema_indicator()

    bb              = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_hi"]     = bb.bollinger_hband()
    df["bb_lo"]     = bb.bollinger_lband()
    df["bb_mid"]    = bb.bollinger_mavg()
    df["bb_width"]  = (df["bb_hi"] - df["bb_lo"]) / df["bb_mid"]

    stoch           = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
    df["stk"]       = stoch.stoch()
    df["std"]       = stoch.stoch_signal()

    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()

    # ADX untuk ukuran kekuatan trend
    adx_indicator   = ta.trend.ADXIndicator(h, l, c, 14)
    df["adx"]       = adx_indicator.adx()
    df["adx_pos"]   = adx_indicator.adx_pos()
    df["adx_neg"]   = adx_indicator.adx_neg()

    df["vol_ma"]    = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol_ma"].replace(0, 1)
    df["buy_ratio"] = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"]      = abs(df["close"] - df["open"])
    df["range_"]    = df["high"] - df["low"]
    df["body_ratio"]= df["body"] / df["range_"].replace(0, 1)
    df["mom5"]      = (c - c.shift(5)) / c.shift(5)
    df["mom3"]      = (c - c.shift(3)) / c.shift(3)
    df["mom1"]      = (c - c.shift(1)) / c.shift(1)

    return df


def _calc_trend(df):
    if df is None or len(df) < 25:
        return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    chg   = (price - c.iloc[-4]) / c.iloc[-4] * 100
    if price > ema9 > ema21 > ema50 and chg > 0:    return "BULL"
    elif price < ema9 < ema21 < ema50 and chg < 0:  return "BEAR"
    elif price > ema21 and chg > -0.2:               return "MILD_BULL"
    elif price < ema21 and chg < 0.2:                return "MILD_BEAR"
    return "SIDEWAYS"


# ════════════════════════════════════════════════════
#  ATR-BASED LEVELS
# ════════════════════════════════════════════════════
def calc_atr_levels(entry, atr, direction):
    raw_sl_dist  = atr * ATR_SL_MULT
    raw_tp1_dist = atr * ATR_TP1_MULT
    raw_tp2_dist = atr * ATR_TP2_MULT

    sl_dist  = max(entry * MIN_SL_PCT, min(raw_sl_dist, entry * MAX_SL_PCT))
    tp1_dist = max(entry * MIN_TP1_PCT, raw_tp1_dist)
    tp2_dist = min(entry * MAX_TP2_PCT, raw_tp2_dist)
    tp2_dist = max(tp2_dist, tp1_dist * 1.5)

    if direction == "LONG":
        sl  = round(entry - sl_dist,  8)
        tp1 = round(entry + tp1_dist, 8)
        tp2 = round(entry + tp2_dist, 8)
        # Trail aktif dari entry dengan ATR_TRAIL_PHASE1
        trail_init = round(entry - atr * ATR_TRAIL_PHASE1, 8)
        trail_init = max(trail_init, sl)
    else:
        sl  = round(entry + sl_dist,  8)
        tp1 = round(entry - tp1_dist, 8)
        tp2 = round(entry - tp2_dist, 8)
        trail_init = round(entry + atr * ATR_TRAIL_PHASE1, 8)
        trail_init = min(trail_init, sl)

    return {
        "sl":         sl,
        "tp1":        tp1,
        "tp2":        tp2,
        "trail_init": trail_init,
        "sl_pct":     sl_dist  / entry,
        "tp1_pct":    tp1_dist / entry,
        "tp2_pct":    tp2_dist / entry,
        "atr":        atr,
        "atr_pct":    atr / entry,
    }


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
        except:
            pass

    if now - _macro["last_btc"] > 4:
        try:
            df_1m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, 30)
            df_5m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df_1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR,   60)
            _macro["btc_trend_1m"]  = _calc_trend(df_1m)
            _macro["btc_trend_5m"]  = _calc_trend(df_5m)
            _macro["btc_trend_15m"] = _calc_trend(df_15m)
            _macro["btc_trend_1h"]  = _calc_trend(df_1h)
            _macro["last_btc"]      = now

            t5m  = _macro["btc_trend_5m"]
            t15m = _macro["btc_trend_15m"]
            if t15m in ("BULL", "BEAR") or t5m in ("BULL", "BEAR"):
                _macro["scalp_mode"] = "TREND"
            else:
                _macro["scalp_mode"] = "MEAN_REV"
        except:
            pass

    if now - _macro["last_breadth"] > 25:
        try:
            bullish = 0
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 10)
                if df is not None and len(df) >= 5:
                    c  = df["close"]
                    e9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    if c.iloc[-1] > e9:
                        bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except:
            pass

    if now - _macro.get("last_news", 0) > 120:
        try:
            data = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg_kw = ["crash","hack","ban","fraud","collapse","seized","scam","plunge"]
            pos_kw = ["institutional","ath","approved","record","bullish","rally","surge"]
            neg = pos = 0
            for post in data.get("results", [])[:8]:
                tl = post.get("title", "").lower()
                if any(w in tl for w in neg_kw): neg += 1
                if any(w in tl for w in pos_kw): pos += 1
            score = pos - neg
            if score <= -3:    _macro["news"] = "strong_negative"
            elif score <= -1:  _macro["news"] = "negative"
            elif score >= 3:   _macro["news"] = "strong_positive"
            else:              _macro["news"] = "neutral"
            _macro["last_news"] = now
        except:
            pass


def update_btc_price():
    try:
        px = get_price("BTCUSDT")
        if px > 0:
            _btc_price_history.append((time.time(), px))
    except:
        pass


def detect_flash_move():
    if len(_btc_price_history) < 2:
        return "none", 0.0
    cutoff  = time.time() - 120
    oldest  = next((px for ts, px in _btc_price_history if ts >= cutoff), None)
    if oldest is None:
        return "none", 0.0
    current = _btc_price_history[-1][1]
    pct     = (current - oldest) / oldest * 100
    if pct <= -1.0: return "crash", abs(pct)
    if pct >= 1.0:  return "pump",  abs(pct)
    return "none", 0.0


# ════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE v15 — UPGRADED
# ════════════════════════════════════════════════════
def get_entry_score(symbol, df_5m, direction):
    if df_5m is None or len(df_5m) < 30:
        return 0, []

    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    prev2 = df_5m.iloc[-3]
    sigs  = []
    score = 0

    # ── A: TREND ALIGNMENT (max 25) ───────────────────────
    e3, e5, e9, e21 = last["ema3"], last["ema5"], last["ema9"], last["ema21"]
    p = last["close"]

    trend_score = 0
    if direction == "LONG":
        if p > e3 > e5 > e9 > e21:
            trend_score = 25; sigs.append("📐EMA_STACK↑")
        elif p > e5 > e9 > e21:
            trend_score = 18; sigs.append("📐EMA↑")
        elif p > e9 > e21:
            trend_score = 12; sigs.append("📐EMA_align↑")
    else:
        if p < e3 < e5 < e9 < e21:
            trend_score = 25; sigs.append("📐EMA_STACK↓")
        elif p < e5 < e9 < e21:
            trend_score = 18; sigs.append("📐EMA↓")
        elif p < e9 < e21:
            trend_score = 12; sigs.append("📐EMA_align↓")
    score += trend_score

    # ── B: MOMENTUM + VOLUME (max 25) ─────────────────────
    mom5    = abs(last.get("mom5", 0))
    vol_rat = last["vol_ratio"]
    adx     = last.get("adx", 0)

    vol_score = 0
    if mom5 >= 0.008 and vol_rat >= 2.5:
        vol_score = 25; sigs.append(f"🚀Mom{mom5*100:.1f}%+Vol{vol_rat:.1f}x")
    elif mom5 >= 0.005 and vol_rat >= 2.0:
        vol_score = 20; sigs.append(f"📈Mom{mom5*100:.1f}%+Vol{vol_rat:.1f}x")
    elif mom5 >= 0.003 and vol_rat >= 1.8:
        vol_score = 15; sigs.append(f"📈Mom{mom5*100:.1f}%")
    elif vol_rat >= 3.0:
        vol_score = 12; sigs.append(f"🔥Vol{vol_rat:.1f}x")

    # ADX bonus
    if adx >= 30:
        vol_score = min(vol_score + 5, 25)
        if adx >= 30 and vol_score >= 15:
            sigs.append(f"💪ADX{adx:.0f}")

    score += vol_score

    # ── C: ORDER FLOW (max 25) ────────────────────────────
    h_now  = last["macd_hist"]
    h_prev = prev["macd_hist"]
    h_p2   = prev2["macd_hist"]
    br     = last["buy_ratio"]

    flow_score = 0
    if direction == "LONG":
        if h_now > 0 and h_now > h_prev > h_p2 and br > 0.58:
            flow_score = 25; sigs.append(f"✅MACD↑↑+Buy{br:.0%}")
        elif h_now > 0 and h_now > h_prev:
            flow_score = 17; sigs.append("✅MACD↑")
        elif h_prev < 0 and h_now >= 0:
            flow_score = 22; sigs.append("⚡MACD_X0↑")
        elif br > 0.62:
            flow_score = 12; sigs.append(f"💧Buy{br:.0%}")
    else:
        if h_now < 0 and h_now < h_prev < h_p2 and br < 0.42:
            flow_score = 25; sigs.append(f"✅MACD↓↓+Sell{1-br:.0%}")
        elif h_now < 0 and h_now < h_prev:
            flow_score = 17; sigs.append("✅MACD↓")
        elif h_prev > 0 and h_now <= 0:
            flow_score = 22; sigs.append("⚡MACD_X0↓")
        elif br < 0.38:
            flow_score = 12; sigs.append(f"💧Sell{1-br:.0%}")
    score += flow_score

    # ── D: MARKET STRUCTURE (max 25) ──────────────────────
    recent_hi    = df_5m.iloc[-6:-1]["high"].max()
    recent_lo    = df_5m.iloc[-6:-1]["low"].min()
    struct_score = 0

    if direction == "LONG":
        if p > recent_hi and last["body_ratio"] > 0.6 and last["vol_ratio"] > 2.0:
            struct_score = 25; sigs.append("🚀BreakoutBull")
        elif last["close"] > last["open"] and last["close"] > prev["high"] and last["body_ratio"] > 0.6:
            struct_score = 20; sigs.append("🕯️Engulf↑")
        elif p > recent_hi:
            struct_score = 12; sigs.append("📈Breakout↑")
    else:
        if p < recent_lo and last["body_ratio"] > 0.6 and last["vol_ratio"] > 2.0:
            struct_score = 25; sigs.append("💥BreakoutBear")
        elif last["close"] < last["open"] and last["close"] < prev["low"] and last["body_ratio"] > 0.6:
            struct_score = 20; sigs.append("🕯️Engulf↓")
        elif p < recent_lo:
            struct_score = 12; sigs.append("📈Breakout↓")
    score += struct_score

    return max(0, min(score, 100)), sigs


def determine_direction(df_5m, df_15m=None):
    if df_5m is None or len(df_5m) < 20:
        return None
    last  = df_5m.iloc[-1]
    price = last["close"]
    e3, e5, e9 = last["ema3"], last["ema5"], last["ema9"]
    long_pts = short_pts = 0

    if price > e3 > e5 > e9:    long_pts  += 4
    elif price < e3 < e5 < e9:  short_pts += 4
    elif price > e5 > e9:       long_pts  += 2
    elif price < e5 < e9:       short_pts += 2

    mom5 = last.get("mom5", 0)
    if mom5 > 0.002:     long_pts  += 3
    elif mom5 < -0.002:  short_pts += 3

    prev = df_5m.iloc[-2]
    if last["macd_hist"] > prev["macd_hist"]: long_pts  += 2
    else:                                     short_pts += 2

    if last["buy_ratio"] > 0.55 and last["close"] > last["open"]:  long_pts  += 2
    elif last["buy_ratio"] < 0.45 and last["close"] < last["open"]: short_pts += 2

    # ADX confirmation
    adx = last.get("adx", 0)
    if adx >= MIN_ADX:
        if last.get("adx_pos", 0) > last.get("adx_neg", 0): long_pts  += 2
        else:                                                  short_pts += 2

    if df_15m is not None and len(df_15m) >= 20:
        l15 = df_15m.iloc[-1]
        if l15["ema9"] > l15["ema21"]: long_pts  += 2
        else:                          short_pts += 2

    btc_t = _macro.get("btc_trend_5m", "UNKNOWN")
    if btc_t in BULL_TRENDS:   long_pts  += 2
    elif btc_t in BEAR_TRENDS:  short_pts += 2

    if long_pts > short_pts and long_pts >= 7:  return "LONG"
    if short_pts > long_pts and short_pts >= 7: return "SHORT"
    return None


# ════════════════════════════════════════════════════
#  ENTRY FILTER v15
# ════════════════════════════════════════════════════
def should_enter(symbol):
    # Kill switch check
    killed, kill_reason = check_kill_switch()
    if killed:
        return None, f"kill:{kill_reason}"

    if is_symbol_cooling_down(symbol):
        return None, "cooldown"

    # Macro filter
    fng  = _macro["fng"]
    news = _macro["news"]
    if fng < MIN_FNG:              return None, f"F&G={fng}"
    if news == "strong_negative":  return None, "bad_news"

    flash_dir, _ = detect_flash_move()
    if flash_dir != "none":        return None, f"flash_{flash_dir}"

    # Volume & liquidity filter
    tickers = fetch_ticker24h_all()
    pct_24h = 0.0
    if symbol in tickers:
        t24 = tickers[symbol]
        if t24["vol24h"] < MIN_VOLUME_USDT:
            return None, f"low_vol(${t24['vol24h']/1e6:.1f}M)"
        pct_24h = t24["pct"]

    # Session filter
    utc_h = time.gmtime().tm_hour
    min_score_now = BAD_HOURS_MIN_SCORE if utc_h in BAD_HOURS_UTC else MIN_SCORE

    # Get OHLCV
    df_5m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE,  80)
    df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60)
    if df_5m is None or len(df_5m) < 30:
        return None, "no_data"

    df_5m = run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m) >= 20:
        df_15m = run_ta(df_15m.copy())

    # Direction detection
    direction = determine_direction(df_5m, df_15m)
    if direction is None:
        return None, "no_direction"

    # ★ NOISE FILTER v2 — deteksi real move vs noise
    is_noisy, noise_score, noise_reasons = is_noise_market(df_5m, direction)
    if is_noisy:
        _stats["skipped_noise"] += 1
        _stats["skipped_chop"] += 1
        return None, f"noise({noise_score}|{'|'.join(noise_reasons[:2])})"

    # Volume surge check
    last = df_5m.iloc[-1]
    vol_ratio = last.get("vol_ratio", 0)
    if vol_ratio < MIN_VOL_SURGE_ENTRY:
        _stats["skipped_no_momentum"] += 1
        return None, f"vol_low({vol_ratio:.1f}x)"

    # ADX check — trend harus kuat
    adx = last.get("adx", 0)
    if adx < MIN_ADX:
        return None, f"adx_weak({adx:.1f})"

    # Momentum check
    mom5 = abs(last.get("mom5", 0))
    if direction == "LONG" and df_5m["mom5"].iloc[-1] < MIN_MOMENTUM_PCT:
        _stats["skipped_no_momentum"] += 1
        return None, f"mom_weak({mom5*100:.2f}%)"
    if direction == "SHORT" and df_5m["mom5"].iloc[-1] > -MIN_MOMENTUM_PCT:
        _stats["skipped_no_momentum"] += 1
        return None, f"mom_weak({mom5*100:.2f}%)"

    # Scalp mode check
    scalp_mode = _macro.get("scalp_mode", "TREND")
    if scalp_mode == "MEAN_REV":
        _stats["skipped_mean_rev"] += 1
        return None, "skip_MEAN_REV"

    # BTC alignment check
    btc_5m  = _macro["btc_trend_5m"]
    btc_15m = _macro["btc_trend_15m"]
    if direction == "LONG"  and btc_5m in BEAR_TRENDS and btc_15m in BEAR_TRENDS:
        return None, f"skip_LONG:BTC_{btc_5m}"
    if direction == "SHORT" and btc_5m in BULL_TRENDS and btc_15m in BULL_TRENDS:
        return None, f"skip_SHORT:BTC_{btc_5m}"
    if direction == "LONG"  and fng > MAX_FNG_LONG:
        return None, f"overbought:F&G={fng}"

    # Entry score
    score, sigs = get_entry_score(symbol, df_5m, direction)
    if score < min_score_now:
        if utc_h in BAD_HOURS_UTC:
            _stats["skipped_session"] += 1
        return None, f"score={score:.0f}<{min_score_now}"

    if len(sigs) < MIN_ENTRY_SIGNALS:
        return None, f"signals={len(sigs)}<{MIN_ENTRY_SIGNALS}"

    # ATR check
    atr   = df_5m["atr"].iloc[-1]
    price = df_5m["close"].iloc[-1]
    if atr / price > MAX_SL_ATR_PCT:
        return None, f"ATR_besar({atr/price*100:.2f}%)"

    levels = calc_atr_levels(price, atr, direction)

    # Spread filter
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=5)
        bid   = float(ob["bids"][0][0])
        ask   = float(ob["asks"][0][0])
        spread = ask - bid
        tp1_dist = abs(levels["tp1"] - price)
        spread_ratio = spread / tp1_dist if tp1_dist > 0 else 1.0
        if spread_ratio > MAX_SPREAD_RATIO:
            _stats["skipped_spread"] += 1
            return None, f"spread({spread_ratio:.2f}x)"
    except:
        spread_ratio = 0.0

    # Order book pressure
    ob_imb, bid_w, ask_w, bid_wall, ask_wall = get_ob_pressure(symbol)

    # Skip kalau OB melawan posisi kita
    if direction == "LONG"  and ob_imb < -MIN_OB_PRESSURE:
        return None, f"OB_bearish({ob_imb:.2f})"
    if direction == "SHORT" and ob_imb > MIN_OB_PRESSURE:
        return None, f"OB_bullish({ob_imb:.2f})"

    # Skip kalau ada big wall yang block arah kita
    if direction == "LONG"  and ask_wall:
        return None, "ask_wall_block"
    if direction == "SHORT" and bid_wall:
        return None, "bid_wall_block"

    # Funding check
    funding_bias, fr = get_funding_bias(symbol)
    if direction == "LONG"  and funding_bias == "bearish_bias" and fr > 0.001:
        return None, f"funding_bearish({fr*100:.3f}%)"
    if direction == "SHORT" and funding_bias == "bullish_bias" and fr < -0.001:
        return None, f"funding_bullish({fr*100:.3f}%)"

    return direction, {
        "score":      score,
        "signals":    sigs,
        "direction":  direction,
        "sl":         levels["sl"],
        "tp1":        levels["tp1"],
        "tp2":        levels["tp2"],
        "trail_init": levels["trail_init"],
        "sl_pct":     levels["sl_pct"],
        "tp1_pct":    levels["tp1_pct"],
        "ob_imb":     ob_imb,
        "atr":        atr,
        "atr_pct":    levels["atr_pct"],
        "mom5":       float(df_5m["mom5"].iloc[-1]),
        "vol_ratio":  float(vol_ratio),
        "adx":        float(adx),
        "pct_24h":    pct_24h,
        "funding":    fr,
        "scalp_mode": scalp_mode,
        "btc_trend":  btc_5m,
        "noise_score": noise_score,
    }


# ════════════════════════════════════════════════════
#  PARALLEL SCANNER v15 — ULTRA FAST
# ════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction, info = should_enter(symbol)
        if direction:
            return symbol, direction, info
    except:
        pass
    return None


def scan_batch_parallel(symbols):
    candidates     = []
    symbols_to_scan = symbols[:25]
    futures = {_executor.submit(scan_symbol_safe, sym): sym for sym in symbols_to_scan}

    try:
        for future in as_completed(futures, timeout=10):
            try:
                result = future.result(timeout=2)
                if result:
                    candidates.append(result)
            except:
                pass
    except TimeoutError:
        done_count    = sum(1 for f in futures if f.done())
        pending_count = len(futures) - done_count
        for future in futures:
            if future.done():
                try:
                    result = future.result(timeout=0)
                    if result:
                        candidates.append(result)
                except:
                    pass
            else:
                future.cancel()
        if pending_count > 0:
            print(f"  ⚠️  Scan partial: {done_count}/{len(futures)} done, {pending_count} cancel")
    except Exception as e:
        print(f"  ❌ Scan error: {e}")

    return candidates


# ════════════════════════════════════════════════════
#  RE-SCAN WORKER — selalu nyari peluang
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
            time.sleep(RE_SCAN_DELAY_SEC)

            slots_free = MAX_POSITIONS - len(open_positions)
            if slots_free <= 0:
                continue

            killed, _ = check_kill_switch()
            if killed:
                continue

            flash_dir, _ = detect_flash_move()
            if flash_dir != "none":
                continue

            if _macro["news"] == "strong_negative":
                continue

            hot  = [s for s in list(_hot_symbols) if s not in open_positions]
            rest = [s for s in symbols_active    if s not in open_positions and s not in hot]
            scan_list = (hot + rest)[:40]

            _stats["rescans"] += 1
            print(f"\n  ⚡ RESCAN [{reason}] — {len(scan_list)} syms | {slots_free} slot free")

            try:
                candidates = scan_batch_parallel(scan_list)
            except Exception as e:
                print(f"  ❌ Rescan error: {e}")
                candidates = []

            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS:
                        break
                    sig_str = " | ".join(info.get("signals", [])[:3])
                    print(f"     ⭐ {sym} {direction} Score:{info['score']:.0f} ADX:{info.get('adx',0):.0f} | {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ Rescan: no setup")
        except queue.Empty:
            pass
        except Exception as e:
            print(f"  ❌ Rescan worker error: {e}")


# ════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    with _lock:
        if symbol in open_positions:
            return
        if len(open_positions) >= MAX_POSITIONS:
            return
        open_positions[symbol] = {"_reserved": True}
        if len(open_positions) > MAX_POSITIONS:
            open_positions.pop(symbol, None)
            return

    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if price == 0:
            with _lock:
                open_positions.pop(symbol, None)
            return

        qty = calc_qty(symbol, price)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty)

        entry  = get_price(symbol)
        atr    = info.get("atr", entry * 0.002)
        levels = calc_atr_levels(entry, atr, direction)

        # ★ Trail aktif DARI ENTRY dengan Phase 1 (lebar)
        trail_init = levels["trail_init"]

        open_positions[symbol] = {
            "side":         direction,
            "entry":        entry,
            "qty":          qty,
            "qty_remain":   qty,
            "sl":           levels["sl"],
            "tp1":          levels["tp1"],
            "tp2":          levels["tp2"],
            "peak":         entry,
            "trail_sl":     trail_init,     # ★ trail aktif dari detik pertama
            "trail_phase":  1,              # mulai dari Phase 1
            "trail_active": True,           # ★ SELALU aktif dari awal
            "tp1_hit":      False,
            "be_active":    False,
            "open_time":    time.time(),
            "score":        info.get("score", 0),
            "signals":      info.get("signals", []),
            "atr":          atr,
            "scalp_mode":   info.get("scalp_mode", "TREND"),
        }

        sl_p  = levels["sl_pct"]  * 100
        tp1_p = levels["tp1_pct"] * 100
        sig_str = " | ".join(info.get("signals", [])[:3])

        print(f"\n  {'🟢' if direction=='LONG' else '🔴'} [{symbol}] {direction} @{entry:.5g}")
        print(f"     ATR:{atr:.5g}({levels['atr_pct']*100:.2f}%) SL:{sl_p:.2f}% TP1:{tp1_p:.2f}% Trail:ACTIVE_FROM_ENTRY")
        print(f"     ADX:{info.get('adx',0):.0f} Vol:{info.get('vol_ratio',0):.1f}x Score:{info['score']:.0f} NoiseScore:{info.get('noise_score',0)}")
        print(f"     {sig_str}")
        _stats["total_trades"] += 1

    except Exception as e:
        with _lock:
            open_positions.pop(symbol, None)
        print(f"  ❌ [{symbol}] Entry error: {e}")


def partial_close_tp1(symbol):
    pos = open_positions.get(symbol)
    if pos is None or pos.get("tp1_hit"):
        return
    try:
        amt = get_exchange_amt(symbol)
        if amt is None or amt == 0:
            pos["tp1_hit"] = True
            return

        close_qty = round_step(abs(amt) * TP1_CLOSE_RATIO, get_sym_info(symbol)["step"])
        close_qty = max(close_qty, get_sym_info(symbol)["minQty"])
        if close_qty > abs(amt):
            close_qty = abs(amt)

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=close_qty,
            reduceOnly=True)

        exit_p  = get_price(symbol)
        side    = pos["side"]
        pnl     = (exit_p - pos["entry"]) * close_qty if side == "LONG" \
                  else (pos["entry"] - exit_p) * close_qty
        hold_s  = time.time() - pos["open_time"]
        print(f"  🎯 [{symbol}] TP1 partial {hold_s:.0f}s PnL:{pnl:+.4f}U")

        pos["tp1_hit"]    = True
        pos["qty_remain"] = abs(amt) - close_qty
        pos["be_active"]  = True

        # Move SL ke BE + sedikit buffer
        if side == "LONG":
            pos["sl"] = round(pos["entry"] * 1.0003, 8)
        else:
            pos["sl"] = round(pos["entry"] * 0.9997, 8)

        # Upgrade ke Phase 2 trail
        pos["trail_phase"] = 2
        pos["peak"]        = exit_p
        atr = pos.get("atr", exit_p * 0.002)
        if side == "LONG":
            new_trail = exit_p - atr * ATR_TRAIL_PHASE2
            pos["trail_sl"] = max(pos["trail_sl"], new_trail)
        else:
            new_trail = exit_p + atr * ATR_TRAIL_PHASE2
            pos["trail_sl"] = min(pos["trail_sl"], new_trail)

        _stats["tp1_hits"]  += 1
        _stats["wins"]      += 1
        _stats["total_pnl"] += pnl
        _stats["pnl_history"].append(pnl)
        update_kill_switch_after_trade(pnl)
        _perf[symbol]["wins"]   += 1
        _perf[symbol]["pnl"]    += pnl
        _perf[symbol]["trades"] += 1
        if pnl > _stats["best_trade"]:
            _stats["best_trade"] = pnl

        trade_log.append({
            "symbol": symbol, "side": side,
            "pnl": round(pnl, 4), "reason": "TP1 Partial",
            "hold_sec": int(hold_s)
        })
        _hot_symbols.appendleft(symbol)
        print_stats_inline()
    except Exception as e:
        print(f"  ❌ [{symbol}] TP1 error: {e}")
        if pos:
            pos["tp1_hit"] = True


def close_trade(symbol, reason=""):
    try:
        amt = get_exchange_amt(symbol)
        if amt is None:
            return False
        if amt == 0:
            with _lock:
                open_positions.pop(symbol, None)
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
            exit_p = get_price(symbol)
            qty_r  = pos.get("qty_remain", pos["qty"])
            side   = pos["side"]
            pnl    = (exit_p - pos["entry"]) * qty_r if side == "LONG" \
                     else (pos["entry"] - exit_p) * qty_r
            pct    = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
            hold_s = time.time() - pos["open_time"]
            emoji  = "🟢" if pnl >= 0 else "🔴"
            print(f"  {emoji} [{symbol}] CLOSE {reason} | {hold_s:.0f}s | PnL:{pnl:+.4f}U ({pct:+.2f}%)")

            trade_log.append({
                "symbol": symbol, "side": side,
                "pnl": round(pnl, 4), "reason": reason,
                "hold_sec": int(hold_s)
            })
            _stats["total_pnl"]   += pnl
            _stats["pnl_history"].append(pnl)
            update_kill_switch_after_trade(pnl)
            _perf[symbol]["trades"] += 1
            _perf[symbol]["pnl"]    += pnl

            if pnl >= 0:
                _stats["wins"] += 1
                _perf[symbol]["wins"] += 1
                if pnl > _stats["best_trade"]:
                    _stats["best_trade"] = pnl
            else:
                _stats["losses"] += 1
                _perf[symbol]["losses"] += 1
                if pnl < _stats["worst_trade"]:
                    _stats["worst_trade"] = pnl

            regime = pos.get("scalp_mode", "UNKNOWN")
            _perf_regime[regime]["pnl"] += pnl
            if pnl >= 0: _perf_regime[regime]["wins"] += 1
            else:        _perf_regime[regime]["losses"] += 1

            if "TP2"     in reason: _stats["tp2_hits"]   += 1
            elif "SL"    in reason or "Stop" in reason: _stats["sl_hits"] += 1
            elif "Force" in reason: _stats["force_closes"] += 1
            elif "Inst"  in reason: _stats["instant_cuts"] += 1

            print_stats_inline()
            set_symbol_cooldown(symbol)
            trigger_rescan(f"close@{symbol}({reason})", priority_symbol=symbol)

        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Close error: {e}")
        return False


# ════════════════════════════════════════════════════
#  POSITION MONITOR v15 — INSTANT CUT + ADAPTIVE TRAIL
# ════════════════════════════════════════════════════
def manage_positions():
    if not open_positions:
        return

    flash_dir, flash_pct = detect_flash_move()

    for symbol in list(open_positions.keys()):
        pos = open_positions.get(symbol)
        if pos is None or pos.get("_reserved"):
            continue

        price = get_price(symbol)
        if price == 0:
            continue

        side  = pos["side"]
        entry = pos["entry"]
        atr   = pos.get("atr", entry * 0.002)

        hold_min = (time.time() - pos["open_time"]) / 60

        # ── FORCE CLOSE — max holding time ────────────────
        if hold_min >= MAX_HOLDING_MIN * 0.95:
            close_trade(symbol, f"⏰Force({hold_min:.1f}m)")
            continue

        # ── FLASH MOVE PROTECTION ─────────────────────────
        if flash_dir == "crash" and side == "LONG":
            close_trade(symbol, f"⚡FlashCrash")
            continue
        elif flash_dir == "pump" and side == "SHORT":
            close_trade(symbol, f"⚡FlashPump")
            continue

        if side == "LONG":
            profit_pct = (price - entry) / entry

            # ★★★ INSTANT CUT — kecepatan cahaya ★★★
            # Kalau baru entry dan langsung minus > INSTANT_CUT_PCT → kabur
            if INSTANT_CUT_ENABLED and not pos.get("tp1_hit"):
                instant_threshold = entry * (1 - INSTANT_CUT_PCT)
                if price <= instant_threshold and hold_min < 1.0:
                    close_trade(symbol, f"⚡InstCut(-{INSTANT_CUT_PCT*100:.2f}%)")
                    continue

            # TP1 hit
            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            # ★ ADAPTIVE TRAIL PHASE UPGRADE ★
            current_phase = pos.get("trail_phase", 1)

            if profit_pct >= TRAIL_PHASE4_PCT and current_phase < 4:
                pos["trail_phase"] = 4
                print(f"     🔒 [{symbol}] Trail Phase 4 ({profit_pct*100:+.2f}%)")
            elif profit_pct >= TRAIL_PHASE3_PCT and current_phase < 3:
                pos["trail_phase"] = 3
                print(f"     ⬆️  [{symbol}] Trail Phase 3 ({profit_pct*100:+.2f}%)")
            elif profit_pct >= TRAIL_PHASE2_PCT and current_phase < 2:
                pos["trail_phase"] = 2
                pos["be_active"]   = True
                pos["sl"]          = round(entry * 1.0003, 8)  # BE+
                print(f"     🔓 [{symbol}] Trail Phase 2 → SL→BE+ ({profit_pct*100:+.2f}%)")

            # Update trail sesuai phase
            phase = pos["trail_phase"]
            trail_mult_map = {1: ATR_TRAIL_PHASE1, 2: ATR_TRAIL_PHASE2,
                              3: ATR_TRAIL_PHASE3, 4: ATR_TRAIL_PHASE4}
            trail_mult = trail_mult_map.get(phase, ATR_TRAIL_PHASE1)

            if price > pos["peak"]:
                pos["peak"] = price
                new_trail   = price - atr * trail_mult
                pos["trail_sl"] = max(pos["trail_sl"], new_trail)

            # TP2
            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨TP2")
                continue

            # Trail SL hit
            if price <= pos["trail_sl"]:
                tag = "🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"
                close_trade(symbol, tag)
                continue

            # Hard SL
            if price <= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            # Status print
            pnl  = (price - entry) * pos.get("qty_remain", pos["qty"])
            phase = pos.get("trail_phase", 1)
            tp_label = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 [{symbol}] L@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) | {pnl:+.3f}U | {hold_min:.1f}m | TSL[P{phase}]:{pos['trail_sl']:.5g} {tp_label}")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            # ★★★ INSTANT CUT — kecepatan cahaya ★★★
            if INSTANT_CUT_ENABLED and not pos.get("tp1_hit"):
                instant_threshold = entry * (1 + INSTANT_CUT_PCT)
                if price >= instant_threshold and hold_min < 1.0:
                    close_trade(symbol, f"⚡InstCut(-{INSTANT_CUT_PCT*100:.2f}%)")
                    continue

            # TP1 hit
            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            # ★ ADAPTIVE TRAIL PHASE UPGRADE ★
            current_phase = pos.get("trail_phase", 1)

            if profit_pct >= TRAIL_PHASE4_PCT and current_phase < 4:
                pos["trail_phase"] = 4
                print(f"     🔒 [{symbol}] Trail Phase 4 ({profit_pct*100:+.2f}%)")
            elif profit_pct >= TRAIL_PHASE3_PCT and current_phase < 3:
                pos["trail_phase"] = 3
                print(f"     ⬆️  [{symbol}] Trail Phase 3 ({profit_pct*100:+.2f}%)")
            elif profit_pct >= TRAIL_PHASE2_PCT and current_phase < 2:
                pos["trail_phase"] = 2
                pos["be_active"]   = True
                pos["sl"]          = round(entry * 0.9997, 8)  # BE+
                print(f"     🔓 [{symbol}] Trail Phase 2 → SL→BE+ ({profit_pct*100:+.2f}%)")

            # Update trail sesuai phase
            phase = pos["trail_phase"]
            trail_mult_map = {1: ATR_TRAIL_PHASE1, 2: ATR_TRAIL_PHASE2,
                              3: ATR_TRAIL_PHASE3, 4: ATR_TRAIL_PHASE4}
            trail_mult = trail_mult_map.get(phase, ATR_TRAIL_PHASE1)

            if price < pos["peak"]:
                pos["peak"] = price
                new_trail   = price + atr * trail_mult
                pos["trail_sl"] = min(pos["trail_sl"], new_trail)

            # TP2
            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨TP2")
                continue

            # Trail SL hit
            if price >= pos["trail_sl"]:
                tag = "🔒TrailBE" if pos.get("be_active") else "🔄TrailStop"
                close_trade(symbol, tag)
                continue

            # Hard SL
            if price >= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            # Status print
            pnl  = (entry - price) * pos.get("qty_remain", pos["qty"])
            phase = pos.get("trail_phase", 1)
            tp_label = f"TP2:{pos['tp2']:.5g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.5g}"
            print(f"  📌 [{symbol}] S@{entry:.5g}→{price:.5g} ({profit_pct*100:+.2f}%) | {pnl:+.3f}U | {hold_min:.1f}m | TSL[P{phase}]:{pos['trail_sl']:.5g} {tp_label}")


# ════════════════════════════════════════════════════
#  PERFORMANCE ANALYTICS
# ════════════════════════════════════════════════════
def calc_expectancy():
    wins   = [t["pnl"] for t in trade_log if t["pnl"] > 0]
    losses = [t["pnl"] for t in trade_log if t["pnl"] < 0]
    if not wins and not losses:
        return 0.0
    wr    = len(wins) / (len(wins) + len(losses))
    avg_w = sum(wins)   / len(wins)   if wins   else 0
    avg_l = abs(sum(losses) / len(losses)) if losses else 0
    return round((wr * avg_w) - ((1 - wr) * avg_l), 5)


def calc_sharpe():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 5:
        return 0.0
    arr  = np.array(pnls)
    std  = float(np.std(arr))
    if std == 0:
        return 0.0
    return round(float(np.mean(arr)) / std, 3)


def calc_max_drawdown():
    pnls = list(_stats["pnl_history"])
    if len(pnls) < 2:
        return 0.0
    equity = np.cumsum(pnls)
    peak   = np.maximum.accumulate(equity)
    dd     = equity - peak
    return round(float(np.min(dd)), 4)


def print_stats_inline():
    n   = _stats["wins"] + _stats["losses"]
    wr  = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["total_pnl"]
    exp = calc_expectancy()
    emoji = "💚" if pnl >= 0 else "🔴"
    bar = ("█" * _stats["wins"] + "░" * _stats["losses"])[-20:]
    print(f"     ┌─ 📊 {n}T | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']} | {emoji}PnL:{pnl:+.4f}U | Exp:{exp:+.4f}U")
    print(f"     └─ TP1:{_stats['tp1_hits']} TP2:{_stats['tp2_hits']} SL:{_stats['sl_hits']} ⚡Cut:{_stats['instant_cuts']} Force:{_stats['force_closes']} [{bar}]")


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
    print(f"  📊 SESSION {sess*60:.0f}m | {tph:.0f} trades/jam | Rescans:{_stats['rescans']}")
    print(f"  🎯 {n} trades | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {emoji} Total P&L: {pnl:+.4f} USDT")
    print(f"  📐 Expectancy:{exp:+.5f}U | Sharpe:{sr:.2f} | MaxDD:{mdd:.4f}U")
    print(f"  📈 Best:{_stats['best_trade']:+.4f}U │ 📉 Worst:{_stats['worst_trade']:+.4f}U")
    print(f"  🎯TP1:{_stats['tp1_hits']} │ ✨TP2:{_stats['tp2_hits']} │ 🛑SL:{_stats['sl_hits']} │ ⚡Cut:{_stats['instant_cuts']} │ ⏰Force:{_stats['force_closes']}")
    print(f"  🚫 Skip: Noise:{_stats['skipped_noise']} NoMom:{_stats['skipped_no_momentum']} Spread:{_stats['skipped_spread']} Session:{_stats['skipped_session']} MeanRev:{_stats['skipped_mean_rev']}")
    print(f"  🛡️  KS:{'ACTIVE('+ks['reason']+')' if ks['active'] else 'OK'} | CL:{ks['consec_losses']} | DailyPnL:{ks['daily_pnl']:+.2f}U | Lag:{ks['api_lag']*1000:.0f}ms")

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
            secs = t.get("hold_sec", 0)
            hold = f"{secs//60}m{secs%60}s"
            print(f"     {e} {t['symbol']:<14} {t['side']} {t['pnl']:+.4f}U ({hold}) — {t['reason'][:30]}")
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
#  MAIN LOOP v15
# ════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  ⚡ BOT SCALPING v15 — QUANTUM SPEED ENGINE                 ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Leverage:{LEVERAGE}x │ Per trade:${ORDER_USDT} │ Max posisi:{MAX_POSITIONS}              ║")
    print(f"║  ★ Instant Cut: -{INSTANT_CUT_PCT*100:.2f}% dalam 60 detik pertama      ║")
    print(f"║  ★ Trail: AKTIF DARI ENTRY (Phase 1→2→3→4)              ║")
    print(f"║    Phase1 ATR×{ATR_TRAIL_PHASE1} │ Phase2 ATR×{ATR_TRAIL_PHASE2} │ Phase3 ATR×{ATR_TRAIL_PHASE3} │ Phase4 ATR×{ATR_TRAIL_PHASE4} ║")
    print(f"║  ★ Noise Filter v2: CI+NR+BB+EMA+TC+MACD combo         ║")
    print(f"║  ★ Vol filter: ${MIN_VOLUME_USDT/1e6:.0f}M+ 24h, Surge {MIN_VOL_SURGE_ENTRY}×             ║")
    print(f"║  ★ ADX minimum: {MIN_ADX} (hanya masuk kalau trend kuat)         ║")
    print(f"║  ★ OB Pressure: 20-level order book analysis             ║")
    print(f"║  💀 KillSwitch: Daily${abs(DAILY_LOSS_LIMIT)} │ ConsecLoss×{CONSEC_LOSS_MAX}           ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    print("\n  ⏳ Validasi symbols...")
    symbols_active = validate_symbols()
    print(f"  📊 {len(symbols_active)} symbols aktif")

    print(f"  📦 Pre-load symbol info...")
    with ThreadPoolExecutor(max_workers=20) as ex:
        list(ex.map(get_sym_info, symbols_active[:60]))

    print(f"  🌐 Refresh macro...")
    refresh_macro()
    update_btc_price()

    print(f"\n  ✅ BTC:{_macro['btc_trend_5m']} | Mode:{_macro['scalp_mode']} | F&G:{_macro['fng']}")
    print(f"  ⚡ Start dalam 3 detik...\n")
    time.sleep(3)

    pm_thread = threading.Thread(target=position_monitor_thread, daemon=True)
    pm_thread.start()
    print("  🔧 Position monitor (0.5s): START ✅")

    rs_thread = threading.Thread(target=instant_rescan_worker, args=(symbols_active,), daemon=True)
    rs_thread.start()
    print("  🔧 Re-scan thread: START ✅\n")

    global _scan_batch_idx
    cycle         = 0
    total_batches = math.ceil(len(symbols_active) / BATCH_SIZE)

    while True:
        cycle += 1
        refresh_macro()
        update_btc_price()

        if cycle % 30 == 0:
            check_api_latency()

        flash_dir, flash_pct = detect_flash_move()
        flash_info = f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir != "none" else ""

        utc_h    = time.gmtime().tm_hour
        sess_tag = f"⚠️ JAM_JELEK(UTC{utc_h})" if utc_h in BAD_HOURS_UTC else ""

        print(f"\n{'═'*67}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} | F&G:{_macro['fng']} | "
              f"BTC1m:{_macro['btc_trend_1m']} 5m:{_macro['btc_trend_5m']} {flash_info} {sess_tag}")
        print(f"  Mode:{_macro['scalp_mode']} | Breadth:{_macro['market_breadth']*100:.0f}% | "
              f"News:{_macro['news']} | Posisi({len(open_positions)}/{MAX_POSITIONS}): "
              f"{list(open_positions.keys()) or '—'}")

        slots_free = MAX_POSITIONS - len(open_positions)
        ks_active, ks_reason = check_kill_switch()

        if ks_active:
            resume_in = max(0, _kill_switch["resume_time"] - time.time())
            print(f"  🚨 KILL SWITCH: {ks_reason} | Resume: {resume_in/60:.1f}m")

        skip_reason = None
        if slots_free == 0:
            skip_reason = "posisi_penuh"
        elif _macro["news"] == "strong_negative":
            skip_reason = "bad_news"
        elif flash_dir != "none":
            skip_reason = f"flash_{flash_dir}"
        elif ks_active:
            skip_reason = f"kill:{ks_reason}"

        if not skip_reason:
            top_mv      = get_top_movers(symbols_active, n=40)
            top_mv_syms = [s for s, _, _ in top_mv if s not in open_positions]

            batch_start   = _scan_batch_idx * BATCH_SIZE
            batch_regular = [s for s in symbols_active[batch_start:batch_start + BATCH_SIZE]
                             if s not in open_positions and s not in top_mv_syms]
            _scan_batch_idx = (_scan_batch_idx + 1) % total_batches

            # Prioritas: top movers dulu (momentum coins), lalu batch regular
            scan_list = top_mv_syms[:20] + batch_regular[:10]

            top_display = [(s, pct) for s, pct, _ in top_mv[:5]]
            top_str     = " | ".join(f"{s}({pct:+.1f}%)" for s, pct in top_display)
            print(f"  📊 TopMovers: {top_str}")
            print(f"  🔍 Scan {len(scan_list)} syms | Noise:{_stats['skipped_noise']} Spread:{_stats['skipped_spread']}")

            try:
                candidates = scan_batch_parallel(scan_list)
            except Exception as e:
                print(f"  ❌ Scan error: {e}")
                candidates = []

            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
                print(f"  🎯 {len(candidates)} setup! Ambil top {min(len(candidates), slots_free)}")
                for sym, direction, info in candidates[:slots_free]:
                    if len(open_positions) >= MAX_POSITIONS:
                        break
                    sig_str = " | ".join(info.get("signals", [])[:3])
                    print(f"     ⭐ {sym} {direction} ADX:{info.get('adx',0):.0f} Vol:{info.get('vol_ratio',0):.1f}x Score:{info['score']:.0f}")
                    print(f"        {sig_str}")
                    open_trade(sym, direction, info)
            else:
                print(f"  ⏳ No setup found")
        else:
            print(f"  ⏸️  Skip: {skip_reason}")

        if cycle % 25 == 0:
            print_stats()

        ks = _kill_switch
        print(f"  ⏱️  Next:{SCAN_INTERVAL}s | KS:{ks['consec_losses']}CL/{ks['daily_pnl']:+.2f}U | "
              f"Rescans:{_stats['rescans']} | Lag:{ks['api_lag']*1000:.0f}ms")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
