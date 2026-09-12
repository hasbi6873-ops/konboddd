"""
Bot Scalping v23.0 — ADVERSARIAL FAILURE MINING + CONDITIONAL REVERSAL + SMART ENTRY (Binance Futures)
=======================================================================================================
INI BUKAN JAMINAN PROFIT. Ini adalah kerangka eksperimen untuk mencari positive expectancy
setelah fee/slippage, dengan cara:
  1. Tidak lagi reverse buta (LONG->SHORT selalu) — reverse hanya jika ada bukti kegagalan.
  2. Signal != Entry — ada ENTRY WINDOW yang menunggu konfirmasi microstructure.
  3. Setiap kandidat (dieksekusi atau tidak) dicatat sebagai SHADOW TRADE dan diamati outcome-nya.
  4. Statistik kegagalan per-bucket (regime x direction x score x rsi x symbol) dipakai untuk
     menghitung FAILURE_SCORE dengan shrinkage/minimum sample, TANPA future leakage.
  5. Exit scalping: TP/SL berbasis ATR (risk manager lama dipertahankan) + TIME DECAY + EARLY EXIT
     jika sinyal invalidated. Trailing stop TETAP DIHAPUS sesuai permintaan.

ARSITEKTUR:
  MARKET DATA -> 5M ANALYSIS -> REGIME DETECTION -> SIGNAL ENGINE -> FAILURE/ADVERSARIAL ENGINE
  -> ENTRY WINDOW (microstructure confirmation) -> EXECUTION -> SMART EXIT
  -> TRADE + SHADOW OUTCOME LOGGER -> FAILURE LEARNING (bucket stats, no look-ahead) -> next decision

Semua modul baru ditandai dengan komentar "# === NEW v23 ===" agar mudah dibedakan dari kode lama.
Websocket, Binance client, scanner batching, order execution, precision handling, user-data stream,
dan risk manager berbasis ATR DIPERTAHANKAN dari versi sebelumnya — hanya dihubungkan ke pipeline baru.
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import os
import json
import time
import math
import threading
import queue
import numpy as np
import pandas as pd
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
import ta

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

try:
    client = Client(api_key, api_secret, testnet=True)
except Exception:
    client = Client(api_key, api_secret)
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

try:
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=True)
except Exception:
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION & INSTITUTIONAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 2.0
MAX_POSITIONS = 3

# Scanning & Concurrency
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01
COOLDOWN_SEC  = 300   # 5 Menit jeda per simbol setelah close

# Scoring & Filter
MIN_SCORE      = 55
SLIPPAGE_GUARD = 0.0015
TTL_5M         = 2

# === NEW v23: SAFETY / MODE ===================================================
# WAJIB testnet secara default (client di atas sudah dipaksa testnet). Tambahan:
# DRY_RUN=True -> tidak mengirim order sungguhan, hanya simulasi (paper) + logging.
# Set False secara SADAR hanya setelah puas dengan hasil shadow/dry-run.
DRY_RUN = False

# STRATEGY_MODE:
#   ADVERSARIAL   -> pipeline penuh (failure engine + conditional reverse + entry window)
#   ORIGINAL      -> selalu ambil arah signal asli jika lolos entry window (tanpa reverse)
#   REVERSE_TEST  -> HANYA untuk eksperimen: selalu reverse jika lolos entry window
#   SHADOW_ONLY   -> tidak pernah eksekusi order sungguhan, semua kandidat jadi shadow trade
STRATEGY_MODE = "ADVERSARIAL"

# ── Dynamic Volatility Risk Management (ATR Multipliers) — DIPERTAHANKAN ───
# Nilai berikut BUKAN rekomendasi optimal — wajib dibacktest/dry-run sebelum dipakai.
ATR_TP_MULTIPLIER = 1.8
ATR_SL_MULTIPLIER = 2.2   # R:R awal ~1:1.2 pada ATR yang sama; ubah & backtest sendiri

MIN_TP_PCT        = 0.006
MAX_TP_PCT        = 0.02
MIN_SL_PCT        = 0.006
MAX_SL_PCT        = 0.02

# === NEW v23: TIME DECAY EXIT (scalping harus responsif, bukan menahan 3 jam) ===
# MAX_HOLD_SECONDS sekarang adalah HARD CUTOFF TERAKHIR setelah semua checkpoint decay gagal.
MAX_HOLD_SECONDS = 900  # 15 menit — jauh lebih pendek dari 10800s versi lama; backtest nilai ini

# Checkpoint time-decay: (detik, minimum_progress_ratio)
# progress_ratio = pnl_pct_saat_ini / tp_pct  (0 = belum bergerak, 1 = sudah kena TP)
# Jika di checkpoint tsb progress belum mencapai ambang, posisi kehilangan 1 "confidence life".
# Kehabisan confidence lives -> EARLY_EXIT_TIMEDECAY (tidak menunggu MAX_HOLD_SECONDS).
TIME_DECAY_CHECKPOINTS = [
    (60,  0.10),
    (120, 0.20),
    (180, 0.30),
    (300, 0.45),
    (600, 0.60),
]
TIME_DECAY_LIVES = 2  # berapa kali boleh gagal checkpoint sebelum dipaksa keluar

# === NEW v23: EARLY EXIT jika sinyal ternyata invalidated (bukan trailing stop) ===
EARLY_EXIT_ENABLED = True
EARLY_EXIT_DELTA_THRESHOLD = -0.30   # delta_ratio berlawanan arah posisi sekuat ini -> exit
EARLY_EXIT_MIN_HOLD_SEC = 20         # jangan early-exit dalam beberapa detik pertama (noise)

# ── Institutional Microstructure (Order Book Depth) — DIPERTAHANKAN ───────
WALL_RATIO_THRESHOLD  = 2.5
WALL_DEPTH_PCT        = 0.35
WALL_PROXIMITY_PCT    = 0.005
IMBALANCE_STRONG_BULL = 0.25
IMBALANCE_STRONG_BEAR = -0.25
SPOOF_DROP_THRESHOLD  = 0.40

# ── Macro BTC Correlation & Flash Crash Engine — DIPERTAHANKAN ────────────
BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# Kill Switch
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

# Learning (weight adaptation lama, dipertahankan)
LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 20

# === NEW v23: FAILURE / ADVERSARIAL ENGINE CONFIG (semua configurable) =========
# Bobot tiap faktor kegagalan (0-100 scale kontribusi maksimum per faktor sebelum normalisasi)
FAILURE_WEIGHTS = {
    "late_extension":     15,   # harga sudah jauh dari EMA acuan (overextended)
    "rsi_extreme":        12,   # RSI sudah di zona ekstrem searah signal
    "volume_unsupportive":10,   # volume/vol-ratio tidak mendukung
    "orderbook_opposing": 15,   # orderbook imbalance berlawanan dengan arah signal
    "delta_opposing":     15,   # taker delta berlawanan dengan arah signal
    "rejection_wick":     10,   # candle sebelumnya menunjukkan rejection wick berlawanan
    "momentum_fading":    10,   # m5 melemah dibanding candle sebelumnya
    "regime_mismatch":     8,   # signal continuation di regime RANGE, atau signal counter-trend di TRENDING kuat
    "historical_bucket":  25,   # empirical failure rate bucket historis (setelah shrinkage sample)
}
FAILURE_VETO_THRESHOLD    = 68   # >= ini & tidak ada reversal edge kuat -> NO_TRADE
FAILURE_PENALTY_MULT      = 0.9  # EDGE_SCORE = SIGNAL_SCORE - FAILURE_SCORE * mult
MIN_EDGE_SCORE            = 18   # ambang minimum edge untuk ambil ORIGINAL
REVERSE_CONFIDENCE_MIN    = 62   # ambang minimum reversal_confidence untuk kandidat REVERSE
FAILURE_MIN_FOR_REVERSE   = 55   # failure_score minimum sebelum reverse dipertimbangkan sama sekali

# Bucket historis untuk failure mining (statistik online, bukan ML berat)
BUCKET_SCORE_STEP = 10     # lebar bucket signal_score
BUCKET_RSI_STEP   = 10     # lebar bucket RSI
MIN_SAMPLE_FOR_TRUST = 20  # minimum sample sebelum bucket dipercaya penuh
BUCKET_PRIOR_WINRATE = 0.5  # prior netral sebelum ada cukup sample (shrinkage)
LEARNING_STATE_FILE = "learning_state_v23.json"
LEARNING_SAVE_EVERY_SEC = 60

# === NEW v23: ENTRY WINDOW (signal != entry) ===================================
ENTRY_WINDOW_SECONDS = 45          # waktu menunggu konfirmasi microstructure
ENTRY_CONFIRM_IMBALANCE = 0.15     # ambang imbalance searah untuk konfirmasi continuation/reverse
ENTRY_CONFIRM_DELTA     = 0.10     # ambang delta_ratio searah untuk konfirmasi
ENTRY_REJECT_PRICE_PCT  = 0.0015   # toleransi price reclaim/failure vs harga saat signal muncul

# === NEW v23: SHADOW TRADE TRACKING ============================================
SHADOW_HORIZONS_SEC = [5, 10, 30, 60, 120, 180, 300, 600]
MAX_SHADOW_TRADES    = 500   # cap memory
SHADOW_PRUNE_KEEP    = 300

# ═══════════════════════════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT", "APEUSDT",
    "CRVUSDT", "1000SHIBUSDT", "COMPUSDT", "MKRUSDT", "SNXUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════════════════════════
#  1. ORDER BOOK ENGINE (MICRO-STRUCTURE & DEPTH) — DIPERTAHANKAN
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    def __init__(self):
        self._cache = {}
        self._history = defaultdict(lambda: deque(maxlen=10))
        self._lock = threading.Lock()

    def update(self, symbol: str, bids_raw: list, asks_raw: list, ts: float = None):
        if ts is None: ts = time.time()
        try:
            bids = [(float(p), float(q)) for p, q in bids_raw]
            asks = [(float(p), float(q)) for p, q in asks_raw]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            bid_vol = sum(q for _, q in bids)
            ask_vol = sum(q for _, q in asks)
            tot_vol = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / (tot_vol + 1e-9)

            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0

            with self._lock:
                self._cache[symbol] = {
                    "bids": bids, "asks": asks, "bid_vol": bid_vol, "ask_vol": ask_vol,
                    "imbalance": imbalance, "best_bid": best_bid, "best_ask": best_ask, "ts": ts
                }
                self._history[symbol].append((ts, bid_vol, ask_vol, best_bid, best_ask))
        except Exception:
            pass

    def get_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._cache.get(symbol)

    def get_imbalance(self, symbol: str) -> float:
        book = self.get_book(symbol)
        return book["imbalance"] if book else 0.0

    def check_walls(self, symbol: str, current_price: float, side: str) -> Tuple[bool, str, float, float, float]:
        book = self.get_book(symbol)
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]; tot_ask = book["ask_vol"]
            if not asks or tot_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)
            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]; tot_bid = book["bid_vol"]
            if not bids or tot_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = tot_bid / len(bids)
            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_bid or qty >= WALL_DEPTH_PCT * tot_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult

        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol: str, side: str) -> Tuple[bool, str]:
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if len(hist) < 3: return False, ""
        curr_ts, curr_b_vol, curr_a_vol, _, _ = hist[-1]
        for ts, b_vol, a_vol, _, _ in hist[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG" and b_vol > 0:
                    if curr_b_vol < b_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_b_vol / b_vol) * 100
                        return True, f"Bid liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
                elif side == "SHORT" and a_vol > 0:
                    if curr_a_vol < a_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_a_vol / a_vol) * 100
                        return True, f"Ask liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
        return False, ""

order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  2. MACRO BTC & TICK CORRELATION ENGINE — DIPERTAHANKAN
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0, "trigger_ts": 0.0}
        self.last_price = 0.0
        self.lock = threading.Lock()

    def update_tick(self, price: float, ts: float = None):
        if ts is None: ts = time.time()
        with self.lock:
            self.last_price = price
            self.tick_history.append((ts, price))
            cutoff = ts - BTC_WINDOW_SEC
            baseline_price = None
            for t_ts, t_px in self.tick_history:
                if t_ts >= cutoff:
                    baseline_price = t_px
                    break
            if baseline_price and baseline_price > 0:
                delta = (price - baseline_price) / baseline_price
                if delta <= BTC_CRASH_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "CRASH"):
                    self.breaker = {"active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN, "delta": delta, "trigger_ts": ts}
                    print(f"\n  🚨 [BTC FLASH CRASH] {delta*100:+.2f}% | Altcoin LONGs LOCKED {BTC_BREAKER_COOLDOWN:.0f}s")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {"active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN, "delta": delta, "trigger_ts": ts}
                    print(f"\n  🚀 [BTC FLASH PUMP] {delta*100:+.2f}% | Altcoin SHORTs LOCKED {BTC_BREAKER_COOLDOWN:.0f}s")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]; delta = self.breaker["delta"]
                    if b_type == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash active ({rem:.0f}s left, {delta*100:+.2f}%)"
                    elif b_type == "PUMP" and side == "SHORT":
                        return True, f"BTC Flash Pump active ({rem:.0f}s left, {delta*100:+.2f}%)"
                else:
                    self.breaker["active"] = False; self.breaker["type"] = "NONE"
        return False, "OK"

    def get_status_str(self) -> str:
        with self.lock:
            px = self.last_price
            active = self.breaker["active"] and time.time() < self.breaker["until"]
            if active:
                rem = self.breaker["until"] - time.time()
                return f"BTC: ${px:.1f} | 🚨BREAKER [{self.breaker['type']} {self.breaker['delta']*100:+.2f}% ({rem:.0f}s)]"
            return f"BTC: ${px:.1f} [NORMAL]"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
#  3. ABSORPTION & MARKET REGIME — DIPERTAHANKAN
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[bool, bool, str]:
        if df is None or len(df) < 25: return False, False, ""
        row = df.iloc[-2]
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0); low = row.get("low", 0.0); high = row.get("high", 0.0); close = row.get("close", 0.0)
        delta_ratio = row.get("delta_ratio", 0.0); buy_ratio = row.get("br", 0.5)
        lw_ratio = row.get("lower_wick_ratio", 0.0); uw_ratio = row.get("upper_wick_ratio", 0.0)

        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        wick_bull = lw_ratio >= 0.38
        close_held_bull = close >= (low + 0.45 * rng)
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)

        heavy_buyer = (delta_ratio > 0.20) or (buy_ratio > 0.60)
        wick_bear = uw_ratio >= 0.38
        close_held_bear = close <= (high - 0.45 * rng)
        bear_absorb = vol_spike and heavy_buyer and (wick_bear or close_held_bear)

        details = []
        if bull_absorb: details.append(f"BullAbsorb(Vol:{row.get('vr',1.0):.1f}x|Δ:{delta_ratio:+.2f}|Wick:{lw_ratio:.0%})")
        if bear_absorb: details.append(f"BearAbsorb(Vol:{row.get('vr',1.0):.1f}x|Δ:{delta_ratio:+.2f}|Wick:{uw_ratio:.0%})")
        return bull_absorb, bear_absorb, " ".join(details)


class DynamicRiskManager:
    """ATR-based TP/SL — dipertahankan strukturnya, HANYA nama multiplier diubah agar
    tidak lagi 'reversed' (v22 menyimpan TP lama sebagai SL baru dsb). Sekarang TP/SL
    normal kembali: TP = ATR_TP_MULTIPLIER * ATR, SL = ATR_SL_MULTIPLIER * ATR."""
    @staticmethod
    def calculate_levels(entry_price: float, execution_side: str, atr: float) -> Dict[str, float]:
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.01
        tp_pct = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_MULTIPLIER * atr_pct))
        sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULTIPLIER * atr_pct))
        if execution_side == "LONG":
            tp_price = entry_price * (1 + tp_pct); sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct); sl_price = entry_price * (1 + sl_pct)
        return {"tp_pct": tp_pct, "sl_pct": sl_pct, "tp_price": tp_price, "sl_price": sl_price, "atr_pct": atr_pct}


class MarketRegime:
    REGIME_TRENDING_BULL = "TRENDING_BULL"
    REGIME_TRENDING_BEAR = "TRENDING_BEAR"
    REGIME_RANGE         = "RANGE"
    REGIME_VOLATILE      = "VOLATILE"
    REGIME_EXHAUSTION    = "EXHAUSTION"

    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < 55: return MarketRegime.REGIME_RANGE, 0, 0
        row, prev = df.iloc[-2], df.iloc[-3]
        close = row["close"]; e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        atr, atr_prev = row["atr"], prev["atr"]; adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull  = close > e9 > e21
        mild_bear  = close < e9 < e21
        strong_trend = adx > 25; very_strong_trend = adx > 35
        atr_expand   = (atr / atr_prev) > 1.2 if atr_prev > 0 else False
        atr_collapse = (atr / atr_prev) < 0.8 if atr_prev > 0 else False
        m5, m5_prev = row["m5"], prev["m5"]
        decelerating = (abs(m5) < abs(m5_prev)) if not np.isnan(m5_prev) else False

        if very_strong_trend and bull_stack: return MarketRegime.REGIME_TRENDING_BULL, min(adx, 100), 1.0
        elif very_strong_trend and bear_stack: return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 100), -1.0
        elif strong_trend and (bull_stack or mild_bull): return MarketRegime.REGIME_TRENDING_BULL, min(adx, 80), 0.7
        elif strong_trend and (bear_stack or mild_bear): return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 80), -0.7
        elif atr_expand and adx < 20: return MarketRegime.REGIME_VOLATILE, 50, 0
        elif (atr_collapse and decelerating) or (adx > 20 and adx < 35 and decelerating): return MarketRegime.REGIME_EXHAUSTION, 40, (1 if m5 > 0 else -1)
        else: return MarketRegime.REGIME_RANGE, 30, 0

# ═══════════════════════════════════════════════════════════════════════════
#  SCORING & ADAPTIVE SIGNAL WEIGHTS — DIPERTAHANKAN
# ═══════════════════════════════════════════════════════════════════════════

class SignalWeights:
    def __init__(self):
        self.weights = {
            "ema_bull_stack": 30, "ema_mild_bull": 20, "ema_weak_bull": 12,
            "mom_strong": 25, "mom_moderate": 15,
            "macd_cross_up": 22, "macd_strengthen": 15,
            "orderflow_delta_bull": 25, "orderflow_buy_high": 15,
            "absorption_bull": 35, "orderbook_imbalance_bull": 20,
            "rsi_bull_flow": 15, "rsi_extreme_ob": 10,
            "ema_bear_stack": 30, "ema_mild_bear": 20, "ema_weak_bear": 12,
            "mom_strong_neg": 25, "mom_moderate_neg": 15,
            "macd_cross_down": 22, "macd_strengthen_neg": 15,
            "orderflow_delta_bear": 25, "orderflow_sell_high": 15,
            "absorption_bear": 35, "orderbook_imbalance_bear": 20,
            "rsi_bear_flow": 15, "rsi_extreme_os": 10,
        }
        self.history = defaultdict(list)
        self.adaptive_enabled = True

    def record_outcome(self, signals: List[str], won: bool):
        for sig in signals:
            base = sig.split('[')[0].strip()
            if base in self.weights:
                self.history[base].append(1 if won else 0)
                if len(self.history[base]) > LEARNING_WINDOW:
                    self.history[base] = self.history[base][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        base = signal_name.split('[')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT: return self.weights.get(base, 10)
        return self.weights.get(base, 10) * max(0.5, min(1.5, 0.5 + sum(hist) / len(hist)))


class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

    def get_signal(self, df: pd.DataFrame, symbol: str = None) -> Tuple[Optional[str], int, List[str], float, str, float]:
        if df is None or len(df) < 55:
            return None, 0, [], 0.0, "UNKNOWN", 0.0
        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)
        atr = df["atr"].iloc[-2]
        bull_absorb, bear_absorb, _ = AbsorptionDetector.detect(df)

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]"); short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]"); long_score -= 20

        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias
        elif regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias
        elif regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias
        elif regime == MarketRegime.REGIME_VOLATILE:
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias
        return None, 0, [], atr, regime, bias

    def _score_long(self, df, symbol):
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
        if p > e5 > e9 > e21 > e50: w = self.weights.get_adjusted_weight("ema_bull_stack"); score += w; signals.append(f"EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21: w = self.weights.get_adjusted_weight("ema_mild_bull"); score += w; signals.append(f"EMA4↑[{w:.0f}]")
        elif p > e5 > e9: w = self.weights.get_adjusted_weight("ema_weak_bull"); score += w; signals.append(f"EMA3↑[{w:.0f}]")
        if row["m5"] > 0.003: w = self.weights.get_adjusted_weight("mom_strong"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        elif row["m5"] > 0.0015: w = self.weights.get_adjusted_weight("mom_moderate"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        if prev["mh"] <= 0 and row["mh"] > 0: w = self.weights.get_adjusted_weight("macd_cross_up"); score += w; signals.append(f"MACD_X↑[{w:.0f}]")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen"); score += w; signals.append(f"MACD↑↑[{w:.0f}]")
        delta_ratio = row.get("delta_ratio", 0.0); buy_ratio = row.get("br", 0.5)
        if delta_ratio > 0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bull"); score += w; signals.append(f"ΔBuy+{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio > 0.55: w = self.weights.get_adjusted_weight("orderflow_buy_high"); score += w; signals.append(f"TakerBuy{buy_ratio*100:.0f}%[{w:.0f}]")
        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs: w = self.weights.get_adjusted_weight("absorption_bull"); score += w; signals.append(f"BullAbsorb[{w:.0f}]")
        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL: w = self.weights.get_adjusted_weight("orderbook_imbalance_bull"); score += w; signals.append(f"BAI+{imb*100:.0f}%[{w:.0f}]")
        if 48 <= row["rsi"] <= 68: w = self.weights.get_adjusted_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] > 68: w = self.weights.get_adjusted_weight("rsi_extreme_ob"); score += w; signals.append(f"RSI{row['rsi']:.0f}OB[{w:.0f}]")
        return score, signals

    def _score_short(self, df, symbol):
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]
        if p < e5 < e9 < e21 < e50: w = self.weights.get_adjusted_weight("ema_bear_stack"); score += w; signals.append(f"EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21: w = self.weights.get_adjusted_weight("ema_mild_bear"); score += w; signals.append(f"EMA4↓[{w:.0f}]")
        elif p < e5 < e9: w = self.weights.get_adjusted_weight("ema_weak_bear"); score += w; signals.append(f"EMA3↓[{w:.0f}]")
        if row["m5"] < -0.003: w = self.weights.get_adjusted_weight("mom_strong_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        elif row["m5"] < -0.0015: w = self.weights.get_adjusted_weight("mom_moderate_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        if prev["mh"] >= 0 and row["mh"] < 0: w = self.weights.get_adjusted_weight("macd_cross_down"); score += w; signals.append(f"MACD_X↓[{w:.0f}]")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen_neg"); score += w; signals.append(f"MACD↓↓[{w:.0f}]")
        delta_ratio = row.get("delta_ratio", 0.0); buy_ratio = row.get("br", 0.5)
        if delta_ratio < -0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bear"); score += w; signals.append(f"ΔSell{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio < 0.45: w = self.weights.get_adjusted_weight("orderflow_sell_high"); score += w; signals.append(f"TakerSell{(1-buy_ratio)*100:.0f}%[{w:.0f}]")
        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs: w = self.weights.get_adjusted_weight("absorption_bear"); score += w; signals.append(f"BearAbsorb[{w:.0f}]")
        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR: w = self.weights.get_adjusted_weight("orderbook_imbalance_bear"); score += w; signals.append(f"BAI{imb*100:.0f}%[{w:.0f}]")
        if 32 <= row["rsi"] <= 52: w = self.weights.get_adjusted_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] < 32: w = self.weights.get_adjusted_weight("rsi_extreme_os"); score += w; signals.append(f"RSI{row['rsi']:.0f}OS[{w:.0f}]")
        return score, signals

# ═══════════════════════════════════════════════════════════════════════════
# === NEW v23: 4. FAILURE / ADVERSARIAL ENGINE ===
# ═══════════════════════════════════════════════════════════════════════════

def _score_bucket(score: float) -> int:
    return int(score // BUCKET_SCORE_STEP) * BUCKET_SCORE_STEP

def _rsi_bucket(rsi: float) -> int:
    return int(rsi // BUCKET_RSI_STEP) * BUCKET_RSI_STEP

class BucketStats:
    __slots__ = ("n", "wins", "pnl_sum", "mfe_sum", "mae_sum")
    def __init__(self):
        self.n = 0; self.wins = 0; self.pnl_sum = 0.0; self.mfe_sum = 0.0; self.mae_sum = 0.0

    def to_dict(self):
        return {"n": self.n, "wins": self.wins, "pnl_sum": self.pnl_sum, "mfe_sum": self.mfe_sum, "mae_sum": self.mae_sum}

    @staticmethod
    def from_dict(d):
        b = BucketStats(); b.n = d.get("n", 0); b.wins = d.get("wins", 0)
        b.pnl_sum = d.get("pnl_sum", 0.0); b.mfe_sum = d.get("mfe_sum", 0.0); b.mae_sum = d.get("mae_sum", 0.0)
        return b


class HistoricalBucketStore:
    """Statistik online per-bucket (regime x direction x score_bucket x rsi_bucket x symbol).
    Shrinkage sederhana (bukan ML): winrate_smoothed = (wins + prior_n*prior_wr) / (n + prior_n).
    PENTING (no look-ahead): update() HANYA dipanggil setelah trade/shadow SELESAI
    (sudah tahu outcome). get_failure_rate() / get_smoothed_winrate() HANYA membaca state
    yang sudah terkumpul SEBELUM waktu keputusan saat ini — tidak pernah membaca masa depan.
    """
    def __init__(self, path=LEARNING_STATE_FILE):
        self.path = path
        self.lock = threading.Lock()
        self.buckets: Dict[str, BucketStats] = defaultdict(BucketStats)
        self.symbol_buckets: Dict[str, BucketStats] = defaultdict(BucketStats)
        self._last_save = 0.0
        self._load()

    @staticmethod
    def make_key(regime: str, direction: str, score: float, rsi: float) -> str:
        return f"{regime}|{direction}|{_score_bucket(score)}|{_rsi_bucket(rsi)}"

    @staticmethod
    def make_symbol_key(symbol: str, regime: str, direction: str) -> str:
        return f"{symbol}|{regime}|{direction}"

    def update(self, key: str, won: bool, pnl_pct: float, mfe_pct: float = 0.0, mae_pct: float = 0.0):
        with self.lock:
            b = self.buckets[key]
            b.n += 1
            if won: b.wins += 1
            b.pnl_sum += pnl_pct
            b.mfe_sum += mfe_pct
            b.mae_sum += mae_pct
        self._maybe_save()

    def update_symbol(self, key: str, won: bool, pnl_pct: float):
        with self.lock:
            b = self.symbol_buckets[key]
            b.n += 1
            if won: b.wins += 1
            b.pnl_sum += pnl_pct
        self._maybe_save()

    def get_smoothed_winrate(self, key: str) -> Tuple[float, int]:
        """Returns (smoothed_winrate, sample_n). Shrinkage toward BUCKET_PRIOR_WINRATE."""
        with self.lock:
            b = self.buckets.get(key)
        if b is None or b.n == 0:
            return BUCKET_PRIOR_WINRATE, 0
        prior_n = MIN_SAMPLE_FOR_TRUST
        smoothed = (b.wins + prior_n * BUCKET_PRIOR_WINRATE) / (b.n + prior_n)
        return smoothed, b.n

    def get_symbol_winrate(self, key: str) -> Tuple[float, int]:
        with self.lock:
            b = self.symbol_buckets.get(key)
        if b is None or b.n == 0:
            return BUCKET_PRIOR_WINRATE, 0
        prior_n = MIN_SAMPLE_FOR_TRUST
        smoothed = (b.wins + prior_n * BUCKET_PRIOR_WINRATE) / (b.n + prior_n)
        return smoothed, b.n

    def _maybe_save(self):
        now = time.time()
        if now - self._last_save < LEARNING_SAVE_EVERY_SEC:
            return
        self._last_save = now
        self.save()

    def save(self):
        try:
            with self.lock:
                data = {
                    "buckets": {k: v.to_dict() for k, v in self.buckets.items()},
                    "symbol_buckets": {k: v.to_dict() for k, v in self.symbol_buckets.items()},
                }
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, self.path)
        except Exception as e:
            print(f"  ⚠️ [learning_save] gagal menyimpan state: {e}")

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            for k, v in data.get("buckets", {}).items():
                self.buckets[k] = BucketStats.from_dict(v)
            for k, v in data.get("symbol_buckets", {}).items():
                self.symbol_buckets[k] = BucketStats.from_dict(v)
            print(f"  📚 [learning_load] {len(self.buckets)} bucket regime, {len(self.symbol_buckets)} bucket symbol dimuat")
        except Exception as e:
            # Jangan crash jika file rusak — mulai dari kosong.
            print(f"  ⚠️ [learning_load] state rusak/tidak terbaca, mulai dari kosong: {e}")
            self.buckets = defaultdict(BucketStats)
            self.symbol_buckets = defaultdict(BucketStats)

bucket_store = HistoricalBucketStore()


@dataclass
class FailureAssessment:
    failure_score: float
    reasons: List[str]
    reverse_confidence: float
    reverse_reasons: List[str]
    bucket_key: str
    bucket_n: int


class FailureScorer:
    """Menghitung FAILURE_SCORE (0-100): seberapa besar bukti bahwa signal ORIGINAL akan gagal.
    Juga menghitung REVERSE_CONFIDENCE terpisah: seberapa besar bukti bahwa arah BERLAWANAN
    punya edge (bukan cuma "original lemah")."""

    @staticmethod
    def assess(df: pd.DataFrame, symbol: str, orig_direction: str, signal_score: float,
               regime: str, atr: float) -> FailureAssessment:
        row = df.iloc[-2]
        prev = df.iloc[-3]
        reasons = []
        score = 0.0
        w = FAILURE_WEIGHTS

        price = row["close"]
        rsi = row["rsi"]
        e21 = row["e21"]
        vr = row.get("vr", 1.0)
        delta_ratio = row.get("delta_ratio", 0.0)
        m5, m5_prev = row["m5"], prev["m5"]
        uw_ratio = row.get("upper_wick_ratio", 0.0)
        lw_ratio = row.get("lower_wick_ratio", 0.0)
        imb = order_book.get_imbalance(symbol)

        is_long = (orig_direction == "LONG")

        # 1. Late extension: harga terlalu jauh dari EMA21 (overextended, telat masuk)
        ema_dist_pct = abs(price - e21) / e21 if e21 else 0.0
        atr_pct = (atr / price) if price else 0.0
        if atr_pct > 0 and ema_dist_pct > 2.2 * atr_pct:
            score += w["late_extension"]; reasons.append(f"LateExtension(dist:{ema_dist_pct*100:.2f}%)")

        # 2. RSI sudah ekstrem searah signal (ruang gerak habis)
        if is_long and rsi >= 70:
            score += w["rsi_extreme"]; reasons.append(f"RSI_Extreme({rsi:.0f})")
        elif (not is_long) and rsi <= 30:
            score += w["rsi_extreme"]; reasons.append(f"RSI_Extreme({rsi:.0f})")

        # 3. Volume tidak mendukung
        if vr < 1.0:
            score += w["volume_unsupportive"]; reasons.append(f"WeakVolume(vr:{vr:.2f})")

        # 4. Orderbook berlawanan arah signal
        if is_long and imb < -0.10:
            score += w["orderbook_opposing"]; reasons.append(f"BookOpposing(BAI:{imb*100:+.0f}%)")
        elif (not is_long) and imb > 0.10:
            score += w["orderbook_opposing"]; reasons.append(f"BookOpposing(BAI:{imb*100:+.0f}%)")

        # 5. Delta (taker flow) berlawanan arah signal
        if is_long and delta_ratio < -0.05:
            score += w["delta_opposing"]; reasons.append(f"DeltaOpposing({delta_ratio*100:+.0f}%)")
        elif (not is_long) and delta_ratio > 0.05:
            score += w["delta_opposing"]; reasons.append(f"DeltaOpposing({delta_ratio*100:+.0f}%)")

        # 6. Rejection wick berlawanan
        if is_long and uw_ratio >= 0.35:
            score += w["rejection_wick"]; reasons.append(f"UpperRejection({uw_ratio:.0%})")
        elif (not is_long) and lw_ratio >= 0.35:
            score += w["rejection_wick"]; reasons.append(f"LowerRejection({lw_ratio:.0%})")

        # 7. Momentum melemah dibanding candle sebelumnya
        if not np.isnan(m5_prev) and abs(m5) < abs(m5_prev) * 0.6:
            score += w["momentum_fading"]; reasons.append("MomentumFading")

        # 8. Regime mismatch: signal continuation di RANGE, atau counter-trend di TRENDING kuat
        if "RANGE" in regime and "ABSORB" not in regime:
            score += w["regime_mismatch"]; reasons.append("RangeNoAbsorb")
        elif regime == MarketRegime.REGIME_TRENDING_BEAR and is_long:
            score += w["regime_mismatch"]; reasons.append("CounterTrendInStrongBear")
        elif regime == MarketRegime.REGIME_TRENDING_BULL and not is_long:
            score += w["regime_mismatch"]; reasons.append("CounterTrendInStrongBull")

        # 9. Historical bucket failure rate (empirical, shrinkage, no look-ahead karena
        #    bucket_store hanya berisi trade/shadow yang SUDAH selesai sebelum saat ini)
        bucket_key = HistoricalBucketStore.make_key(regime, orig_direction, signal_score, rsi)
        smoothed_wr, bucket_n = bucket_store.get_smoothed_winrate(bucket_key)
        hist_failure_rate = 1.0 - smoothed_wr
        score += w["historical_bucket"] * hist_failure_rate
        if bucket_n >= MIN_SAMPLE_FOR_TRUST:
            reasons.append(f"HistFailRate({hist_failure_rate*100:.0f}%,n={bucket_n})")
        else:
            reasons.append(f"HistFailRate(prior,n={bucket_n})")

        failure_score = max(0.0, min(100.0, score))

        # === REVERSE CONFIDENCE ===
        # Reverse hanya masuk akal jika (a) failure_score sudah cukup tinggi DAN
        # (b) ada bukti aktif bahwa arah berlawanan yang diuntungkan (bukan cuma original lemah).
        reverse_reasons = []
        reverse_conf = 0.0
        opp_direction = "SHORT" if is_long else "LONG"
        if failure_score >= FAILURE_MIN_FOR_REVERSE:
            # microstructure mendukung arah berlawanan secara aktif
            if is_long and imb < -IMBALANCE_STRONG_BULL:
                reverse_conf += 25; reverse_reasons.append(f"BookFavorsShort({imb*100:+.0f}%)")
            elif (not is_long) and imb > IMBALANCE_STRONG_BULL:
                reverse_conf += 25; reverse_reasons.append(f"BookFavorsLong({imb*100:+.0f}%)")

            if is_long and delta_ratio < -0.20:
                reverse_conf += 20; reverse_reasons.append(f"DeltaFavorsShort({delta_ratio*100:+.0f}%)")
            elif (not is_long) and delta_ratio > 0.20:
                reverse_conf += 20; reverse_reasons.append(f"DeltaFavorsLong({delta_ratio*100:+.0f}%)")

            if is_long and uw_ratio >= 0.45:
                reverse_conf += 15; reverse_reasons.append("StrongRejectionTop")
            elif (not is_long) and lw_ratio >= 0.45:
                reverse_conf += 15; reverse_reasons.append("StrongRejectionBottom")

            # historical bucket utk arah berlawanan pada kondisi ini
            opp_key = HistoricalBucketStore.make_key(regime, opp_direction, signal_score, rsi)
            opp_wr, opp_n = bucket_store.get_smoothed_winrate(opp_key)
            if opp_n >= MIN_SAMPLE_FOR_TRUST and opp_wr > 0.5:
                reverse_conf += min(25.0, (opp_wr - 0.5) * 100)
                reverse_reasons.append(f"OppositeHistEdge({opp_wr*100:.0f}%,n={opp_n})")

            # symbol-level historical performance pada regime ini untuk arah asli buruk?
            sym_key = HistoricalBucketStore.make_symbol_key(symbol, regime, orig_direction)
            sym_wr, sym_n = bucket_store.get_symbol_winrate(sym_key)
            if sym_n >= MIN_SAMPLE_FOR_TRUST and sym_wr < 0.4:
                reverse_conf += 10; reverse_reasons.append(f"SymbolPoorInRegime({sym_wr*100:.0f}%,n={sym_n})")

        reverse_conf = max(0.0, min(100.0, reverse_conf))

        return FailureAssessment(
            failure_score=failure_score, reasons=reasons,
            reverse_confidence=reverse_conf, reverse_reasons=reverse_reasons,
            bucket_key=bucket_key, bucket_n=bucket_n,
        )


# ═══════════════════════════════════════════════════════════════════════════
# === NEW v23: 5. DECISION ENGINE (Conditional Reversal, bukan reverse buta) ===
# ═══════════════════════════════════════════════════════════════════════════

class DecisionEngine:
    @staticmethod
    def decide(orig_direction: str, signal_score: float, assessment: FailureAssessment) -> Tuple[str, str, float, str]:
        """
        Returns: (decision, execution_direction_candidate, edge_score, reason)
        decision in {"ORIGINAL", "REVERSE", "NO_TRADE"}
        execution_direction_candidate masih HARUS lolos ENTRY WINDOW sebelum benar2 dieksekusi.
        """
        failure_score = assessment.failure_score
        reverse_conf = assessment.reverse_confidence
        opp_direction = "SHORT" if orig_direction == "LONG" else "LONG"

        edge_score = signal_score - failure_score * FAILURE_PENALTY_MULT

        if STRATEGY_MODE == "SHADOW_ONLY":
            return "NO_TRADE", orig_direction, edge_score, "SHADOW_ONLY_MODE"

        if STRATEGY_MODE == "ORIGINAL":
            if edge_score >= MIN_EDGE_SCORE and failure_score < FAILURE_VETO_THRESHOLD:
                return "ORIGINAL", orig_direction, edge_score, "MODE_ORIGINAL_EDGE_OK"
            return "NO_TRADE", orig_direction, edge_score, "MODE_ORIGINAL_EDGE_LOW"

        if STRATEGY_MODE == "REVERSE_TEST":
            # HANYA eksperimen: selalu kandidat reverse, tetap harus lolos entry window (failure confirmation)
            return "REVERSE", opp_direction, edge_score, "MODE_REVERSE_TEST"

        # ADVERSARIAL (default)
        if failure_score >= FAILURE_VETO_THRESHOLD and reverse_conf < REVERSE_CONFIDENCE_MIN:
            return "NO_TRADE", orig_direction, edge_score, f"HighFailureNoReverseEdge(F={failure_score:.0f},RevConf={reverse_conf:.0f})"

        if reverse_conf >= REVERSE_CONFIDENCE_MIN and failure_score >= FAILURE_MIN_FOR_REVERSE:
            return "REVERSE", opp_direction, edge_score, f"FailureConfirmed+ReverseEdge(F={failure_score:.0f},RevConf={reverse_conf:.0f})"

        if edge_score >= MIN_EDGE_SCORE and failure_score < FAILURE_VETO_THRESHOLD:
            return "ORIGINAL", orig_direction, edge_score, f"EdgeOk(edge={edge_score:.0f})"

        return "NO_TRADE", orig_direction, edge_score, f"NoEdge(edge={edge_score:.0f},F={failure_score:.0f},RevConf={reverse_conf:.0f})"


# ═══════════════════════════════════════════════════════════════════════════
# === NEW v23: 6. ENTRY WINDOW — signal != entry, tunggu konfirmasi microstructure ===
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class PendingEntry:
    symbol: str
    decision: str                 # "ORIGINAL" or "REVERSE"
    orig_direction: str
    execution_side: str           # arah yang akan dieksekusi JIKA terkonfirmasi
    signal_score: float
    failure_score: float
    reverse_confidence: float
    regime: str
    sigs: List[str]
    reasons: List[str]
    atr: float
    signal_price: float
    risk_profile: Dict[str, float]
    bucket_key: str
    opened_ts: float = field(default_factory=time.time)
    reason: str = ""


class EntryWindowManager:
    """Menahan kandidat entry selama ENTRY_WINDOW_SECONDS dan menunggu konfirmasi
    microstructure sebelum benar-benar mengirim order. Jika decision == REVERSE,
    konfirmasi yang dicari adalah bukti KEGAGALAN signal asli (breakout failure),
    bukan sekadar "microstructure searah reverse", supaya sesuai requirement:
    'reverse harus terjadi SETELAH kegagalan signal terkonfirmasi'."""

    def __init__(self):
        self.pending: Dict[str, PendingEntry] = {}
        self.lock = threading.Lock()

    def add(self, entry: PendingEntry):
        with self.lock:
            if entry.symbol in self.pending or entry.symbol in live_positions:
                return False
            self.pending[entry.symbol] = entry
        log_decision_line(entry.symbol, entry, decision_override=entry.decision, extra="ENTRY_WINDOW_OPEN")
        return True

    def _check_confirmation(self, entry: PendingEntry) -> Tuple[bool, str]:
        sym = entry.symbol
        book = order_book.get_book(sym)
        imb = order_book.get_imbalance(sym)
        with _kline_lock:
            df = _kline_cache.get(sym)
        delta_ratio = 0.0
        if df is not None and len(df) >= 3 and "delta_ratio" in df.columns:
            delta_ratio = df["delta_ratio"].iloc[-2]
        px = price_live(sym)
        if px <= 0:
            return False, "NONE"

        price_move_pct = (px - entry.signal_price) / entry.signal_price if entry.signal_price else 0.0

        if entry.decision == "ORIGINAL":
            side = entry.execution_side
            if side == "LONG":
                if imb >= ENTRY_CONFIRM_IMBALANCE and delta_ratio >= ENTRY_CONFIRM_DELTA:
                    return True, "BUY_RECLAIM"
                if price_move_pct >= ENTRY_REJECT_PRICE_PCT and delta_ratio > 0:
                    return True, "MICRO_BREAKOUT"
            else:  # SHORT
                if imb <= -ENTRY_CONFIRM_IMBALANCE and delta_ratio <= -ENTRY_CONFIRM_DELTA:
                    return True, "SELL_RECLAIM"
                if price_move_pct <= -ENTRY_REJECT_PRICE_PCT and delta_ratio < 0:
                    return True, "MICRO_BREAKDOWN"
            return False, "NONE"

        else:  # REVERSE -> butuh bukti KEGAGALAN arah original, baru eksekusi execution_side
            orig = entry.orig_direction
            if orig == "LONG":
                # original LONG gagal jika: harga balik turun di bawah signal_price, delta jual meningkat, imbalance jual
                failed = (price_move_pct <= -ENTRY_REJECT_PRICE_PCT) and (delta_ratio <= -ENTRY_CONFIRM_DELTA) and (imb <= -ENTRY_CONFIRM_IMBALANCE * 0.6)
                if failed:
                    return True, "SELL_PRESSURE"
            else:
                failed = (price_move_pct >= ENTRY_REJECT_PRICE_PCT) and (delta_ratio >= ENTRY_CONFIRM_DELTA) and (imb >= ENTRY_CONFIRM_IMBALANCE * 0.6)
                if failed:
                    return True, "BUY_PRESSURE"
            return False, "NONE"

    def tick(self):
        now = time.time()
        with self.lock:
            syms = list(self.pending.keys())
        for sym in syms:
            with self.lock:
                entry = self.pending.get(sym)
            if entry is None:
                continue
            if len(live_positions) >= MAX_POSITIONS or ks_check()[0]:
                continue
            try:
                confirmed, confirm_type = self._check_confirmation(entry)
            except Exception as e:
                _log_err(f"entry_window_{sym}", e)
                confirmed, confirm_type = False, "NONE"

            if confirmed:
                with self.lock:
                    self.pending.pop(sym, None)
                log_decision_line(sym, entry, decision_override=entry.decision, extra=f"ENTRY_CONFIRMATION={confirm_type} -> EXECUTE")
                if STRATEGY_MODE == "SHADOW_ONLY":
                    shadow_tracker.register(entry, confirm_type, executed=False)
                else:
                    executed = live_open_from_entry(entry, confirm_type)
                    shadow_tracker.register(entry, confirm_type, executed=executed)
                continue

            if now - entry.opened_ts >= ENTRY_WINDOW_SECONDS:
                with self.lock:
                    self.pending.pop(sym, None)
                log_decision_line(sym, entry, decision_override="NO_TRADE", extra="NO_CONFIRMATION (window expired)")
                shadow_tracker.register(entry, "NONE", executed=False)

entry_window = EntryWindowManager()


def log_decision_line(sym, entry: 'PendingEntry', decision_override: str, extra: str = ""):
    print(f"  {sym} | REGIME={entry.regime} | SIGNAL={entry.orig_direction} | "
          f"SIGNAL_SCORE={entry.signal_score:.0f} | FAILURE_SCORE={entry.failure_score:.0f} | "
          f"REVERSAL_CONF={entry.reverse_confidence:.0f} | DECISION={decision_override} | {extra}")


# ═══════════════════════════════════════════════════════════════════════════
# === NEW v23: 7. SHADOW / GHOST TRADE SYSTEM ===
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ShadowTrade:
    symbol: str
    orig_direction: str
    execution_candidate: str
    decision: str
    signal_score: float
    failure_score: float
    reverse_confidence: float
    regime: str
    entry_price: float
    atr: float
    bucket_key: str
    confirm_type: str
    executed: bool
    tp_pct: float
    sl_pct: float
    opened_ts: float = field(default_factory=time.time)
    horizons_done: set = field(default_factory=set)
    mfe_orig: float = 0.0
    mae_orig: float = 0.0
    mfe_rev: float = 0.0
    mae_rev: float = 0.0
    peak_px: float = 0.0
    trough_px: float = 0.0
    finalized: bool = False


class ShadowTradeTracker:
    """Mencatat SEMUA kandidat (dieksekusi maupun tidak) dan mengamati outcome-nya di beberapa
    horizon waktu. Hasil dipakai untuk mengisi bucket_store (failure mining) — TIDAK PERNAH
    dipakai untuk mengubah keputusan trade yang SEDANG berjalan (no look-ahead)."""

    def __init__(self):
        self.active: List[ShadowTrade] = []
        self.lock = threading.Lock()
        self.stats = {"n": 0, "wins": 0, "pnl_sum": 0.0}

    def register(self, entry: PendingEntry, confirm_type: str, executed: bool):
        px = price_live(entry.symbol)
        if px <= 0:
            px = entry.signal_price
        st = ShadowTrade(
            symbol=entry.symbol, orig_direction=entry.orig_direction,
            execution_candidate=entry.execution_side, decision=entry.decision,
            signal_score=entry.signal_score, failure_score=entry.failure_score,
            reverse_confidence=entry.reverse_confidence, regime=entry.regime,
            entry_price=px, atr=entry.atr, bucket_key=entry.bucket_key,
            confirm_type=confirm_type, executed=executed,
            tp_pct=entry.risk_profile.get("tp_pct", 0.01), sl_pct=entry.risk_profile.get("sl_pct", 0.01),
            peak_px=px, trough_px=px,
        )
        with self.lock:
            self.active.append(st)
            if len(self.active) > MAX_SHADOW_TRADES:
                self.active = self.active[-SHADOW_PRUNE_KEEP:]

    def tick(self):
        now = time.time()
        with self.lock:
            items = list(self.active)
        for st in items:
            if st.finalized:
                continue
            px = price_live(st.symbol)
            if px <= 0:
                continue
            st.peak_px = max(st.peak_px, px)
            st.trough_px = min(st.trough_px, px)

            elapsed = now - st.opened_ts
            for h in SHADOW_HORIZONS_SEC:
                if h in st.horizons_done or elapsed < h:
                    continue
                st.horizons_done.add(h)
                if h == SHADOW_HORIZONS_SEC[-1]:
                    self._finalize(st, px)

    def _finalize(self, st: ShadowTrade, px: float):
        st.finalized = True
        # Outcome utk arah ORIGINAL (hipotetis jika orig_direction dieksekusi apa adanya)
        if st.orig_direction == "LONG":
            pnl_orig = (px - st.entry_price) / st.entry_price
            mfe_orig = (st.peak_px - st.entry_price) / st.entry_price
            mae_orig = (st.trough_px - st.entry_price) / st.entry_price
        else:
            pnl_orig = (st.entry_price - px) / st.entry_price
            mfe_orig = (st.entry_price - st.trough_px) / st.entry_price
            mae_orig = (st.entry_price - st.peak_px) / st.entry_price
        pnl_rev = -pnl_orig
        won_orig = pnl_orig > 0
        won_rev = pnl_rev > 0

        rev_direction = "SHORT" if st.orig_direction == "LONG" else "LONG"
        bucket_store.update(st.bucket_key, won_orig, pnl_orig, mfe_orig, max(0.0, -mae_orig))
        rev_key = HistoricalBucketStore.make_key(st.regime, rev_direction, st.signal_score, 50)
        bucket_store.update(rev_key, won_rev, pnl_rev)
        sym_key_orig = HistoricalBucketStore.make_symbol_key(st.symbol, st.regime, st.orig_direction)
        bucket_store.update_symbol(sym_key_orig, won_orig, pnl_orig)

        with self.lock:
            self.stats["n"] += 1
            self.stats["wins"] += 1 if (won_rev if st.decision == "REVERSE" else won_orig) else 0
            self.stats["pnl_sum"] += (pnl_rev if st.decision == "REVERSE" else pnl_orig)

        if not st.executed:
            tag = "SHADOW"
        else:
            tag = "SHADOW(also live)"
        print(f"  👻 [{tag}] {st.symbol} orig={st.orig_direction} decision={st.decision} "
              f"origPnL%={pnl_orig*100:+.2f} revPnL%={pnl_rev*100:+.2f} conf={st.confirm_type}")

    def prune(self):
        with self.lock:
            self.active = [s for s in self.active if not s.finalized][-SHADOW_PRUNE_KEEP:]

shadow_tracker = ShadowTradeTracker()

# ═══════════════════════════════════════════════════════════════════════════
#  TRADE RECORDS & LEARNING LAYER (weight adaptation lama) — DIPERTAHANKAN
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol: str; direction: str; orig_direction: str; decision: str
    entry_price: float; exit_price: float; pnl: float; pnl_pct: float; won: bool
    regime: str; signals: List[str]; score: float; failure_score: float
    atr_entry: float; hold_seconds: float; exit_reason: str; peak_pct: float
    bucket_key: str
    timestamp: float = field(default_factory=time.time)

class LearningLayer:
    def __init__(self, signal_weights: SignalWeights):
        self.signal_weights = signal_weights
        self.trades = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self.stats_by_symbol = defaultdict(lambda: {"wins": 0, "losses": 0})
        # === NEW v23: pisahkan statistik ORIGINAL vs REVERSED ===
        self.stats_by_decision = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "fees": 0.0})

    def add_trade(self, trade: TradeRecord):
        self.trades.append(trade)
        r = trade.regime
        self.stats_by_regime[r]["wins"] += 1 if trade.won else 0
        self.stats_by_regime[r]["losses"] += 0 if trade.won else 1
        self.stats_by_regime[r]["pnl"] += trade.pnl
        self.stats_by_symbol[trade.symbol]["wins"] += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        d = self.stats_by_decision[trade.decision]
        d["wins"] += 1 if trade.won else 0
        d["losses"] += 0 if trade.won else 1
        d["pnl"] += trade.pnl
        self.signal_weights.record_outcome(trade.signals, trade.won)
        # === NEW v23: update bucket store dari trade REAL (bukan hipotetis) — no look-ahead,
        # karena hanya dipanggil setelah posisi benar2 close.
        bucket_store.update(trade.bucket_key, trade.won, trade.pnl_pct)
        if len(self.trades) > 1000: self.trades = self.trades[-500:]

    def get_global_winrate(self) -> float:
        w = sum(s["wins"] for s in self.stats_by_regime.values())
        l = sum(s["losses"] for s in self.stats_by_regime.values())
        return w / (w + l) if (w + l) > 0 else 0.5

    def avg_win(self) -> float:
        wins = [t.pnl for t in self.trades if t.won]
        return sum(wins) / len(wins) if wins else 0.0

    def avg_loss(self) -> float:
        losses = [abs(t.pnl) for t in self.trades if not t.won]
        return sum(losses) / len(losses) if losses else 0.0

    def profit_factor(self) -> float:
        gp = sum(t.pnl for t in self.trades if t.pnl > 0)
        gl = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        return gp / gl if gl > 0 else (float('inf') if gp > 0 else 0.0)

    def expectancy(self) -> float:
        if not self.trades: return 0.0
        return sum(t.pnl for t in self.trades) / len(self.trades)

    def avg_hold(self) -> float:
        if not self.trades: return 0.0
        return sum(t.hold_seconds for t in self.trades) / len(self.trades)

    def decision_wr(self, decision: str) -> Tuple[float, int]:
        d = self.stats_by_decision.get(decision)
        if not d: return 0.0, 0
        n = d["wins"] + d["losses"]
        return (d["wins"] / n if n else 0.0), n

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ticker_cache = {}
_ticker_ts = 0
_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q = queue.Queue()
_hot_syms = deque(maxlen=30)

_ws_mark_price = {}
_kline_cache = {}
_kline_lock = threading.Lock()
_ws_ticker_cache = {}
_ws_ticker_ts = 0
_ws_last_msg_ts = time.time()
WS_STALE_SEC = 30
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "regime_block": 0, "time_decay_exit": 0, "early_exit": 0, "time_limit_exit": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "absorb_entries": 0,
    "no_trade": 0, "no_confirmation": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

live_positions = {}
cooldown_list = {}
trade_log = []
signal_weights = SignalWeights()
scorer = SignalScorer(signal_weights)
learning = LearningLayer(signal_weights)

_last_err_print = defaultdict(float)
_api_fail_streak = 0
_api_ok_last = time.time()

def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now

def _log_warn(tag, msg, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {msg}")
        _last_err_print[tag] = now

def _api_ok():
    global _api_fail_streak, _api_ok_last
    _api_fail_streak = 0
    _api_ok_last = time.time()

def _api_fail(tag):
    global _api_fail_streak
    _api_fail_streak += 1
    if _api_fail_streak in (20, 100, 300) or _api_fail_streak % 1000 == 0:
        idle = time.time() - _api_ok_last
        print(f"  🚨 API GAGAL BERUNTUN {_api_fail_streak}x (idle {idle:.0f}s) — trigger: {tag}")

def get_precision(symbol):
    if symbol in _precision_cache: return _precision_cache[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except Exception as e:
        _log_err("get_precision", e)
    return 2

def qty(symbol, price):
    raw = (ORDER_USDT * LEVERAGE) / price
    return round(raw, get_precision(symbol))

def price_live(symbol):
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if px > 0 and (time.time() - ts) < MARKPRICE_FRESH_SEC:
            return px
    try:
        px = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        _api_ok()
        return px
    except Exception as e:
        _log_err(f"price_live_{symbol}", e)
        _api_fail(f"price_live_{symbol}")
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if _ws_ticker_cache and (now - _ws_ticker_ts) < 15:
        return _ws_ticker_cache
    if now - _ticker_ts < 2 and _ticker_cache: return _ticker_cache
    try:
        raw = client.futures_ticker()
        _ticker_cache = {t["symbol"]: {"pct": float(t["priceChangePercent"]), "vol": float(t["quoteVolume"]), "last": float(t["lastPrice"])} for t in raw}
        _ticker_ts = now
        _api_ok()
    except Exception as e:
        _log_err("tickers_all", e)
        _api_fail("tickers_all")
    return _ticker_cache

def _compute_indicators(df):
    close = df["close"]; high = df["high"]; low = df["low"]
    volume = df["volume"].replace(0, 1e-9)
    tbbase = df["tbbase"]

    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    df["mh"] = ta.trend.MACD(close, 12, 26, 9).macd_diff()
    df["e5"] = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"] = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()

    df["vm"] = volume.rolling(20).mean()
    df["vr"] = volume / df["vm"].replace(0, 1e-9)

    taker_buy = tbbase
    taker_sell = (volume - taker_buy).clip(lower=0)
    df["delta"] = taker_buy - taker_sell
    df["delta_ratio"] = df["delta"] / volume
    df["br"] = taker_buy / volume
    df["cvd"] = df["delta"].rolling(10).sum()

    df["rng"] = (high - low).replace(0, 1e-9)
    df["upper_wick"] = high - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - low
    df["body"] = (close - df["open"]).abs()
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]
    df["br2"] = df["body"] / df["rng"]

    df["m5"] = (close - close.shift(5)) / close.shift(5)
    df["m3"] = (close - close.shift(3)) / close.shift(3)
    return df

def run_ta(df):
    if "delta_ratio" not in df.columns or "rsi" not in df.columns:
        df = _compute_indicators(df)
    return df

def _bootstrap_klines(symbol, interval, limit=100):
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]: df[c] = df[c].astype(float)
        df = _compute_indicators(df)
        with _kline_lock: _kline_cache[symbol] = df
        _api_ok()
        return df
    except Exception as e:
        _log_err(f"bootstrap_klines_{symbol}", e)
        _api_fail(f"bootstrap_klines_{symbol}")
        return None

def _append_kline_from_ws(symbol, k):
    try:
        base_cols = ["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"]
        new_row = {
            "time": int(k["t"]), "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]),
            "close": float(k["c"]), "volume": float(k["v"]), "ct": int(k["T"]), "qv": float(k.get("q", 0)),
            "trades": int(k.get("n", 0)), "tbbase": float(k.get("V", 0)), "tbquote": float(k.get("Q", 0)),
            "ignore": 0,
        }
        with _kline_lock:
            df = _kline_cache.get(symbol)
            if df is None: return
            if len(df) > 0 and int(df.iloc[-1]["time"]) == new_row["time"]:
                df = df.iloc[:-1]
            df_base = df[base_cols] if all(c in df.columns for c in base_cols) else df
            df_base = pd.concat([df_base, pd.DataFrame([new_row])], ignore_index=True)
            if len(df_base) > 300: df_base = df_base.iloc[-300:].reset_index(drop=True)
            _kline_cache[symbol] = _compute_indicators(df_base)
    except Exception as e:
        _log_err(f"append_kline_{symbol}", e)

def ohlcv(symbol, interval, limit=100):
    with _kline_lock:
        df = _kline_cache.get(symbol)
    if df is not None: return df
    return _bootstrap_klines(symbol, interval, limit)

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]: k["active"], k["consec"] = False, 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"], k["day_reset"] = 0.0, day
    if k["daily"] <= DAILY_LOSS:
        k["active"], k["reason"], k["resume"] = True, f"daily({k['daily']:.2f})", day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"], k["reason"], k["resume"] = True, f"consec({k['consec']})", now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

def get_real_fill_price(sym, order_resp):
    try:
        cum_quote = float(order_resp.get('cumQuote', 0))
        exec_qty = float(order_resp.get('executedQty', 0))
        if exec_qty > 0 and cum_quote > 0:
            return cum_quote / exec_qty
        avg_px = float(order_resp.get('avgPrice', 0))
        if avg_px > 0:
            return avg_px
        order_id = order_resp.get('orderId')
        if order_id:
            for _ in range(2):
                time.sleep(0.5)
                info = client.futures_get_order(symbol=sym, orderId=order_id)
                c_quote = float(info.get('cumQuote', 0))
                e_qty = float(info.get('executedQty', 0))
                if e_qty > 0 and c_quote > 0:
                    return c_quote / e_qty
                a_px = float(info.get('avgPrice', 0))
                if a_px > 0:
                    return a_px
    except Exception:
        pass
    return 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  8. CORE EXECUTION & POSITION MONITORING
# ═══════════════════════════════════════════════════════════════════════════

def live_open_from_entry(entry: PendingEntry, confirm_type: str) -> bool:
    """Eksekusi order SETELAH lolos entry window. Menggantikan live_open lama yang
    dipanggil langsung dari scanner. Jika DRY_RUN=True, tidak mengirim order sungguhan
    (paper trading) tapi tetap membentuk posisi virtual agar exit logic tetap teruji."""
    sym = entry.symbol
    execution_side = entry.execution_side

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return False
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    price = px_now if px_now > 0 else entry.signal_price

    try:
        q_val = qty(sym, price)
    except Exception:
        with _lock: live_positions.pop(sym, None)
        return False

    risk_profile = entry.risk_profile
    tp_pct, sl_pct = risk_profile["tp_pct"], risk_profile["sl_pct"]
    tp_price, sl_price = risk_profile["tp_price"], risk_profile["sl_price"]

    pos = {
        "side": execution_side, "orig_signal": entry.orig_direction, "decision": entry.decision,
        "entry": price, "qty": q_val, "open_time": time.time(),
        "score": entry.signal_score, "failure_score": entry.failure_score, "sigs": entry.sigs,
        "atr": entry.atr, "regime": entry.regime, "bucket_key": entry.bucket_key,
        "tp_pct": tp_pct, "sl_pct": sl_pct, "tp_price": tp_price, "sl_price": sl_price,
        "peak_price": price, "decay_lives": TIME_DECAY_LIVES, "confirm_type": confirm_type,
    }
    with _lock: live_positions[sym] = pos

    if DRY_RUN:
        print(f"         🧪 DRY_RUN — order disimulasikan (tidak dikirim ke exchange)")
    else:
        try:
            client.futures_change_leverage(symbol=sym, leverage=LEVERAGE)
        except Exception:
            pass
        try:
            order = client.futures_create_order(
                symbol=sym, side='BUY' if execution_side == 'LONG' else 'SELL',
                type='MARKET', quantity=q_val, newOrderRespType='RESULT'
            )
            real_px = get_real_fill_price(sym, order)
            if real_px > 0:
                price = real_px
                new_risk = DynamicRiskManager.calculate_levels(price, execution_side, entry.atr)
                with _lock:
                    if sym in live_positions and not live_positions[sym].get('_r'):
                        live_positions[sym].update({
                            'entry': price, 'tp_pct': new_risk["tp_pct"], 'sl_pct': new_risk["sl_pct"],
                            'tp_price': new_risk["tp_price"], 'sl_price': new_risk["sl_price"], 'peak_price': price,
                        })
            print(f"         ✅ ORDER #{order.get('orderId')} | fill:{price:.6g} | qty:{q_val}")
        except Exception as e:
            print(f"  ❌ ORDER GAGAL {sym}: {e}")
            with _lock: live_positions.pop(sym, None)
            return False

    d = "🟢" if execution_side == "LONG" else "🔴"
    print(f"\n  {d} [v23 {entry.decision}] {sym} EXEC:{execution_side} (Signal:{entry.orig_direction}) @{price:.6g} | "
          f"TP:{tp_pct*100:.2f}% SL:{sl_pct*100:.2f}% | Regime:{entry.regime} | Confirm:{confirm_type}")
    print(f"         Signals: {' | '.join(entry.sigs[:6])}")
    _stats["trades"] += 1
    if any("Absorb" in s for s in entry.sigs):
        _stats["absorb_entries"] += 1
    return True

def live_close(sym, reason, price=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    if price is None:
        price = price_live(sym)

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]

    if not DRY_RUN:
        try:
            close_order = client.futures_create_order(
                symbol=sym, side='SELL' if side == 'LONG' else 'BUY',
                type='MARKET', quantity=q_val, reduceOnly=True, newOrderRespType='RESULT'
            )
            _api_ok()
            real_px = get_real_fill_price(sym, close_order)
            if real_px > 0:
                price = real_px
            elif price == 0:
                price = entry
            print(f"         ✅ CLOSE ORDER #{close_order.get('orderId')} | fill:{price:.6g}")
        except Exception as e:
            _log_err(f"close_order_{sym}", e, cooldown=5)
            print(f"  ⚠️ CLOSE ORDER GAGAL {sym}: {e}")
            with _lock: live_positions[sym] = pos
            return
    else:
        if price == 0:
            price = price_live(sym) or entry

    gross_pnl = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    fee_rate = 0.0005
    total_fee = (entry * q_val + price * q_val) * fee_rate
    pnl = gross_pnl - total_fee
    pct = (price - entry) / entry if side == "LONG" else (entry - price) / entry
    hold = time.time() - pos["open_time"]
    won = pnl >= 0
    e_icon = "🟢" if won else "🔴"

    peak_px = pos.get("peak_price", entry)
    peak_pct = (peak_px - entry) / entry if side == "LONG" else (entry - peak_px) / entry

    decision = pos.get("decision", "ORIGINAL")
    print(f"  {e_icon} [v23 {decision}] {sym} {side} CLOSE — {reason} | peak:{peak_pct*100:+.3f}% | fee:{total_fee:.5f}U")
    print(f"     {entry:.6g}→{price:.6g} ({pct*100:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    trade = TradeRecord(
        symbol=sym, direction=side, orig_direction=pos.get("orig_signal", side), decision=decision,
        entry_price=entry, exit_price=price, pnl=pnl, pnl_pct=pct, won=won,
        regime=pos.get("regime", "UNKNOWN"), signals=pos.get("sigs", []), score=pos.get("score", 0),
        failure_score=pos.get("failure_score", 0), atr_entry=pos.get("atr", 0), hold_seconds=hold,
        exit_reason=reason, peak_pct=peak_pct, bucket_key=pos.get("bucket_key", "UNKNOWN|UNKNOWN|0|0"),
    )
    learning.add_trade(trade)

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    if _stats["pnl"] > _stats["ath_pnl"]:
        _stats["ath_pnl"] = _stats["pnl"]

    ks_upd(pnl)

    if won:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if "SL" == reason: _stats["hard_sl"] += 1
    elif "TP" == reason: _stats["tp_exit"] += 1
    elif "TIME_DECAY" in reason: _stats["time_decay_exit"] += 1
    elif "EARLY_EXIT" in reason: _stats["early_exit"] += 1
    elif "TIME_LIMIT" in reason: _stats["time_limit_exit"] += 1

    trade_log.append({
        "sym": sym, "side": side, "decision": decision, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })

    with _lock: cooldown_list[sym] = time.time() + COOLDOWN_SEC
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue

        hold_time = time.time() - pos["open_time"]

        px = price_live(sym)
        if px == 0:
            pos["_fail_count"] = pos.get("_fail_count", 0) + 1
            fc = pos["_fail_count"]
            if fc in (5, 20, 60) or fc % 300 == 0:
                print(f"  ⚠️ {sym}: price_live gagal {fc}x — SL/TP monitoring tertunda")
            continue
        pos["_fail_count"] = 0

        side, tp_px, sl_px, tp_pct = pos["side"], pos["tp_price"], pos["sl_price"], pos["tp_pct"]

        if side == "LONG":
            if px > pos["peak_price"]: pos["peak_price"] = px
        else:
            if px < pos["peak_price"]: pos["peak_price"] = px

        # TP / SL — DIPERTAHANKAN, tanpa trailing
        if side == "LONG":
            if px >= tp_px: live_close(sym, "TP", tp_px); continue
            if px <= sl_px: live_close(sym, "SL", sl_px); continue
        else:
            if px <= tp_px: live_close(sym, "TP", tp_px); continue
            if px >= sl_px: live_close(sym, "SL", sl_px); continue

        # === NEW v23: TIME DECAY EXIT (checkpoint-based, bukan menahan sampai MAX_HOLD) ===
        cur_pct = (px - pos["entry"]) / pos["entry"] if side == "LONG" else (pos["entry"] - px) / pos["entry"]
        progress_ratio = cur_pct / tp_pct if tp_pct else 0.0
        cps_done_key = "_decay_checked"
        checked = pos.setdefault(cps_done_key, set())
        for sec_thresh, min_progress in TIME_DECAY_CHECKPOINTS:
            if sec_thresh in checked or hold_time < sec_thresh:
                continue
            checked.add(sec_thresh)
            if progress_ratio < min_progress:
                pos["decay_lives"] = pos.get("decay_lives", TIME_DECAY_LIVES) - 1
                print(f"  ⏳ {sym}: checkpoint {sec_thresh}s gagal (progress {progress_ratio*100:.0f}% < {min_progress*100:.0f}%) — decay_lives={pos['decay_lives']}")
                if pos["decay_lives"] <= 0:
                    live_close(sym, "TIME_DECAY", px)
                    break
        else:
            pass
        if sym not in live_positions:
            continue

        # === NEW v23: EARLY EXIT jika signal invalidated (bukan trailing stop) ===
        if EARLY_EXIT_ENABLED and hold_time >= EARLY_EXIT_MIN_HOLD_SEC:
            with _kline_lock:
                df = _kline_cache.get(sym)
            if df is not None and len(df) >= 3 and "delta_ratio" in df.columns:
                delta_ratio = df["delta_ratio"].iloc[-2]
                if side == "LONG" and delta_ratio <= EARLY_EXIT_DELTA_THRESHOLD:
                    live_close(sym, "EARLY_EXIT_INVALIDATED", px); continue
                elif side == "SHORT" and delta_ratio >= -EARLY_EXIT_DELTA_THRESHOLD:
                    live_close(sym, "EARLY_EXIT_INVALIDATED", px); continue

        # Hard cutoff terakhir (safety net) — jauh lebih pendek dari versi lama
        if hold_time > MAX_HOLD_SECONDS:
            live_close(sym, "TIME_LIMIT", px)

# ═══════════════════════════════════════════════════════════════════════════
#  9. SCANNER THREAD & HARD VETO FILTERS + FAILURE/DECISION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def scan_one(sym):
    """Sekarang HANYA menghasilkan kandidat mentah (tidak eksekusi apa pun)."""
    try:
        time.sleep(0.002)
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None: return None
        df_ta = run_ta(df.copy())
        px_candle, atr_val = df_ta["close"].iloc[-2], df_ta["atr"].iloc[-2]
        if px_candle == 0 or np.isnan(atr_val): return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df_ta, sym)
        if orig_direction is None: return None

        px_live = price_live(sym)
        if px_live == 0: return None

        # Veto filters lama tetap berlaku SEBELUM masuk ke failure engine (hard veto, bukan scoring)
        btc_vetoed, btc_reason = btc_macro.check_veto(orig_direction)
        if btc_vetoed:
            _stats["btc_breaker_veto"] += 1
            return None

        has_wall, wall_type, wall_px, wall_qty, wall_mult = order_book.check_walls(sym, px_live, orig_direction)
        if has_wall:
            _stats["wall_veto"] += 1
            return None

        is_spoof, spoof_reason = order_book.detect_spoofing(sym, orig_direction)
        if is_spoof:
            _stats["spoof_veto"] += 1
            return None

        # === NEW v23: Failure / Adversarial assessment ===
        assessment = FailureScorer.assess(df_ta, sym, orig_direction, score, regime, atr_val)
        decision, exec_candidate, edge_score, reason = DecisionEngine.decide(orig_direction, score, assessment)

        risk_profile = DynamicRiskManager.calculate_levels(px_live, exec_candidate, atr_val)

        return {
            "sym": sym, "orig_direction": orig_direction, "score": score, "sigs": sigs,
            "px_live": px_live, "atr": atr_val, "regime": regime, "bias": bias,
            "assessment": assessment, "decision": decision, "exec_candidate": exec_candidate,
            "edge_score": edge_score, "reason": reason, "risk_profile": risk_profile,
        }
    except Exception as e:
        _log_err(f"scan_one_{sym}", e)
        return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    for f in as_completed(fut, timeout=5):
        try:
            r = f.result(timeout=1)
            if r: res.append(r)
        except Exception:
            pass
    return res

def process_candidate(cand: dict):
    """=== NEW v23 ===: Mengganti pemanggilan live_open langsung. Kandidat NO_TRADE dicatat
    sebagai shadow. Kandidat ORIGINAL/REVERSE masuk ke ENTRY WINDOW (belum langsung eksekusi)."""
    sym = cand["sym"]
    assessment: FailureAssessment = cand["assessment"]
    decision = cand["decision"]

    entry = PendingEntry(
        symbol=sym, decision=decision if decision != "NO_TRADE" else "ORIGINAL",
        orig_direction=cand["orig_direction"], execution_side=cand["exec_candidate"],
        signal_score=cand["score"], failure_score=assessment.failure_score,
        reverse_confidence=assessment.reverse_confidence, regime=cand["regime"],
        sigs=cand["sigs"], reasons=assessment.reasons + assessment.reverse_reasons,
        atr=cand["atr"], signal_price=cand["px_live"], risk_profile=cand["risk_profile"],
        bucket_key=assessment.bucket_key, reason=cand["reason"],
    )

    if decision == "NO_TRADE":
        _stats["no_trade"] += 1
        log_decision_line(sym, entry, decision_override="NO_TRADE", extra=f"REASON={cand['reason']}")
        shadow_tracker.register(entry, "NONE", executed=False)
        return

    if len(live_positions) >= MAX_POSITIONS or sym in live_positions or sym in entry_window.pending:
        # slot penuh / sudah ada posisi / sudah menunggu window -> tetap catat sebagai shadow
        _stats["no_trade"] += 1
        log_decision_line(sym, entry, decision_override=decision, extra="NO_SLOT (shadow only)")
        shadow_tracker.register(entry, "NO_SLOT", executed=False)
        return

    entry.decision = decision
    added = entry_window.add(entry)
    if not added:
        shadow_tracker.register(entry, "NO_SLOT", executed=False)

def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    aw = learning.avg_win()
    e = "💚" if pnl >= 0 else "🔴"
    orig_wr, orig_n = learning.decision_wr("ORIGINAL")
    rev_wr, rev_n = learning.decision_wr("REVERSE")
    print(f"       ┌ [v23 {STRATEGY_MODE}] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U)")
    print(f"       └ TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} Decay:{_stats['time_decay_exit']} Early:{_stats['early_exit']} TimeLimit:{_stats['time_limit_exit']} | ORIG_WR:{orig_wr*100:.0f}%(n={orig_n}) REV_WR:{rev_wr*100:.0f}%(n={rev_n}) | AvgWin:{aw:+.4f}U")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if pnl >= 0 else "🔴"
    aw, al = learning.avg_win(), learning.avg_loss()
    bep = al / (al + aw) * 100 if (al + aw) > 0 else 50
    pf = learning.profit_factor()
    expct = learning.expectancy()
    avg_hold = learning.avg_hold()
    orig_wr, orig_n = learning.decision_wr("ORIGINAL")
    rev_wr, rev_n = learning.decision_wr("REVERSE")
    shadow_n = shadow_tracker.stats["n"]
    shadow_wr = (shadow_tracker.stats["wins"] / shadow_n) if shadow_n else 0.0

    print(f"\n  {'─'*76}")
    print(f"    🔔 ADVERSARIAL SCALPING v23 LIVE DASHBOARD | MODE={STRATEGY_MODE} | DRY_RUN={DRY_RUN}")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} ({tph:.1f}T/hr) | NoTrade:{_stats['no_trade']}")
    print(f"    {e} PnL Net:{pnl:+.5f}U | ATH:{_stats['ath_pnl']:+.5f}U | Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"    📊 ProfitFactor:{pf:.2f} | Expectancy:{expct:+.5f}U/trade | AvgWin:{aw:+.4f} AvgLoss:{al:+.4f} | BEP_WR:{bep:.1f}%")
    print(f"    ⏱️ AvgHold:{avg_hold:.0f}s | Exit[TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} Decay:{_stats['time_decay_exit']} Early:{_stats['early_exit']} TimeLimit:{_stats['time_limit_exit']}]")
    print(f"    🔀 ORIGINAL_WR:{orig_wr*100:.1f}%(n={orig_n}) | REVERSED_WR:{rev_wr*100:.1f}%(n={rev_n}) | SHADOW_WR:{shadow_wr*100:.1f}%(n={shadow_n})")
    print(f"    🛡️ Veto: Wall:{_stats['wall_veto']} BTC:{_stats['btc_breaker_veto']} Spoof:{_stats['spoof_veto']} | Absorb entries:{_stats['absorb_entries']}")
    print(f"    📚 Bucket store: {len(bucket_store.buckets)} regime-buckets | {len(bucket_store.symbol_buckets)} symbol-buckets")

    if trade_log:
        print(f"    {'─'*66}\n    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"       {em} {t['sym']:<16} {t['side']} [{t['decision']}] {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*76}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except Exception as e:
            _log_err("t_monitor", e)
        time.sleep(MONITOR_INT)

def t_entry_window():
    # === NEW v23 ===
    while True:
        try:
            entry_window.tick()
        except Exception as e:
            _log_err("t_entry_window", e)
        time.sleep(0.5)

def t_shadow_tracker():
    # === NEW v23 ===
    while True:
        try:
            shadow_tracker.tick()
            shadow_tracker.prune()
        except Exception as e:
            _log_err("t_shadow_tracker", e)
        time.sleep(1.0)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions) - len(entry_window.pending)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT); continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and s not in entry_window.pending
                              and (s not in cooldown_list or now > cooldown_list[s])]

            hot = [s for s in _hot_syms if s in valid_syms]
            mv = top_movers(valid_syms, 30)
            bs = scan_idx * BATCH_SIZE
            reg = [s for s in valid_syms[bs:bs+BATCH_SIZE] if s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            scan_list = list(dict.fromkeys(hot[:5] + mv[:20] + reg[:15]))[:BATCH_SIZE]

            if not scan_list:
                time.sleep(SLOT_FILL_INT); continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x["edge_score"], reverse=True)
                for r in res:
                    process_candidate(r)
        except Exception as e:
            _log_err("t_slot_filler", e)
        time.sleep(SLOT_FILL_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            slots = MAX_POSITIONS - len(live_positions) - len(entry_window.pending)
            if slots <= 0 or ks_check()[0]: continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and s not in entry_window.pending
                              and (s not in cooldown_list or now > cooldown_list[s])]

            hot = [s for s in _hot_syms if s in valid_syms]
            rest = [s for s in valid_syms if s not in hot]
            res = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x["edge_score"], reverse=True)
                for r in res:
                    process_candidate(r)
        except Exception:
            pass

def t_macro():
    while True:
        try:
            df_btc = ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80)
            if df_btc is not None and len(df_btc) >= 55:
                regime, strength, bias = MarketRegime.detect(df_btc)
                _macro["btc"] = regime
                _btc_macro["regime"] = regime
                row = df_btc.iloc[-2]
                _btc_macro["m5"] = row.get("m5", 0.0)
                _btc_macro["delta_ratio"] = row.get("delta_ratio", 0.0)
                _btc_macro["cvd"] = row.get("cvd", 0.0)
        except Exception as e:
            _log_err("t_macro", e)
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  10. WEBSOCKET HANDLERS & WATCHDOG — DIPERTAHANKAN
# ═══════════════════════════════════════════════════════════════════════════

def handle_all_ticker(msg):
    global _ws_last_msg_ts, _ws_ticker_cache, _ws_ticker_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        arr = data if isinstance(data, list) else [data]
        cache = {}
        for d in arr:
            if not isinstance(d, dict): continue
            sym = d.get("s")
            if not sym: continue
            try:
                cache[sym] = {"pct": float(d.get("P", 0)), "vol": float(d.get("q", 0)), "last": float(d.get("c", 0))}
            except (TypeError, ValueError):
                continue
        if cache:
            _ws_ticker_cache = cache
            _ws_ticker_ts = time.time()
    except Exception as e:
        _log_err("handle_all_ticker", e)

def handle_mark_price(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        arr = msg if isinstance(msg, list) else [msg]
        now = time.time()
        for d in arr:
            sym, px = d.get("s"), d.get("p")
            if sym and px:
                pf = float(px)
                if pf > 0: _ws_mark_price[sym] = (pf, now)
    except Exception as e:
        _log_err("handle_mark_price", e)

def handle_kline_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        k = data.get("k")
        if not k: return
        sym = data.get("s") or k.get("s")
        if sym and k.get("x"):
            _append_kline_from_ws(sym, k)
    except Exception as e:
        _log_err("handle_kline_multiplex", e)

def handle_btc_aggtrade(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict): return
        p = data.get("p"); t = data.get("T")
        if p is not None:
            ts = float(t) / 1000.0 if t else time.time()
            btc_macro.update_tick(float(p), ts)
    except Exception as e:
        _log_err("handle_btc_aggtrade", e)

def handle_depth_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict): return
        sym = data.get("s")
        if not sym:
            stream = msg.get("stream", "")
            if "@depth" in stream:
                sym = stream.split("@")[0].upper()
        if sym:
            bids = data.get("b", []); asks = data.get("a", [])
            order_book.update(sym, bids, asks)
    except Exception as e:
        _log_err("handle_depth_multiplex", e)

def handle_user_data(msg):
    try:
        etype = msg.get("e")
        if etype == "ORDER_TRADE_UPDATE":
            o = msg.get("o", {})
            if o.get("X") in ("FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"):
                print(f"  📡 [WS ORDER] {o.get('s')} {o.get('S')} {o.get('X')} qty={o.get('z')} avgPx={o.get('ap')}")
    except Exception as e:
        _log_err("handle_user_data", e)

def bootstrap_all_klines(syms):
    print(f"  📥 Bootstrap history awal ({len(syms)} simbol) via REST...")
    futs = {_executor.submit(_bootstrap_klines, s, Client.KLINE_INTERVAL_5MINUTE, 100): s for s in syms}
    ok = 0
    for f in as_completed(futs, timeout=90):
        try:
            if f.result(timeout=15) is not None: ok += 1
        except Exception: pass
    print(f"  ✅ Bootstrap selesai: {ok}/{len(syms)} simbol siap dipantau")

def t_ws_watchdog():
    while True:
        idle = time.time() - _ws_last_msg_ts
        if idle > WS_STALE_SEC:
            print(f"  🚨 WEBSOCKET DIAM {idle:.0f}s — fallback REST aktif.")
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  11. BOT LAUNCHER & MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def run_bot():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  💎 BOT SCALPING v23.0 — ADVERSARIAL FAILURE MINING                ║")
    print(f"║  MODE={STRATEGY_MODE:<12} DRY_RUN={str(DRY_RUN):<6}                              ║")
    print("║  Signal != Entry. Reverse hanya setelah kegagalan terkonfirmasi.   ║")
    print("║  Shadow trades mengamati SEMUA kandidat, dieksekusi atau tidak.    ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    try: valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
    except Exception: valid = set(SYMBOLS)
    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))

    bootstrap_all_klines(syms)

    twm.start()
    twm.start_all_mark_price_socket(callback=handle_mark_price, fast=True)
    twm.start_futures_multiplex_socket(callback=handle_all_ticker, streams=["!ticker@arr"])

    kline_streams = [f"{s.lower()}@kline_5m" for s in syms]
    twm.start_futures_multiplex_socket(callback=handle_kline_multiplex, streams=kline_streams)

    twm.start_futures_multiplex_socket(callback=handle_btc_aggtrade, streams=["btcusdt@aggtrade"])

    depth_streams = [f"{s.lower()}@depth10" for s in syms]
    twm.start_futures_multiplex_socket(callback=handle_depth_multiplex, streams=depth_streams)

    try:
        twm.start_futures_user_socket(callback=handle_user_data)
    except Exception as e:
        _log_err("user_data_stream_start", e, cooldown=0)

    threading.Thread(target=t_ws_watchdog, daemon=True).start()
    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_entry_window, daemon=True).start()   # === NEW v23 ===
    threading.Thread(target=t_shadow_tracker, daemon=True).start() # === NEW v23 ===
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    time.sleep(2)
    tickers_all()

    cycle = 0
    try:
        while True:
            cycle += 1
            slots = MAX_POSITIONS - len(live_positions)
            print(f"\n{'═'*68}")
            api_flag = f" | ⚠️API_FAIL:{_api_fail_streak}" if _api_fail_streak >= 20 else ""
            ws_idle = time.time() - _ws_last_msg_ts
            ws_flag = f" | ⚠️WS_IDLE:{ws_idle:.0f}s" if ws_idle > WS_STALE_SEC else ""

            btc_status = btc_macro.get_status_str()
            veto_summary = f"Veto[Wall:{_stats['wall_veto']}|BTC:{_stats['btc_breaker_veto']}|Spoof:{_stats['spoof_veto']}]"

            print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC_5M:{_macro['btc']} ({len(live_positions)}/{MAX_POSITIONS}, pending:{len(entry_window.pending)}) "
                  f"PnL:{_stats['pnl']:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U) | {veto_summary}{api_flag}{ws_flag}")
            print(f"        ↳ {btc_status}")

            if (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}")
            elif slots == 0: print(f"  ✅ Slots full — monitoring posisi terbuka (TP/SL/Decay/EarlyExit)")
            else: print(f"  🔍 {slots} slot kosong — scanning & evaluating failure/edge...")
            if cycle % 30 == 0: print_full()
            time.sleep(SCAN_INTERVAL)
    finally:
        bucket_store.save()

if __name__ == "__main__":
    run_bot()
