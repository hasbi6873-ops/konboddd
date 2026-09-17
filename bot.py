"""
Bot Scalping v23.0 PRO — QUANTITATIVE MOMENTUM & RECONCILIATION ENGINE (Binance Futures)
========================================================================================
PILIHAN LOGIKA UTAMA:
- Mode Eksekusi: DIRECT (Trend Continuation / Flow Alignment) — Default
  * LONG saat momentum bullish kuat (EMA stack + Buyer Delta + MACD up)
  * SHORT saat momentum bearish kuat (EMA bear stack + Seller Delta + MACD down)
  * Mencegah stop-out massal akibat fading/counter-trend di altcoins.
- Anti-Overtrading & Fee Bleed Guard:
  * MIN_SCORE dinaikkan ke 72 (Hanya setup A-Grade berkonfluensi tinggi)
  * Filter kekuatan tren ADX >= 20 (Anti-sideways / dead chop)
  * Cooldown 300s (5 menit) normal, dan 600s (10 menit) penalty cooldown untuk koin yang baru terkena SL (Anti-Revenge)
  * SLOT_FILL_INT 1.5s (Pacing tenang, hemat CPU, anti order-spamming)
- Dynamic Volatility ATR Risk Management:
  * TP: 1.8x ATR (Risk-Reward > 1.6 : 1)
  * SL: 1.1x ATR (Terkendali & terukur)
  * BEP Lock: Mengunci profit di Entry + 0.20% saat floating >= 0.8x ATR (Dijamin Net Win setelah fee 0.10%)
  * Trailing Stop: Aktif saat profit >= 1.2x ATR, jarak trailing 0.6x ATR dari harga puncak
  * Perbaikan Bug Prioritas: TRAIL_SL kini diprioritaskan sebelum BEP_SL
- Full Institutional Order Flow & Veto Engine:
  * Order Book Depth Wall Detection & Spoofing Guard
  * BTC Macro Circuit Breaker (Flash Crash / Flash Pump protection)
  * Reconciler Real-time Sinkronisasi Akun Binance (Anti-Ghost)
  * Adaptive Learning Weights yang terpetakan secara presisi
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import os
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
# Default False agar order riil dikirim ke Binance Demo/Testnet sesuai akun user.
PAPER_TRADE   = os.getenv("PAPER_TRADE", "false").lower() in ("true", "1")

# ── LOGIKA TRADING: DIRECT (TREND CONTINUATION) ─────────────────────────────
# False (DEFAULT & RECOMMENDED): Ikuti arah momentum (LONG saat Bull, SHORT saat Bear)
# True: Membalik sinyal (Fading). Berdasarkan pengujian 12 jam, Fading terbukti loss 72%.
INVERT_SIGNALS = os.getenv("INVERT_SIGNALS", "false").lower() in ("true", "1")

# Scanning & Concurrency (Tempo Tenang & Presisi IQ)
SCAN_INTERVAL       = 2.0
MONITOR_INT         = 0.1
BATCH_SIZE          = 15
MAX_WORKERS         = 5
SLOT_FILL_INT       = 1.5   # 1.5 Detik jeda pengecekan slot (Mencegah over-spamming & CPU spikes)
COOLDOWN_SEC        = 300   # 5 Menit cooldown standar per simbol setelah trade selesai
SL_PENALTY_COOLDOWN = 600   # 10 Menit penalty cooldown jika terkena SL (Mencegah revenge trading)

# Scoring & Filter Kuantitatif (Konfluensi Ketat untuk Sinyal A-Grade)
MIN_SCORE      = 72     # Dinaikkan ke 72 agar menyaring noise sideways & menghentikan overtrading
MIN_ADX        = 20     # Wajib tren aktif (Menghindari market mati tanpa volume)
SLIPPAGE_GUARD = 0.0015
TTL_5M         = 2

# ── Dynamic Volatility Risk Management (ATR Multipliers) ───────────────────
ATR_TP_MULTIPLIER       = 1.8   # TP pada 1.8x ATR (Risk-Reward > 1.6 : 1)
ATR_SL_MULTIPLIER       = 1.1   # SL pada 1.1x ATR (Terkendali & simetris)
ATR_BEP_TRIGGER         = 0.8   # BEP aktif saat profit mencapai 0.8x ATR
BEP_LOCK_PCT            = 0.0020# Kunci di Entry ± 0.20% (Menjamin Net Profit setelah fee taker 0.10%)
ATR_TRAILING_TRIGGER    = 1.2   # Trailing aktif saat profit mencapai 1.2x ATR
ATR_TRAILING_DIST       = 0.6   # Jarak trailing stop 0.6x ATR dari peak price

MIN_TP_PCT        = 0.010  # Minimal 1.0% (Menjamin target profit jauh di atas fee)
MAX_TP_PCT        = 0.045  # Maksimal 4.5%
MIN_SL_PCT        = 0.006  # Minimal 0.6%
MAX_SL_PCT        = 0.022  # Maksimal 2.2%

# Waktu Tahan Posisi: Scalping cepat & disiplin
INVALIDATION_SECONDS = 600   # 10 Menit: Jika momentum mati dan PnL <= 0, cut loss dini
MAX_HOLD_SECONDS     = 1200  # 20 Menit batas maksimal mutlak posisi scalping
# ──────────────────────────────────────────────────────────────────────────

# ── Institutional Microstructure (Order Book Depth) ───────────────────────
WALL_RATIO_THRESHOLD  = 2.5   # Volume level antrean >= 2.5x rata-rata 10 level
WALL_DEPTH_PCT        = 0.35  # Atau >= 35% dari total volume sisi tersebut
WALL_PROXIMITY_PCT    = 0.005 # Dalam radius 0.5% dari harga pasar saat ini
IMBALANCE_STRONG_BULL = 0.25  # BAI > +0.25
IMBALANCE_STRONG_BEAR = -0.25 # BAI < -0.25
SPOOF_DROP_THRESHOLD  = 0.40  # Penurunan likuiditas mendadak > 40% dalam < 2.5 detik
# ──────────────────────────────────────────────────────────────────────────

# ── Macro BTC Correlation & Flash Crash Engine ────────────────────────────
BTC_CRASH_THRESHOLD  = -0.003 # -0.3% dalam sliding window detik
BTC_PUMP_THRESHOLD   = 0.003  # +0.3% dalam sliding window detik
BTC_WINDOW_SEC       = 8.0    # Window lookback perbandingan harga BTC
BTC_BREAKER_COOLDOWN = 120.0  # 2 Menit circuit breaker blokir sinyal Altcoin berlawanan
# ──────────────────────────────────────────────────────────────────────────

# Kill Switch (Disetel fresh & toleransi terukur)
DAILY_LOSS   = float(os.getenv("DAILY_LOSS", "-25.0"))
CONSEC_MAX   = 8
CONSEC_PAUSE = 180

# Learning
LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 10

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
#  1. ORDER BOOK ENGINE (MICRO-STRUCTURE & DEPTH)
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
                    "bids": bids,
                    "asks": asks,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "imbalance": imbalance,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "ts": ts
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
        """
        Deteksi apakah ada Limit Wall tebal yang menghalangi pergerakan harga.
        Returns: (has_wall, wall_type, wall_price, wall_qty, wall_multiplier)
        """
        book = self.get_book(symbol)
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]
            tot_ask = book["ask_vol"]
            if not asks or tot_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)

            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]
            tot_bid = book["bid_vol"]
            if not bids or tot_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = tot_bid / len(bids)

            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_bid or qty >= WALL_DEPTH_PCT * tot_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult

        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol: str, side: str) -> Tuple[bool, str]:
        """
        Deteksi apakah ada penarikan likuiditas mendadak (spoofing trap).
        """
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
#  2. MACRO BTC & TICK CORRELATION ENGINE
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
                    self.breaker = {
                        "active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚨 [BTC FLASH CRASH DETECTED] Drop: {delta*100:+.2f}% in {ts - cutoff:.1f}s | Price: {price:.1f} | Altcoin LONGs LOCKED for {BTC_BREAKER_COOLDOWN:.0f}s!")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {
                        "active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚀 [BTC FLASH PUMP DETECTED] Surge: {delta*100:+.2f}% in {ts - cutoff:.1f}s | Price: {price:.1f} | Altcoin SHORTs LOCKED for {BTC_BREAKER_COOLDOWN:.0f}s!")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]
                    delta = self.breaker["delta"]
                    if b_type == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash active ({rem:.0f}s left, drop {delta*100:+.2f}%)"
                    elif b_type == "PUMP" and side == "SHORT":
                        return True, f"BTC Flash Pump active ({rem:.0f}s left, surge {delta*100:+.2f}%)"
                else:
                    self.breaker["active"] = False
                    self.breaker["type"] = "NONE"
        return False, "OK"

    def get_status_str(self) -> str:
        with self.lock:
            px = self.last_price
            active = self.breaker["active"] and time.time() < self.breaker["until"]
            if active:
                rem = self.breaker["until"] - time.time()
                return f"BTC: ${px:.1f} | 🚨BREAKER ACTIVE [{self.breaker['type']} {self.breaker['delta']*100:+.2f}% ({rem:.0f}s left)]"
            return f"BTC: ${px:.1f} [NORMAL]"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
#  3. ABSORPTION & ORDER FLOW ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[bool, bool, str]:
        """
        Deteksi Institutional Absorption:
        - Bullish: Seller volume meledak tapi harga tertahan di support/membuat lower wick panjang.
        - Bearish: Buyer volume meledak tapi harga tertahan di resistance/membuat upper wick panjang.
        """
        if df is None or len(df) < 25: return False, False, ""
        row = df.iloc[-2]
        
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0)
        low = row.get("low", 0.0)
        high = row.get("high", 0.0)
        close = row.get("close", 0.0)
        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        lw_ratio = row.get("lower_wick_ratio", 0.0)
        uw_ratio = row.get("upper_wick_ratio", 0.0)

        # Bullish Absorption
        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        wick_bull = lw_ratio >= 0.38
        close_held_bull = close >= (low + 0.45 * rng)
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)

        # Bearish Absorption
        heavy_buyer = (delta_ratio > 0.20) or (buy_ratio > 0.60)
        wick_bear = uw_ratio >= 0.38
        close_held_bear = close <= (high - 0.45 * rng)
        bear_absorb = vol_spike and heavy_buyer and (wick_bear or close_held_bear)

        details = []
        if bull_absorb:
            details.append(f"BullAbsorb(Vol:{row.get('vr', 1.0):.1f}x|Δ:{delta_ratio:+.2f}|Wick:{lw_ratio:.0%})")
        if bear_absorb:
            details.append(f"BearAbsorb(Vol:{row.get('vr', 1.0):.1f}x|Δ:{delta_ratio:+.2f}|Wick:{uw_ratio:.0%})")

        return bull_absorb, bear_absorb, " ".join(details)

# ═══════════════════════════════════════════════════════════════════════════
#  4. VOLATILITY-ADJUSTED RISK MANAGEMENT (PROFIT ENGINE + BEP + TRAILING)
# ═══════════════════════════════════════════════════════════════════════════

class DynamicRiskManager:
    @staticmethod
    def calculate_levels(entry_price: float, execution_side: str, atr: float) -> Dict[str, float]:
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.015

        # Scalping Presisi: TP = 1.8x ATR, SL = 1.1x ATR (Risk-Reward > 1.6 : 1)
        tp_pct = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_MULTIPLIER * atr_pct))
        sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULTIPLIER * atr_pct))
        bep_pct = ATR_BEP_TRIGGER * atr_pct
        trail_pct = ATR_TRAILING_TRIGGER * atr_pct
        trail_dist_pct = ATR_TRAILING_DIST * atr_pct

        if execution_side == "LONG":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else: # SHORT
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "bep_pct": bep_pct,
            "trail_pct": trail_pct,
            "trail_dist_pct": trail_dist_pct,
            "atr_pct": atr_pct
        }

# ═══════════════════════════════════════════════════════════════════════════
#  MARKET REGIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════

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
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        atr, atr_prev = row["atr"], prev["atr"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull  = close > e9 > e21
        mild_bear  = close < e9 < e21
        strong_trend      = adx > 25
        very_strong_trend = adx > 35
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
#  SCORING & ADAPTIVE SIGNAL WEIGHTS (MAPPING DIPERBAIKI SECARA PRESISI)
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
        mapping = {
            "EMA5↑": "ema_bull_stack", "EMA4↑": "ema_mild_bull", "EMA3↑": "ema_weak_bull",
            "EMA5↓": "ema_bear_stack", "EMA4↓": "ema_mild_bear", "EMA3↓": "ema_weak_bear",
            "MACD_X↑": "macd_cross_up", "MACD↑↑": "macd_strengthen",
            "MACD_X↓": "macd_cross_down", "MACD↓↓": "macd_strengthen_neg",
            "BullAbsorb": "absorption_bull", "BearAbsorb": "absorption_bear",
        }
        for sig in signals:
            base = sig.split('[')[0].strip()
            target_key = mapping.get(base)
            if not target_key:
                if "Mom+" in base: target_key = "mom_strong" if "↑" in base else "mom_moderate"
                elif "Mom-" in base: target_key = "mom_strong_neg" if "↓" in base else "mom_moderate_neg"
                elif "ΔBuy" in base: target_key = "orderflow_delta_bull"
                elif "ΔSell" in base: target_key = "orderflow_delta_bear"
                elif "TakerBuy" in base: target_key = "orderflow_buy_high"
                elif "TakerSell" in base: target_key = "orderflow_sell_high"
                elif "BAI+" in base: target_key = "orderbook_imbalance_bull"
                elif "BAI-" in base: target_key = "orderbook_imbalance_bear"
                elif "RSI" in base and "OB" not in base and "OS" not in base: target_key = "rsi_bull_flow"
            
            if target_key and target_key in self.weights:
                self.history[target_key].append(1 if won else 0)
                if len(self.history[target_key]) > LEARNING_WINDOW:
                    self.history[target_key] = self.history[target_key][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        hist = self.history.get(signal_name, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT: return self.weights.get(signal_name, 10)
        return self.weights.get(signal_name, 10) * max(0.6, min(1.4, 0.5 + sum(hist) / len(hist)))

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
        adx = df["adx"].iloc[-2]

        bull_absorb, bear_absorb, _ = AbsorptionDetector.detect(df)

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        # Filter Ketat: Pastikan ada kekuatan tren (ADX >= MIN_ADX) agar tidak terjebak sideways
        if adx < MIN_ADX and not (bull_absorb or bear_absorb):
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime == MarketRegime.REGIME_VOLATILE:
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        return None, 0, [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
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

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio > 0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bull"); score += w; signals.append(f"ΔBuy+{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio > 0.55: w = self.weights.get_adjusted_weight("orderflow_buy_high"); score += w; signals.append(f"TakerBuy{buy_ratio*100:.0f}%[{w:.0f}]")

        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs:
            w = self.weights.get_adjusted_weight("absorption_bull"); score += w; signals.append(f"BullAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bull"); score += w; signals.append(f"BAI+{imb*100:.0f}%[{w:.0f}]")

        if 48 <= row["rsi"] <= 68: w = self.weights.get_adjusted_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] > 68: w = self.weights.get_adjusted_weight("rsi_extreme_ob"); score += w; signals.append(f"RSI{row['rsi']:.0f}OB[{w:.0f}]")

        return score, signals

    def _score_short(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
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

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio < -0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bear"); score += w; signals.append(f"ΔSell{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio < 0.45: w = self.weights.get_adjusted_weight("orderflow_sell_high"); score += w; signals.append(f"TakerSell{(1-buy_ratio)*100:.0f}%[{w:.0f}]")

        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs:
            w = self.weights.get_adjusted_weight("absorption_bear"); score += w; signals.append(f"BearAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bear"); score += w; signals.append(f"BAI{imb*100:.0f}%[{w:.0f}]")

        if 32 <= row["rsi"] <= 52: w = self.weights.get_adjusted_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] < 32: w = self.weights.get_adjusted_weight("rsi_extreme_os"); score += w; signals.append(f"RSI{row['rsi']:.0f}OS[{w:.0f}]")

        return score, signals

# ═══════════════════════════════════════════════════════════════════════════
#  LEARNING LAYER & TRADE HISTORY
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:       str
    direction:    str
    entry_price:  float
    exit_price:   float
    pnl:          float
    won:          bool
    regime:       str
    signals:      List[str]
    score:        int
    atr_entry:    float
    hold_seconds: float
    exit_reason:  str
    peak_pct:     float
    timestamp:    float = field(default_factory=time.time)

class LearningLayer:
    def __init__(self, signal_weights: SignalWeights):
        self.signal_weights  = signal_weights
        self.trades          = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self.stats_by_symbol = defaultdict(lambda: {"wins": 0, "losses": 0})

    def add_trade(self, trade: TradeRecord):
        self.trades.append(trade)
        r = trade.regime
        self.stats_by_regime[r]["wins"]   += 1 if trade.won else 0
        self.stats_by_regime[r]["losses"] += 0 if trade.won else 1
        self.stats_by_regime[r]["pnl"]    += trade.pnl
        if trade.won:
            self.stats_by_regime[r].setdefault("peak_sum", 0.0)
            self.stats_by_regime[r]["peak_sum"] += trade.peak_pct
        self.stats_by_symbol[trade.symbol]["wins"]   += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        self.signal_weights.record_outcome(trade.signals, trade.won)
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
    def avg_peak_win(self) -> float:
        peaks = [t.peak_pct for t in self.trades if t.won]
        return sum(peaks) / len(peaks) if peaks else 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ticker_cache    = {}
_ticker_ts       = 0
_lock            = threading.Lock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q        = queue.Queue()
_hot_syms        = deque(maxlen=30)

_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()
_ws_ticker_cache = {}
_ws_ticker_ts    = 0
_ws_last_msg_ts  = time.time()
WS_STALE_SEC     = 30
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks = {"daily": 0.0, "consec": 0, "active": False, "reason": "", "resume": 0.0, "day_reset": 0.0}

_stats = {
    "wins": 0, "losses": 0, "pnl": 0.0, "ath_pnl": 0.0, "start": time.time(),
    "best": 0.0, "worst": 0.0, "hist": deque(maxlen=500),
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "regime_block": 0,
    "absorb_entries": 0, "tp_exit": 0, "bep_exit": 0, "trail_exit": 0, "hard_sl": 0, "time_exit": 0
}

signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)
learning       = LearningLayer(signal_weights)
live_positions = {}
cooldown_list  = {}
trade_log      = []

_last_err_print   = defaultdict(float)
_api_fail_streak  = 0
_api_ok_last      = time.time()
_symbol_rules     = {}

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

def get_symbol_rules(symbol: str) -> Dict[str, Any]:
    """Mengambil dan meng-cache aturan presisi, LOT_SIZE (stepSize, minQty), dan MIN_NOTIONAL dari Binance."""
    if symbol in _symbol_rules: return _symbol_rules[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info.get('symbols', []):
            sym = s.get('symbol')
            prec = int(s.get('quantityPrecision', 2))
            step_size = 10 ** (-prec)
            min_qty = step_size
            min_notional = 5.0
            for f in s.get('filters', []):
                if f.get('filterType') == 'LOT_SIZE':
                    step_size = float(f.get('stepSize', step_size))
                    min_qty = float(f.get('minQty', min_qty))
                elif f.get('filterType') in ('MIN_NOTIONAL', 'NOTIONAL'):
                    min_notional = float(f.get('notional', f.get('minNotional', 5.0)))
            _symbol_rules[sym] = {
                "precision": prec,
                "step_size": step_size,
                "min_qty": min_qty,
                "min_notional": min_notional
            }
        if symbol in _symbol_rules: return _symbol_rules[symbol]
    except Exception as e:
        _log_err("get_symbol_rules", e)
    return {"precision": 2, "step_size": 0.01, "min_qty": 0.01, "min_notional": 5.0}

def format_qty(symbol: str, raw_qty: float) -> float:
    """Format quantity agar sesuai eksak dengan stepSize Binance (mencegah LOT_SIZE/Precision error)."""
    rules = get_symbol_rules(symbol)
    step = rules["step_size"]
    prec = rules["precision"]
    if step > 0:
        rounded = math.floor(raw_qty / step) * step
    else:
        rounded = round(raw_qty, prec)
    if prec == 0:
        return float(int(rounded))
    return float(f"{rounded:.{prec}f}")

def qty(symbol: str, price: float) -> float:
    if price <= 0: return 0.0
    rules = get_symbol_rules(symbol)
    target_notional = max(ORDER_USDT * LEVERAGE, rules["min_notional"] * 1.05)
    raw = target_notional / price
    q = format_qty(symbol, raw)
    if q < rules["min_qty"]:
        q = rules["min_qty"]
    return q

def price_live(symbol):
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if (time.time() - ts) < MARKPRICE_FRESH_SEC and px > 0:
            return px
    if _ws_ticker_cache:
        t = _ws_ticker_cache.get(symbol)
        if t and t["last"] > 0:
            return t["last"]
    if _ticker_cache:
        t = _ticker_cache.get(symbol)
        if t and t["last"] > 0:
            return t["last"]
    try:
        r = client.futures_symbol_ticker(symbol=symbol)
        _api_ok()
        px = float(r["price"])
        if px > 0:
            _ws_mark_price[symbol] = (px, time.time())
            return px
    except Exception as e:
        _log_err(f"price_live_{symbol}", e)
        _api_fail(f"price_live_{symbol}")
    return 0.0

def sync_binance_positions():
    """Rekonsiliasi posisi riil Binance dengan state memory bot."""
    if PAPER_TRADE:
        return
    try:
        raw_pos = client.futures_position_information()
        _api_ok()
        active = [p for p in raw_pos if abs(float(p.get("positionAmt", 0))) > 1e-6]
        active_syms = set()

        with _lock:
            for p in active:
                sym = p["symbol"]
                amt = float(p["positionAmt"])
                entry_px = float(p.get("entryPrice", 0))
                side = "LONG" if amt > 0 else "SHORT"
                abs_qty = abs(amt)
                active_syms.add(sym)

                if sym not in live_positions or live_positions[sym].get("_r"):
                    px_curr = price_live(sym) or entry_px
                    df = _kline_cache.get(sym)
                    atr_val = df["atr"].iloc[-2] if (df is not None and "atr" in df.columns) else (px_curr * 0.015)
                    risk = DynamicRiskManager.calculate_levels(entry_px if entry_px > 0 else px_curr, side, atr_val)

                    live_positions[sym] = {
                        "side": side,
                        "orig_signal": side,
                        "entry": entry_px if entry_px > 0 else px_curr,
                        "qty": abs_qty,
                        "open_time": time.time(),
                        "score": 75,
                        "sigs": ["BINANCE_RECONCILED"],
                        "atr": atr_val,
                        "regime": "SYNCED",
                        "bias": 0.0,
                        "tp_pct": risk["tp_pct"],
                        "sl_pct": risk["sl_pct"],
                        "tp_price": risk["tp_price"],
                        "sl_price": risk["sl_price"],
                        "bep_pct": risk["bep_pct"],
                        "trail_pct": risk["trail_pct"],
                        "trail_dist_pct": risk["trail_dist_pct"],
                        "bep_activated": False,
                        "trailing_active": False,
                        "trailing_sl": risk["sl_price"],
                        "peak_price": px_curr
                    }
                    print(f"  🔄 [RECONCILE] Posisi riil Binance terdeteksi & disinkron: {sym} {side} amt:{abs_qty} @{entry_px:.6g}")
                else:
                    live_positions[sym]["qty"] = abs_qty
                    if entry_px > 0 and live_positions[sym].get("entry", 0) == 0:
                        live_positions[sym]["entry"] = entry_px

            # Hapus posisi dari memory bot jika sudah tertutup di Binance
            for sym in list(live_positions.keys()):
                if not live_positions[sym].get("_r") and sym not in active_syms:
                    print(f"  ℹ️ [RECONCILE] Posisi {sym} sudah tertutup di Binance -> Bersihkan dari tracking bot")
                    live_positions.pop(sym, None)
    except Exception as e:
        _log_err("sync_binance_positions", e, cooldown=15)

def t_position_reconciler():
    """Background loop rekonsiliasi akun setiap 10 detik."""
    while True:
        try:
            sync_binance_positions()
        except Exception as e:
            _log_err("t_position_reconciler", e)
        time.sleep(10)

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
        return _ticker_cache
    except Exception as e:
        _log_err("tickers_all", e)
        _api_fail("tickers_all")
        return _ticker_cache

def _compute_indicators(df):
    high, low, close = df["high"], df["low"], df["close"]
    volume, tbbase = df["volume"], df["tbbase"]
    
    df["e5"]  = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"]  = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    
    macd = ta.trend.MACD(close, 12, 26, 9)
    df["macd"] = macd.macd()
    df["ms"]   = macd.macd_signal()
    df["mh"]   = macd.macd_diff()
    
    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()
    
    df["vm"]  = volume.rolling(20).mean()
    df["vr"]  = volume / df["vm"].replace(0, 1e-9)

    taker_buy = tbbase
    taker_sell = (volume - taker_buy).clip(lower=0)
    df["delta"] = taker_buy - taker_sell
    df["delta_ratio"] = df["delta"] / volume
    df["br"]  = taker_buy / volume
    df["cvd"] = df["delta"].rolling(10).sum()

    df["rng"] = (high - low).replace(0, 1e-9)
    df["upper_wick"] = high - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - low
    df["body"] = (close - df["open"]).abs()
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]
    df["br2"]  = df["body"] / df["rng"]

    df["m5"]   = (close - close.shift(5)) / close.shift(5)
    df["m3"]   = (close - close.shift(3)) / close.shift(3)
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
        avg_px = float(order_resp.get('avgPrice', 0))
        if avg_px > 0: return avg_px
        cum_quote = float(order_resp.get('cumQuote', 0))
        exec_qty = float(order_resp.get('executedQty', 0))
        if exec_qty > 0 and cum_quote > 0:
            return cum_quote / exec_qty
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
#  5. CORE EXECUTION & POSITION MONITORING
# ═══════════════════════════════════════════════════════════════════════════

def live_open(orig_direction, score, sigs, price, atr, regime, bias, sym, risk_profile):
    if INVERT_SIGNALS:
        execution_side = "SHORT" if orig_direction == "LONG" else "LONG"
    else:
        execution_side = orig_direction

    if execution_side not in ("LONG", "SHORT"):
        return

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS: return
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    if px_now > 0:
        price = px_now

    try: q_val = qty(sym, price)
    except: 
        with _lock: live_positions.pop(sym, None)
        return

    tp_pct         = risk_profile["tp_pct"]
    sl_pct         = risk_profile["sl_pct"]
    tp_price       = risk_profile["tp_price"]
    sl_price       = risk_profile["sl_price"]
    bep_pct        = risk_profile.get("bep_pct", 0.008)
    trail_pct      = risk_profile.get("trail_pct", 0.012)
    trail_dist_pct = risk_profile.get("trail_dist_pct", 0.006)

    pos = {
        "side": execution_side,
        "orig_signal": orig_direction,
        "entry": price, "qty": q_val,
        "open_time": time.time(), "score": score, "sigs": sigs,
        "atr": atr, "regime": regime, "bias": bias,
        "tp_pct": tp_pct, "sl_pct": sl_pct,
        "tp_price": tp_price, "sl_price": sl_price,
        "bep_pct": bep_pct, "trail_pct": trail_pct, "trail_dist_pct": trail_dist_pct,
        "bep_activated": False,
        "trailing_active": False,
        "trailing_sl": sl_price,
        "peak_price": price
    }
    with _lock: live_positions[sym] = pos

    if PAPER_TRADE:
        new_risk = DynamicRiskManager.calculate_levels(price, execution_side, atr)
        with _lock:
            if sym in live_positions:
                live_positions[sym].update({
                    "entry": price,
                    "tp_pct": new_risk["tp_pct"],
                    "sl_pct": new_risk["sl_pct"],
                    "tp_price": new_risk["tp_price"],
                    "sl_price": new_risk["sl_price"],
                    "bep_pct": new_risk["bep_pct"],
                    "trail_pct": new_risk["trail_pct"],
                    "trail_dist_pct": new_risk["trail_dist_pct"],
                    "trailing_sl": new_risk["sl_price"],
                    "peak_price": price
                })
        print(f"         📝 PAPER ENTRY | fill:{price:.6g} | qty:{q_val}")
    else:
        try: client.futures_change_leverage(symbol=sym, leverage=LEVERAGE)
        except Exception: pass

        try:
            order = client.futures_create_order(
                symbol=sym, side='BUY' if execution_side == 'LONG' else 'SELL',
                type='MARKET', quantity=q_val, newOrderRespType='RESULT'
            )
            real_px = get_real_fill_price(sym, order)
            if real_px > 0:
                price = real_px
                new_risk = DynamicRiskManager.calculate_levels(price, execution_side, atr)
                with _lock:
                    if sym in live_positions:
                        live_positions[sym].update({
                            "entry": price,
                            "tp_pct": new_risk["tp_pct"],
                            "sl_pct": new_risk["sl_pct"],
                            "tp_price": new_risk["tp_price"],
                            "sl_price": new_risk["sl_price"],
                            "bep_pct": new_risk["bep_pct"],
                            "trail_pct": new_risk["trail_pct"],
                            "trail_dist_pct": new_risk["trail_dist_pct"],
                            "trailing_sl": new_risk["sl_price"],
                            "peak_price": price
                        })
            _api_ok()
            print(f"         ✅ REAL ENTRY #{order.get('orderId')} | fill:{price:.6g} | qty:{q_val}")
        except Exception as e:
            _log_err(f"open_order_{sym}", e, cooldown=5)
            _api_fail(f"open_order_{sym}")
            with _lock: live_positions.pop(sym, None)
            return

    inv_str = " [INVERTED]" if INVERT_SIGNALS else " [DIRECT]"
    side_icon = "🟢 LONG" if execution_side == "LONG" else "🔴 SHORT"
    print(f"\n  🚀 [ENTRY{inv_str}] {sym} {side_icon} @{price:.6g} (score:{score}) | TP:{pos['tp_price']:.6g} (+{pos['tp_pct']*100:.2f}%) SL:{pos['sl_price']:.6g} (-{pos['sl_pct']*100:.2f}%)")
    print(f"     Signals: {' '.join(sigs[:3])} | Regime: {regime}")

def live_close(sym, reason, price=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if not pos or pos.get("_r"): return

    if price is None:
        price = price_live(sym)

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]

    if PAPER_TRADE:
        if price <= 0:
            price = entry
        print(f"         📝 PAPER CLOSE | fill:{price:.6g}")
    else:
        try:
            close_qty = format_qty(sym, q_val)
            close_order = client.futures_create_order(
                symbol=sym, side='SELL' if side == 'LONG' else 'BUY',
                type='MARKET', quantity=close_qty, reduceOnly=True, newOrderRespType='RESULT'
            )
            _api_ok()
            real_px = get_real_fill_price(sym, close_order)
            if real_px > 0:
                price = real_px
            elif price == 0:
                print(f"  ⚠️ {sym}: close terkirim tapi harga fill 0 — estimasi pakai entry")
                price = entry
            print(f"         ✅ CLOSE ORDER #{close_order.get('orderId')} | fill:{price:.6g}")
        except Exception as e:
            _log_err(f"close_order_{sym}", e, cooldown=5)
            print(f"  ⚠️ CLOSE ORDER GAGAL {sym}: {e}")
            with _lock: live_positions[sym] = pos
            return

    gross_pnl  = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    fee_rate   = 0.0005
    total_fee  = (entry * q_val + price * q_val) * fee_rate
    pnl        = gross_pnl - total_fee
    pct        = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold       = time.time() - pos["open_time"]
    won        = pnl >= 0
    e_icon     = "🟢" if won else "🔴"

    peak_px  = pos.get("peak_price", entry)
    peak_pct = (peak_px - entry) / entry if side == "LONG" else (entry - peak_px) / entry

    print(f"  {e_icon} [SCALP v23.0 PRO] {sym} {side} CLOSE — {reason} | peak:{peak_pct*100:+.3f}%")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U (Fee:{total_fee:.5f}U)")

    trade = TradeRecord(
        symbol=sym, direction=side, entry_price=entry, exit_price=price,
        pnl=pnl, won=won, regime=pos.get("regime", "UNKNOWN"),
        signals=pos.get("sigs", []), score=pos.get("score", 0),
        atr_entry=pos.get("atr", 0), hold_seconds=hold, exit_reason=reason, peak_pct=peak_pct,
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

    if "TRAIL" in reason: _stats["trail_exit"] += 1
    elif "BEP" in reason: _stats["bep_exit"] += 1
    elif "TP" in reason: _stats["tp_exit"] += 1
    elif "SL" in reason: _stats["hard_sl"] += 1
    elif "TIME" in reason or "INVALIDATION" in reason: _stats["time_exit"] += 1

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })

    # Cooldown adaptif: Jika exit karena SL, beri penalty cooldown 10 menit (anti-revenge)
    penalty = SL_PENALTY_COOLDOWN if ("SL" in reason and "BEP" not in reason and "TRAIL" not in reason) else COOLDOWN_SEC
    with _lock: cooldown_list[sym] = time.time() + penalty
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue

        hold_time = time.time() - pos["open_time"]
        side, entry = pos["side"], pos["entry"]
        tp_px, sl_px = pos["tp_price"], pos["sl_price"]

        px = price_live(sym)
        if px == 0:
            pos["_fail_count"] = pos.get("_fail_count", 0) + 1
            fc = pos["_fail_count"]
            if fc in (5, 20, 60) or fc % 300 == 0:
                print(f"  ⚠️ {sym}: price_live gagal {fc}x — SL/TP monitoring tertunda")
            continue
        pos["_fail_count"] = 0

        # Update peak price
        if side == "LONG":
            if px > pos.get("peak_price", entry): pos["peak_price"] = px
        else:
            if px < pos.get("peak_price", entry): pos["peak_price"] = px

        # ── 1. HARD TP & SL HIT CHECK (PRIORITAS TRAILING > BEP > SL DIPERBAIKI) ──
        if side == "LONG":
            if px >= tp_px:
                live_close(sym, "TP", tp_px)
                continue
            if px <= sl_px:
                if pos.get("trailing_active"): reason = "TRAIL_SL"
                elif pos.get("bep_activated"): reason = "BEP_SL"
                else: reason = "SL"
                live_close(sym, reason, sl_px)
                continue
        elif side == "SHORT":
            if px <= tp_px:
                live_close(sym, "TP", tp_px)
                continue
            if px >= sl_px:
                if pos.get("trailing_active"): reason = "TRAIL_SL"
                elif pos.get("bep_activated"): reason = "BEP_SL"
                else: reason = "SL"
                live_close(sym, reason, sl_px)
                continue

        # ── 2. BREAK-EVEN PROTECTION (BEP) ─────────────────────────────────
        # Saat profit mencapai >= 0.8x ATR, kunci SL ke Entry + BEP_LOCK_PCT (Net Win Menjamin Fee Taker Ditutup!)
        bep_thresh = pos.get("bep_pct", 0.008)
        if not pos.get("bep_activated", False):
            if side == "LONG" and px >= entry * (1 + bep_thresh):
                new_sl = entry * (1 + BEP_LOCK_PCT)
                if new_sl > pos["sl_price"]:
                    pos["sl_price"] = new_sl
                    pos["bep_activated"] = True
                    print(f"  🛡️ [BEP LOCK] {sym} LONG terkunci di @{new_sl:.6g} (px:{px:.6g}) — Risiko = 0 & Net Profit!")
            elif side == "SHORT" and px <= entry * (1 - bep_thresh):
                new_sl = entry * (1 - BEP_LOCK_PCT)
                if new_sl < pos["sl_price"]:
                    pos["sl_price"] = new_sl
                    pos["bep_activated"] = True
                    print(f"  🛡️ [BEP LOCK] {sym} SHORT terkunci di @{new_sl:.6g} (px:{px:.6g}) — Risiko = 0 & Net Profit!")

        # ── 3. DYNAMIC TRAILING STOP ───────────────────────────────────────
        # Saat profit mencapai >= 1.2x ATR, aktifkan trailing stop berjarak 0.6x ATR
        trail_thresh = pos.get("trail_pct", 0.012)
        trail_dist = pos.get("trail_dist_pct", 0.006)
        if side == "LONG":
            if px >= entry * (1 + trail_thresh):
                pos["trailing_active"] = True
            if pos.get("trailing_active", False):
                trail_stop = px * (1 - trail_dist)
                if trail_stop > pos["sl_price"]:
                    pos["sl_price"] = trail_stop
                    pos["trailing_sl"] = trail_stop
        elif side == "SHORT":
            if px <= entry * (1 - trail_thresh):
                pos["trailing_active"] = True
            if pos.get("trailing_active", False):
                trail_stop = px * (1 + trail_dist)
                if trail_stop < pos["sl_price"]:
                    pos["sl_price"] = trail_stop
                    pos["trailing_sl"] = trail_stop

        # ── 4. MOMENTUM DECAY & INVALIDATION (Exit Cepat jika Momentum Mati) ──
        cur_pnl_pct = (px - entry) / entry if side == "LONG" else (entry - px) / entry
        if hold_time > INVALIDATION_SECONDS:
            df = _kline_cache.get(sym)
            if df is not None and len(df) >= 5:
                last_m5 = df["m5"].iloc[-2]
                if (side == "LONG" and last_m5 < -0.002 and cur_pnl_pct <= 0) or \
                   (side == "SHORT" and last_m5 > 0.002 and cur_pnl_pct <= 0):
                    print(f"  ⚡ {sym}: Momentum mati & arah berbalik ({hold_time:.0f}s) — MOMENTUM_INVALIDATION close")
                    live_close(sym, "MOMENTUM_INVALIDATION", px)
                    continue

        # ── 5. MAX TIME LIMIT (20 Menit Maksimal) ──────────────────────────
        if hold_time > MAX_HOLD_SECONDS:
            reason = "TIME_LIMIT_PROFIT" if cur_pnl_pct > 0 else "TIME_LIMIT_CUT"
            print(f"  ⏰ {sym}: MAX_HOLD_SECONDS tercapai ({hold_time:.0f}s, PnL:{cur_pnl_pct*100:+.2f}%) — {reason}")
            live_close(sym, reason, px)
            continue

# ═══════════════════════════════════════════════════════════════════════════
#  6. SCANNER THREAD & HARD VETO FILTERS
# ═══════════════════════════════════════════════════════════════════════════

def scan_one(sym):
    try:
        time.sleep(0.002)
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None: return None
        df_ta = run_ta(df.copy())
        px_candle, atr_val = df_ta["close"].iloc[-2], df_ta["atr"].iloc[-2]
        if px_candle == 0 or np.isnan(atr_val): return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df_ta, sym)
        if orig_direction is None: return None

        if INVERT_SIGNALS:
            execution_side = "SHORT" if orig_direction == "LONG" else "LONG"
        else:
            execution_side = orig_direction

        if execution_side not in ("LONG", "SHORT"):
            return None

        px_live = price_live(sym)
        if px_live == 0: return None

        # ── VETO FILTER 1: BTC Flash Crash / Pump Circuit Breaker ────────────
        btc_vetoed, btc_reason = btc_macro.check_veto(execution_side)
        if btc_vetoed:
            _stats["btc_breaker_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED by BTC Circuit Breaker: {btc_reason}")
            return None

        # ── VETO FILTER 2: Order Book Wall Detection ──────────────────────────
        has_wall, wall_type, wall_px, wall_qty, wall_mult = order_book.check_walls(sym, px_live, execution_side)
        if has_wall:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED by {wall_type} @ {wall_px:.6g} (qty:{wall_qty:.1f}, {wall_mult:.1f}x avg depth)")
            return None

        # ── VETO FILTER 3: Spoofing & Liquidity Pull Detection ────────────────
        is_spoof, spoof_reason = order_book.detect_spoofing(sym, execution_side)
        if is_spoof:
            _stats["spoof_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED: Spoofing detected ({spoof_reason})")
            return None

        # ── VETO FILTER 4: Order Book Imbalance Guard ─────────────────────────
        imb = order_book.get_imbalance(sym)
        if execution_side == "LONG" and imb < -0.40:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] LONG VETOED: Heavy Ask Queue Imbalance ({imb*100:.0f}%)")
            return None
        elif execution_side == "SHORT" and imb > 0.40:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] SHORT VETOED: Heavy Bid Queue Imbalance ({imb*100:.0f}%)")
            return None

        # ── Hitung Dynamic Risk Profile Berbasis Execution Side ───────────────
        risk_profile = DynamicRiskManager.calculate_levels(px_live, execution_side, atr_val)

        return (sym, orig_direction, score, sigs, px_live, atr_val, regime, bias, risk_profile)
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
        except: pass
    return res

def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    aw = learning.avg_win()
    avg_pk = learning.avg_peak_win()
    e = "💚" if pnl >= 0 else "🔴"
    print(f"       ┌ [SCALP v23.0 PRO] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U)")
    print(f"       └ TP:{_stats['tp_exit']} BEP:{_stats['bep_exit']} Trail:{_stats['trail_exit']} SL:{_stats['hard_sl']} Cut:{_stats['time_exit']} | AvgWin:{aw:+.4f}U | Peak:{avg_pk*100:.3f}%")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if pnl >= 0 else "🔴"
    aw, al = learning.avg_win(), learning.avg_loss()
    bep = al / (al + aw) * 100 if (al + aw) > 0 else 50

    mode_str = "PAPER MODE" if PAPER_TRADE else "BINANCE LIVE/TESTNET"
    inv_mode_str = " | INVERTED (FADING)" if INVERT_SIGNALS else " | DIRECT (FLOW CONTINUATION)"
    print(f"\n  {'─'*72}")
    print(f"    🔔 INSTITUTIONAL SCALPING v23.0 PRO DASHBOARD ({mode_str}{inv_mode_str})")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} ({tph:.1f}T/hr)")
    print(f"    {e} PnL Net:{pnl:+.5f}U | ATH PnL:{_stats['ath_pnl']:+.5f}U | Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"    📈 Exit Breakdown: TP:{_stats['tp_exit']} | BEP:{_stats['bep_exit']} | Trail:{_stats['trail_exit']} | SL:{_stats['hard_sl']} | TimeCut:{_stats['time_exit']}")
    print(f"    🛡️ Veto Stats: Wall Veto:{_stats['wall_veto']} | BTC Breaker:{_stats['btc_breaker_veto']} | Spoof:{_stats['spoof_veto']}")
    print(f"    ⚡ Absorption Entries: {_stats['absorb_entries']} | BEP WR Target:{bep:.1f}%")

    if trade_log:
        print(f"    {'─'*62}\n    📋 Last 5 Trades:")
        for t in trade_log[-5:]:
            ico = "🟢" if t["pnl"] >= 0 else "🔴"
            print(f"       {ico} {t['sym']:<16} {t['side']:<5} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*72}\n")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT); continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and (s not in cooldown_list or now > cooldown_list[s])]

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
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except Exception as e:
            _log_err("t_slot_filler", e)
        time.sleep(SLOT_FILL_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]: continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and (s not in cooldown_list or now > cooldown_list[s])]

            hot = [s for s in _hot_syms if s in valid_syms]
            rest = [s for s in valid_syms if s not in hot]
            res = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except: pass

def t_macro():
    while True:
        try:
            # Fallback REST berkala agar harga btc_macro selalu segar
            try:
                btc_tick = client.futures_symbol_ticker(symbol="BTCUSDT")
                if btc_tick and "price" in btc_tick:
                    btc_macro.update_tick(float(btc_tick["price"]))
            except Exception:
                pass

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
        time.sleep(5)

# ═══════════════════════════════════════════════════════════════════════════
#  7. WEBSOCKET HANDLERS & WATCHDOG
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
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict): return
        k = data.get("k")
        if not k: return
        sym = k.get("s")
        if sym: _append_kline_from_ws(sym, k)
    except Exception as e:
        _log_err("handle_kline_multiplex", e)

def handle_btc_aggtrade(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict): return
        p = data.get("p")
        t = data.get("T")
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
            bids = data.get("b", [])
            asks = data.get("a", [])
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
            print(f"  🚨 WEBSOCKET DIAM {idle:.0f}s — tidak ada data masuk. Fallback otomatis ke REST aktif.")
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  8. BOT LAUNCHER & MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def run_bot():
    mode_text = "PAPER TRADING (SIMULASI)" if PAPER_TRADE else "BINANCE LIVE/TESTNET (REAL ORDERS)"
    strat_text = "INVERTED (Fade Fakeouts / Local Tops)" if INVERT_SIGNALS else "DIRECT (Trend Continuation / Flow Alignment)"
    print("╔════════════════════════════════════════════════════════════════════╗")
    print(f"║  💎 BOT SCALPING v23.0 PRO — QUANTITATIVE MOMENTUM ENGINE          ║")
    print(f"║  Mode: {mode_text:<59} ║")
    print(f"║  Strategy: {strat_text:<55} ║")
    print("║  1. Confluence Filter (Score >= 72 & ADX >= 20): Anti-Chop & Sideway║")
    print("║  2. Dynamic Risk: TP 1.8x ATR | SL 1.1x ATR | BEP Lock @ +0.20% Net║")
    print("║  3. Dynamic Trailing Stop @ +1.2x ATR | Anti-Revenge Penalty (10m) ║")
    print("║  4. Binance Reconciliation: Auto-Sync Akun Real-time (Anti-Ghost)  ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    # Inisialisasi harga BTC awal via REST
    try:
        btc_tick = client.futures_symbol_ticker(symbol="BTCUSDT")
        if btc_tick and "price" in btc_tick:
            btc_macro.update_tick(float(btc_tick["price"]))
            print(f"  ⚡ BTC Macro terinisialisasi @ ${float(btc_tick['price']):.1f}")
    except Exception as e:
        _log_err("btc_prime", e)

    # Sinkronisasi posisi riil Binance saat startup
    sync_binance_positions()

    try: valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
    except: valid = set(SYMBOLS)
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
    threading.Thread(target=t_position_reconciler, daemon=True).start()
    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    time.sleep(2)
    tickers_all()
    
    cycle = 0
    while True:
        cycle += 1
        actual_open = len([k for k, v in live_positions.items() if not v.get("_r")])
        slots = max(0, MAX_POSITIONS - actual_open)
        print(f"\n{'═'*68}")
        api_flag = f" | ⚠️API_FAIL:{_api_fail_streak}" if _api_fail_streak >= 20 else ""
        ws_idle = time.time() - _ws_last_msg_ts
        ws_flag = f" | ⚠️WS_IDLE:{ws_idle:.0f}s" if ws_idle > WS_STALE_SEC else ""

        btc_status = btc_macro.get_status_str()
        veto_summary = f"Veto[Wall:{_stats['wall_veto']}|BTC:{_stats['btc_breaker_veto']}|Spoof:{_stats['spoof_veto']}]"

        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC_5M:{_macro['btc']} ({actual_open}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U) | {veto_summary}{api_flag}{ws_flag}")
        print(f"        ↳ {btc_status}")

        if (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}")
        elif slots == 0: print(f"  ✅ Slots full ({actual_open}/{MAX_POSITIONS}) — monitoring posisi terbuka (TP/BEP/SL/Trail)")
        else: print(f"  🔍 {slots} slot kosong ({actual_open}/{MAX_POSITIONS}) — scanning order book & flow (Score>={MIN_SCORE})...")
        if cycle % 30 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
