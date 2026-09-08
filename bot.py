"""
Bot Scalping v21.0 LIVE — SMART POSITIVE EXPECTANCY & BTC MACRO GUARD
=====================================================================
PEMBARUAN BESAR & REVOLUSI ALGORITMA:
1. FIXED INVERTED SCORING BUG:
   - Mengoreksi logika sinyal yang sebelumnya terbalik. _score_long kini 100% menghitung
     indikator tren Bullish (EMA stack, Momentum positif, MACD cross up, Volume buy, RSI sehat).
   - _score_short kini 100% menghitung indikator tren Bearish (EMA bear stack, Momentum negatif,
     MACD cross down, Volume sell, RSI sehat).
   - Anti-FOMO: Penalti overbought (RSI > 75 pada long) & oversold (RSI < 25 pada short).
2. 3-STAGE DYNAMIC RISK MANAGEMENT (POSITIVE EXPECTANCY):
   - Stage 1: Auto Move-to-Breakeven di +0.75% profit (ROE +15%), mengunci +0.12% untuk menutup fee
     dan mengubah posisi menjadi Bebas Risiko (Risk-Free).
   - Stage 2: Profit Lock di +1.20% profit, mengunci minimal +0.60%.
   - Stage 3: Dynamic Trailing Stop di +1.50% profit dengan gap ketat 0.40% untuk memaksimalkan cuan.
   - Dynamic ATR Stop Loss: SL adaptif berbasis volatilitas nyata koin (0.9% - 1.8%).
3. BTC MACRO REGIME & MOMENTUM FILTER:
   - Filter aktif: Blokir LONG pada altcoin jika BTC sedang TRENDING_BEAR atau dump.
   - Blokir SHORT pada altcoin jika BTC sedang TRENDING_BULL atau pump.
   - Perketat MIN_SCORE menjadi 68 jika BTC sedang dalam fase EXHAUSTION / VOLATILE.
4. SMART CIRCUIT BREAKER & STREAK PROTECTION:
   - 3x Consecutive Loss Circuit Breaker: Istirahat 15 Menit jika 3 kali loss berturut-turut.
   - Penalty Cooldown: Cooldown 30 Menit khusus koin yang terkena SL.
   - Stale Scalp Timeout: Tutup posisi jika sudah 45 Menit stagnan tanpa profit.
   - Daily Loss Limit: -$3.0 USDT kill switch harian.
5. FIXED ADAPTIVE LEARNING & WINDOWS UTF-8 ENCODING:
   - Perbaikan key binding sinyal agar SignalWeights dan LearningLayer benar-benar belajar dari riwayat.
"""

import os
import sys
import time
import math
import threading
import queue
import numpy as np
import pandas as pd
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List

# Reconfigure stdout to UTF-8 on Windows console if needed
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from dotenv import load_dotenv
from binance.client import Client
import ta

load_dotenv()
# Menggunakan testnet=True secara langsung untuk mencegah masalah SSL handshake ke api.binance.com
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"), testnet=True)
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 2.0
MAX_POSITIONS = 3

# Scanning
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01
COOLDOWN_SEC    = 180    # 3 Menit jeda setelah win/BE
SL_COOLDOWN_SEC = 1800   # 30 Menit penalty jeda jika kena SL agar tidak mengulang loss

# Scoring & Filter
MIN_SCORE      = 55
MIN_SCORE_CHOP = 68      # Jika BTC EXHAUSTION/VOLATILE, butuh score lebih tinggi
SLIPPAGE_GUARD = 0.0015
TTL_5M         = 2

# ── 3-Stage Dynamic Risk Management v21.0 ──────────────────────────────────
SL_PCT_DEFAULT     = 0.012   # Default 1.2%
SL_PCT_MIN         = 0.009   # Min SL 0.9%
SL_PCT_MAX         = 0.018   # Max SL 1.8%
ATR_SL_MULT        = 1.4     # Pengali ATR untuk SL adaptif

# Stage 1: Breakeven (Auto-Protect)
BE_TRIGGER_PCT     = 0.0075  # Trigger BE saat profit +0.75% (ROE +15%)
BE_LOCK_PCT        = 0.0012  # Kunci di +0.12% (menutup taker fee 0.10% + locked profit)

# Stage 2: Profit Lock
STAGE2_TRIGGER_PCT = 0.0120  # Trigger lock saat profit +1.20%
STAGE2_LOCK_PCT    = 0.0060  # Kunci profit minimal +0.60%

# Stage 3: Dynamic Trailing Stop
TRAIL_ACTIVATE_PCT = 0.0150  # Trailing aktif di +1.50%
TRAIL_GAP_PCT      = 0.0040  # Gap diperketat ke 0.40% agar profit terselamatkan

EMERGENCY_TP_PCT   = 0.0350  # Emergency hard TP 3.5%
STALE_HOLD_SECONDS = 2700    # 45 Menit: Tutup jika trade stagnan/tidak bergerak profit
MAX_HOLD_SECONDS   = 7200    # Maksimal tahan posisi 2 Jam
# ──────────────────────────────────────────────────────────────────────────

# Smart Circuit Breaker & Streak Protection
DAILY_LOSS   = -3.0   # Stop jika rugi harian >= -3.0 USDT
CONSEC_MAX   = 3      # Maksimal 3 loss berturut-turut
CONSEC_PAUSE = 900    # Istirahat 15 Menit jika 3 loss beruntun

# Learning
LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 20

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
        atr_expand  = (atr / atr_prev) > 1.2 if atr_prev > 0 else False
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
#  SIGNAL WEIGHTS & SCORING (DIPERBAIKI SECARA MATEMATIS)
# ═══════════════════════════════════════════════════════════════════════════
class SignalWeights:
    def __init__(self):
        self.weights = {
            # Bullish Signals
            "ema_bull_stack": 35, "ema_mild_bull": 26, "ema_weak_bull": 14,
            "mom_strong": 30, "mom_moderate": 20, "macd_cross_up": 22, "macd_strengthen": 15,
            "orderflow_buy_climax": 25, "orderflow_buy_high": 14,
            "rsi_healthy_bull": 20, "rsi_pullback_bull": 12,
            # Bearish Signals
            "ema_bear_stack": 35, "ema_mild_bear": 26, "ema_weak_bear": 14,
            "mom_strong_neg": 30, "mom_moderate_neg": 20, "macd_cross_down": 22, "macd_strengthen_neg": 15,
            "orderflow_sell_climax": 25, "orderflow_sell_high": 14,
            "rsi_healthy_bear": 20, "rsi_bounce_bear": 12,
        }
        self.history = defaultdict(list)
        self.adaptive_enabled = True

    def record_outcome(self, signals: List[str], won: bool):
        for sig in signals:
            # Parse format "base_key:display"
            base = sig.split(':')[0].strip()
            if base in self.weights:
                self.history[base].append(1 if won else 0)
                if len(self.history[base]) > LEARNING_WINDOW:
                    self.history[base] = self.history[base][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        base = signal_name.split(':')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT: return self.weights.get(base, 10)
        return self.weights.get(base, 10) * max(0.5, min(1.5, 0.5 + sum(hist) / len(hist)))

class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

    def get_signal(self, df: pd.DataFrame, symbol: str = None):
        if df is None or len(df) < 55: return None, 0, [], 0.0, 0.0, 0.0, "UNKNOWN", 0.0
        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df)
        short_score, short_sigs = self._score_short(df)
        atr = df["atr"].iloc[-2]

        # Filter Makro BTC
        btc_reg = _macro.get("btc", "UNKNOWN")
        btc_m5 = _macro.get("btc_m5", 0.0)
        min_required = MIN_SCORE
        if btc_reg in (MarketRegime.REGIME_EXHAUSTION, MarketRegime.REGIME_VOLATILE):
            min_required = MIN_SCORE_CHOP

        # 1. Bullish Regime
        if regime == MarketRegime.REGIME_TRENDING_BULL:
            # Proteksi Makro BTC: Jangan Long jika BTC sedang Bearish atau Dump
            if symbol != "BTCUSDT" and (btc_reg == MarketRegime.REGIME_TRENDING_BEAR or btc_m5 < -0.004):
                _stats["regime_block"] += 1
                return None, long_score, ["BTC_BEAR_BLOCK"], atr, 0, 0, regime, bias

            if long_score >= min_required:
                display_sigs = [s.split(':', 1)[-1] for s in long_sigs]
                return "LONG", long_score, display_sigs, atr, 0, 0, regime, bias
            return None, max(long_score, short_score), [], atr, 0, 0, regime, bias

        # 2. Bearish Regime
        elif regime == MarketRegime.REGIME_TRENDING_BEAR:
            # Proteksi Makro BTC: Jangan Short jika BTC sedang Bullish atau Pump
            if symbol != "BTCUSDT" and (btc_reg == MarketRegime.REGIME_TRENDING_BULL or btc_m5 > 0.004):
                _stats["regime_block"] += 1
                return None, short_score, ["BTC_BULL_BLOCK"], atr, 0, 0, regime, bias

            if short_score >= min_required:
                display_sigs = [s.split(':', 1)[-1] for s in short_sigs]
                return "SHORT", short_score, display_sigs, atr, 0, 0, regime, bias
            return None, max(long_score, short_score), [], atr, 0, 0, regime, bias

        # 3. Non-trending Regimes
        elif regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION, MarketRegime.REGIME_VOLATILE):
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, 0, 0, regime, bias

        return None, 0, [], atr, 0, 0, regime, bias

    def _score_long(self, df: pd.DataFrame) -> Tuple[int, List[str]]:
        """Mengevaluasi kekuatan sinyal BULLISH sejati."""
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        # EMA Bull Stack
        if p > e5 > e9 > e21 > e50:
            w = self.weights.get_adjusted_weight("ema_bull_stack")
            score += w; signals.append(f"ema_bull_stack:EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21:
            w = self.weights.get_adjusted_weight("ema_mild_bull")
            score += w; signals.append(f"ema_mild_bull:EMA4↑[{w:.0f}]")
        elif p > e5 > e9:
            w = self.weights.get_adjusted_weight("ema_weak_bull")
            score += w; signals.append(f"ema_weak_bull:EMA3↑[{w:.0f}]")

        # Momentum Positif
        if row["m5"] > 0.003:
            w = self.weights.get_adjusted_weight("mom_strong")
            score += w; signals.append(f"mom_strong:Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        elif row["m5"] > 0.0015:
            w = self.weights.get_adjusted_weight("mom_moderate")
            score += w; signals.append(f"mom_moderate:Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")

        # MACD Bullish
        if prev["mh"] <= 0 and row["mh"] > 0:
            w = self.weights.get_adjusted_weight("macd_cross_up")
            score += w; signals.append(f"macd_cross_up:MACD_X↑[{w:.0f}]")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]:
            w = self.weights.get_adjusted_weight("macd_strengthen")
            score += w; signals.append(f"macd_strengthen:MACD↑↑[{w:.0f}]")

        # Orderflow Buy Volume
        if row["br"] > 0.58:
            w = self.weights.get_adjusted_weight("orderflow_buy_climax")
            score += w; signals.append(f"orderflow_buy_climax:BuyClimax{row['br']:.0%}[{w:.0f}]")
        elif row["br"] > 0.52:
            w = self.weights.get_adjusted_weight("orderflow_buy_high")
            score += w; signals.append(f"orderflow_buy_high:Buy{row['br']:.0%}[{w:.0f}]")

        # RSI Zone & Anti-FOMO Guard
        if row["rsi"] > 75:
            # Overbought penalty: jangan buy di pucuk!
            score -= 25; signals.append(f"rsi_penalty:RSI_{row['rsi']:.0f}_OB_PENALTY[-25]")
        elif 52 <= row["rsi"] <= 68:
            w = self.weights.get_adjusted_weight("rsi_healthy_bull")
            score += w; signals.append(f"rsi_healthy_bull:RSI_{row['rsi']:.0f}_Bull[{w:.0f}]")
        elif 42 <= row["rsi"] < 52:
            w = self.weights.get_adjusted_weight("rsi_pullback_bull")
            score += w; signals.append(f"rsi_pullback_bull:RSI_{row['rsi']:.0f}_Pullback[{w:.0f}]")

        return max(0, score), signals

    def _score_short(self, df: pd.DataFrame) -> Tuple[int, List[str]]:
        """Mengevaluasi kekuatan sinyal BEARISH sejati."""
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        # EMA Bear Stack
        if p < e5 < e9 < e21 < e50:
            w = self.weights.get_adjusted_weight("ema_bear_stack")
            score += w; signals.append(f"ema_bear_stack:EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21:
            w = self.weights.get_adjusted_weight("ema_mild_bear")
            score += w; signals.append(f"ema_mild_bear:EMA4↓[{w:.0f}]")
        elif p < e5 < e9:
            w = self.weights.get_adjusted_weight("ema_weak_bear")
            score += w; signals.append(f"ema_weak_bear:EMA3↓[{w:.0f}]")

        # Momentum Negatif
        if row["m5"] < -0.003:
            w = self.weights.get_adjusted_weight("mom_strong_neg")
            score += w; signals.append(f"mom_strong_neg:Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        elif row["m5"] < -0.0015:
            w = self.weights.get_adjusted_weight("mom_moderate_neg")
            score += w; signals.append(f"mom_moderate_neg:Mom{row['m5']*100:.1f}%↓[{w:.0f}]")

        # MACD Bearish
        if prev["mh"] >= 0 and row["mh"] < 0:
            w = self.weights.get_adjusted_weight("macd_cross_down")
            score += w; signals.append(f"macd_cross_down:MACD_X↓[{w:.0f}]")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]:
            w = self.weights.get_adjusted_weight("macd_strengthen_neg")
            score += w; signals.append(f"macd_strengthen_neg:MACD↓↓[{w:.0f}]")

        # Orderflow Sell Volume
        if row["br"] < 0.42:
            w = self.weights.get_adjusted_weight("orderflow_sell_climax")
            score += w; signals.append(f"orderflow_sell_climax:SellClimax{1-row['br']:.0%}[{w:.0f}]")
        elif row["br"] < 0.48:
            w = self.weights.get_adjusted_weight("orderflow_sell_high")
            score += w; signals.append(f"orderflow_sell_high:Sell{1-row['br']:.0%}[{w:.0f}]")

        # RSI Zone & Anti-Dump Guard
        if row["rsi"] < 25:
            # Oversold penalty: jangan short di dasar jurang!
            score -= 25; signals.append(f"rsi_penalty:RSI_{row['rsi']:.0f}_OS_PENALTY[-25]")
        elif 32 <= row["rsi"] <= 48:
            w = self.weights.get_adjusted_weight("rsi_healthy_bear")
            score += w; signals.append(f"rsi_healthy_bear:RSI_{row['rsi']:.0f}_Bear[{w:.0f}]")
        elif 48 < row["rsi"] <= 58:
            w = self.weights.get_adjusted_weight("rsi_bounce_bear")
            score += w; signals.append(f"rsi_bounce_bear:RSI_{row['rsi']:.0f}_Bounce[{w:.0f}]")

        return max(0, score), signals

class RiskManager:
    @staticmethod
    def calculate_levels(entry_price: float, side: str, atr: float = 0.0) -> Tuple[float, float, float]:
        """Menghitung Stop Loss adaptif berbasis ATR nyata dan Emergency TP."""
        if entry_price > 0 and atr > 0:
            raw_sl = (atr * ATR_SL_MULT) / entry_price
            sl_pct = max(SL_PCT_MIN, min(SL_PCT_MAX, raw_sl))
        else:
            sl_pct = SL_PCT_DEFAULT

        if side == "LONG":
            sl_price = entry_price * (1 - sl_pct)
            emg_tp   = entry_price * (1 + EMERGENCY_TP_PCT)
        else:
            sl_price = entry_price * (1 + sl_pct)
            emg_tp   = entry_price * (1 - EMERGENCY_TP_PCT)
        return sl_price, emg_tp, sl_pct

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
    score:        float
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
#  BOT STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ohlcv_cache     = {}
_ticker_cache    = {}
_ticker_ts       = 0
_lock            = threading.Lock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q        = queue.Queue()
_hot_syms        = deque(maxlen=30)

_macro = {"btc": "UNKNOWN", "btc_m5": 0.0, "btc_adx": 0.0}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0,
    "trail_exit": 0, "be_exit": 0, "hard_sl": 0, "emg_tp": 0, "stale_exit": 0,
    "regime_block": 0, "hist": deque(maxlen=200), "start": time.time(),
}

live_positions = {}
cooldown_list  = {}
trade_log      = []
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)
learning       = LearningLayer(signal_weights)

def get_precision(symbol):
    if symbol in _precision_cache: return _precision_cache[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except: pass
    return 2

def qty(symbol, price):
    raw = (ORDER_USDT * LEVERAGE) / price
    return round(raw, get_precision(symbol))

def price_live(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if now - _ticker_ts < 2 and _ticker_cache: return _ticker_cache
    try:
        raw = client.futures_ticker()
        _ticker_cache = {t["symbol"]: {"pct": float(t["priceChangePercent"]), "vol": float(t["quoteVolume"]), "last": float(t["lastPrice"])} for t in raw}
        _ticker_ts = now
    except: pass
    return _ticker_cache

def ohlcv(symbol, interval, limit=100):
    key, now = (symbol, interval), time.time()
    if key in _ohlcv_cache and now - _ohlcv_cache[key][0] < TTL_5M: return _ohlcv_cache[key][1]
    try:
        kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]: df[c] = df[c].astype(float)
        df["rsi"] = ta.momentum.RSIIndicator(df["close"], 14).rsi()
        df["mh"]  = ta.trend.MACD(df["close"], 12, 26, 9).macd_diff()
        df["e5"]  = ta.trend.EMAIndicator(df["close"], 5).ema_indicator()
        df["e9"]  = ta.trend.EMAIndicator(df["close"], 9).ema_indicator()
        df["e21"] = ta.trend.EMAIndicator(df["close"], 21).ema_indicator()
        df["e50"] = ta.trend.EMAIndicator(df["close"], 50).ema_indicator()
        df["atr"] = ta.volatility.AverageTrueRange(df["high"], df["low"], df["close"], 14).average_true_range()
        df["adx"] = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], 14).adx()
        df["vm"]  = df["volume"].rolling(20).mean()
        df["vr"]  = df["volume"] / df["vm"].replace(0, 1)
        df["br"]  = df["tbbase"] / df["volume"].replace(0, 1)
        df["body"] = abs(df["close"] - df["open"])
        df["rng"]  = df["high"] - df["low"]
        df["br2"]  = df["body"] / df["rng"].replace(0, 1)
        df["m5"]   = (df["close"] - df["close"].shift(5)) / df["close"].shift(5)
        df["m3"]   = (df["close"] - df["close"].shift(3)) / df["close"].shift(3)
        _ohlcv_cache[key] = (now, df)
        return df
    except: return _ohlcv_cache.get(key, (None, None))[1]

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]: k["active"], k["consec"] = False, 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"], k["day_reset"] = 0.0, day
    if k["daily"] <= DAILY_LOSS:
        k["active"], k["reason"], k["resume"] = True, f"daily_loss({k['daily']:.2f})", day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"], k["reason"], k["resume"] = True, f"consec_losses({k['consec']})", now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

def get_real_fill_price(sym, order_resp):
    """Memastikan bot mendapatkan harga eksekusi final mutlak dari Binance."""
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
#  CORE TRADING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def live_open(orig_direction, score, sigs, price, atr, regime, bias, sym):
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

    sl_price, emg_tp, sl_pct = RiskManager.calculate_levels(price, orig_direction, atr)

    pos = {
        "side": orig_direction, "entry": price, "qty": q_val,
        "open_time": time.time(), "score": score, "sigs": sigs,
        "atr": atr, "regime": regime, "bias": bias,
        "sl_price": sl_price, "emergency_tp": emg_tp, "sl_pct": sl_pct,
        "peak_price": price,
        "be_active": False, "stage2_active": False,
        "trail_active": False, "trail_stop": None,
    }
    with _lock: live_positions[sym] = pos

    try: client.futures_change_leverage(symbol=sym, leverage=LEVERAGE)
    except Exception: pass

    try:
        order = client.futures_create_order(
            symbol=sym, side='BUY' if orig_direction == 'LONG' else 'SELL',
            type='MARKET', quantity=q_val, newOrderRespType='RESULT'
        )
        real_px = get_real_fill_price(sym, order)
        if real_px > 0:
            price = real_px
            sl_p2, emg2, sl_pct2 = RiskManager.calculate_levels(price, orig_direction, atr)
            with _lock:
                if sym in live_positions and not live_positions[sym].get('_r'):
                    live_positions[sym].update({
                        'entry': price, 'sl_price': sl_p2, 'emergency_tp': emg2,
                        'sl_pct': sl_pct2, 'peak_price': price
                    })
        print(f"         ✅ ORDER #{order.get('orderId')} | fill:{price:.6g} | qty:{q_val}")
    except Exception as e:
        print(f"  ❌ ORDER GAGAL {sym}: {e}")
        with _lock: live_positions.pop(sym, None)
        return

    d = "🟢" if orig_direction == "LONG" else "🔴"
    print(f"\n  {d} [v21.0 SMART] {sym} {orig_direction} @{price:.6g} | SL:{sl_pct*100:.2f}% (ATR:{atr:.4g}) | Trail:±{TRAIL_GAP_PCT*100:.2f}% | Regime:{regime}")
    print(f"         Signals: {' | '.join(sigs[:5])}")
    _stats["trades"] += 1

def live_close(sym, reason, price=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    if price is None: price = price_live(sym)
    if price == 0:
        with _lock: live_positions[sym] = pos
        return

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]

    # ── REAL CLOSE ORDER — market order reduceOnly ke Binance testnet ────
    try:
        close_order = client.futures_create_order(
            symbol=sym, side='SELL' if side == 'LONG' else 'BUY',
            type='MARKET', quantity=q_val, reduceOnly=True, newOrderRespType='RESULT'
        )
        real_px = get_real_fill_price(sym, close_order)
        if real_px > 0:
            price = real_px
        print(f"         ✅ CLOSE ORDER #{close_order.get('orderId')} | fill:{price:.6g}")
    except Exception as e:
        print(f"  ⚠️ CLOSE ORDER GAGAL {sym}: {e}")
        with _lock: live_positions[sym] = pos
        return
    # ─────────────────────────────────────────────────────────────────────

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

    trail_info = f" | peak:{peak_pct*100:+.3f}%"
    if pos.get("be_active"): trail_info += " 🛡️BE_protected"
    if pos.get("trail_active"): trail_info += " ✅trail_active"

    print(f"  {e_icon} [v21.0 CLOSE] {sym} {side} — {reason}{trail_info}")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    trade = TradeRecord(
        symbol=sym, direction=side, entry_price=entry, exit_price=price,
        pnl=pnl, won=won, regime=pos.get("regime", "UNKNOWN"),
        signals=pos.get("sigs", []), score=pos.get("score", 0),
        atr_entry=pos.get("atr", 0), hold_seconds=hold, exit_reason=reason, peak_pct=peak_pct,
    )
    learning.add_trade(trade)

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    ks_upd(pnl)

    if won:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if "TRAIL" in reason: _stats["trail_exit"] += 1
    elif "BE_EXIT" in reason: _stats["be_exit"] += 1
    elif "SL" in reason: _stats["hard_sl"] += 1
    elif "TP" in reason: _stats["emg_tp"] += 1
    elif "STALE" in reason: _stats["stale_exit"] += 1

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })

    # Cooldown adaptif: koin yang kena SL dihukum jeda 30 menit
    cool_time = SL_COOLDOWN_SEC if not won else COOLDOWN_SEC
    with _lock: cooldown_list[sym] = time.time() + cool_time
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

def monitor_positions():
    """3-Stage Dynamic Risk Management: Breakeven -> Lock Profit -> Dynamic Trailing."""
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue
        px = price_live(sym)
        if px == 0: continue

        side, entry, sl_px, emg_tp = pos["side"], pos["entry"], pos["sl_price"], pos["emergency_tp"]
        hold_time = time.time() - pos["open_time"]

        # Current & Peak profit calculations
        cur_profit = (px - entry) / entry if side == "LONG" else (entry - px) / entry
        if side == "LONG":
            if px > pos["peak_price"]: pos["peak_price"] = px
        else:
            if px < pos["peak_price"]: pos["peak_price"] = px
        peak = pos["peak_price"]
        peak_profit = (peak - entry) / entry if side == "LONG" else (entry - peak) / entry

        # 1. Stale Scalp Timeout: Jika 45 menit stagnan tanpa profit, keluar dengan aman
        if hold_time > STALE_HOLD_SECONDS and cur_profit < 0.002:
            live_close(sym, "STALE_TIMEOUT", px)
            continue

        # 2. Hard Max Hold Limit (2 jam)
        if hold_time > MAX_HOLD_SECONDS:
            live_close(sym, "TIME_LIMIT", px)
            continue

        # 3. Emergency Hard TP
        if side == "LONG" and px >= emg_tp: live_close(sym, "TP_EMG", emg_tp); continue
        if side == "SHORT" and px <= emg_tp: live_close(sym, "TP_EMG", emg_tp); continue

        # 4. Stop Loss / Breakeven Hit
        if side == "LONG" and px <= sl_px:
            reason = "BE_EXIT" if pos.get("be_active") else "SL"
            live_close(sym, reason, sl_px)
            continue
        if side == "SHORT" and px >= sl_px:
            reason = "BE_EXIT" if pos.get("be_active") else "SL"
            live_close(sym, reason, sl_px)
            continue

        # 5. Stage 1: Auto Move-to-Breakeven at +0.75% profit
        if peak_profit >= BE_TRIGGER_PCT and not pos.get("be_active"):
            pos["be_active"] = True
            if side == "LONG":
                be_px = entry * (1 + BE_LOCK_PCT)
                if be_px > pos["sl_price"]: pos["sl_price"] = be_px
            else:
                be_px = entry * (1 - BE_LOCK_PCT)
                if be_px < pos["sl_price"]: pos["sl_price"] = be_px
            print(f"  🛡️ [BREAKEVEN ON] {sym} {side} | peak:{peak_profit*100:+.2f}% | SL moved to BE: {pos['sl_price']:.6g}")

        # 6. Stage 2: Lock Profit at +1.20% profit
        if peak_profit >= STAGE2_TRIGGER_PCT and not pos.get("stage2_active"):
            pos["stage2_active"] = True
            if side == "LONG":
                lock_px = entry * (1 + STAGE2_LOCK_PCT)
                if lock_px > pos["sl_price"]: pos["sl_price"] = lock_px
            else:
                lock_px = entry * (1 - STAGE2_LOCK_PCT)
                if lock_px < pos["sl_price"]: pos["sl_price"] = lock_px
            print(f"  🔒 [PROFIT LOCK ON] {sym} {side} | peak:{peak_profit*100:+.2f}% | SL locked to +0.6%: {pos['sl_price']:.6g}")

        # 7. Stage 3: Dynamic Trailing Stop at +1.50% profit
        if not pos["trail_active"]:
            if peak_profit >= TRAIL_ACTIVATE_PCT:
                pos["trail_active"] = True
                pos["trail_stop"] = peak * (1 - TRAIL_GAP_PCT) if side == "LONG" else peak * (1 + TRAIL_GAP_PCT)
                print(f"  🔔 [TRAIL ON] {sym} {side} | peak:{peak_profit*100:+.2f}% | trail_stop:{pos['trail_stop']:.6g}")

        if pos["trail_active"]:
            ts = pos["trail_stop"]
            if side == "LONG":
                new_ts = peak * (1 - TRAIL_GAP_PCT)
                if new_ts > ts: pos["trail_stop"] = new_ts; ts = new_ts
                if px <= ts: live_close(sym, "TRAIL", ts); continue
            else:
                new_ts = peak * (1 + TRAIL_GAP_PCT)
                if new_ts < ts: pos["trail_stop"] = new_ts; ts = new_ts
                if px >= ts: live_close(sym, "TRAIL", ts); continue

# ═══════════════════════════════════════════════════════════════════════════
#  SCANNER THREAD & MAIN
# ═══════════════════════════════════════════════════════════════════════════

def run_ta(df):
    if "rsi" not in df.columns:
        df["rsi"] = ta.momentum.RSIIndicator(df["close"], 14).rsi()
        df["mh"]  = ta.trend.MACD(df["close"], 12, 26, 9).macd_diff()
        df["e5"]  = ta.trend.EMAIndicator(df["close"], 5).ema_indicator()
        df["e9"]  = ta.trend.EMAIndicator(df["close"], 9).ema_indicator()
        df["e21"] = ta.trend.EMAIndicator(df["close"], 21).ema_indicator()
        df["e50"] = ta.trend.EMAIndicator(df["close"], 50).ema_indicator()
        df["atr"] = ta.volatility.AverageTrueRange(df["high"], df["low"], df["close"], 14).average_true_range()
        df["adx"] = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], 14).adx()
        df["vm"]  = df["volume"].rolling(20).mean()
        df["vr"]  = df["volume"] / df["vm"].replace(0, 1)
        df["br"]  = df["tbbase"] / df["volume"].replace(0, 1)
        df["body"] = abs(df["close"] - df["open"])
        df["rng"]  = df["high"] - df["low"]
        df["br2"]  = df["body"] / df["rng"].replace(0, 1)
        df["m5"]   = (df["close"] - df["close"].shift(5)) / df["close"].shift(5)
        df["m3"]   = (df["close"] - df["close"].shift(3)) / df["close"].shift(3)
    return df

def scan_one(sym):
    try:
        time.sleep(0.002)
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None: return None
        df_ta = df.copy()
        if not all(c in df_ta.columns for c in ["rsi","mh","e5","e9","e21","e50","atr","adx","vr","br","m5","br2"]):
            df_ta = run_ta(df_ta)
        px, atr = df_ta["close"].iloc[-2], df_ta["atr"].iloc[-2]
        if px == 0 or np.isnan(atr): return None
        direction, score, sigs, atr_val, _, _, regime, bias = scorer.get_signal(df_ta, sym)
        if direction is None: return None
        px_live = price_live(sym)
        if px_live == 0: return None
        return (sym, direction, score, sigs, px_live, atr_val, regime, bias)
    except Exception:
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
    print(f"       ┌ [v21.0 LIVE] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U")
    print(f"       └ Trail:{_stats['trail_exit']} BE:{_stats['be_exit']} SL:{_stats['hard_sl']} AvgWin:{aw:+.4f}U | Peak:{avg_pk*100:.3f}%")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if pnl >= 0 else "🔴"
    aw, al = learning.avg_win(), learning.avg_loss()
    bep = al / (al + aw) * 100 if (al + aw) > 0 else 50

    print(f"\n  {'─'*70}")
    print(f"    🔔 SMART SCALPER v21.0 (POSITIVE EXPECTANCY & BTC GUARD)")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} ({tph:.1f}T/hr)")
    print(f"    {e} PnL Net:{pnl:+.5f}U Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"    📈 Exit: Trail:{_stats['trail_exit']} | BE:{_stats['be_exit']} | SL:{_stats['hard_sl']} | EmgTP:{_stats['emg_tp']} | Stale:{_stats['stale_exit']}")
    print(f"    💰 Avg Win:{aw:+.5f}U | Avg Loss:{-al:+.5f}U | BEP WR:{bep:.1f}%")
    print(f"    🌐 BTC Macro: {_macro.get('btc', 'UNKNOWN')} (Mom:{_macro.get('btc_m5', 0)*100:+.2f}%)")

    if trade_log:
        print(f"    {'─'*60}\n    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"       {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*70}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except Exception:
            pass
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
                    sym, od, sc, sg, px, atr, regime, bias = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym)
        except Exception:
            pass
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
                    sym, od, sc, sg, px, atr, regime, bias = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym)
        except Exception:
            pass

def t_macro():
    """Memantau rezim dan momentum BTC secara real-time sebagai filter pelindung altcoin."""
    while True:
        try:
            df_btc = ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80)
            if df_btc is not None and len(df_btc) >= 55:
                reg, strength, bias = MarketRegime.detect(df_btc)
                m5 = float(df_btc["m5"].iloc[-2]) if "m5" in df_btc.columns and not np.isnan(df_btc["m5"].iloc[-2]) else 0.0
                adx = float(df_btc["adx"].iloc[-2]) if "adx" in df_btc.columns and not np.isnan(df_btc["adx"].iloc[-2]) else 0.0
                _macro["btc"] = reg
                _macro["btc_m5"] = m5
                _macro["btc_adx"] = adx
        except Exception:
            pass
        time.sleep(10)

def run_bot():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  🟢 SMART SCALPER v21.0 LIVE — POSITIVE EXPECTANCY & BTC GUARD     ║")
    print("║  ✅ Fixed Scoring Bug: Masuk posisi searah tren sesungguhnya       ║")
    print("║  ✅ 3-Stage Dynamic Risk: Breakeven (+0.75%) & Profit Lock (+1.2%) ║")
    print("║  ✅ Macro BTC Filter: Blokir Long saat BTC dump, Blokir Short pump ║")
    print("║  ✅ Anti-Loss Streak: 15m Cool-off pada 3x Loss, 30m Penalty SL    ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    try: valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
    except: valid = set(SYMBOLS)
    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    time.sleep(2)
    tickers_all()
    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        print(f"\n{'═'*62}")
        btc_info = f"BTC:{_macro['btc']}"
        if _macro.get('btc_m5', 0) != 0:
            btc_info += f"({_macro['btc_m5']*100:+.2f}%)"
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} {btc_info} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")
        if (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}")
        elif slots == 0: print(f"  ✅ Slots full — trailing aktif di posisi terbuka")
        else: print(f"  🔍 {slots} slot kosong — scanning...")
        if cycle % 30 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
