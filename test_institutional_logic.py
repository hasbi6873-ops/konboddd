"""
Unit tests and simulation for Institutional Trading Engine Logic:
1. Microstructure Order Book (BAI, Wall Detection, Spoofing)
2. BTC Flash Crash / Pump Circuit Breaker
3. Order Flow Delta & Institutional Absorption Detector
4. Dynamic ATR Risk Management
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import time
import pandas as pd
import numpy as np
from collections import deque, defaultdict

# ═══════════════════════════════════════════════════════════════════════════
# 1. ORDER BOOK ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    WALL_RATIO_THRESHOLD = 2.5
    WALL_DEPTH_PCT = 0.35
    WALL_PROXIMITY_PCT = 0.005 # 0.5%
    SPOOF_DROP_THRESHOLD = 0.40

    def __init__(self):
        self.cache = {}
        self.history = defaultdict(lambda: deque(maxlen=10))

    def update(self, symbol: str, bids: list, asks: list, ts: float = None):
        if ts is None: ts = time.time()
        # Parse [price, qty]
        b_clean = [(float(p), float(q)) for p, q in bids]
        a_clean = [(float(p), float(q)) for p, q in asks]
        
        b_clean.sort(key=lambda x: x[0], reverse=True)
        a_clean.sort(key=lambda x: x[0])
        
        bid_vol = sum(q for _, q in b_clean)
        ask_vol = sum(q for _, q in a_clean)
        total_vol = bid_vol + ask_vol
        imbalance = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0
        
        self.cache[symbol] = {
            "bids": b_clean,
            "asks": a_clean,
            "bid_vol": bid_vol,
            "ask_vol": ask_vol,
            "imbalance": imbalance,
            "ts": ts
        }
        best_bid = b_clean[0][0] if b_clean else 0.0
        best_ask = a_clean[0][0] if a_clean else 0.0
        self.history[symbol].append((ts, bid_vol, ask_vol, best_bid, best_ask))

    def check_walls(self, symbol: str, current_price: float, side: str):
        book = self.cache.get(symbol)
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0
        
        if side == "LONG":
            # Check for Sell Wall directly above current price
            asks = book["asks"]
            total_ask = book["ask_vol"]
            if not asks or total_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = total_ask / len(asks)
            
            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= self.WALL_PROXIMITY_PCT:
                    if qty >= self.WALL_RATIO_THRESHOLD * avg_ask or qty >= self.WALL_DEPTH_PCT * total_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult
                        
        elif side == "SHORT":
            # Check for Buy Wall directly below current price
            bids = book["bids"]
            total_bid = book["bid_vol"]
            if not bids or total_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = total_bid / len(bids)
            
            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= self.WALL_PROXIMITY_PCT:
                    if qty >= self.WALL_RATIO_THRESHOLD * avg_bid or qty >= self.WALL_DEPTH_PCT * total_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult
                        
        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol: str, side: str):
        hist = self.history.get(symbol)
        if not hist or len(hist) < 3: return False, ""
        now = time.time()
        curr_ts, curr_b_vol, curr_a_vol, _, _ = hist[-1]
        
        for ts, b_vol, a_vol, _, _ in list(hist)[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG":
                    # Check if Bids suddenly evaporated
                    if b_vol > 0 and curr_b_vol < b_vol * (1 - self.SPOOF_DROP_THRESHOLD):
                        return True, f"Bid liquidity pulled (down {(1 - curr_b_vol/b_vol)*100:.0f}%)"
                elif side == "SHORT":
                    # Check if Asks suddenly evaporated
                    if a_vol > 0 and curr_a_vol < a_vol * (1 - self.SPOOF_DROP_THRESHOLD):
                        return True, f"Ask liquidity pulled (down {(1 - curr_a_vol/a_vol)*100:.0f}%)"
        return False, ""


# ═══════════════════════════════════════════════════════════════════════════
# 2. BTC MACRO & FLASH CRASH ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    BTC_CRASH_THRESHOLD = -0.003  # -0.3%
    BTC_PUMP_THRESHOLD  = 0.003   # +0.3%
    BTC_WINDOW_SEC      = 8.0
    BTC_COOLDOWN_SEC    = 120.0

    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0, "trigger_ts": 0.0}

    def update_tick(self, price: float, ts: float = None):
        if ts is None: ts = time.time()
        self.tick_history.append((ts, price))
        
        # Check window
        cutoff = ts - self.BTC_WINDOW_SEC
        baseline_price = None
        for t_ts, t_px in self.tick_history:
            if t_ts >= cutoff:
                baseline_price = t_px
                break
                
        if baseline_price and baseline_price > 0:
            delta = (price - baseline_price) / baseline_price
            if delta <= self.BTC_CRASH_THRESHOLD:
                self.breaker = {
                    "active": True, "type": "CRASH", "until": ts + self.BTC_COOLDOWN_SEC,
                    "delta": delta, "trigger_ts": ts
                }
            elif delta >= self.BTC_PUMP_THRESHOLD:
                self.breaker = {
                    "active": True, "type": "PUMP", "until": ts + self.BTC_COOLDOWN_SEC,
                    "delta": delta, "trigger_ts": ts
                }

    def check_veto(self, side: str, now: float = None):
        if now is None: now = time.time()
        if self.breaker["active"] and now < self.breaker["until"]:
            rem = self.breaker["until"] - now
            b_type = self.breaker["type"]
            delta = self.breaker["delta"]
            if b_type == "CRASH" and side == "LONG":
                return True, f"BTC Flash Crash active ({rem:.0f}s left, drop {delta*100:+.2f}%)"
            elif b_type == "PUMP" and side == "SHORT":
                return True, f"BTC Flash Pump active ({rem:.0f}s left, surge {delta*100:+.2f}%)"
        elif self.breaker["active"] and now >= self.breaker["until"]:
            self.breaker["active"] = False
        return False, "OK"


# ═══════════════════════════════════════════════════════════════════════════
# 3. ABSORPTION & ORDER FLOW DETECTOR
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame):
        if df is None or len(df) < 25: return False, False, ""
        row = df.iloc[-2]
        
        vol_spike = row["vr"] >= 1.4
        
        # Bullish absorption:
        # Huge seller volume (negative delta / br < 0.40)
        # BUT price holds: long lower wick >= 38% of range and close in upper half
        heavy_seller = (row["delta_ratio"] < -0.20) or (row["br"] < 0.40)
        wick_bull = row["lower_wick_ratio"] >= 0.38
        close_held_bull = row["close"] >= (row["low"] + 0.45 * row["rng"])
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)
        
        # Bearish absorption:
        # Huge buyer volume (positive delta / br > 0.60)
        # BUT price holds: long upper wick >= 38% of range and close in lower half
        heavy_buyer = (row["delta_ratio"] > 0.20) or (row["br"] > 0.60)
        wick_bear = row["upper_wick_ratio"] >= 0.38
        close_held_bear = row["close"] <= (row["high"] - 0.45 * row["rng"])
        bear_absorb = vol_spike and heavy_buyer and (wick_bear or close_held_bear)
        
        details = []
        if bull_absorb:
            details.append(f"BullAbsorb(Vol:{row['vr']:.1f}x|Δ:{row['delta_ratio']:+.2f}|Wick:{row['lower_wick_ratio']:.0%})")
        if bear_absorb:
            details.append(f"BearAbsorb(Vol:{row['vr']:.1f}x|Δ:{row['delta_ratio']:+.2f}|Wick:{row['upper_wick_ratio']:.0%})")
            
        return bull_absorb, bear_absorb, " ".join(details)


# ═══════════════════════════════════════════════════════════════════════════
# 4. DYNAMIC ATR RISK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

class DynamicRiskManager:
    ATR_SL_MULTIPLIER         = 1.8
    ATR_TRAIL_ACT_MULTIPLIER = 1.6
    ATR_TRAIL_GAP_MULTIPLIER = 0.8
    ATR_EMG_TP_MULTIPLIER   = 3.5

    MIN_SL_PCT        = 0.008  # 0.8%
    MAX_SL_PCT        = 0.035  # 3.5%
    MIN_TRAIL_ACT_PCT = 0.010  # 1.0%
    MAX_TRAIL_ACT_PCT = 0.040  # 4.0%
    MIN_TRAIL_GAP_PCT = 0.004  # 0.4%
    MAX_TRAIL_GAP_PCT = 0.015  # 1.5%
    MIN_EMG_TP_PCT    = 0.025  # 2.5%
    MAX_EMG_TP_PCT    = 0.080  # 8.0%

    @classmethod
    def calculate_levels(cls, entry_price: float, side: str, atr: float):
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.015
        
        sl_pct = max(cls.MIN_SL_PCT, min(cls.MAX_SL_PCT, cls.ATR_SL_MULTIPLIER * atr_pct))
        trail_act_pct = max(cls.MIN_TRAIL_ACT_PCT, min(cls.MAX_TRAIL_ACT_PCT, cls.ATR_TRAIL_ACT_MULTIPLIER * atr_pct))
        trail_gap_pct = max(cls.MIN_TRAIL_GAP_PCT, min(cls.MAX_TRAIL_GAP_PCT, cls.ATR_TRAIL_GAP_MULTIPLIER * atr_pct))
        emg_tp_pct = max(cls.MIN_EMG_TP_PCT, min(cls.MAX_EMG_TP_PCT, cls.ATR_EMG_TP_MULTIPLIER * atr_pct))
        
        if side == "LONG":
            sl_price = entry_price * (1 - sl_pct)
            emg_tp   = entry_price * (1 + emg_tp_pct)
        else:
            sl_price = entry_price * (1 + sl_pct)
            emg_tp   = entry_price * (1 - emg_tp_pct)
            
        return {
            "sl_pct": sl_pct,
            "trail_act_pct": trail_act_pct,
            "trail_gap_pct": trail_gap_pct,
            "emg_tp_pct": emg_tp_pct,
            "sl_price": sl_price,
            "emg_tp_price": emg_tp,
            "atr_pct": atr_pct
        }


# ═══════════════════════════════════════════════════════════════════════════
# VERIFICATION SUITE
# ═══════════════════════════════════════════════════════════════════════════

def run_tests():
    print("=== Running Institutional Strategy Unit Tests ===")
    
    # 1. Test Order Book Wall Veto
    ob = OrderBookEngine()
    current_px = 100.0
    # Asks with normal volumes except a massive sell wall at 100.30 (within 0.5%)
    bids = [[99.9, 10], [99.8, 12], [99.7, 15], [99.6, 11], [99.5, 10]]
    asks = [[100.1, 8], [100.2, 9], [100.3, 120], [100.4, 7], [100.5, 10]] # 120 is massive!
    ob.update("TESTUSDT", bids, asks)
    
    vetoed, wall_type, wall_px, wall_q, mult = ob.check_walls("TESTUSDT", current_px, "LONG")
    assert vetoed and wall_type == "SELL_WALL" and wall_px == 100.3, "Sell wall detection failed!"
    print("✅ Test 1 Passed: Sell Wall successfully detected and vetoes LONG.")

    # Buy Wall test
    bids_wall = [[99.9, 8], [99.8, 150], [99.7, 9], [99.6, 11], [99.5, 10]]
    asks_norm = [[100.1, 10], [100.2, 10], [100.3, 10], [100.4, 10], [100.5, 10]]
    ob.update("TESTUSDT", bids_wall, asks_norm)
    vetoed_s, wall_type_s, wall_px_s, _, _ = ob.check_walls("TESTUSDT", current_px, "SHORT")
    assert vetoed_s and wall_type_s == "BUY_WALL" and wall_px_s == 99.8, "Buy wall detection failed!"
    print("✅ Test 2 Passed: Buy Wall successfully detected and vetoes SHORT.")

    # 2. Test BTC Flash Crash Circuit Breaker
    btc = BTCMacroEngine()
    t0 = time.time()
    btc.update_tick(60000.0, ts=t0)
    btc.update_tick(59950.0, ts=t0 + 1)
    btc.update_tick(59750.0, ts=t0 + 3) # Drop of (59750 - 60000)/60000 = -0.417% (Crash!)
    
    veto_long, reason = btc.check_veto("LONG", now=t0 + 4)
    assert veto_long, f"BTC Flash Crash should veto LONG! Got: {reason}"
    veto_short, _ = btc.check_veto("SHORT", now=t0 + 4)
    assert not veto_short, "BTC Flash Crash should NOT veto SHORT!"
    print(f"✅ Test 3 Passed: BTC Flash Crash circuit breaker triggered ({reason}).")

    # 3. Test Absorption Detection
    # Create mock 30 candles dataframe
    dates = pd.date_range(start="2026-01-01", periods=30, freq="5min")
    data = {
        "open": [100.0]*30, "high": [101.0]*30, "low": [99.0]*30, "close": [100.0]*30,
        "volume": [1000.0]*30, "tbbase": [500.0]*30
    }
    df = pd.DataFrame(data, index=dates)
    df["vm"] = df["volume"].rolling(20).mean()
    df["vr"] = df["volume"] / df["vm"].replace(0, 1)
    df["delta"] = 2 * df["tbbase"] - df["volume"]
    df["delta_ratio"] = df["delta"] / df["volume"]
    df["br"] = df["tbbase"] / df["volume"]
    df["rng"] = df["high"] - df["low"]
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
    df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]
    
    # Configure candle at index -2 as Bullish Absorption:
    # Volume spike (2.0x avg), huge sell volume (br=0.25, delta_ratio=-0.50)
    # Long lower wick (lower_wick_ratio = 0.50), closed up at 100.20
    df.loc[df.index[-2], "volume"] = 2500.0
    df.loc[df.index[-2], "vr"] = 2.5
    df.loc[df.index[-2], "tbbase"] = 625.0 # 25% buy, 75% sell!
    df.loc[df.index[-2], "delta"] = 2 * 625.0 - 2500.0 # -1250
    df.loc[df.index[-2], "delta_ratio"] = -0.50
    df.loc[df.index[-2], "br"] = 0.25
    df.loc[df.index[-2], "open"] = 99.8
    df.loc[df.index[-2], "close"] = 100.2
    df.loc[df.index[-2], "low"] = 98.0
    df.loc[df.index[-2], "high"] = 100.5
    rng = 100.5 - 98.0 # 2.5
    df.loc[df.index[-2], "rng"] = rng
    df.loc[df.index[-2], "lower_wick"] = 99.8 - 98.0 # 1.8
    df.loc[df.index[-2], "lower_wick_ratio"] = 1.8 / 2.5 # 0.72 (huge wick!)
    
    bull_abs, bear_abs, det = AbsorptionDetector.detect(df)
    assert bull_abs, f"Bullish Absorption should be detected! Got: {det}"
    assert not bear_abs, "Bearish absorption should NOT be detected here!"
    print(f"✅ Test 4 Passed: Bullish Absorption successfully detected: {det}")

    # 4. Test Dynamic ATR Risk Management
    # High volatility asset: Price = 100, ATR = 2.5 (2.5% ATR)
    high_vol = DynamicRiskManager.calculate_levels(100.0, "LONG", 2.5)
    # 1.8 * 2.5% = 4.5% -> clamped to MAX_SL_PCT (3.5%)
    assert high_vol["sl_pct"] == 0.035, f"High vol SL should be clamped to 3.5%, got {high_vol['sl_pct']}"
    assert high_vol["trail_gap_pct"] >= 0.015, f"High vol Trail Gap should be widened, got {high_vol['trail_gap_pct']}"
    
    # Low volatility asset: Price = 100, ATR = 0.2 (0.2% ATR)
    low_vol = DynamicRiskManager.calculate_levels(100.0, "LONG", 0.2)
    # 1.8 * 0.2% = 0.36% -> clamped to MIN_SL_PCT (0.8%)
    assert low_vol["sl_pct"] == 0.008, f"Low vol SL should be clamped to 0.8%, got {low_vol['sl_pct']}"
    assert low_vol["trail_gap_pct"] == 0.004, f"Low vol Trail Gap should be clamped to 0.4%, got {low_vol['trail_gap_pct']}"
    print(f"✅ Test 5 Passed: Dynamic ATR Risk scaling correctly adjusts SL and Trail Gap (High vol SL: {high_vol['sl_pct']*100:.1f}%, Low vol SL: {low_vol['sl_pct']*100:.1f}%).")

    print("\n🎉 ALL 5 INSTITUTIONAL UNIT TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    run_tests()
