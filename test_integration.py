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
import bot

def test_integration():
    print("=== Testing bot.py End-to-End Pipeline Integration ===")
    
    # 1. Order Book Wall Veto on bot.order_book
    bot.order_book.update("SOLUSDT", 
        bids_raw=[["140.0", "50"], ["139.5", "60"], ["139.0", "40"]],
        asks_raw=[["140.5", "30"], ["140.6", "450"], ["141.0", "20"]] # 450 is a huge Sell Wall at 140.6 (<0.5% above 140.2)
    )
    has_wall, wall_type, px, qty, mult = bot.order_book.check_walls("SOLUSDT", 140.2, "LONG")
    assert has_wall and wall_type == "SELL_WALL", f"Failed Sell Wall check: {has_wall}, {wall_type}"
    print(f"✅ Microstructure Wall Veto verified on bot.order_book: {wall_type} @ {px} ({mult:.1f}x avg)")

    # 2. BTC Flash Crash Breaker on bot.btc_macro
    t_now = time.time()
    bot.btc_macro.update_tick(65000.0, ts=t_now)
    bot.btc_macro.update_tick(64750.0, ts=t_now + 2) # -0.38% drop in 2s
    vetoed, reason = bot.btc_macro.check_veto("LONG", now=t_now + 3)
    assert vetoed, f"Failed BTC Flash Crash check: {vetoed}, {reason}"
    print(f"✅ BTC Flash Crash Breaker verified on bot.btc_macro: {reason}")

    # 3. Dynamic Risk Manager on bot.DynamicRiskManager
    risk = bot.DynamicRiskManager.calculate_levels(entry_price=140.0, side="LONG", atr=2.1)
    assert 0.008 <= risk["sl_pct"] <= 0.035, f"SL % out of range: {risk['sl_pct']}"
    assert 0.004 <= risk["trail_gap_pct"] <= 0.015, f"Trail gap % out of range: {risk['trail_gap_pct']}"
    print(f"✅ Dynamic ATR Risk verified: SL: {risk['sl_pct']*100:.2f}% | Trail Gap: ±{risk['trail_gap_pct']*100:.2f}% | Target Act: {risk['trail_act_pct']*100:.2f}%")

    # 4. TA & Scoring Engine
    dates = pd.date_range("2026-01-01", periods=60, freq="5min")
    df = pd.DataFrame({
        "open": np.linspace(100, 110, 60),
        "high": np.linspace(101, 111, 60),
        "low": np.linspace(99, 109, 60),
        "close": np.linspace(100.5, 110.5, 60),
        "volume": [1000.0] * 60,
        "tbbase": [650.0] * 60
    }, index=dates)
    df_ta = bot.run_ta(df)
    assert "delta_ratio" in df_ta.columns and "lower_wick_ratio" in df_ta.columns, "Indicators missing in df_ta!"
    dir_sig, score, sigs, atr_val, reg, bias = bot.scorer.get_signal(df_ta, "SOLUSDT")
    print(f"✅ TA & Scorer verified: Direction={dir_sig}, Score={score}, Regime={reg}, ATR={atr_val:.4f}")
    
    print("\n🎉 ALL PIPELINE INTEGRATION TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    test_integration()
