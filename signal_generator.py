import yfinance as yf
import pandas as pd
import numpy as np
import os

TICKERS = ["SPY","QQQ","GLD","TLT","DBC"]
ROBUST_PARAMS = {t:(20,2.0,3) for t in TICKERS}
CAPITAL = 100000
HISTORY_FILE = "multi_etf_trades.csv"
COLUMNS = ["Date","Ticker","Action","Shares","Entry_Price","Exit_Price","Stop","PnL_USD","PnL_PCT","Pyramids"]

dfs = {}
for t in TICKERS:
    df = yf.download(t, period="1y", auto_adjust=True)[["Open","High","Low","Close","Volume"]].copy()
    df = df.apply(pd.to_numeric, errors="coerce").dropna()
    tr = pd.concat([df.High - df.Low, (df.High - df.Close.shift()).abs(), (df.Low - df.Close.shift()).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    df["ret"] = df.Close.pct_change()
    df["MA200"] = df.Close.rolling(200).mean()
    dfs[t] = df

if os.path.exists(HISTORY_FILE):
    history = pd.read_csv(HISTORY_FILE)
else:
    history = pd.DataFrame(columns=COLUMNS)

# SIGNAL GENERATOR
today_signals = []
for i,t in enumerate(TICKERS):
    df = dfs[t]
    breakout_n, atr_mult, pyramids = ROBUST_PARAMS[t]
    last = df.iloc[-1]
    close = last["Close"].item()
    ma200 = last["MA200"].item()
    atr = last["ATR"].item()
    breakout = df.High.rolling(breakout_n).max().shift(1).iloc[-1].item()
    open_pos = history[(history["Ticker"]==t) & (history["Action"]=="BUY")].tail(1)

    action=None; shares=0; stop=None; entry_price=None; exit_price=None; pnl_usd=None; pnl_pct=None; layers=0; shares_per_layer=0

    # NEW BUY
    if pd.notna(ma200) and pd.notna(atr) and pd.notna(breakout) and close>breakout and close>ma200 and open_pos.empty:
        action="BUY"
        total_shares=int(CAPITAL/close) 
        shares=total_shares
        layers=pyramids
        shares_per_layer=total_shares//pyramids
        stop=close-atr_mult*atr
        entry_price=close

    # SELL / trailing stop
    if not open_pos.empty:
        prev=open_pos.iloc[0]
        entry_price=prev["Entry_Price"]
        shares=int(prev["Shares"])
        layers=int(prev["Pyramids"])
        prev_stop=prev["Stop"]
        stop=max(prev_stop, close-atr_mult*atr)
        if close<=stop:
            action="SELL"
            exit_price=close
            pnl_usd=(exit_price-entry_price)*shares
            pnl_pct=((exit_price-entry_price)/entry_price)*100

    today_signals.append({
        "Date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "Ticker":t,"Action":action if action else "HOLD","Shares":shares,
        "Entry_Price":entry_price,"Exit_Price":exit_price,"Stop":round(stop,2) if stop else None,
        "PnL_USD":round(pnl_usd,2) if pnl_usd else None,"PnL_PCT":round(pnl_pct,2) if pnl_pct else None,"Pyramids":layers
    })

sig_df=pd.DataFrame(today_signals)
history=pd.concat([history,sig_df],ignore_index=True)
history.to_csv(HISTORY_FILE,index=False)