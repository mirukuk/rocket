#!/usr/bin/env python3
"""
Unified Screener V4 — The "Master Router" for Extreme Returns

Enhancements in V4:
- Trailing Stop After T1 Hit (breakeven on remaining 2/3)
- Pullback Entry Filter (RSI < 50, avoid chasing extended)
- Short Squeeze Candidate Detection (high short interest + covering)
- Volatility-Adjusted Allocation (VIX extremes mapped to regime_mult)

Usage:
  python run.py
"""

import sys, os, json, re, argparse
from datetime import datetime
from collections import Counter

# Force UTF-8 stdout/stderr so emoji and box-drawing chars print on consoles
# whose default codec (e.g. Windows cp950) can't encode them.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yfinance as yf
import requests
import pandas as pd
import logging
import numpy as np
import warnings

logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)

# Silence pandas FutureWarnings (e.g. float() on single-element Series) so the
# console output stays readable and stable across pandas versions.
warnings.simplefilter("ignore", category=FutureWarning)

ROOT = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(ROOT, 'history')
MAX_HISTORY = 10

MAX_POSITIONS = 5
POSITION_SIZE = 20.0
TODAY_OUTPERFORMANCE_MARGIN = 1.0
OVEREXTENSION_PCT = 20.0

CONVICTION_SIZE = 35.0
MOMENTUM_EXIT_3D = -2.0

# V5: HARD DRAWDOWN STOPS (from peak) — critical for margin positions
LEVERAGED_HARD_STOP_PCT = 25.0   # 3x ETFs: SELL if down 25% from peak (no margin tightening)
STOCK_HARD_STOP_PCT = 15.0       # Regular stocks: SELL if down 15% from peak
MARGIN_TIGHTEN_FACTOR = 0.80     # Margin positions: tighten stops by 20% (stocks only)
MARGIN_MODE = True                # Set True when using margin — tighter stops for stocks

# V2 ENHANCEMENTS
MIN_PRICE = 5.0
MAX_ATR_PCT_STOCK = 10.0
SCORE_CEILING = 300
SCORE_FLOOR = -200
MIN_FREQ_FOR_BONUS = 4
SHORT_INTEREST_THRESHOLD = 0.20  # 20% - exclude longs above this

# V4 ENHANCEMENTS
PULLBACK_RSI_MAX = 50  # Max RSI for pullback entry
SQUEEZE_SHORT_MIN = 0.15  # Min 15% short interest for squeeze candidate
SQUEEZE_BONUS = 30  # Score bonus for squeeze candidates

# Sector ETFs for rotation
SECTOR_ETFS = ['XLK', 'XLE', 'XLV', 'XLY', 'XLF', 'XLRE']

# Always-visible watchlist tickers
WATCHLIST_TICKERS = ['SOXL', 'DRAM', 'KORU', 'TECL','SOXX','EWT']


def get_sector_rotation():
    """Compute relative strength of sector ETFs vs SPY"""
    sector_perfs = {}
    try:
        sector_data = yf.download(SECTOR_ETFS + ['SPY'], period="1mo", auto_adjust=True, progress=False)
        if sector_data.empty:
            return {'top': None, 'bottom': None, 'map': {}}
        spy_perf = pct(sector_data['Close']['SPY'], 20) if 'SPY' in sector_data.columns else 0
        for etf in SECTOR_ETFS:
            if etf in sector_data.columns:
                s_perf = pct(sector_data['Close'][etf], 20) or 0
                rel = s_perf - spy_perf if spy_perf else 0
                sector_perfs[etf] = {'perf': s_perf, 'rel': rel}
    except Exception as e:
        print(f"   Sector rotation error: {e}")
        return {'top': None, 'bottom': None, 'map': {}}
    
    if not sector_perfs:
        return {'top': None, 'bottom': None, 'map': {}}
    
    sorted_sectors = sorted(sector_perfs.items(), key=lambda x: x[1]['rel'], reverse=True)
    top = sorted_sectors[0][0] if sorted_sectors else None
    bottom = sorted_sectors[-1][0] if sorted_sectors else None
    return {'top': top, 'bottom': bottom, 'map': sector_perfs}

LEVERAGED_TICKERS = {
    'TQQQ', 'SOXL', 'UPRO', 'SPXL', 'TECL', 'FNGU', 'LABU', 'TNA',
    'UDOW', 'CURE', 'DFEN', 'DRN', 'DUSL', 'FAS', 'HIBL', 'MIDU',
    'NAIL', 'RETL', 'TPOR', 'WANT', 'WEBL',
    'SQQQ', 'SPXS', 'SDOW', 'FAZ', 'TZA', 'LABD', 'FNGD',
}

BEAR_ETFS = [
    'SQQQ', 'SPXS', 'SDOW', 'SRTY', 'SPXU', 'LABD', 'FAZ', 'SOXS', 'TECS',
    'YANG', 'WEBS', 'HIBS', 'TZA', 'DRV', 'DUG', 'PST', 'TMV', 'OILD', 'KOLD', 'FNGD'
]

BULL_ETFS = [
    'TQQQ', 'SOXL', 'UPRO', 'SPXL', 'TECL', 'FNGU', 'LABU', 'TNA',
    'UDOW', 'CURE', 'DFEN', 'DRN', 'DUSL', 'FAS', 'HIBL', 'MIDU',
    'NAIL', 'RETL', 'TPOR', 'WANT', 'WEBL'
]

ETF_URL = "https://finviz.com/screener.ashx?v=411&f=ind_exchangetradedfund%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&o=-volume"
# Stock screener URLs - simplified filters for more results
STOCK_URL = "https://finviz.com/screener.ashx?v=152&f=sh_price_o10,ta_change_u,ta_perf_13w50o&ft=4&o=-perf13w"
STOCK_URL2 = "https://finviz.com/screener.ashx?v=152&f=sh_avgvol_o200,sh_price_o10,ta_change_u&ft=4&o=-volume"
STOCK_URL3 = "https://finviz.com/screener.ashx?v=152&f=sh_price_o10,ta_change_u,ta_perf_1w10o&ft=4&o=-perf1w"

SEP = "-" * 72
SEP2 = "=" * 72


def pct(series, n):
    if len(series) < n:
        return None
    return round((float(series.iloc[-1]) / float(series.iloc[-n]) - 1) * 100, 2)


def fmt_pct(value):
    return "N/A" if value is None else f"{value:+.2f}%"


def fmt_dollar_volume(value):
    if value is None:
        return "N/A"
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    if value >= 1e6:
        return f"${value / 1e6:.1f}M"
    return f"${value:,.0f}"


def is_leveraged(ticker):
    return ticker in LEVERAGED_TICKERS


def fetch_finviz(url, limit=200):
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        r.raise_for_status()
        seen = set()
        # Finviz ticker links now use the form `stock?t=TICKER` (older pages used
        # `quote?t=TICKER`). Match both so the screener keeps working across
        # Finviz HTML changes. Allow '.'/'-' for tickers like BRK.B / BF-B.
        tickers = re.findall(r"(?:stock|quote)\?t=([A-Z][A-Z.\-]*)", r.text)
        return [t for t in tickers
                if not (t in seen or seen.add(t))][:limit]
    except Exception as e:
        print(f"   Finviz error: {e}")
        return []


def bulk_download(tickers, period="1y"):
    if not tickers:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        _stderr = sys.stderr
        sys.stderr = open(os.devnull, 'w')
        try:
            data = yf.download(tickers, period=period, auto_adjust=True,
                               threads=True, progress=False)
        finally:
            sys.stderr.close()
            sys.stderr = _stderr
        if data.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        # yfinance 1.1+ always returns a MultiIndex DataFrame regardless of
        # ticker count, so data.get('Close') is the correct path for both
        # single and multi-ticker downloads.
        return (data.get('Close', pd.DataFrame()),
                data.get('Volume', pd.DataFrame()),
                data.get('Open', pd.DataFrame()),
                data.get('High', pd.DataFrame()),
                data.get('Low', pd.DataFrame()))
    except Exception as e:
        print(f"   Download error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# TradingView Technical Analysis API - fallback using yfinance computed values
TV_API_URL = "https://scanner.tradingview.com/america/scan"

def compute_tv_technicals_from_data(ticker, close_df, high_df, low_df, open_df):
    """
    Compute TradingView-style technical analysis from yfinance data.
    Returns dict with oscillators, moving_averages, rsi, macd signals.
    """
    result = {
        'oscillators': 'NEUTRAL',
        'moving_averages': 'NEUTRAL',
        'rsi': None,
        'macd': None,
    }
    
    try:
        # Handle both multi-index (multiple tickers) and single ticker cases
        if ticker in close_df.columns:
            prices = close_df[ticker].dropna()
        else:
            # Try multi-index format
            try:
                prices = close_df[(slice(None), ticker)].droplevel(1)
            except:
                return result
        
        # Handle high_df
        if ticker in high_df.columns:
            highs = high_df[ticker].dropna()
        else:
            try:
                highs = high_df[(slice(None), ticker)].droplevel(1).dropna()
            except:
                highs = prices  # fallback
        
        # Handle low_df
        if ticker in low_df.columns:
            lows = low_df[ticker].dropna()
        else:
            try:
                lows = low_df[(slice(None), ticker)].droplevel(1).dropna()
            except:
                lows = prices  # fallback
        
        # Handle open_df
        if ticker in open_df.columns:
            opens = open_df[ticker].dropna()
        else:
            try:
                opens = open_df[(slice(None), ticker)].droplevel(1).dropna()
            except:
                opens = prices  # fallback
        
        if len(prices) < 50:
            return result
        
        price = float(prices.iloc[-1])
        
        # === RSI (14-period) ===
        delta = prices.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss
        rsi_series = 100 - (100 / (1 + rs))
        rsi = float(rsi_series.iloc[-1])
        if pd.isna(rsi):
            rsi = 50.0  # Neutral default
        result['rsi'] = round(rsi, 2)
        
        # === MACD (12, 26, 9) ===
        ema12 = prices.ewm(span=12, adjust=False).mean()
        ema26 = prices.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = float((macd_line - signal_line).iloc[-1])
        if pd.isna(macd_hist):
            macd_hist = 0.0
        result['macd'] = round(macd_hist, 4)
        
        # V4: Entry Quality based on RSI
        # Pullback = RSI < 50 (ideal entry), Breakout = RSI 50-60, Momentum = RSI > 60
        if rsi < PULLBACK_RSI_MAX:
            result['entry_quality'] = 'PULLBACK'
        elif rsi < 60:
            result['entry_quality'] = 'BREAKOUT'
        else:
            result['entry_quality'] = 'MOMENTUM'
        
        # === Stochastic (14,3) ===
        stoch_k = 100 * (prices - lows.rolling(14).min()) / (highs.rolling(14).max() - lows.rolling(14).min() + 0.0001)
        stoch_d = stoch_k.rolling(3).mean()
        stoch_k_val = float(stoch_k.iloc[-1]) if not pd.isna(stoch_k.iloc[-1]) else 50.0
        stoch_d_val = float(stoch_d.iloc[-1]) if not pd.isna(stoch_d.iloc[-1]) else 50.0
        stoch_k_prev = float(stoch_k.iloc[-2]) if len(stoch_k) >= 2 and not pd.isna(stoch_k.iloc[-2]) else 50.0
        stoch_d_prev = float(stoch_d.iloc[-2]) if len(stoch_d) >= 2 and not pd.isna(stoch_d.iloc[-2]) else 50.0
        
        # === CCI (20-period) ===
        typical = (highs + lows + prices) / 3
        cci_series = (typical - typical.rolling(20).mean()) / (0.015 * typical.rolling(20).std() + 0.0001)
        cci_val = float(cci_series.iloc[-1]) if not pd.isna(cci_series.iloc[-1]) else 0.0
        
        # === ADX (14-period) ===
        plus_dm = highs.diff()
        minus_dm = -lows.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        tr1 = highs - lows
        tr2 = (highs - prices.shift()).abs()
        tr3 = (lows - prices.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        
        plus_di = 100 * (plus_dm.rolling(14).mean() / (atr + 0.0001))
        minus_di = 100 * (minus_dm.rolling(14).mean() / (atr + 0.0001))
        di_sum = plus_di + minus_di
        adx_series = 100 * (abs(plus_di - minus_di) / (di_sum + 0.0001)).rolling(14).mean()
        plus_di_val = float(plus_di.iloc[-1]) if not pd.isna(plus_di.iloc[-1]) else 50.0
        minus_di_val = float(minus_di.iloc[-1]) if not pd.isna(minus_di.iloc[-1]) else 50.0
        
        # === Awesome Oscillator ===
        ao_series = prices.rolling(5).mean() - prices.rolling(34).mean()
        ao = float(ao_series.iloc[-1]) if not pd.isna(ao_series.iloc[-1]) else 0.0
        
        # === Moving Averages ===
        sma20 = float(prices.rolling(20).mean().iloc[-1]) if not pd.isna(prices.rolling(20).mean().iloc[-1]) else price
        sma50 = float(prices.rolling(50).mean().iloc[-1]) if not pd.isna(prices.rolling(50).mean().iloc[-1]) else price
        sma200 = float(prices.rolling(200).mean().iloc[-1]) if len(prices) >= 200 and not pd.isna(prices.rolling(200).mean().iloc[-1]) else None
        ema20 = float(prices.ewm(span=20, adjust=False).mean().iloc[-1]) if not pd.isna(prices.ewm(span=20, adjust=False).mean().iloc[-1]) else price
        ema50 = float(prices.ewm(span=50, adjust=False).mean().iloc[-1]) if not pd.isna(prices.ewm(span=50, adjust=False).mean().iloc[-1]) else price
        
        # === Count Oscillator Signals ===
        osc_buy = 0
        osc_sell = 0
        osc_neutral = 0
        
        # RSI - proper neutral zone
        if rsi < 30:
            osc_buy += 1
        elif rsi > 70:
            osc_sell += 1
        else:
            osc_neutral += 1
        
        # Stochastic - proper neutral zone
        if stoch_k_val < 20:
            osc_buy += 1
        elif stoch_k_val > 80:
            osc_sell += 1
        elif stoch_k_val > stoch_d_val and stoch_k_prev <= stoch_d_prev:
            osc_buy += 1
        elif stoch_k_val < stoch_d_val and stoch_k_prev >= stoch_d_prev:
            osc_sell += 1
        else:
            osc_neutral += 1
        
        # CCI - proper neutral zone
        if cci_val < -100:
            osc_buy += 1
        elif cci_val > 100:
            osc_sell += 1
        else:
            osc_neutral += 1
        
        # MACD histogram - proper neutral zone
        if macd_hist > 0:
            osc_buy += 1
        elif macd_hist < 0:
            osc_sell += 1
        else:
            osc_neutral += 1
        
        # ADX + DI - proper neutral zone
        if plus_di_val > minus_di_val:
            osc_buy += 1
        elif plus_di_val < minus_di_val:
            osc_sell += 1
        else:
            osc_neutral += 1
        
        # Awesome Oscillator - proper neutral zone
        if ao > 0:
            osc_buy += 1
        elif ao < 0:
            osc_sell += 1
        else:
            osc_neutral += 1
        
        # Determine Oscillator Signal
        if osc_buy >= osc_sell + 2:
            result['oscillators'] = 'BUY'
        elif osc_sell >= osc_buy + 2:
            result['oscillators'] = 'SELL'
        else:
            result['oscillators'] = 'NEUTRAL'
        # Include counts
        result['osc_buy'] = osc_buy
        result['osc_sell'] = osc_sell
        result['osc_neutral'] = osc_neutral
        
        # === Count Moving Average Signals ===
        ma_buy = 0
        ma_sell = 0
        ma_neutral = 0
        
        # Price vs MAs
        if price > sma20:
            ma_buy += 1
        elif price < sma20:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        if price > sma50:
            ma_buy += 1
        elif price < sma50:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        if sma200:
            if price > sma200:
                ma_buy += 1
            elif price < sma200:
                ma_sell += 1
            else:
                ma_neutral += 1
        
        if price > ema20:
            ma_buy += 1
        elif price < ema20:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        if price > ema50:
            ma_buy += 1
        elif price < ema50:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        # MA alignment (bullish: short MA > long MA)
        if sma20 > sma50:
            ma_buy += 1
        elif sma20 < sma50:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        if ema20 > ema50:
            ma_buy += 1
        elif ema20 < ema50:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        # EMA > SMA (momentum confirmation)
        if ema50 > sma50:
            ma_buy += 1
        elif ema50 < sma50:
            ma_sell += 1
        else:
            ma_neutral += 1
        
        # Determine MA Signal
        if ma_buy >= ma_sell + 3:
            result['moving_averages'] = 'BUY'
        elif ma_sell >= ma_buy + 3:
            result['moving_averages'] = 'SELL'
        else:
            result['moving_averages'] = 'NEUTRAL'
        # Include counts
        result['ma_buy'] = ma_buy
        result['ma_sell'] = ma_sell
        result['ma_neutral'] = ma_neutral
            
    except Exception as e:
        # Silently return defaults on error
        pass
    
    return result


def fetch_tradingview_technicals(tickers):
    """
    Fetch TradingView technical analysis for multiple tickers.
    Returns dict: { ticker: { 'oscillators': buy/sell/neutral, 'moving_averages': buy/sell/neutral } }
    Falls back to yfinance computation if TV API fails.
    """
    return {}  # Return empty - will use local computation in run_screener


def fetch_tradingview_technicals_v2(ticker, close_df, high_df, low_df, open_df):
    """Compute TV-style technicals from local data"""
    return compute_tv_technicals_from_data(ticker, close_df, high_df, low_df, open_df)


def enrich_name(ticker):
    try:
        return yf.Ticker(ticker).info.get('shortName', ticker)
    except Exception:
        return ticker


def get_short_interest(ticker):
    """Fetch short interest as % of float"""
    try:
        info = yf.Ticker(ticker).info
        short_pct = info.get('shortPercentOfFloat', 0)
        if short_pct and short_pct > 0:
            return short_pct
        # Alternative: shortRatio field
        return info.get('shortRatio', 0)
    except Exception:
        return 0


def get_short_squeeze_data(ticker):
    """Fetch short interest metrics for squeeze detection"""
    try:
        info = yf.Ticker(ticker).info
        short_pct = info.get('shortPercentOfFloat', 0) or 0
        days_to_cover = info.get('daysToCover', 0) or 0
        short_ratio = info.get('shortRatio', 0) or 0
        
        # Squeeze score: short_pct * days_to_cover (higher = better squeeze candidate)
        squeeze_score = short_pct * days_to_cover * 100 if days_to_cover > 0 else 0
        
        return {
            'short_pct': short_pct,
            'days_to_cover': days_to_cover,
            'short_ratio': short_ratio,
            'squeeze_score': squeeze_score,
        }
    except Exception:
        return {'short_pct': 0, 'days_to_cover': 0, 'short_ratio': 0, 'squeeze_score': 0}


def _history(ticker, period):
    """Fetch daily history and drop rows with a NaN Close.

    yfinance often appends a placeholder row for the current (unfinished)
    session whose Close is NaN. Left in place that NaN propagates through
    every `.iloc[-1]` / rolling-mean call and silently corrupts the regime
    score and benchmarks, so we strip incomplete rows up front.
    """
    h = yf.Ticker(ticker).history(period=period)
    if not h.empty and 'Close' in h.columns:
        h = h[h['Close'].notna()]
    return h


def bench_perf(ticker):
    try:
        h = _history(ticker, "3mo")
        if len(h) >= 2:
            latest_close = float(h['Close'].iloc[-1])
            prev_close = float(h['Close'].iloc[-2])
            perf_today = round((latest_close / prev_close - 1) * 100, 2) if prev_close > 0 else None
        else:
            perf_today = None
        return {
            'ticker': ticker,
            'perf_today': perf_today,
            'perf_5d': pct(h['Close'], 5) or 0,
            'perf_15d': pct(h['Close'], 15) or 0,
            'perf_13w': pct(h['Close'], 60) or 0,  # ~13 weeks (60 trading days in 3mo)
        }
    except Exception:
        return {'ticker': ticker, 'perf_today': None, 'perf_5d': 0, 'perf_15d': 0, 'perf_13w': 0}


def _is_today_leader(r, bench_today, margin=TODAY_OUTPERFORMANCE_MARGIN):
    today_pct = r.get('Today %')
    if today_pct is None or bench_today is None:
        return False
    return today_pct >= bench_today + margin


def get_regime():
    regime_score = 0
    components = {}
    smh_bounce = {'bounce': False, 'drawdown': 0, 'recovery': 0}

    spy_hist = None
    try:
        spy_hist = _history('SPY', "2y")
        if len(spy_hist) >= 200:
            spy_price = float(spy_hist['Close'].iloc[-1])
            ma200 = float(spy_hist['Close'].rolling(200).mean().iloc[-1])
            ma200_prev = float(spy_hist['Close'].rolling(200).mean().iloc[-21])
            above_200 = spy_price > ma200
            ma200_rising = ma200 > ma200_prev
            components['spy_price'] = round(spy_price, 2)
            components['spy_ma200'] = round(ma200, 2)
            components['spy_above_200'] = above_200
            components['ma200_rising'] = ma200_rising
            if above_200:
                regime_score += 35
            if ma200_rising:
                regime_score += 15
    except Exception:
        pass

    try:
        rsp_hist = _history('RSP', "3mo")
        if len(rsp_hist) > 20 and spy_hist is not None and len(spy_hist) > 20:
             rsp_perf = pct(rsp_hist['Close'], 20)
             spy_perf = pct(spy_hist['Close'], 20)
             if rsp_perf is not None and spy_perf is not None:
                 components['rsp_vs_spy'] = round(rsp_perf - spy_perf, 2)
                 if rsp_perf > spy_perf:
                      regime_score += 10 # Expanding breadth!
                 else:
                      regime_score -= 10 # Contracting breadth!
    except Exception as e:
        print(f"Error calculating breadth: {e}")

    try:
        vix_hist = _history('^VIX', "5d")
        vix = round(float(vix_hist['Close'].iloc[-1]), 2)
        components['vix'] = vix
        vix_level = ("LOW" if vix < 15 else "NORMAL" if vix < 20 else
                     "ELEVATED" if vix < 25 else "HIGH" if vix < 30 else "EXTREME")
        components['vix_level'] = vix_level
        if vix < 18:
            regime_score += 25
        elif vix < 22:
            regime_score += 15
        elif vix < 28:
            regime_score += 5
        
        # V4: Volatility-adjusted allocation multiplier
        if vix < 12:
            regime_mult = 1.5  # OVERWEIGHT in extreme calm
        elif vix < 18:
            regime_mult = 1.2
        elif vix < 22:
            regime_mult = 1.0
        elif vix < 28:
            regime_mult = 0.7
        elif vix < 35:
            regime_mult = 0.4
        else:
            regime_mult = 0.2  # MINIMAL exposure
        components['regime_mult'] = regime_mult
    except Exception:
        pass

    try:
        # CNN now rejects bare User-Agent requests with HTTP 418 ("I'm a
        # teapot. You're a bot."). A full browser header set with the
        # edition.cnn.com Origin/Referer and sec-ch-ua hints gets a 200.
        r = requests.get(
            'https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
            headers={
                'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                               'AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/124.0.0.0 Safari/537.36'),
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://edition.cnn.com',
                'Referer': 'https://edition.cnn.com/',
                'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
            },
            timeout=15)
        r.raise_for_status()
        fng = int(round(float(r.json()['fear_and_greed']['score'])))
        fng_label = ("EXTREME FEAR" if fng <= 25 else "FEAR" if fng <= 45 else
                     "NEUTRAL" if fng <= 55 else "GREED" if fng <= 75 else "EXTREME GREED")
        components['fng'] = fng
        components['fng_label'] = fng_label
        # Heavily weighted two-sided sentiment. Fear is the dominant override:
        # extreme fear alone (-45) can knock the regime down ~1.5 tiers even
        # with a healthy trend, forcing a defensive posture.
        if fng > 75:        # EXTREME GREED
            regime_score += 25
        elif fng > 55:      # GREED
            regime_score += 15
        elif fng > 45:      # NEUTRAL
            regime_score += 0
        elif fng > 25:      # FEAR
            regime_score -= 25
        else:               # EXTREME FEAR
            regime_score -= 45
    except Exception:
        pass

    try:
        h = _history('SMH', "6mo")
        if len(h) >= 50:
            cur = float(h['Close'].iloc[-1])
            high50 = float(h['Close'].iloc[-50:].max())
            p5 = float(h['Close'].iloc[-5])
            dd = round((high50 - cur) / high50 * 100, 2)
            rec = round((cur - p5) / p5 * 100, 2)
            smh_bounce = {'bounce': dd > 15 and rec > 2, 'drawdown': dd, 'recovery': rec}
    except Exception:
        pass

    regime_score = max(0, min(100, regime_score))

    if regime_score >= 75:
        level, alloc, label = 4, 100, "FULL RISK ON"
    elif regime_score >= 55:
        level, alloc, label = 3, 60, "MODERATE"
    elif regime_score >= 35:
        level, alloc, label = 2, 30, "CAUTIOUS"
    else:
        # V3 strictly forces bear mode here
        level, alloc, label = 1, 0, "RISK OFF (BEAR MODE ACTIVE)"

    if smh_bounce['bounce'] and level <= 1:
        level, alloc, label = 2, 30, "CAUTIOUS (SMH DIP BUY)"

    return {
        'score': regime_score,
        'level': level,
        'allocation_pct': alloc,
        'label': label,
        'components': components,
        'smh_bounce': smh_bounce,
    }


def score_ticker(ticker, close_df, vol_df, open_df, high_df, low_df):
    if ticker not in close_df.columns:
        return None

    prices = close_df[ticker].dropna()
    vols = (vol_df[ticker].dropna() if ticker in vol_df.columns else pd.Series(dtype=float))
    highs = (high_df[ticker].dropna() if ticker in high_df.columns else pd.Series(dtype=float))
    lows = (low_df[ticker].dropna() if ticker in low_df.columns else pd.Series(dtype=float))

    if len(prices) < 10:
        return None

    price = float(prices.iloc[-1])
    if price <= 0:
        return None
        
    atr_14, atr_pct = 0, 0
    if len(prices) >= 14 and len(highs) == len(prices) and len(lows) == len(prices):
        tr1 = highs - lows
        tr2 = (highs - prices.shift()).abs()
        tr3 = (lows - prices.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_series = tr.rolling(14).mean().dropna()
        atr_14 = float(atr_series.iloc[-1]) if not atr_series.empty else 0
        atr_pct = round((atr_14 / price * 100), 2) if price > 0 else 0

    p3 = pct(prices, 3) or 0
    p5 = pct(prices, 5) or 0
    p15 = pct(prices, 15) or 0
    p13w = pct(prices, 60) or 0  # ~13 weeks (60 trading days in 3mo)

    ma20 = float(prices.tail(20).mean())
    ma50 = float(prices.tail(50).mean()) if len(prices) >= 50 else None
    ma200 = (float(prices.tail(200).mean())
             if len(prices) >= 200 else None)

    ma50_rising = False
    if ma50 is not None and len(prices) >= 70:
        ma50_prev = float(prices.iloc[:-20].tail(50).mean())
        ma50_rising = ma50 > ma50_prev

    above_200ma = price > ma200 if ma200 else False
    above_ma20 = price > ma20
    trend_quality = ma50_rising and above_200ma

    extension_pct = round((price - ma50) / ma50 * 100, 2) if ma50 and ma50 > 0 else 0
    overextended = extension_pct > OVEREXTENSION_PCT

    peak_window = min(len(prices), 126)
    peak = float(prices.tail(peak_window).max())
    drawdown_pct = round((price - peak) / peak * 100, 2) if peak > 0 else 0

    stop_distance = atr_14 * 2.5
    dynamic_stop_pct = round(((stop_distance) / peak * 100), 2) if peak > 0 else 15.0

    # V5: Hard drawdown stop — absolute max loss from peak
    is_lev = is_leveraged(ticker)
    hard_stop = LEVERAGED_HARD_STOP_PCT if is_lev else STOCK_HARD_STOP_PCT
    if MARGIN_MODE and not is_lev:
        hard_stop *= MARGIN_TIGHTEN_FACTOR  # Tighter stops on margin (stocks only)

    # Trigger stop if EITHER ATR-based or hard drawdown is hit
    atr_stop_triggered = drawdown_pct <= -dynamic_stop_pct
    hard_stop_triggered = drawdown_pct <= -hard_stop
    stop_triggered = atr_stop_triggered or hard_stop_triggered

    rate3 = (p3 / 3) if p3 > 0 else 0
    rate5 = (p5 / 5) if p5 > 0 else 0
    acceleration = rate3 > rate5 and p3 > 1.0

    avg20_vol = float(vols.tail(20).mean()) if len(vols) >= 20 else 0
    avg5_vol = float(vols.tail(5).mean()) if len(vols) >= 5 else 0
    vol_surge = round(avg5_vol / avg20_vol, 2) if avg20_vol > 0 else 1.0
    dollar_volume = price * avg20_vol

    near_high = ((peak - price) / peak < 0.02) if peak > 0 else False
    high20 = float(prices.tail(20).max())
    near_high_20 = ((high20 - price) / high20 < 0.02) if high20 > 0 else False
    breakout = near_high and vol_surge > 1.0

    today_pct = None
    if len(prices) >= 2:
        prev_close = float(prices.iloc[-2])
        if prev_close > 0:
            today_pct = round((price / prev_close - 1) * 100, 2)

    return {
        'Ticker': ticker,
        'Price': round(price, 2),
        'ATR': round(atr_14, 2),
        'ATR %': atr_pct,
        'Today %': today_pct,
        '3D %': p3,
        '5D %': p5,
        '15D %': p15,
        '13W %': p13w,
        'MA20': round(ma20, 2),
        'MA50': round(ma50, 2) if ma50 else None,
        'MA200': round(ma200, 2) if ma200 else None,
        'MA50 Rising': ma50_rising,
        'Above 200MA': above_200ma,
        'Above MA20': above_ma20,
        'Trend Quality': trend_quality,
        'Extension %': extension_pct,
        'Overextended': overextended,
        'Drawdown %': drawdown_pct,
        'Stop Triggered': stop_triggered,
        'Hard Stop Triggered': hard_stop_triggered,
        'Hard Stop %': hard_stop,
        'Stop Threshold': dynamic_stop_pct,
        'Vol Surge': vol_surge,
        'Acceleration': acceleration,
        'Breakout': breakout,
        'Near High': near_high_20,
        'Dollar Volume': dollar_volume,
        'Leveraged': is_leveraged(ticker),
        # TradingView Technicals (populated later via fetch_tradingview_technicals)
        'TV_Oscillators': 'NEUTRAL',
        'TV_MovingAverages': 'NEUTRAL',
        'TV_RSI': None,
        'TV_MACD': None,
        # V4: Entry Quality & Squeeze Detection
        'Entry Quality': 'MOMENTUM',
        'Short Interest': 0,
        'Days To Cover': 0,
        'Squeeze Score': 0,
    }


def calc_composite(r, bench, regime_level=4):
    def clamp(x):
        return max(-500.0, min(500.0, x))

    p3 = clamp(r.get('3D %', 0) or 0)
    p5 = clamp(r.get('5D %', 0) or 0)
    p15 = clamp(r.get('15D %', 0) or 0)
    p13w = clamp(r.get('13W %', 0) or 0)  # 13-week (month) performance
    vs = r.get('Vol Surge', 1.0)
    atr_pct = r.get('ATR %', 5.0)

    # V2: Regime-adaptive momentum weights
    if regime_level >= 4:  # FULL RISK ON
        w3, w5, w15, w13w = 3.5, 2.0, 0.8, 1.5
    elif regime_level == 3:  # MODERATE
        w3, w5, w15, w13w = 3.0, 2.0, 1.0, 1.2
    elif regime_level == 2:  # CAUTIOUS - weight longer term
        w3, w5, w15, w13w = 1.5, 2.0, 2.0, 1.5
    else:  # RISK OFF
        w3, w5, w15, w13w = -1.0, -1.5, -2.0, -1.0

    abs_mom = p3 * w3 + p5 * w5 + p15 * w15 + p13w * w13w

    rel_vs_5 = p5 - bench.get('perf_5d', 0)
    rel_vs_15 = p15 - bench.get('perf_15d', 0)
    rel_vs_13w = p13w - bench.get('perf_13w', 0)
    if rel_vs_5 > 5:
        abs_mom += rel_vs_5 * 1.5
    if rel_vs_15 > 10:
        abs_mom += rel_vs_15 * 0.8
    if rel_vs_13w > 15:
        abs_mom += rel_vs_13w * 1.0

    if r.get('Trend Quality'):
        abs_mom *= 1.20
    elif r.get('Above 200MA'):
        abs_mom *= 1.05
    elif r.get('MA50 Rising'):
        abs_mom *= 0.90
    else:
        abs_mom *= 0.60

    if r.get('Acceleration'):
        abs_mom *= 1.15
    if r.get('Breakout'):
        abs_mom *= 1.08
    if vs > 1.5:
        abs_mom *= 1.08

    if 'BEAR' in bench.get('name',''):
        # Don't penalize overextension as heavily in bear panics
        pass
    else:
        if r.get('Overextended'):
            ext = r.get('Extension %', 0)
            penalty = max(0.70, 1.0 - (ext - OVEREXTENSION_PCT) / 100)
            abs_mom *= penalty
        if p5 > 35:
            abs_mom *= 0.85
        if p5 > 50:
            abs_mom *= 0.80
        if p15 > 80:
            abs_mom *= 0.80

    if p3 <= 0:
        abs_mom *= 0.65

    if r.get('Stop Triggered'):
        abs_mom *= 0.15

    # V5: Progressive drawdown penalty — score drops as you approach hard stop
    drawdown = abs(r.get('Drawdown %', 0) or 0)
    hard_stop = r.get('Hard Stop %', 25) or 25
    if drawdown > 0 and hard_stop > 0:
        # At 50% of hard stop: slight penalty. At 75%: heavy penalty. At 100%: destroyed.
        dd_ratio = drawdown / hard_stop
        if dd_ratio >= 0.75:
            abs_mom *= 0.30  # Near stop — almost certainly SELL
        elif dd_ratio >= 0.50:
            abs_mom *= 0.55  # Getting dangerous
        elif dd_ratio >= 0.25:
            abs_mom *= 0.80  # Caution zone

    # V2: Volatility-normalized score (ATR penalty for high-vol names)
    if atr_pct > 0:
        atr_factor = min(2.0, max(0.5, atr_pct / 5.0))
        abs_mom = abs_mom / atr_factor

    # V2: Score ceiling + floor
    final_score = max(SCORE_FLOOR, min(SCORE_CEILING, round(abs_mom, 2)))
    
    # V4: Pullback Entry Bonus - reward RSI < 50 entries
    rsi = r.get('TV_RSI', 50)
    is_bear_mode = 'BEAR' in bench.get('name', '')
    if rsi and rsi < PULLBACK_RSI_MAX and not is_bear_mode:
        pullback_pct = (PULLBACK_RSI_MAX - rsi) / PULLBACK_RSI_MAX
        final_score *= (1.0 + 0.15 * pullback_pct)
    
    # V4: Short Squeeze Candidate Bonus
    squeeze_score = r.get('Squeeze Score', 0)
    if squeeze_score > SQUEEZE_BONUS:
        final_score += SQUEEZE_BONUS
    
    return final_score


def _signal(r, freq, rank, total, regime_level=4):
    score = r.get('Score', 0)
    vs = r.get('Vol Surge', 1.0)
    acc = r.get('Acceleration', False)
    brk = r.get('Breakout', False)
    p3 = r.get('3D %', 0)
    atr_pct = r.get('ATR %', 0)

    # V5: HARD STOP OVERRIDE — force SELL regardless of other signals
    if r.get('Hard Stop Triggered', False):
        return 'SELL'
    if r.get('Stop Triggered', False):
        return 'SELL'

    if score <= 0:
        return 'SELL'
    top_half = rank <= max(total // 2, 1)
    has_momentum = vs > 1.3 or acc
    is_conviction = (score > 150 and freq >= 6 and atr_pct < 8.0 and
                 not r.get('Overextended', False))
    is_speculative = (score > 80 and freq >= 4 and acc)
    if is_conviction:
        return 'STRONG BUY'
    if score > 100 and has_momentum and freq >= 3:
        return 'BUY'
    if top_half and has_momentum and freq >= 2:
        return 'BUY'
    if top_half and freq >= 4:
        return 'BUY'
    if score > 50 and has_momentum:
        return 'BUY'
    if score > 50 and brk:
        return 'BUY'
    if p3 > 3 and vs > 1.5:
        return 'BUY'
    if is_speculative:
        return 'BUY'
    return 'HOLD'


def _position_size(r, freq, signal, regime_level=4, is_bear_mode=False):
    """
    Calculate position size: use BOTH Score AND TradingView signals for differentiation.
    Each ticker gets a unique % based on multiple factors.
    Best score gets full size, others get 10% less.
    """
    # Get TradingView signals
    tv_osc = r.get('TV_Oscillators', 'NEUTRAL')
    tv_ma = r.get('TV_MovingAverages', 'NEUTRAL')
    tv_rsi = r.get('TV_RSI', 50)
    
    # Convert TV signals to numeric scores
    tv_score = 0
    if tv_osc == 'BUY': tv_score += 15
    elif tv_osc == 'SELL': tv_score -= 15
    if tv_ma == 'BUY': tv_score += 15
    elif tv_ma == 'SELL': tv_score -= 15
    # RSI extremes bonus/penalty
    if tv_rsi and tv_rsi < 30: tv_score += 10  # Oversold
    elif tv_rsi and tv_rsi > 70: tv_score -= 10  # Overbought
    
    score = r.get('Score', 0)
    atr_pct = r.get('ATR %', 5.0)
    
    # Base from our signal
    if signal == 'STRONG BUY':
        base = 20
    elif signal == 'BUY':
        base = 10
    else:
        return '5%'
    
    # Score multiplier - more granular
    if score >= 150:
        score_mult = 2.5
    elif score >= 120:
        score_mult = 2.0
    elif score >= 100:
        score_mult = 1.5
    elif score >= 80:
        score_mult = 1.2
    elif score >= 60:
        score_mult = 1.0
    elif score >= 40:
        score_mult = 0.8
    else:
        score_mult = 0.5
    
    # Frequency multiplier
    if freq >= 8:
        freq_mult = 1.2
    elif freq >= 6:
        freq_mult = 1.15
    elif freq >= 4:
        freq_mult = 1.1
    elif freq >= 2:
        freq_mult = 1.0
    else:
        freq_mult = 0.9
    
    # ATR multiplier
    if atr_pct <= 3:
        atr_mult = 1.1
    elif atr_pct <= 5:
        atr_mult = 1.0
    elif atr_pct <= 8:
        atr_mult = 0.9
    else:
        atr_mult = 0.7
    
    # Regime multiplier
    if regime_level >= 4:
        regime_mult = 1.2
    elif regime_level == 3:
        regime_mult = 1.0
    elif regime_level == 2:
        regime_mult = 0.6
    else:
        regime_mult = 0.3
    
    # Bear mode
    bear_mult = 0.5 if is_bear_mode else 1.0
    
    # V4: Pullback entry bonus - bigger size for pullback setups
    entry_quality = r.get('Entry Quality', 'MOMENTUM')
    pullback_mult = 1.15 if entry_quality == 'PULLBACK' else 1.0
    
    # V4: Short squeeze bonus - size up for squeeze candidates
    squeeze_score = r.get('Squeeze Score', 0)
    squeeze_mult = 1.20 if squeeze_score > SQUEEZE_BONUS else 1.0
    
    # Calculate raw size
    raw = base * score_mult * freq_mult * atr_mult * regime_mult * bear_mult * pullback_mult * squeeze_mult
    
    # Add TV signal bonus (before clamping)
    final = raw + tv_score
    
    # Clamp to valid sizes with fine-grained differentiation
    if final >= 60:
        return '50%'
    elif final >= 45:
        return '40%'
    elif final >= 32:
        return '30%'
    elif final >= 22:
        return '25%'
    elif final >= 14:
        return '20%'
    elif final >= 8:
        return '15%'
    elif final >= 4:
        return '10%'
    else:
        return '5%'


def _get_adjusted_size(r, rank_in_list, best_score, freq, signal, regime_level=4, is_bear_mode=False):
    """
    Get position size with rank adjustment.
    Best score (rank 1) gets full size, others get 10% less.
    """
    base_size = _position_size(r, freq, signal, regime_level, is_bear_mode)
    
    # Remove % sign and convert to number
    if base_size.endswith('%'):
        size_val = int(base_size.replace('%', ''))
    else:
        return base_size
    
    # If not the best score (rank > 1), reduce by 10%
    if rank_in_list > 1 and size_val > 10:
        new_size = size_val - 10
        # Ensure minimum 5%
        if new_size < 5:
            new_size = 5
        return f'{new_size}%'
    
    return base_size


def run_screener(name, finviz_urls, bench_ticker, min_dv=30e6,
                 dl_limit=200, top_n=15, ensure_ticker=None, ticker_list=None,
                 force_show=False):
    print(f"\n[{name}]")
    bench = bench_perf(bench_ticker)
    bench['name'] = name
    print(
        f"   {bench_ticker} - Today: {fmt_pct(bench['perf_today'])} | "
        f"5D: {bench['perf_5d']:+.2f}% | 15D: {bench['perf_15d']:+.2f}% | 13W: {bench['perf_13w']:+.2f}%"
    )

    all_tickers = []
    if ticker_list is not None:
        all_tickers = ticker_list
        print(f"   Using {len(all_tickers)} provided tickers")
    else:
        if isinstance(finviz_urls, str):
            finviz_urls = [finviz_urls]
        seen = set()
        for url in finviz_urls:
            for t in fetch_finviz(url):
                if t not in seen:
                    seen.add(t)
                    all_tickers.append(t)
        print(f"   Found {len(all_tickers)} unique tickers "
              f"from {len(finviz_urls)} source(s)")

    dl = list(set(all_tickers[:dl_limit]) |
              ({ensure_ticker} if ensure_ticker else set()))
    # Use longer period for watchlist to get enough data for proper MA calculations
    period = "3mo" if "WATCHLIST" in name else "1y"
    print(f"   Downloading {len(dl)} tickers (period={period})...")

    down_data = bulk_download(dl, period=period)
    if len(down_data) < 5: 
        return [], bench
    close_df, vol_df, open_df, high_df, low_df = down_data

    results, ref_data = [], None
    total = len(dl)

    for i, t in enumerate(dl):
        sys.stdout.write(f"\r   Scoring {i+1}/{total}  " + " " * 20)
        sys.stdout.flush()
        d = score_ticker(t, close_df, vol_df, open_df, high_df, low_df)
        if d is None:
            continue

        d['Score'] = calc_composite(d, bench)

        if t == ensure_ticker:
            ref_data = dict(d)

        if d['Dollar Volume'] < min_dv:
            if 'WATCHLIST' in name:
                pass  # Skip volume filter for watchlist
            else:
                continue
        # V2: Micro-cap filter - exclude low-priced high-vol stocks
        price = d.get('Price', 0)
        atr_pct = d.get('ATR %', 0)
        is_stock = 'STOCK' in name and not d.get('Leveraged', False)
        if is_stock and 'WATCHLIST' not in name:
            if price > 0 and price < MIN_PRICE:
                continue  # Too cheap - pump-and-dump risk
            if atr_pct > MAX_ATR_PCT_STOCK:
                continue  # Too volatile for stable positions
        if not d.get('Above MA20') and t != ensure_ticker:
            pass # V3 allows testing even if under MA20 if momentum is wild (ATR checks cover stops)
        if d['3D %'] <= 0 and d['5D %'] <= 0 and t != ensure_ticker:
            if 'WATCHLIST' not in name and 'BULL' not in name:
                continue

        # V2: Hard exclude stop-triggered (skip for watchlist)
        if d.get('Stop Triggered', False) and 'WATCHLIST' not in name:
            continue
        
        # V2: Short interest filter for stocks (skip high short interest - squeeze risk)
        if is_stock:
            short_pct = d.get('Short Interest', 0)  # Would need to fetch externally
            if short_pct > SHORT_INTEREST_THRESHOLD:
                continue  # Too much short interest - avoid

        if 'STOCK' in name:
            # Stocks: only require beating benchmark on ONE time frame (relaxed from ALL)
            beats_bench = (
                _is_today_leader(d, bench['perf_today'])
                or d['5D %'] > bench['perf_5d']
                or d['15D %'] > bench['perf_15d']
            )
        else:
            beats_bench = (d['5D %'] > bench['perf_5d']
                           or d['15D %'] > bench['perf_15d'])

        if 'BULL' in name:
            beats_bench = True

        if "BEAR" in name:
            beats_bench = True # If Bear mode, allow inverse ETFs to pass easily to spot best

        if beats_bench or 'WATCHLIST' in name:
            results.append(d)

    sys.stdout.write("\r" + " " * 50 + "\r")
    sys.stdout.flush()

    sys.stdout.write(f"   Enriching names for {len(results)} tickers...")
    sys.stdout.flush()
    for d in results:
        d['Name'] = enrich_name(d['Ticker'])
    if ref_data:
        ref_data['Name'] = enrich_name(ref_data['Ticker'])
    sys.stdout.write(f"\r   {len(results)} passed filters" + " " * 20 + "\n")
    sys.stdout.flush()

    if (ensure_ticker and ref_data
            and not any(r['Ticker'] == ensure_ticker for r in results)):
        ref_data['_reference'] = True
        results.append(ref_data)

    # === Compute TradingView-style Technicals from local data ===
    if results:
        sys.stdout.write(f"   Computing TV-style technicals for {len(results)} tickers...")
        sys.stdout.flush()
        for r in results:
            tv = compute_tv_technicals_from_data(
                r['Ticker'], close_df, high_df, low_df, open_df
            )
            r['TV_Oscillators'] = tv.get('oscillators', 'NEUTRAL')
            r['TV_MovingAverages'] = tv.get('moving_averages', 'NEUTRAL')
            r['TV_RSI'] = tv.get('rsi')
            r['TV_MACD'] = tv.get('macd')
            # Copy counts for display
            r['osc_buy'] = tv.get('osc_buy', 0)
            r['osc_sell'] = tv.get('osc_sell', 0)
            r['osc_neutral'] = tv.get('osc_neutral', 0)
            r['ma_buy'] = tv.get('ma_buy', 0)
            r['ma_sell'] = tv.get('ma_sell', 0)
            r['ma_neutral'] = tv.get('ma_neutral', 0)
            # V4: Update Entry Quality based on pullback detection
            r['Entry Quality'] = tv.get('entry_quality', 'MOMENTUM')
        sys.stdout.write(f"\r   TV-style technicals computed." + " " * 20 + "\n")
        sys.stdout.flush()
        
        # V4: Enrich with short squeeze data
        sys.stdout.write(f"   Fetching short interest data for {len(results)} tickers...")
        sys.stdout.flush()
        for r in results:
            sq_data = get_short_squeeze_data(r['Ticker'])
            r['Short Interest'] = sq_data['short_pct']
            r['Days To Cover'] = sq_data['days_to_cover']
            r['Squeeze Score'] = sq_data['squeeze_score']
        sys.stdout.write(f"\r   Short interest data fetched." + " " * 20 + "\n")
        sys.stdout.flush()

    results.sort(key=lambda x: x.get('Dollar Volume', 0), reverse=True)
    return results[:top_n], bench


def save_history(stocks, etfs=None, bear_etfs=None):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    if datetime.now().weekday() >= 5:
        print("   Skipping (weekend)")
        return
    path = os.path.join(HISTORY_DIR, f'{datetime.now():%Y-%m-%d}.json')
    data = {'stocks': stocks}
    if etfs:
        data['etfs'] = etfs
    if bear_etfs:
        data['bear_etfs'] = bear_etfs
    with open(path, 'w') as f:
        json.dump(data, f)
    print(f"   Saved history: {path}")
    files = sorted(os.listdir(HISTORY_DIR))
    for old in files[:-MAX_HISTORY]:
        os.remove(os.path.join(HISTORY_DIR, old))


def load_history():
    sh, eh, bh = [], [], []
    if not os.path.exists(HISTORY_DIR):
        return sh, eh, bh
    for fname in sorted(os.listdir(HISTORY_DIR))[-MAX_HISTORY:]:
        with open(os.path.join(HISTORY_DIR, fname)) as f:
            data = json.load(f)
        date = fname.replace('.json', '')
        if 'stocks' in data:
            sh.append({'date': date, 'stocks': data['stocks']})
        if 'etfs' in data:
            eh.append({'date': date, 'etfs': data['etfs']})
        if 'bear_etfs' in data:
            bh.append({'date': date, 'bear_etfs': data['bear_etfs']})
    return sh, eh, bh


def freq_count(history, key='stocks'):
    c = Counter()
    for day in history:
        for item in day.get(key, []):
            c[item['Ticker']] += 1
    return c


CSS = """* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0d1117; color:#c9d1d9; min-height:100vh; padding:2rem; }
.c { max-width:1200px; margin:0 auto; }
h1 { font-size:1.5rem; margin-bottom:.5rem; color:#58a6ff; }
h2 { font-size:1.1rem; margin:1.5rem 0 .5rem; color:#8b949e; border-bottom:1px solid #30363d; padding-bottom:.5rem; }
.date { color:#8b949e; font-size:.875rem; margin-bottom:2rem; }
.sig { font-size:2.5rem; font-weight:bold; margin:1rem 0; padding:1rem 2rem; border-radius:.5rem; display:inline-block; }
.sig.buy { background:#238636; color:#fff; }
.sig.sell { background:#da3633; color:#fff; }
.sig.cautious { background:#9e6a03; color:#fff; }
.metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1rem; margin-bottom:2rem; }
.m { background:#161b22; padding:1rem; border-radius:.5rem; }
.ml { color:#8b949e; font-size:.75rem; margin-bottom:.25rem; }
.mv { font-size:1.25rem; font-weight:600; }
.ms { font-size:.75rem; margin-top:.25rem; color:#8b949e; }
table { width:100%; border-collapse:collapse; margin-bottom:1rem; }
th,td { text-align:left; padding:.5rem .75rem; border-bottom:1px solid #30363d; }
th { color:#8b949e; font-weight:500; font-size:.75rem; text-transform:uppercase; }
td { font-size:.85rem; } tr:hover { background:#161b22; }
.tv-sig { display:inline-block; padding:2px 8px; border-radius:4px; font-size:0.7rem; font-weight:600; text-align:center; min-width:52px; }
.tv-buy { background:#1a4d2e; color:#4ade80; }
.tv-sell { background:#4d1a1a; color:#f87171; }
.tv-neutral { background:#3d3d1a; color:#facc15; }
.tv-header { font-size:0.65rem; line-height:1.2; }
.copy-btn { background:#238636; color:#fff; border:1px solid #2ea043; padding:6px 14px; border-radius:6px; cursor:pointer; font-size:0.8rem; font-weight:600; margin:0 0 8px 8px; }
.copy-btn:hover { background:#2ea043; }
.copy-btn:active { background:#1f6f2e; }
.top-featured { margin-bottom:1rem; }
.top-featured h2 { color:#00ff7f; border-bottom:1px solid #00ff7f40; }
.top-featured tr:hover td { background:rgba(0,255,127,0.05); }
.sell-section { background:linear-gradient(135deg, #2d1a1a 0%, #251515 100%); border:1px solid #f85149; border-radius:6px; margin-bottom:1rem; }
.sell-section h2 { color:#f85149; border-bottom:1px solid #f8514940; }
.avoid-section { background:linear-gradient(135deg, #2d1a1a 0%, #200d0d 100%); border:1px solid #6e4066; border-radius:6px; margin-bottom:1rem; }
.clickable-row { cursor:pointer; transition:background 0.2s; }
.clickable-row:hover { background:rgba(0,255,127,0.1); }
.clickable-row.expanded td:first-child::before { content:"▼ "; }
.clickable-row td:first-child::before { content:"▶ "; }
.details-row td { background:#0d1117; padding:12px; }
.detail-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:8px; }
.detail-item { background:#1a1f2e; border-radius:6px; padding:8px; text-align:center; }
.detail-label { font-size:0.7rem; color:#8b949e; text-transform:uppercase; margin-bottom:4px; }
.detail-value { font-size:0.9rem; font-weight:bold; color:#c9d1d9; }
.detail-value.warning { color:#d29922; }
.detail-value.sell { color:#f85149; }
.avoid-section h2 { color:#f87171; border-bottom:1px solid #6e406640; }
.clickable-row { cursor:pointer; transition:background 0.2s; }
.clickable-row:hover td { background:rgba(0,255,127,0.1); }
.clickable-row:active td { background:rgba(0,255,127,0.15); }
.details-row td { background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%); padding:12px 16px; font-size:0.85rem; }
.detail-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(140px,1fr)); gap:8px; }
.detail-item { background:rgba(255,255,255,0.05); padding:8px 10px; border-radius:4px; }
.detail-label { color:#888; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.5px; }
.detail-value { color:#00ff7f; font-weight:bold; margin-top:2px; }
.detail-value.sell { color:#f85149; }
.detail-value.warning { color:#f0883e; }
"""

def _sig_html(sig):
    colors = {'STRONG BUY': '#00ff7f', 'BUY': '#3fb950',
              'SELL': '#f85149', 'HOLD': '#d29922'}
    c = colors.get(sig, '#d29922')
    return f"<span style='color:{c};font-weight:bold'>{sig}</span>"

def _tv_sig_html(sig, tooltip=""):
    """Render TradingView signal as styled badge - simple BUY/SELL/NEUTRAL"""
    cls = {'BUY': 'tv-buy', 'SELL': 'tv-sell', 'NEUTRAL': 'tv-neutral'}.get(sig, 'tv-neutral')
    title_attr = f' title="{tooltip}"' if tooltip else ''
    return f"<span class='tv-sig {cls}'{title_attr}>{sig}</span>"

def _format_tv_signal(r):
    """Format TV signals - simple BUY/SELL/NEUTRAL like TradingView Summary"""
    # Get the oscillator signal (BUY/SELL/NEUTRAL)
    osc_sig = r.get('TV_Oscillators', 'NEUTRAL')
    
    # Get the moving average signal (BUY/SELL/NEUTRAL)
    ma_sig = r.get('TV_MovingAverages', 'NEUTRAL')
    
    return osc_sig, ma_sig

def generate_html_v3(regime, sections, mode, all_freqs):
    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    alloc = regime['allocation_pct']
    regime_cls = 'sell' if alloc == 0 else ('buy' if alloc >= 60 else 'cautious')
    
    c = regime['components']
    spy_p = c.get('spy_price', 'N/A')
    spy_ma = c.get('spy_ma200', 'N/A')
    spy_above = c.get('spy_above_200', False)

    # CNN Fear & Greed tile — color by sentiment (red=fear, green=greed)
    fng = c.get('fng')
    fng_label = c.get('fng_label', 'N/A')
    if fng is None:
        fng_disp, fng_color = 'N/A', '#8b949e'
    else:
        fng_disp = fng
        fng_color = ('#f85149' if fng <= 25 else '#f0883e' if fng <= 45
                     else '#d29922' if fng <= 55 else '#3fb950' if fng <= 75
                     else '#00ff7f')

    # JavaScript for expandable rows - stored outside f-string to avoid parsing issues
    toggle_js = """
<script>
function toggleDetails(row){
  var next=row.nextElementSibling;
  if(next && next.classList.contains('details-row')){
    var cur=next.style.display;
    next.style.display=(cur==='none' || cur==='')?'table-row':'none';
    row.classList.toggle('expanded');
  }
}
function copyWatchlist(){
  var watchlistData = window.watchlistData || [];
  if(watchlistData.length === 0){
    alert('No watchlist data available');
    return;
  }
  var text = 'today stocks:\\n';
  watchlistData.forEach(function(item){
    text += item.ticker + ' ' + item.signal + '\\n';
  });
  navigator.clipboard.writeText(text).then(function(){
    var btn = document.querySelector('.copy-btn');
    var orig = btn.textContent;
    btn.textContent = '✓ Copied!';
    setTimeout(function(){ btn.textContent = orig; }, 2000);
  }).catch(function(err){
    alert('Failed to copy: ' + err);
  });
}
document.addEventListener('DOMContentLoaded',function(){
  var tables=document.querySelectorAll('table');
  tables.forEach(function(tbl){
    var ths=tbl.querySelectorAll('th');
    ths.forEach(function(th){
      if(th.textContent.indexOf('Today')!==-1){
        th.style.cursor='pointer';
        th.addEventListener('click',function(){
          var idx=Array.prototype.slice.call(ths).indexOf(th);
          var cells=tbl.querySelectorAll('td:nth-child('+(idx+1)+')');
          cells.forEach(function(c){c.style.display=(c.style.display==='none')?'table-cell':'none';});
        });
      }
    });
  });
});
</script>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>V4 Master Router: {mode}</title><style>{CSS}</style>{toggle_js}
</head>
<body><div class="c">
<h1>V4 Master Router ({mode})</h1><p class="date">{now}</p>
<div class="sig {regime_cls}">{regime['label']}</div>
<button class="copy-btn" onclick="copyWatchlist()">📋 Copy Watchlist</button>
<div class="metrics">
<div class="m"><div class="ml">Regime Score</div><div class="mv">{regime['score']}/100</div></div>
<div class="m"><div class="ml">Allocation</div><div class="mv">{alloc}%</div><div class="ms">{'OVERWEIGHT' if c.get('regime_mult', 1.0) > 1.0 else ('UNDERWEIGHT' if c.get('regime_mult', 1.0) < 1.0 else 'NEUTRAL')}</div></div>
<div class="m"><div class="ml">Breadth Eq-W vs SPY</div><div class="mv">{regime['components'].get('rsp_vs_spy', 'N/A')}%</div></div>
<div class="m"><div class="ml">VIX</div><div class="mv">{regime['components'].get('vix', 'N/A')}</div></div>
<div class="m"><div class="ml">Fear &amp; Greed</div><div class="mv" style="color:{fng_color}">{fng_disp}</div><div class="ms">{fng_label}</div></div>
<div class="m"><div class="ml">SPY vs 200MA</div><div class="mv">${spy_p}</div><div class="ms">{'ABOVE' if spy_above else 'BELOW'} ${spy_ma}</div></div>
<div class="m"><div class="ml">Margin Mode</div><div class="mv" style="color:{'#f85149' if MARGIN_MODE else '#3fb950'}">{'⚠️ ON' if MARGIN_MODE else 'OFF'}</div><div class="ms">{'Stops tightened 20%' if MARGIN_MODE else 'Normal stops'}</div></div>
<div class="m"><div class="ml">Hard Stops</div><div class="mv">{LEVERAGED_HARD_STOP_PCT:.0f}%/{STOCK_HARD_STOP_PCT * (MARGIN_TIGHTEN_FACTOR if MARGIN_MODE else 1):.0f}%</div><div class="ms">Leveraged / Stock (from peak)</div></div>
</div>"""

    def table(results, title, exclude_cols=None):
        if exclude_cols is None:
            exclude_cols = []
        
        # Determine section styling class
        section_class = ""
        if "★ TOP 5" in title:
            section_class = "top-featured"
        elif "SELL Signal" in title:
            section_class = "sell-section"
        elif "Score ≤ 0" in title:
            section_class = "avoid-section"
        
        is_sell_section = 'SELL' in title

        # Number of visible columns in this table — used for the details-row
        # colspan so the expanded panel spans the full width.
        # Main columns: #, Ticker, [Today, [15D]], $Vol, Score, Signal, BUY TODAY.
        # ATR% and Freq now live inside the click-to-expand details panel.
        ncols = 2  # '#', 'Ticker'
        if 'Today' not in exclude_cols:
            ncols += 1
            if '15D' not in exclude_cols:
                ncols += 1
        ncols += 2  # '$Vol', 'Score'
        ncols += 1  # 'Signal'
        ncols += 1  # 'BUY TODAY'

        rows = ""
        for i, r in enumerate(results[:15], 1):
            tk = r['Ticker']
            freq = all_freqs.get(tk, 0)
            s = _signal(r, freq, i, len(results), regime.get('level', 4))
            best_score = results[0].get('Score', 0) if results else 0
            size = _get_adjusted_size(r, i, best_score, freq, s, regime.get('level', 4), 'BEAR' in mode)
            cols = f"<td>{i}</td><td>{tk}</td>"
            if 'Today' not in exclude_cols:
                cols += f"<td>{fmt_pct(r.get('Today %'))}</td>"
                if '15D' not in exclude_cols:
                    cols += f"<td>{fmt_pct(r.get('15D %'))}</td>"
            cols += f"<td>{fmt_dollar_volume(r.get('Dollar Volume'))}</td><td>{r.get('Score', 0):.0f}</td>"

            # Own Technical Analysis (script)
            cols += f"<td>{_sig_html(s)}</td>"
            
            # TradingView signals still used for BUY TODAY calculation but not shown as columns
            osc, ma = _format_tv_signal(r)

            # V4: Calculate BUY TODAY % for max return - based on score + TV signals
            score = r.get('Score', 0)
            tv_rsi = r.get('TV_RSI', 50)
            tv_score = (10 if osc == 'BUY' else -10 if osc == 'SELL' else 0) + \
                       (10 if ma == 'BUY' else -10 if ma == 'SELL' else 0) + \
                       (5 if tv_rsi and tv_rsi < 30 else -5 if tv_rsi and tv_rsi > 70 else 0)
            base = 10 if s == 'STRONG BUY' else 5 if s == 'BUY' else 2
            score_mult = 2.0 if score >= 150 else 1.5 if score >= 120 else 1.2 if score >= 100 else 1.0 if score >= 80 else 0.8
            raw = base * score_mult
            buy_today_pct = max(2, min(25, int(raw + tv_score)))
            
            # Position Size - show BUY TODAY % with green highlight
            cols += f"<td style='font-weight:bold;color:#00ff7f'>{buy_today_pct}%</td>"
            
            # Build hidden details for expanded view
            short_pct = r.get('Short Interest', 0) or 0
            si_cls = 'sell' if short_pct >= 20 else 'warning' if short_pct >= 10 else ''
            days_3 = r.get('3D %', 0) or 0
            days_5 = r.get('5D %', 0) or 0
            days_15 = r.get('15D %', 0) or 0
            weeks_13 = r.get('13W %', 0) or 0
            vol_surge = r.get('Vol Surge') or 1.0
            accel = '✓' if r.get('Acceleration', False) else '✗'
            breakout = '✓' if r.get('Breakout', False) else '✗'
            overext = r.get('Overextended', False)
            sq_score = r.get('Squeeze Score') or 0
            days_cover = r.get('Days to Cover') or 0
            tv_rsi_val = tv_rsi if tv_rsi else 0
            drawdown = r.get('Drawdown %', 0) or 0
            hard_stop_pct = r.get('Hard Stop %', 25) or 25
            hard_stop_hit = r.get('Hard Stop Triggered', False)
            atr_pct_val = r.get('ATR %', 0) or 0

            details = f"""
            <tr class="details-row" style="display:none">
                <td colspan="{ncols}">
                    <div class="detail-grid">
                        <div class="detail-item"><div class="detail-label">ATR %</div><div class="detail-value {'warning' if atr_pct_val > 8 else ''}">{atr_pct_val:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">Freq</div><div class="detail-value">{freq}/{MAX_HISTORY}</div></div>
                        <div class="detail-item"><div class="detail-label">Drawdown from Peak</div><div class="detail-value {'sell' if hard_stop_hit else ('warning' if drawdown < -10 else '')}">{drawdown:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">Hard Stop</div><div class="detail-value {'sell' if hard_stop_hit else ''}">{'-' if hard_stop_hit else ''}{hard_stop_pct:.0f}% {'⚠️ TRIGGERED' if hard_stop_hit else ''}</div></div>
                        <div class="detail-item"><div class="detail-label">Margin Mode</div><div class="detail-value {'warning' if MARGIN_MODE else ''}">{'ON ⚠️' if MARGIN_MODE else 'OFF'}</div></div>
                        <div class="detail-item"><div class="detail-label">Short Int.</div><div class="detail-value {si_cls}">{short_pct:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">Days to Cover</div><div class="detail-value">{days_cover:.1f}</div></div>
                        <div class="detail-item"><div class="detail-label">Squeeze Score</div><div class="detail-value {'warning' if sq_score > 5 else ''}">{sq_score:.1f}</div></div>
                        <div class="detail-item"><div class="detail-label">3D %</div><div class="detail-value {'warning' if days_3 >= 10 else ''}">{days_3:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">5D %</div><div class="detail-value">{days_5:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">15D %</div><div class="detail-value">{days_15:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">13W %</div><div class="detail-value">{weeks_13:.1f}%</div></div>
                        <div class="detail-item"><div class="detail-label">Vol Surge</div><div class="detail-value {'warning' if vol_surge >= 2 else ''}">{vol_surge:.1f}x</div></div>
                        <div class="detail-item"><div class="detail-label">Acceleration</div><div class="detail-value {'sell' if accel == '✗' else ''}">{accel}</div></div>
                        <div class="detail-item"><div class="detail-label">Breakout</div><div class="detail-value {'warning' if breakout == '✓' else 'sell'}">{breakout}</div></div>
                        <div class="detail-item"><div class="detail-label">Overextended</div><div class="detail-value {'sell' if overext else ''}">{'✗' if overext else '✓'}</div></div>
                        <div class="detail-item"><div class="detail-label">TV RSI</div><div class="detail-value {'warning' if tv_rsi_val and tv_rsi_val < 30 else ('sell' if tv_rsi_val and tv_rsi_val > 70 else '')}">{tv_rsi_val:.0f}</div></div>
                    </div>
                </td>
            </tr>
            """
            
            rows += f"<tr class='clickable-row' onclick='toggleDetails(this)'>{cols}</tr>{details}"
        
        header = "<th>#</th><th>Ticker</th>"
        if 'Today' not in exclude_cols:
            header += "<th>Today</th>"
            if '15D' not in exclude_cols:
                header += "<th>15D</th>"
        header += "<th>$Vol</th><th>Score</th>"
        header += "<th class='tv-header'>Signal</th>"
        header += "<th class='tv-header'>BUY<br>TODAY</th>"
        
        section_attrs = f" class='{section_class}'" if section_class else ""
        return f"<h2>{title}</h2><table{section_attrs}><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>"

    for item in sections:
        if len(item) == 3:
            results, title, exclude_cols = item
        else:
            results, title = item
            exclude_cols = []
        if results:
            html += table(results, title, exclude_cols)
            
    hist_rows = ""
    for rank, (ticker, cnt) in enumerate(all_freqs.most_common(20), 1):
        hist_rows += (f"<tr><td>{rank}</td><td>{ticker}</td>"
                      f"<td>{cnt}/{MAX_HISTORY}</td></tr>")
    if hist_rows:
        html += (
            f"<h2>Stock Score Ranking (Last {MAX_HISTORY} Days)</h2>"
            f"<table><thead><tr><th>#</th><th>Ticker</th>"
            f"<th>Freq</th></tr></thead>"
            f"<tbody>{hist_rows}</tbody></table>"
        )
    
    # Generate watchlist data for copy button
    watchlist_data = []
    for item in sections:
        if len(item) == 3:
            results, title, exclude_cols = item
        else:
            results, title = item
            exclude_cols = []

        if 'Watchlist' in title and results:
            for i, r in enumerate(results, 1):
                tk = r['Ticker']
                freq = all_freqs.get(tk, 0)
                sig = _signal(r, freq, i, len(results), regime.get('level', 4))
                watchlist_data.append({'ticker': tk, 'signal': sig})
            break

    # Inject watchlist data into JavaScript using proper JSON encoding
    watchlist_js = "<script>window.watchlistData = " + json.dumps(watchlist_data) + ";</script>"

    html += watchlist_js + "</div></body></html>"


    return html

def print_regime(regime):
    print(f"\n{SEP2}")
    print(f"  MARKET REGIME: {regime['label']}  "
          f"(Score: {regime['score']}/100)")
    if MARGIN_MODE:
        print(f"  ⚠️  MARGIN MODE ACTIVE — Stocks tightened by {int((1-MARGIN_TIGHTEN_FACTOR)*100)}% (leveraged ETFs: no tightening)")
        print(f"  ⚠️  Leveraged ETF stop: {LEVERAGED_HARD_STOP_PCT:.0f}% | Stock stop: {STOCK_HARD_STOP_PCT * MARGIN_TIGHTEN_FACTOR:.0f}%")
    print(f"{SEP2}")

    c = regime['components']
    if 'spy_price' in c:
        status = "ABOVE" if c.get('spy_above_200') else "BELOW"
        rising = "Rising" if c.get('ma200_rising') else "Falling"
        print(f"  SPY 200MA: {status} ${c['spy_ma200']} ({rising})")
    if 'rsp_vs_spy' in c:
        print(f"  Breadth (RSP vs SPY): {c['rsp_vs_spy']:+.2f}%")
    if 'vix' in c:
        print(f"  VIX: {c['vix']} ({c.get('vix_level', '')})")
    if 'fng' in c:
        print(f"  Fear & Greed: {c['fng']} ({c.get('fng_label', '')})")
    # V4: Display volatility-adjusted allocation multiplier
    if 'regime_mult' in c:
        mult_pct = int(c['regime_mult'] * 100)
        alloc_label = "OVERWEIGHT" if mult_pct > 100 else "UNDERWEIGHT" if mult_pct < 100 else "NEUTRAL"
        print(f"  Allocation: {alloc_label} ({mult_pct}%)")

def print_table(title, results, bench_ticker, bench, all_freqs, regime_level=4, is_bear_mode=False):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")
    if bench:
        print(f"  {bench_ticker}: Today {fmt_pct(bench.get('perf_today'))} | "
              f"5D {bench.get('perf_5d', 0):+.2f}% | 15D {bench.get('perf_15d', 0):+.2f}%")

    if not results:
        print("  No results")
        return

    hdr = f"  {'#':>3}  {'Ticker':<7} {'Today':>7} {'$Vol':>10} {'Score':>7} {'ATR%':>7} {'Freq':>6} {'Signal':<12} {'Size':>7}"
    print(hdr)
    print("  " + "-" * 82)

    for i, r in enumerate(results[:15], 1):
        tk = r['Ticker']
        freq = all_freqs.get(tk, 0)
        sig = _signal(r, freq, i, len(results), regime_level)
        best_score = results[0].get('Score', 0) if results else 0
        size = _get_adjusted_size(r, i, best_score, freq, sig, regime_level, is_bear_mode)
        print(f"  {i:>3}  {tk:<7} {fmt_pct(r.get('Today %')):>7} "
              f"{fmt_dollar_volume(r.get('Dollar Volume')):>10} "
              f"{r.get('Score', 0):>7.0f} {r.get('ATR %', 0):>6.1f}% "
              f"{freq:>4}/{MAX_HISTORY} {sig:<12} {size:>7}")

def print_portfolio_v3(picks, is_bear, all_freqs):
    print(f"\n{SEP2}")
    margin_warn = " ⚠️ MARGIN MODE" if MARGIN_MODE else ""
    print(f"  PORTFOLIO V2 (ATR Sizing + Partial Profit-Taking) -- {'BEAR MODE' if is_bear else 'BULL MODE'}{margin_warn}")
    print(f"{SEP2}")

    if not picks:
        print("  No qualifying picks.")
        return

    for i, r in enumerate(picks[:MAX_POSITIONS], 1):
        atr_pct = max(r.get('ATR %', 5.0), 1.0)
        price = r['Price']
        atr = r['ATR']
        is_lev = r.get('Leveraged', False)

        # V5: Hard drawdown stop (absolute limit from peak)
        hard_stop = r.get('Hard Stop %', LEVERAGED_HARD_STOP_PCT if is_lev else STOCK_HARD_STOP_PCT)
        drawdown = r.get('Drawdown %', 0) or 0

        # V2: ATR-based stop - tighter for leveraged ETFs
        atr_mult = 1.5 if is_lev else 2.0
        stop_pct = round((atr * atr_mult / price) * 100, 2) if price > 0 else 20.0
        stop_price = round(price * (1 - stop_pct / 100), 2)

        # V5: Hard stop price from current peak
        peak_window = 126
        hard_stop_price = round(price / (1 + drawdown/100) * (1 - hard_stop/100), 2) if drawdown != 0 else round(price * (1 - hard_stop/100), 2)

        # V2: Partial profit-taking targets (ATR multiples from entry)
        target_1 = round(price * (1 + 3 * atr_mult * atr / price), 2)
        target_2 = round(price * (1 + 5 * atr_mult * atr / price), 2)
        target_3 = round(price * (1 + 8 * atr_mult * atr / price), 2)

        p3 = r.get('3D %', 0)
        p5 = r.get('5D %', 0)
        score = r.get('Score', 0)
        freq = all_freqs.get(r['Ticker'], 0)

        # V2: Tier display
        if score > 150 and freq >= 6:
            tier = "CONVICTION"
        elif score > 80 and freq >= 4:
            tier = "SPECULATIVE"
        else:
            tier = "STANDARD"

        tag = " *** TOP CONVICTION" if i == 1 else ""
        print(f"\n  #{i}  {r['Ticker']}{tag}")
        print(f"      Price: ${price:.2f}  |  Score: {score:.1f}  |  Tier: {tier}")
        print(f"      Drawdown: {drawdown:.1f}% from peak  |  Hard Stop: -{hard_stop:.0f}% (${hard_stop_price:.2f})")
        if r.get('Hard Stop Triggered'):
            print(f"      >>> ⚠️  HARD STOP TRIGGERED — SELL IMMEDIATELY <<<")
        if MARGIN_MODE:
            print(f"      ⚠️ MARGIN: Extra risk! Stop tightened to {hard_stop:.0f}%")
        print(f"      ATR: ${atr:.2f} ({atr_pct:.1f}%)  |  ATR Stop: ${stop_price:.2f} (-{stop_pct:.2f}%)")
        print(f"      Targets: T1 ${target_1:.2f} | T2 ${target_2:.2f} | T3 ${target_3:.2f}")

        # V2: Partial exit plan
        print(f"      Exit Plan: Take 1/3 at T1, 1/3 at T2, hold 1/3 with trailing stop")

        # V4: Trailing Stop indicator - shows breakeven after T1
        breakeven_stop = round(price * 1.002, 2)
        print(f"      Trailing Stop: After T1 hit → move stop to ${breakeven_stop} (breakeven)")

        if p3 > 5.0:
            print(f"      >>> PYRAMID OPPORTUNITY: +{p3:.1f}% in 3D. Consider adding 10% size.")
        if p3 < -2.0:
            print(f"      !! MOMENTUM EXIT TRIGGERED: 3D = {p3:.1f}% - Consider scaling out")

        # V4: Short squeeze and pullback indicators
        if r.get('Squeeze Score', 0) > SQUEEZE_BONUS:
            print(f"      >>> SHORT SQUEEZE CANDIDATE: {r['Short Interest']*100:.1f}% short, {r['Days To Cover']:.1f} days to cover")
        if r.get('Entry Quality') == 'PULLBACK':
            print(f"      >>> PULLBACK ENTRY: RSI={r.get('TV_RSI', 'N/A')} - ideal risk/reward setup")

def main():
    print("Initializing V4 Master Router...")
    
    # Load history for Freq
    sh, eh, bh = load_history()
    sf = freq_count(sh, 'stocks')
    ef = freq_count(eh, 'etfs')
    bf = freq_count(bh, 'bear_etfs')
    all_freqs = Counter()
    all_freqs.update(sf)
    all_freqs.update(ef)
    all_freqs.update(bf)
    
    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    regime = get_regime()
    print_regime(regime)

    is_bear_mode = regime['level'] <= 1

    print("\n>>> RUNNING BULL SCREENERS <<<")
    stocks, stock_bench = run_screener(
        "STOCK SCREENER",
        [STOCK_URL, STOCK_URL2, STOCK_URL3],
        bench_ticker="SOXL", ensure_ticker='SOXL',
        min_dv=30e6, dl_limit=200,
    )
    etfs, etf_bench = run_screener(
        "ETF SCREENER",
        [ETF_URL],
        bench_ticker="QQQ",
        min_dv=50e6, dl_limit=80,
    )
    
    # Adding BULL_ETFS (like FNGD in Top Bear Shorts)
    bull_etfs_res, bull_etf_bench = run_screener(
        "TOP BULL LEVERAGED ETFs (vs QQQ)",
        finviz_urls=[],
        bench_ticker="QQQ", ensure_ticker='TQQQ',
        min_dv=5e6, dl_limit=80,
        ticker_list=BULL_ETFS
    )
    
    # Watchlist: always show SOXL, DRAM (skip all filters for watchlist)
    watchlist_res, watchlist_bench = run_screener(
        "WATCHLIST",
        finviz_urls=[],
        bench_ticker="QQQ",
        min_dv=0,
        dl_limit=50,
        ticker_list=WATCHLIST_TICKERS,
        force_show=True  # Bypass all filters
    )
    
    # Sort each table by 15D performance
    stocks.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    etfs.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    bull_etfs_res.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    watchlist_res.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    print_table("STOCKS", stocks, "SOXL", stock_bench, all_freqs, regime['level'], is_bear_mode)
    print_table("BULL ETFs", etfs, "QQQ", etf_bench, all_freqs, regime['level'], is_bear_mode)
    print_table("TOP BULL LONGS", bull_etfs_res, "QQQ", bull_etf_bench, all_freqs, regime['level'], is_bear_mode)
    print_table("WATCHLIST", watchlist_res, "QQQ", watchlist_bench, all_freqs, regime['level'], is_bear_mode)

    bear_etfs = []
    if is_bear_mode:
        print("\n>>> CRITICAL: RISK OFF DETECTED. ADDING BEAR ETF SCREENER <<<")
        bear_etfs, etf_bench_bear = run_screener(
            "BEAR ETF SCREENER (vs SPY)",
            finviz_urls=[],
            bench_ticker="SPY",
            min_dv=5e6, dl_limit=80,
            ticker_list=BEAR_ETFS
        )
        bear_etfs.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
        print_table("BEAR ETFs", bear_etfs, "SPY", etf_bench_bear, all_freqs, regime['level'], is_bear_mode)
        print_portfolio_v3(bear_etfs, True, all_freqs)
    else:
        all_picks = sorted(stocks + etfs, key=lambda x: x.get('Score',0), reverse=True)
        valid_picks = [p for p in all_picks if p.get('Score',0) > 0 and not p.get('_reference')]
        print_portfolio_v3(valid_picks, False, all_freqs)

        # Save the new history today
    save_history(stocks, etfs, bear_etfs)
    
    # Reload frequencies to include today's run
    sh, eh, bh = load_history()
    sf = freq_count(sh, 'stocks')
    ef = freq_count(eh, 'etfs')
    bf = freq_count(bh, 'bear_etfs')
    all_freqs = Counter()
    all_freqs.update(sf)
    all_freqs.update(ef)
    all_freqs.update(bf)

    mode_str = "BEAR MODE" if is_bear_mode else "BULL MODE"
    
    # Combine all ETFs for signal grouping
    all_etf_combined = list(etfs) + list(bull_etfs_res)
    watch_etf_tickers = {w['Ticker'] for w in watchlist_res}
    seen_tickers = {e['Ticker'] for e in etfs} | {b['Ticker'] for b in bull_etfs_res}
    for w in watchlist_res:
        if w['Ticker'] not in seen_tickers:
            all_etf_combined.append(w)
            seen_tickers.add(w['Ticker'])
    
    # Categorize all tickers by signal
    all_strong_buy = []
    all_buy = []
    all_hold = []
    all_sell = []
    all_low_score = []
    
    for i, t in enumerate(all_etf_combined, 1):
        tk = t['Ticker']
        freq = all_freqs.get(tk, 0)
        sig = _signal(t, freq, i, len(all_etf_combined))
        score = t.get('Score', 0)
        
        if sig == 'STRONG BUY':
            all_strong_buy.append(t)
        elif sig == 'BUY':
            all_buy.append(t)
        elif sig == 'HOLD':
            all_hold.append(t)
        elif sig == 'SELL':
            all_sell.append(t)
        
        if score <= 0:
            all_low_score.append(t)
    
    # Sort each category by score descending
    all_strong_buy.sort(key=lambda x: x.get('Score', 0), reverse=True)
    all_buy.sort(key=lambda x: x.get('Score', 0), reverse=True)
    all_hold.sort(key=lambda x: x.get('Score', 0), reverse=True)
    all_sell.sort(key=lambda x: x.get('Score', 0), reverse=True)
    all_low_score.sort(key=lambda x: x.get('Score', 0), reverse=True)
    
    
    # Re-sort all categories by 15D performance
    all_strong_buy.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    all_buy.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    all_hold.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    all_sell.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    all_low_score.sort(key=lambda x: x.get('15D %', 0) or 0, reverse=True)

    
    # Top 3 Featured ETFs - prioritize by 20-day (15D) performance and TV signals
    # Sort by 13W performance when available, making best monthly performer #1
    # Sort by 20-day (15D) performance when available, making best performer #1
    # Require minimum $100M volume for featured (exclude low liquidity ETFs)
    MIN_VOL_FEATURED = 5_000_000
    def featured_soft(soft):
        score = soft.get('Score', 0)
        p15d = soft.get('15D %', 0) if soft.get('15D %') else 0  # ~20 trading days
        tv_buy = 1 if soft.get('TV_Oscillators') == 'BUY' else 0
        tv_buy += 1 if soft.get('TV_MovingAverages') == 'BUY' else 0
        # Use Dollar Volume (not $Vol) - penalize low volume heavily
        vol = soft.get('Dollar Volume', 0)
        if vol < MIN_VOL_FEATURED:
            # Too low - push to bottom
            return -10000 + p15d
        # Good volume - use full ranking, prioritizing 20-day performance
        return (p15d * 5 + score * 0.5 + tv_buy * 50)
    
    # Get all STRONG BUY + BUY ETFs, add KORU if not present (has strong TV BUY signals)
    # Use ALL ETFs as the pool — rank by 20-day performance, TV signals, and volume
    all_etfs_ranked = sorted(all_etf_combined, key=featured_soft, reverse=True)
    top_featured = all_etfs_ranked[:3]
    # sections: (results, title, exclude_cols)
    sections = []
    
    # Top 3 Featured ETFs - highlighted section
    if top_featured:
        sections.append((top_featured, "★ TOP 3 FEATURED ETFS ★", ['Today', 'ATR%']))
    
    # ALL ETFs section - shows everything
    all_etfs_sorted = sorted(all_etf_combined, key=lambda x: x.get('15D %', 0) or 0, reverse=True)
    if all_etfs_sorted:
        sections.append((all_etfs_sorted, "ALL ETFs", []))
    
    # Other sections
    sections.extend([
        (bull_etfs_res, "Top Bull Longs", []),
        (stocks, "Top Stocks", []),
        (watchlist_res, "Watchlist", []),
    ])
    
    # SELL Signals section only
    if all_sell:
        sections.append((all_sell, "SELL Signals", []))
    if all_low_score:
        sections.append((all_low_score, "Score ≤ 0 (Avoid)", []))
    
    if is_bear_mode:
        sections.insert(0, (bear_etfs, "Top Bear Shorts", []))

    html = generate_html_v3(regime, sections, mode_str, all_freqs)

    # Output Dashboard
    html_path = os.path.join(ROOT, "index.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  Dashboard saved: {html_path}")
    print(f"{SEP2}\n")

if __name__ == '__main__':
    main()
