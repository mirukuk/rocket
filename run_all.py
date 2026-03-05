#!/usr/bin/env python3
"""
Run all screeners and generate HTML report
"""

import sys
import os
import json
from datetime import datetime, timedelta
from collections import Counter
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yfinance as yf
import requests
import pandas as pd
import numpy as np
import re

HISTORY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'history')
MAX_DAYS = 10

# Leveraged ETFs (from backtest_v3)
LEVERAGED_ETFS = {'SOXL', 'TQQQ', 'NVDL', 'UPRO', 'TECL', 'FAS', 'LABU', 'FNGU',
                  'TNA', 'SPXL', 'QLD', 'ROM', 'BULZ', 'KORU', 'YINN', 'EDC'}

def save_daily_history(stock_results, etf_results=None):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    today = datetime.now()
    
    # Skip weekends (Sat=5, Sun=6) — market is closed, data is duplicate
    if today.weekday() >= 5:
        print(f"   Skipping history save (weekend: {today.strftime('%A')})")
        return
    
    date_str = today.strftime('%Y-%m-%d')
    data = {'stocks': stock_results}
    if etf_results:
        data['etfs'] = etf_results
    
    filepath = os.path.join(HISTORY_DIR, f'{date_str}.json')
    with open(filepath, 'w') as f:
        json.dump(data, f)
    print(f"   Saved history: {filepath}")
    
    cleanup_old_history()

def cleanup_old_history():
    files = sorted(os.listdir(HISTORY_DIR))
    if len(files) > MAX_DAYS:
        for f in files[:-MAX_DAYS]:
            os.remove(os.path.join(HISTORY_DIR, f))
            print(f"   Removed old: {f}")

def load_history():
    stock_history = []
    etf_history = []
    if os.path.exists(HISTORY_DIR):
        files = sorted(os.listdir(HISTORY_DIR))[-MAX_DAYS:]
        for f in files:
            with open(os.path.join(HISTORY_DIR, f), 'r') as fp:
                data = json.load(fp)
                date = f.replace('.json', '')
                if 'stocks' in data:
                    stock_history.append({'date': date, 'stocks': data['stocks']})
                if 'etfs' in data:
                    etf_history.append({'date': date, 'etfs': data['etfs']})
    return stock_history, etf_history

def calculate_frequency(history, key='stocks'):
    counter = Counter()
    for day in history:
        for item in day.get(key, []):
            counter[item['Ticker']] += 1
    return counter

def get_vix():
    try:
        hist = yf.Ticker('^VIX').history(period="5d")
        if hist.empty:
            return None
        v = round(hist['Close'].iloc[-1], 2)
        p = round(hist['Close'].iloc[-2], 2)
        level = "LOW" if v < 15 else "NORMAL" if v < 20 else "ELEVATED" if v < 25 else "HIGH" if v < 30 else "EXTREME"
        return {'value': v, 'change': round(v - p, 2), 'level': level}
    except:
        return None

def get_fear_greed():
    try:
        url = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*', 'Origin': 'https://www.cnn.com', 'Referer': 'https://www.cnn.com/',
        }
        r = requests.get(url, headers=headers, timeout=15)
        s = int(r.json()['fear_and_greed']['score'])
        status = "EXTREME FEAR" if s <= 20 else "FEAR" if s <= 40 else "NEUTRAL" if s <= 60 else "GREED" if s <= 80 else "EXTREME GREED"
        return {'score': s, 'status': status}
    except:
        return None

def get_spy_ma():
    try:
        h = yf.Ticker('SPY').history(period="1y")
        if h.empty or len(h) < 200:
            return None
        price = h['Close'].iloc[-1]
        ma200 = h['Close'].rolling(200).mean().iloc[-1]
        return {'price': round(price, 2), 'ma200': round(ma200, 2), 'above': price > ma200}
    except:
        return None

def calc_market_score(vix_val, spy_price, spy_sma200):
    """Market score 0-100: BULLISH >= 70, BEARISH < 50, else NEUTRAL."""
    score = 50
    if vix_val is not None:
        if vix_val < 18:
            score += 15
        elif vix_val < 22:
            score += 5
        elif vix_val > 28:
            score -= 15
        elif vix_val > 25:
            score -= 5
    if spy_price and spy_sma200 and spy_price > spy_sma200:
        score += 15
    score = max(0, min(100, score))
    label = "BULLISH" if score >= 70 else "BEARISH" if score < 50 else "NEUTRAL"
    return {'score': score, 'label': label}

def get_smh_bounce():
    """Detect SMH buy-the-dip: dropped >15% from 50D high but 5D return >2%."""
    try:
        h = yf.Ticker('SMH').history(period="6mo")
        if h.empty or len(h) < 50:
            return {'bounce': False, 'drawdown': 0, 'recovery': 0}
        cur = float(h['Close'].iloc[-1])
        high_50d = float(h['Close'].iloc[-50:].max())
        p5 = float(h['Close'].iloc[-5])
        drawdown = round((high_50d - cur) / high_50d * 100, 2)
        recovery = round((cur - p5) / p5 * 100, 2)
        bounce = drawdown > 15 and recovery > 2
        return {'bounce': bounce, 'drawdown': drawdown, 'recovery': recovery}
    except:
        return {'bounce': False, 'drawdown': 0, 'recovery': 0}

def calc_performance(hist):
    if len(hist) < 15:
        return 0, 0, 0
    current = float(hist['Close'].iloc[-1])
    perf_3d = ((current - hist['Close'].iloc[-3]) / hist['Close'].iloc[-3]) * 100 if len(hist) >= 3 else 0
    perf_5d = ((current - hist['Close'].iloc[-5]) / hist['Close'].iloc[-5]) * 100
    perf_15d = ((current - hist['Close'].iloc[-15]) / hist['Close'].iloc[-15]) * 100 if len(hist) >= 15 else 0
    return round(perf_3d, 2), round(perf_5d, 2), round(perf_15d, 2)

def get_etfs(limit=100):
    url = "https://finviz.com/screener.ashx?v=411&f=ind_exchangetradedfund%2Csh_price_o10%2Cta_change_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&o=-volume"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tickers = re.findall(r'quote\.ashx\?t=([A-Z]+)', response.text)
        seen = set()
        return [t for t in tickers if not (t in seen or seen.add(t))][:limit]
    except Exception as e:
        print(f"Error fetching from Finviz: {e}")
        return []

def get_us_stocks(limit=100):
    url = "https://finviz.com/screener.ashx?v=411&f=sh_price_o10%2Cta_change_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&ft=3&o=-volume"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tickers = re.findall(r'quote\.ashx\?t=([A-Z]+)', response.text)
        seen = set()
        return [t for t in tickers if not (t in seen or seen.add(t))][:limit]
    except Exception as e:
        print(f"Error fetching from Finviz: {e}")
        return []

def process_ticker_from_bulk(ticker, close_df, volume_df, min_bars=15):
    """Extract data for a single ticker from bulk-downloaded DataFrames."""
    try:
        if ticker not in close_df.columns or ticker not in volume_df.columns:
            return None
        prices = close_df[ticker].dropna()
        vols = volume_df[ticker].dropna()
        if len(prices) < min_bars:
            return None

        current_price = float(prices.iloc[-1])
        if current_price <= 0:
            return None

        perf_3d, perf_5d, perf_15d = calc_performance(prices.to_frame(name='Close'))

        ma20 = float(prices.iloc[-20:].mean()) if len(prices) >= 20 else current_price
        ma50 = float(prices.iloc[-50:].mean()) if len(prices) >= 50 else current_price

        above_ma20 = current_price > ma20
        above_ma50 = current_price > ma50

        avg_vol_20 = float(vols.tail(20).mean())
        avg_vol_5 = float(vols.tail(5).mean())
        dollar_volume = current_price * avg_vol_20
        vol_surge = round(avg_vol_5 / avg_vol_20, 2) if avg_vol_20 > 0 else 1.0

        # Momentum acceleration: 3D return > 60% of 5D return
        acceleration = (perf_3d > perf_5d * 0.6 and perf_3d > 0) if perf_5d != 0 else False

        return {
            'Ticker': ticker,
            'Price': round(current_price, 2),
            '3D %': perf_3d,
            '5D %': perf_5d,
            '15D %': perf_15d,
            'Dollar Volume': dollar_volume,
            'Above MA20': above_ma20,
            'Above MA50': above_ma50,
            'Vol Surge': vol_surge,
            'Acceleration': acceleration,
        }
    except Exception:
        return None

def bulk_download(tickers):
    """Download all tickers at once using yf.download (much faster)."""
    if not tickers:
        return pd.DataFrame(), pd.DataFrame()
    try:
        data = yf.download(tickers, period="3mo", auto_adjust=True, threads=True, progress=False)
        if data.empty:
            return pd.DataFrame(), pd.DataFrame()
        if len(tickers) == 1:
            close = data[['Close']].rename(columns={'Close': tickers[0]})
            volume = data[['Volume']].rename(columns={'Volume': tickers[0]})
        else:
            close = data['Close'] if 'Close' in data.columns.get_level_values(0) else pd.DataFrame()
            volume = data['Volume'] if 'Volume' in data.columns.get_level_values(0) else pd.DataFrame()
        return close, volume
    except Exception as e:
        print(f"   Bulk download error: {e}")
        return pd.DataFrame(), pd.DataFrame()

def enrich_with_info(data, ticker):
    """Add name from yfinance info (lightweight call)."""
    try:
        info = yf.Ticker(ticker).info
        data['Name'] = info.get('shortName', ticker)
        data['Market Cap'] = info.get('marketCap', 0)
        data['Sector'] = info.get('sector', 'Unknown')
    except:
        data['Name'] = ticker
        data['Market Cap'] = 0
        data['Sector'] = 'Unknown'
    return data

def run_market_analysis():
    print("\n[MARKET ANALYSIS]")
    vix = get_vix()
    fng = get_fear_greed()
    spy = get_spy_ma()
    smh_bounce = get_smh_bounce()
    
    # Binary signal (backward compat)
    signal = "SELL"
    if spy and vix:
        if spy['above'] and vix['value'] < 27:
            signal = "BUY"
        elif smh_bounce['bounce']:
            signal = "BUY THE DIP"
    
    # Market score 0-100
    vix_val = vix['value'] if vix else None
    spy_price = spy['price'] if spy else None
    spy_ma200 = spy['ma200'] if spy else None
    market_score = calc_market_score(vix_val, spy_price, spy_ma200)
    
    print(f"   Market Score: {market_score['score']}/100 [{market_score['label']}]")
    print(f"   Signal: {signal}")
    if smh_bounce['bounce']:
        print(f"   SMH Bounce Detected! Drawdown: {smh_bounce['drawdown']:.1f}% | Recovery: {smh_bounce['recovery']:.1f}%")
    
    return {
        'vix': vix,
        'fng': fng,
        'spy': spy,
        'signal': signal,
        'market_score': market_score,
        'smh_bounce': smh_bounce
    }

def run_etf_screener():
    print("\n[ETF SCREENER]")
    qqq_perf = get_benchmark_performance('QQQ')
    print(f"   QQQ - 5D: {qqq_perf['perf_5d']:+.2f}% | 15D: {qqq_perf['perf_15d']:+.2f}%")
    
    tickers = get_etfs(limit=200)
    print(f"   Found {len(tickers)} ETFs")
    
    # Bulk download all ETF data at once — always include SOXL
    dl_tickers = list(set(tickers[:50]) | {'SOXL'})
    print(f"   Bulk downloading {len(dl_tickers)} ETFs...")
    close_df, volume_df = bulk_download(dl_tickers)
    
    results = []
    soxl_data = None  # track SOXL separately
    for i, ticker in enumerate(dl_tickers):
        print(f"   Scoring {i+1}/{len(dl_tickers)}: {ticker}     ", end='\r')
        data = process_ticker_from_bulk(ticker, close_df, volume_df, min_bars=15)
        
        # Keep SOXL data even if it fails filters
        if ticker == 'SOXL':
            if data is not None:
                data['5D vs QQQ'] = round(data['5D %'] - qqq_perf['perf_5d'], 2)
                data['15D vs QQQ'] = round(data['15D %'] - qqq_perf['perf_15d'], 2)
                avg_rel_return = (data['5D vs QQQ'] + data['15D vs QQQ']) / 2
                data['Avg Rel Return'] = round(avg_rel_return, 2)
                # Use absolute performance for SOXL score, scaled to match other ETFs
                avg_abs = (data['5D %'] + data['15D %']) / 2
                score = avg_abs * (data['Dollar Volume'] / 1_000_000_000)
                if data['Vol Surge'] > 1.2:
                    score *= min(data['Vol Surge'], 1.5)
                score *= 2.0  # leveraged bonus
                if data['Acceleration']:
                    score *= 1.3
                data['Composite Score'] = round(score, 2)
                enrich_with_info(data, ticker)
                soxl_data = data
        
        if data is None:
            continue
        if data['Dollar Volume'] < 50000000:
            continue
        if not (data['Above MA20'] and data['Above MA50']):
            if ticker != 'SOXL':
                continue
        
        data['5D vs QQQ'] = round(data['5D %'] - qqq_perf['perf_5d'], 2)
        data['15D vs QQQ'] = round(data['15D %'] - qqq_perf['perf_15d'], 2)
        
        if data['5D %'] > qqq_perf['perf_5d'] and data['15D %'] > qqq_perf['perf_15d']:
            avg_rel_return = (data['5D vs QQQ'] + data['15D vs QQQ']) / 2
            data['Avg Rel Return'] = round(avg_rel_return, 2)
            score = avg_rel_return * (data['Dollar Volume'] / 1_000_000)
            
            # Volume surge bonus (from backtest_v3)
            if data['Vol Surge'] > 1.2:
                score *= min(data['Vol Surge'], 1.5)
            
            # Leveraged ETF bonus (from backtest_v3)
            if ticker in LEVERAGED_ETFS and score > 0:
                score *= 2.0
            
            # Momentum acceleration bonus (from backtest_v3)
            if data['Acceleration']:
                score *= 1.3
            
            data['Composite Score'] = round(score, 2)
            
            # Fetch name for display
            if ticker != 'SOXL':  # already enriched above
                enrich_with_info(data, ticker)
            results.append(data)
    
    print(f"\n   {len(results)} passed filters")
    
    # Ensure SOXL is always in results
    if soxl_data and not any(r['Ticker'] == 'SOXL' for r in results):
        soxl_data['_reference'] = True  # mark as reference row
        results.append(soxl_data)
    
    if results:
        df = pd.DataFrame(results).sort_values('Composite Score', ascending=False)
        return df.head(10).to_dict('records'), qqq_perf
    return [], qqq_perf

def run_stock_screener():
    print("\n[STOCK SCREENER]")
    soxl_perf = get_benchmark_performance('SOXL')
    print(f"   SOXL - 5D: {soxl_perf['perf_5d']:+.2f}% | 15D: {soxl_perf['perf_15d']:+.2f}%")
    
    tickers = get_us_stocks(limit=200)
    print(f"   Found {len(tickers)} stocks")
    
    # Bulk download all stock data at once
    dl_tickers = list(set(tickers[:100]))
    print(f"   Bulk downloading {len(dl_tickers)} stocks...")
    close_df, volume_df = bulk_download(dl_tickers)
    
    results = []
    for i, ticker in enumerate(dl_tickers):
        print(f"   Scoring {i+1}/{len(dl_tickers)}: {ticker}     ", end='\r')
        data = process_ticker_from_bulk(ticker, close_df, volume_df, min_bars=50)
        
        if data is None:
            continue
        if data['Dollar Volume'] < 75000000:
            continue
        if not (data['Above MA20'] and data['Above MA50']):
            continue
        
        data['5D vs SOXL'] = round(data['5D %'] - soxl_perf['perf_5d'], 2)
        data['15D vs SOXL'] = round(data['15D %'] - soxl_perf['perf_15d'], 2)
        
        if data['5D %'] > soxl_perf['perf_5d'] and data['15D %'] > soxl_perf['perf_15d']:
            avg_rel_return = (data['5D vs SOXL'] + data['15D vs SOXL']) / 2
            data['Avg Rel Return'] = round(avg_rel_return, 2)
            score = avg_rel_return * (data['Dollar Volume'] / 100_000_000)
            
            # Volume surge bonus (from backtest_v3)
            if data['Vol Surge'] > 1.2:
                score *= min(data['Vol Surge'], 1.5)
            
            # Momentum acceleration bonus (from backtest_v3)
            if data['Acceleration']:
                score *= 1.3
            
            data['Composite Score'] = round(score, 2)
            
            # Fetch name/sector for display
            enrich_with_info(data, ticker)
            results.append(data)
    
    print(f"\n   {len(results)} passed filters")
    
    if results:
        df = pd.DataFrame(results).sort_values('Composite Score', ascending=False)
        return df.head(15).to_dict('records'), soxl_perf
    return [], soxl_perf

def get_benchmark_performance(benchmark='QQQ'):
    try:
        hist = yf.Ticker(benchmark).history(period="3mo")
        perf_3d, perf_5d, perf_15d = calc_performance(hist)
        return {'perf_3d': perf_3d, 'perf_5d': perf_5d, 'perf_15d': perf_15d, 'ticker': benchmark}
    except Exception as e:
        print(f"Error fetching {benchmark}: {e}")
        return {'perf_3d': 0, 'perf_5d': 0, 'perf_15d': 0, 'ticker': benchmark}

def build_history_score_html(stock_history):
    """Build an HTML table showing all stocks from history ranked by latest composite score."""
    if not stock_history or len(stock_history) == 0:
        return ""

    # Collect all unique stocks from all history days with their latest data
    stock_map = {}
    for day in stock_history:
        for stock in day.get('stocks', []):
            ticker = stock['Ticker']
            stock_map[ticker] = stock  # Latest occurrence overwrites

    if not stock_map:
        return ""

    # Sort by composite score descending
    sorted_stocks = sorted(stock_map.values(), key=lambda x: x.get('Composite Score', 0), reverse=True)

    # Build rows
    rows = ""
    for i, stock in enumerate(sorted_stocks):
        ticker = stock['Ticker']
        score = stock.get('Composite Score', 0)
        name = stock.get('Name', ticker)[:25]
        vs = stock.get('Vol Surge', 1.0)
        vs_badge = f"<span style='color:#3fb950;'>&#x2191;{vs:.1f}x</span>" if vs > 1.2 else f"{vs:.1f}x"
        acc = "&#x1F525;" if stock.get('Acceleration', False) else ""

        # Calculate frequency from history
        freq = sum(1 for day in stock_history if any(s['Ticker'] == ticker for s in day.get('stocks', [])))

        rows += f"<tr><td>{i+1}</td><td>{ticker}</td><td>{name}</td><td>{score:.2f}</td><td>{vs_badge} {acc}</td><td>{freq}/10</td></tr>"

    return f"""
        <h2>Stock Score Ranking</h2>
        <p class="history-info">All stocks from history ranked by latest composite score</p>
        <table>
            <thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Score</th><th>Vol/Acc</th><th>Freq</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    """

def generate_html(market, etf_results, etf_bench, stock_results, stock_bench, stock_frequency=None, stock_history=None, etf_frequency=None, etf_history=None):
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    if stock_frequency is None:
        stock_frequency = {}
    if stock_history is None:
        stock_history = []
    if etf_frequency is None:
        etf_frequency = {}
    if etf_history is None:
        etf_history = []
    
    vix_val = market['vix']['value'] if market['vix'] else 'N/A'
    vix_level = market['vix']['level'] if market['vix'] else 'N/A'
    fng_score = market['fng']['score'] if market['fng'] else 'N/A'
    fng_status = market['fng']['status'] if market['fng'] else 'N/A'
    spy_price = market['spy']['price'] if market['spy'] else 'N/A'
    spy_ma = market['spy']['ma200'] if market['spy'] else 'N/A'
    spy_above = 'ABOVE' if market['spy'] and market['spy']['above'] else 'BELOW'
    
    signal_class = 'buy' if market['signal'] in ('BUY', 'BUY THE DIP') else 'sell'
    
    # Market score
    ms = market.get('market_score', {'score': 50, 'label': 'NEUTRAL'})
    ms_score = ms['score']
    ms_label = ms['label']
    ms_class = 'status-greed' if ms_score >= 70 else 'status-fear' if ms_score < 50 else 'status-neutral'
    
    # SMH bounce
    smh = market.get('smh_bounce', {'bounce': False, 'drawdown': 0, 'recovery': 0})
    smh_badge = f"<span style='color:#f0883e;font-weight:bold;'>&#x26A0; SMH BOUNCE (DD:{smh['drawdown']:.1f}% Rec:{smh['recovery']:.1f}%)</span>" if smh['bounce'] else ""
    
    try:
        fng_score_int = int(fng_score) if fng_score != 'N/A' else 50
        fng_status_class = 'status-fear' if fng_score_int <= 40 else 'status-greed' if fng_score_int >= 60 else 'status-neutral'
    except:
        fng_status_class = 'status-neutral'
    
    etf_rows = ""
    for i, row in enumerate(etf_results[:10]):
        freq = etf_frequency.get(row['Ticker'], 0)
        vs = row.get('Vol Surge', 1.0)
        vs_badge = f"<span style='color:#3fb950;'>&#x2191;{vs:.1f}x</span>" if vs > 1.2 else f"{vs:.1f}x"
        acc = "&#x1F525;" if row.get('Acceleration', False) else ""
        lev = "&#x26A1;" if row.get('Ticker', '') in LEVERAGED_ETFS else ""
        is_ref = row.get('_reference', False)
        ref_style = " style='background:#1c2333;opacity:0.85;'" if is_ref else ""
        ref_tag = " <span style='color:#8b949e;font-size:0.75rem;'>(ref)</span>" if is_ref else ""
        etf_rows += f"<tr{ref_style}><td>{i+1}</td><td>{lev}{row['Ticker']}{ref_tag}</td><td>{row.get('Name', row['Ticker'])[:25]}</td><td>{row['Composite Score']:.2f}</td><td>{vs_badge} {acc}</td><td>{freq}/10</td></tr>"
    
    # If SOXL is not in top 10, add it as an extra reference row
    soxl_in_top = any(r['Ticker'] == 'SOXL' for r in etf_results[:10])
    if not soxl_in_top:
        soxl_row = next((r for r in etf_results if r['Ticker'] == 'SOXL'), None)
        if soxl_row:
            freq = etf_frequency.get('SOXL', 0)
            vs = soxl_row.get('Vol Surge', 1.0)
            vs_badge = f"<span style='color:#3fb950;'>&#x2191;{vs:.1f}x</span>" if vs > 1.2 else f"{vs:.1f}x"
            acc = "&#x1F525;" if soxl_row.get('Acceleration', False) else ""
            etf_rows += f"<tr style='background:#1c2333;border-top:2px solid #30363d;'><td>&#x2605;</td><td>&#x26A1;SOXL <span style='color:#8b949e;font-size:0.75rem;'>(ref)</span></td><td>{soxl_row.get('Name', 'SOXL')[:25]}</td><td style='color:{'#3fb950' if soxl_row.get('Composite Score', 0) >= 0 else '#f85149'};'>{soxl_row.get('Composite Score', 0):.2f}</td><td>{vs_badge} {acc}</td><td>{freq}/10</td></tr>"
    
    stock_rows = ""
    for i, row in enumerate(stock_results[:15]):
        freq = stock_frequency.get(row['Ticker'], 0)
        vs = row.get('Vol Surge', 1.0)
        vs_badge = f"<span style='color:#3fb950;'>&#x2191;{vs:.1f}x</span>" if vs > 1.2 else f"{vs:.1f}x"
        acc = "&#x1F525;" if row.get('Acceleration', False) else ""
        stock_rows += f"<tr><td>{i+1}</td><td>{row['Ticker']}</td><td>{row.get('Name', row['Ticker'])[:25]}</td><td>{row['Composite Score']:.2f}</td><td>{vs_badge} {acc}</td><td>{freq}/10</td></tr>"
    
    stock_history_days = ", ".join([d['date'] for d in stock_history]) if stock_history else "No history"
    etf_history_days = ", ".join([d['date'] for d in etf_history]) if etf_history else "No history"
    
    # Build stock score history table
    history_score_html = build_history_score_html(stock_history)
    
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Market Analysis</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0d1117; color: #c9d1d9; min-height: 100vh; padding: 2rem; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; color: #58a6ff; }}
        h2 {{ font-size: 1.1rem; margin: 1.5rem 0 0.5rem; color: #8b949e; border-bottom: 1px solid #30363d; padding-bottom: 0.5rem; }}
        .date {{ color: #8b949e; font-size: 0.875rem; margin-bottom: 2rem; }}
        .signal {{ font-size: 3rem; font-weight: bold; margin: 1rem 0; padding: 1rem 2rem; border-radius: 0.5rem; display: inline-block; }}
        .signal.buy {{ background: #238636; color: #fff; }}
        .signal.sell {{ background: #da3633; color: #fff; }}
        .signal.dip {{ background: #f0883e; color: #fff; }}
        .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 2rem; }}
        .metric {{ background: #161b22; padding: 1rem; border-radius: 0.5rem; }}
        .metric-label {{ color: #8b949e; font-size: 0.75rem; margin-bottom: 0.25rem; }}
        .metric-value {{ font-size: 1.25rem; font-weight: 600; }}
        .metric-status {{ font-size: 0.75rem; margin-top: 0.25rem; }}
        .status-fear {{ color: #f85149; }}
        .status-greed {{ color: #3fb950; }}
        .status-neutral {{ color: #d29922; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 1rem; }}
        th, td {{ text-align: left; padding: 0.75rem; border-bottom: 1px solid #30363d; }}
        th {{ color: #8b949e; font-weight: 500; font-size: 0.75rem; text-transform: uppercase; }}
        td {{ font-size: 0.9rem; }}
        tr:hover {{ background: #161b22; }}
        .bench-info {{ color: #8b949e; font-size: 0.875rem; margin-bottom: 1rem; }}
        .freq-high {{ color: #3fb950; font-weight: bold; }}
        .freq-mid {{ color: #d29922; }}
        .freq-low {{ color: #8b949e; }}
        .history-info {{ color: #58a6ff; font-size: 0.875rem; margin-bottom: 1rem; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Market Analysis</h1>
        <p class="date">{now}</p>
        
        <div class="signal {signal_class}{' dip' if market['signal'] == 'BUY THE DIP' else ''}">{market['signal']}</div>
        {smh_badge}
        
        <div class="metrics">
            <div class="metric">
                <div class="metric-label">Market Score</div>
                <div class="metric-value">{ms_score}/100</div>
                <div class="metric-status {ms_class}">{ms_label}</div>
            </div>
            <div class="metric">
                <div class="metric-label">VIX</div>
                <div class="metric-value">{vix_val}</div>
                <div class="metric-status">{vix_level}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Fear & Greed</div>
                <div class="metric-value">{fng_score}</div>
                <div class="metric-status {fng_status_class}">{fng_status}</div>
            </div>
            <div class="metric">
                <div class="metric-label">SPY vs 200-MA</div>
                <div class="metric-value">${spy_price}</div>
                <div class="metric-status">{spy_above} ${spy_ma}</div>
            </div>
        </div>
        
        <h2>ETF Screener (Beat QQQ)</h2>
        <p class="bench-info">QQQ Benchmark: 5D {etf_bench['perf_5d']:+.2f}% | 15D {etf_bench['perf_15d']:+.2f}%</p>
        <p class="history-info">History: {etf_history_days}</p>
        <table>
            <thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Score</th><th>Vol/Acc</th><th>Freq</th></tr></thead>
            <tbody>{etf_rows}</tbody>
        </table>
        
        <h2>Stock Screener (Beat SOXL)</h2>
        <p class="bench-info">SOXL Benchmark: 5D {stock_bench['perf_5d']:+.2f}% | 15D {stock_bench['perf_15d']:+.2f}%</p>
        <p class="history-info">History: {stock_history_days}</p>
        <table>
            <thead><tr><th>#</th><th>Ticker</th><th>Name</th><th>Score</th><th>Vol/Acc</th><th>Freq</th></tr></thead>
            <tbody>{stock_rows}</tbody>
        </table>
        
        {history_score_html}
    </div>
</body>
</html>'''
    
    return html

def main(save_history=True):
    print("=" * 60)
    print("  RUNNING ALL SCREENERS")
    print("=" * 60)
    
    market = run_market_analysis()
    etf_results, etf_bench = run_etf_screener()
    stock_results, stock_bench = run_stock_screener()
    
    if save_history:
        print("\n[SAVING HISTORY]")
        save_daily_history(stock_results, etf_results)
    
    stock_history, etf_history = load_history()
    stock_frequency = calculate_frequency(stock_history, 'stocks')
    etf_frequency = calculate_frequency(etf_history, 'etfs')
    print(f"   Loaded {len(stock_history)} days stock history, {len(etf_history)} days ETF history")
    
    print("\n[GENERATING HTML]")
    html = generate_html(market, etf_results, etf_bench, stock_results, stock_bench, stock_frequency, stock_history, etf_frequency, etf_history)
    
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html')
    with open(output_path, 'w') as f:
        f.write(html)
    
    print(f"   Saved to: {output_path}")
    
    # Git commit/push is handled by GitHub Actions workflow
    print("\n[GIT] Commit/push handled by CI workflow")
    
    print("\n[DONE]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--save-history', action='store_true', default=True)
    parser.add_argument('--no-save-history', action='store_false', dest='save_history')
    args = parser.parse_args()
    main(save_history=args.save_history)
