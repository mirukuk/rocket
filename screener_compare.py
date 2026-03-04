#!/usr/bin/env python3
"""
5-Day & 15-Day Performance Stock Screener - Beats SOXL
With custom ticker comparison feature
"""

import yfinance as yf
import pandas as pd
import requests
import re
from datetime import datetime


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


def calc_performance(hist):
    if len(hist) < 15:
        return 0, 0
    current = float(hist['Close'].iloc[-1])
    perf_5d = ((current - hist['Close'].iloc[-5]) / hist['Close'].iloc[-5]) * 100
    perf_15d = ((current - hist['Close'].iloc[-15]) / hist['Close'].iloc[-15]) * 100 if len(hist) >= 15 else 0
    return round(perf_5d, 2), round(perf_15d, 2)


def get_benchmark_performance(benchmark='SOXL'):
    try:
        hist = yf.Ticker(benchmark).history(period="3mo")
        perf_5d, perf_15d = calc_performance(hist)
        return {'perf_5d': perf_5d, 'perf_15d': perf_15d}
    except Exception as e:
        print(f"Error fetching {benchmark}: {e}")
        return {'perf_5d': 0, 'perf_15d': 0}


def get_stock_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="3mo")
        info = stock.info
        
        if len(hist) < 50:
            return None
        
        current_price = float(hist['Close'].iloc[-1])
        prev_price = float(hist['Close'].iloc[-2])
        perf_5d, perf_15d = calc_performance(hist)
        
        ma20 = hist['Close'].rolling(20).mean().iloc[-1]
        ma50 = hist['Close'].rolling(50).mean().iloc[-1]
        
        above_ma20 = current_price > ma20
        above_ma50 = current_price > ma50
        
        dollar_volume = current_price * hist['Volume'].tail(20).mean()
        
        rec_key = info.get('recommendationKey', 'hold')
        analyst_rec = rec_key.replace('_', ' ').title()
        
        return {
            'Ticker': ticker,
            'Name': info.get('shortName', ticker),
            'Price': round(current_price, 2),
            'Daily Change %': round((current_price / prev_price - 1) * 100, 2),
            '5D %': perf_5d,
            '15D %': perf_15d,
            'MA20': round(ma20, 2),
            'MA50': round(ma50, 2),
            'Above MA20': above_ma20,
            'Above MA50': above_ma50,
            'Dollar Volume': dollar_volume,
            'Recommendation': analyst_rec,
            'Market Cap': info.get('marketCap', 0),
            'Sector': info.get('sector', 'Unknown')
        }
    except Exception:
        return None


def screen_stocks(min_dollar_volume=75000000):
    print("[INFO] Fetching benchmark data (SOXL)...")
    soxl_perf = get_benchmark_performance('SOXL')
    
    print(f"   SOXL - 5D: {soxl_perf['perf_5d']:+.2f}% | 15D: {soxl_perf['perf_15d']:+.2f}%\n")
    
    print("[INFO] Fetching stock list from Finviz...")
    tickers = get_us_stocks(limit=200)
    if not tickers:
        print("[ERROR] No tickers fetched")
        return pd.DataFrame(), soxl_perf
    print(f"   Found {len(tickers)} stocks to analyze\n")
    
    results = []
    for i, ticker in enumerate(tickers):
        print(f"[PROGRESS] Processing {i+1}/{len(tickers)}: {ticker}...", end='\r')
        data = get_stock_data(ticker)
        
        if data is None:
            continue
            
        if data['Dollar Volume'] < min_dollar_volume:
            continue
        
        if not (data['Above MA20'] and data['Above MA50']):
            continue
        
        data['5D vs SOXL'] = round(data['5D %'] - soxl_perf['perf_5d'], 2)
        data['15D vs SOXL'] = round(data['15D %'] - soxl_perf['perf_15d'], 2)
        
        if data['5D %'] > soxl_perf['perf_5d'] and data['15D %'] > soxl_perf['perf_15d']:
            avg_rel_return = (data['5D vs SOXL'] + data['15D vs SOXL']) / 2
            data['Avg Rel Return'] = round(avg_rel_return, 2)
            data['Composite Score'] = round(avg_rel_return * (data['Dollar Volume'] / 1_000_000), 2)
            results.append(data)
    
    print(f"\n[DONE] Processed {len(tickers)} stocks, {len(results)} passed filters\n")
    
    if not results:
        return pd.DataFrame(), soxl_perf
    
    df = pd.DataFrame(results).sort_values('Composite Score', ascending=False)
    
    return df, soxl_perf


def compare_custom_tickers(custom_tickers, df, soxl_perf, volume_weight=1000000):
    if not custom_tickers:
        return
    
    print("\n" + "=" * 80)
    print("[COMPARISON] Custom Tickers vs Screener Results")
    print("=" * 80 + "\n")
    
    print(f"Fetching data for: {', '.join(custom_tickers)}...\n")
    
    comparison_results = []
    for ticker in custom_tickers:
        data = get_stock_data(ticker)
        
        if data is None:
            print(f"  {ticker}: Failed to fetch data")
            continue
        
        data['5D vs SOXL'] = round(data['5D %'] - soxl_perf['perf_5d'], 2)
        data['15D vs SOXL'] = round(data['15D %'] - soxl_perf['perf_15d'], 2)
        
        avg_rel_return = (data['5D vs SOXL'] + data['15D vs SOXL']) / 2
        data['Avg Rel Return'] = round(avg_rel_return, 2)
        data['Composite Score'] = round(avg_rel_return * (data['Dollar Volume'] / volume_weight), 2)
        
        rank = None
        if not df.empty:
            rank_match = df[df['Ticker'] == ticker]
            if not rank_match.empty:
                rank = df.index.get_loc(rank_match.index[0]) + 1
        
        comparison_results.append({
            'Ticker': ticker,
            'Name': data['Name'],
            'Price': data['Price'],
            '5D %': data['5D %'],
            '15D %': data['15D %'],
            '5D vs SOXL': data['5D vs SOXL'],
            '15D vs SOXL': data['15D vs SOXL'],
            'Composite Score': data['Composite Score'],
            'Rank': rank
        })
    
    comp_df = pd.DataFrame(comparison_results)
    if not comp_df.empty:
        comp_df = comp_df.sort_values('Composite Score', ascending=False)
        print(comp_df.to_string(index=False))
        
        print("\n[SUMMARY]")
        for _, row in comp_df.iterrows():
            print(f"  {row['Ticker']}: Composite Score = {row['Composite Score']:.2f} | Rank #{row['Rank'] if row['Rank'] else 'N/A'}")


def display_results(df, soxl_perf, top_n=30):
    if df.empty:
        print("[WARNING] No stocks found matching criteria")
        print("[SIGNAL] 100% CASH")
        return
    
    df_display = df.head(top_n).copy()
    
    display_cols = ['Ticker', 'Name', 'Composite Score']
    df_output = df_display[display_cols].reset_index(drop=True)
    df_output.index = df_output.index + 1
    
    print("=" * 150)
    print(f"[RESULTS] 5-DAY & 15-DAY PERFORMANCE SCREENER - BEAT SOXL")
    print(f"   Benchmark: SOXL (5D: {soxl_perf['perf_5d']:+.2f}%) | SOXL (15D: {soxl_perf['perf_15d']:+.2f}%)")
    print(f"   Filters: Above MA20 & MA50 | Dollar Volume >= $75M")
    print(f"   Ranking: Composite Score = Avg Rel Return x Dollar Volume")
    print(f"   Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 150 + "\n")
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 20)
    print(df_output.to_string(), "\n")
    
    print("=" * 150)
    print(f"[SIGNAL] EQUAL-WEIGHT PORTFOLIO - {top_n} STOCKS @ {100/top_n:.1f}% EACH")
    print(f"[HOLD] EXACTLY 1 WEEK")
    print("=" * 150 + "\n")


def get_tickers_from_input():
    import sys
    if len(sys.argv) > 1:
        tickers = [t.strip().upper() for t in sys.argv[1].split(',') if t.strip()]
        seen = dict.fromkeys(tickers)
        return list(seen)
    
    print("\n" + "=" * 80)
    print("Enter tickers to compare (comma-separated, e.g.: AAPL,NVDA,TSLA)")
    print("Or press Enter to skip: ", end="")
    user_input = input().strip()
    
    if not user_input:
        return []
    
    tickers = [t.strip().upper() for t in user_input.split(',') if t.strip()]
    seen = dict.fromkeys(tickers)
    return list(seen)


def main():
    print("\n" + "=" * 80)
    print("   5-DAY & 15-DAY PERFORMANCE STOCK SCREENER")
    print("   Compare custom tickers vs SOXL")
    print("=" * 80 + "\n")
    
    custom_tickers = get_tickers_from_input()
    
    if not custom_tickers:
        print("No tickers entered. Exiting.")
        return
    
    try:
        df, soxl_perf = screen_stocks(min_dollar_volume=75000000)
        
        volume_weight = 10000000  # Lower dollar volume impact (10x higher = 10x less weight)
        
        compare_custom_tickers(custom_tickers, df, soxl_perf, volume_weight)
            
    except KeyboardInterrupt:
        print("\n\n[WARNING] Interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] {e}")


if __name__ == "__main__":
    main()
