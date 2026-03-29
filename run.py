#!/usr/bin/env python3
"""
Unified Screener V3 — The "Master Router" for 200% Returns

Enhancements in V3:
- Fully Autonomous Master Router (auto-switches to Bear Screener in Risk-Off)
- Dynamic ATR-based Stop Losses and Position Sizing
- Market Breadth Regime Modifier (RSP vs SPY)
- Pyramiding logic indicators for pressing winners

Usage:
  python run_all_console_v3.py
"""

import sys, os, json, re, argparse
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yfinance as yf
import requests
import pandas as pd
import logging

logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)

ROOT = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(ROOT, 'history')
MAX_HISTORY = 10

MAX_POSITIONS = 5
POSITION_SIZE = 20.0
TODAY_OUTPERFORMANCE_MARGIN = 1.0
OVEREXTENSION_PCT = 20.0

CONVICTION_SIZE = 35.0
MOMENTUM_EXIT_3D = -2.0

LEVERAGED_TICKERS = {
    'TQQQ', 'SOXL', 'UPRO', 'SPXL', 'TECL', 'FNGU', 'LABU', 'TNA',
    'UDOW', 'CURE', 'DFEN', 'DRN', 'DUSL', 'FAS', 'HIBL', 'MIDU',
    'NAIL', 'RETL', 'TPOR', 'WANT', 'WEBL',
    'SQQQ', 'SPXS', 'SDOW', 'FAZ', 'TZA', 'LABD',
}

BEAR_ETFS = [
    'SQQQ', 'SPXS', 'SDOW', 'SRTY', 'SPXU', 'LABD', 'FAZ', 'SOXS', 'TECS',
    'YANG', 'WEBS', 'HIBS', 'TZA', 'DRV', 'DUG', 'PST', 'TMV', 'OILD', 'KOLD'
]

ETF_URL = "https://finviz.com/screener.ashx?v=411&f=ind_exchangetradedfund%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&o=-volume"
STOCK_URL = "https://finviz.com/screener.ashx?v=411&f=sh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&ft=3&o=-volume"
STOCK_URL2 = "https://finviz.com/screener.ashx?v=411&f=sh_avgvol_o400%2Csh_price_o10%2Cta_change_u%2Cta_perf_1w10o%2Cta_sma20_pa&ft=3&o=-perf1w"
STOCK_URL3 = "https://finviz.com/screener.ashx?v=411&f=sh_avgvol_o400%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_1w10o%2Cta_sma20_pa&ft=3&o=-perf1w"

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
        return [t for t in re.findall(r'quote\.ashx\?t=([A-Z]+)', r.text)
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
        if len(tickers) == 1:
            return (data[['Close']].rename(columns={'Close': tickers[0]}),
                    data[['Volume']].rename(columns={'Volume': tickers[0]}),
                    data[['Open']].rename(columns={'Open': tickers[0]}),
                    data[['High']].rename(columns={'High': tickers[0]}),
                    data[['Low']].rename(columns={'Low': tickers[0]}))
        return (data.get('Close', pd.DataFrame()),
                data.get('Volume', pd.DataFrame()),
                data.get('Open', pd.DataFrame()),
                data.get('High', pd.DataFrame()),
                data.get('Low', pd.DataFrame()))
    except Exception as e:
        print(f"   Download error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def enrich_name(ticker):
    try:
        return yf.Ticker(ticker).info.get('shortName', ticker)
    except Exception:
        return ticker


def bench_perf(ticker):
    try:
        h = yf.Ticker(ticker).history(period="3mo")
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
        }
    except Exception:
        return {'ticker': ticker, 'perf_today': None, 'perf_5d': 0, 'perf_15d': 0}


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
        spy_hist = yf.Ticker('SPY').history(period="2y")
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
        rsp_hist = yf.Ticker('RSP').history(period="3mo")
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
        vix_hist = yf.Ticker('^VIX').history(period="5d")
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
    except Exception:
        pass

    try:
        r = requests.get(
            'https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
            headers={'User-Agent': 'Mozilla/5.0', 'Accept': '*/*',
                     'Origin': 'https://www.cnn.com',
                     'Referer': 'https://www.cnn.com/'},
            timeout=15)
        fng = int(r.json()['fear_and_greed']['score'])
        fng_label = ("EXTREME FEAR" if fng <= 20 else "FEAR" if fng <= 40 else
                     "NEUTRAL" if fng <= 60 else "GREED" if fng <= 80 else "EXTREME GREED")
        components['fng'] = fng
        components['fng_label'] = fng_label
        if fng > 60:
            regime_score += 15
        elif fng > 40:
            regime_score += 10
        elif fng > 25:
            regime_score += 5
    except Exception:
        pass

    try:
        h = yf.Ticker('SMH').history(period="6mo")
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

    if len(prices) < 20:
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
    
    stop_triggered = drawdown_pct <= -dynamic_stop_pct

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
        'Stop Threshold': dynamic_stop_pct,
        'Vol Surge': vol_surge,
        'Acceleration': acceleration,
        'Breakout': breakout,
        'Near High': near_high_20,
        'Dollar Volume': dollar_volume,
        'Leveraged': is_leveraged(ticker),
    }


def calc_composite(r, bench):
    def clamp(x):
        return max(-500.0, min(500.0, x))

    p3 = clamp(r.get('3D %', 0) or 0)
    p5 = clamp(r.get('5D %', 0) or 0)
    p15 = clamp(r.get('15D %', 0) or 0)
    vs = r.get('Vol Surge', 1.0)

    abs_mom = p3 * 3.0 + p5 * 2.0 + p15 * 1.0

    rel_vs_5 = p5 - bench.get('perf_5d', 0)
    rel_vs_15 = p15 - bench.get('perf_15d', 0)
    if rel_vs_5 > 5:
        abs_mom += rel_vs_5 * 1.5
    if rel_vs_15 > 10:
        abs_mom += rel_vs_15 * 0.8

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

    return round(abs_mom, 2)


def _signal(r, freq, rank, total):
    score = r.get('Score', 0)
    vs = r.get('Vol Surge', 1.0)
    acc = r.get('Acceleration', False)
    brk = r.get('Breakout', False)
    p3 = r.get('3D %', 0)
    if score <= 0:
        return 'SELL'
    top_half = rank <= max(total // 2, 1)
    has_momentum = vs > 1.3 or acc
    if score > 100 and has_momentum and freq >= 2:
        return 'STRONG BUY'
    if score > 100 and brk and freq >= 2:
        return 'STRONG BUY'
    if top_half and has_momentum and freq >= 2:
        return 'BUY'
    if top_half and freq >= 3:
        return 'BUY'
    if score > 50 and has_momentum:
        return 'BUY'
    if score > 50 and brk:
        return 'BUY'
    if p3 > 3 and vs > 1.5:
        return 'BUY'
    return 'HOLD'


def run_screener(name, finviz_urls, bench_ticker, min_dv=30e6,
                 dl_limit=200, top_n=15, ensure_ticker=None, ticker_list=None):
    print(f"\n[{name}]")
    bench = bench_perf(bench_ticker)
    bench['name'] = name
    print(
        f"   {bench_ticker} - Today: {fmt_pct(bench['perf_today'])} | "
        f"5D: {bench['perf_5d']:+.2f}% | 15D: {bench['perf_15d']:+.2f}%"
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
    print(f"   Downloading {len(dl)} tickers (period=1y)...")
    
    down_data = bulk_download(dl, period="1y")
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
            continue
        if not d.get('Above MA20') and t != ensure_ticker:
            pass # V3 allows testing even if under MA20 if momentum is wild (ATR checks cover stops)
        if d['3D %'] <= 0 and d['5D %'] <= 0 and t != ensure_ticker:
            continue

        if 'STOCK' in name:
            beats_bench = (
                _is_today_leader(d, bench['perf_today'])
                and d['5D %'] > bench['perf_5d']
                and d['15D %'] > bench['perf_15d']
            )
        else:
            beats_bench = (d['5D %'] > bench['perf_5d']
                           or d['15D %'] > bench['perf_15d'])

        if "BEAR" in name:
            beats_bench = True # If Bear mode, allow inverse ETFs to pass easily to spot best

        if beats_bench:
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
.metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1rem; margin-bottom:2rem; }
.m { background:#161b22; padding:1rem; border-radius:.5rem; }
.ml { color:#8b949e; font-size:.75rem; margin-bottom:.25rem; }
.mv { font-size:1.25rem; font-weight:600; }
.ms { font-size:.75rem; margin-top:.25rem; color:#8b949e; }
table { width:100%; border-collapse:collapse; margin-bottom:1rem; }
th,td { text-align:left; padding:.5rem .75rem; border-bottom:1px solid #30363d; }
th { color:#8b949e; font-weight:500; font-size:.75rem; text-transform:uppercase; }
td { font-size:.85rem; } tr:hover { background:#161b22; }
"""

def _sig_html(sig):
    colors = {'STRONG BUY': '#00ff7f', 'BUY': '#3fb950',
              'SELL': '#f85149', 'HOLD': '#d29922'}
    c = colors.get(sig, '#d29922')
    return f"<span style='color:{c};font-weight:bold'>{sig}</span>"

def generate_html_v3(regime, sections, mode, all_freqs):
    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    alloc = regime['allocation_pct']
    regime_cls = 'sell' if alloc == 0 else ('buy' if alloc >= 60 else 'cautious')
    
    c = regime['components']
    spy_p = c.get('spy_price', 'N/A')
    spy_ma = c.get('spy_ma200', 'N/A')
    spy_above = c.get('spy_above_200', False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>V3 Master Router: {mode}</title><style>{CSS}</style></head>
<body><div class="c">
<h1>V3 Master Router ({mode})</h1><p class="date">{now}</p>
<div class="sig {regime_cls}">{regime['label']}</div>
<div class="metrics">
<div class="m"><div class="ml">Regime Score</div><div class="mv">{regime['score']}/100</div></div>
<div class="m"><div class="ml">Breadth Eq-W vs SPY</div><div class="mv">{regime['components'].get('rsp_vs_spy', 'N/A')}%</div></div>
<div class="m"><div class="ml">VIX</div><div class="mv">{regime['components'].get('vix', 'N/A')}</div></div>
<div class="m"><div class="ml">SPY vs 200MA</div><div class="mv">${spy_p}</div><div class="ms">{'ABOVE' if spy_above else 'BELOW'} ${spy_ma}</div></div>
<div class="m"><div class="ml">Stops</div><div class="mv">15%/25%</div><div class="ms">Stock / Leveraged</div></div>
</div>"""

    def table(results, title):
        rows = ""
        for i, r in enumerate(results[:15], 1):
            tk = r['Ticker']
            freq = all_freqs.get(tk, 0)
            s = _signal(r, freq, i, len(results))
            rows += f"<tr><td>{i}</td><td>{tk}</td><td>{fmt_pct(r.get('Today %'))}</td><td>{fmt_dollar_volume(r.get('Dollar Volume'))}</td><td>{r.get('Score', 0):.0f}</td><td>{r.get('ATR %', 0):.1f}%</td><td>{freq}/{MAX_HISTORY}</td><td>{_sig_html(s)}</td></tr>"
        return f"<h2>{title}</h2><table><thead><tr><th>#</th><th>Ticker</th><th>Today</th><th>$Vol</th><th>Score</th><th>ATR% (Vol)</th><th>Freq</th><th>Action</th></tr></thead><tbody>{rows}</tbody></table>"

    for results, title in sections:
        if results:
            html += table(results, title)
            
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
    
    html += "</div></body></html>"
    return html

def print_regime(regime):
    print(f"\n{SEP2}")
    print(f"  MARKET REGIME: {regime['label']}  "
          f"(Score: {regime['score']}/100)")
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

def print_table(title, results, bench_ticker, bench, all_freqs):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")
    if bench:
        print(f"  {bench_ticker}: Today {fmt_pct(bench.get('perf_today'))} | "
              f"5D {bench.get('perf_5d', 0):+.2f}% | 15D {bench.get('perf_15d', 0):+.2f}%")

    if not results:
        print("  No results")
        return

    hdr = f"  {'#':>3}  {'Ticker':<7} {'Today':>7} {'$Vol':>10} {'Score':>7} {'ATR%':>7} {'Freq':>6} {'Action':<12}"
    print(hdr)
    print("  " + "-" * 75)

    for i, r in enumerate(results[:15], 1):
        tk = r['Ticker']
        freq = all_freqs.get(tk, 0)
        sig = _signal(r, freq, i, len(results))
        print(f"  {i:>3}  {tk:<7} {fmt_pct(r.get('Today %')):>7} "
              f"{fmt_dollar_volume(r.get('Dollar Volume')):>10} "
              f"{r.get('Score', 0):>7.0f} {r.get('ATR %', 0):>6.1f}% "
              f"{freq:>4}/{MAX_HISTORY} {sig:<12}")

def print_portfolio_v3(picks, is_bear):
    print(f"\n{SEP2}")
    print(f"  PORTFOLIO V3 (ATR Sizing & Dynamic Stops) -- {'BEAR MODE' if is_bear else 'BULL MODE'}")
    print(f"{SEP2}")

    if not picks:
        print("  No qualifying picks.")
        return

    for i, r in enumerate(picks[:MAX_POSITIONS], 1):
        atr_pct = max(r.get('ATR %', 5.0), 1.0)
        # Sizing scales inverse to ATR. If ATR is huge (e.g. 10%), base size drops.
        base_conviction = CONVICTION_SIZE if i == 1 else POSITION_SIZE
        scale_factor = 5.0 / atr_pct # Base 5% daily ATR baseline
        eff_size = min(base_conviction * scale_factor, 100.0) 
        
        stop_pct = r.get('Stop Threshold', 20.0)
        stop_price = r['Price'] * (1 - stop_pct / 100)

        p3 = r.get('3D %', 0)
        momentum_exit = p3 < MOMENTUM_EXIT_3D
        tag = " *** TOP CONVICTION" if i==1 else ""

        print(f"\n  #{i}  {r['Ticker']}{tag}")
        print(f"      Price: ${r['Price']:.2f}  |  Score: {r.get('Score', 0):.1f}")
        print(f"      Size Alloc: {eff_size:.1f}% (Scaled by ATR: {atr_pct:.1f}%)")
        print(f"      ATR T-Stop: ${stop_price:.2f} (-{stop_pct:.1f}%)")
        if p3 > 5.0:
            print(f"      >>> PYRAMID OPPORTUNITY: Winner is +{p3:.1f}% in 3D. Add 10% size.")
        if momentum_exit:
            print(f"      !! MOMENTUM EXIT TRIGGERED: 3D = {p3:.1f}%")

def main():
    print("Initializing V3 Master Router...")
    
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
        bench_ticker="SOXL",
        min_dv=30e6, dl_limit=200,
    )
    etfs, etf_bench = run_screener(
        "ETF SCREENER",
        [ETF_URL],
        bench_ticker="QQQ",
        min_dv=50e6, dl_limit=80,
    )
    print_table("STOCKS", stocks, "SOXL", stock_bench, all_freqs)
    print_table("BULL ETFs", etfs, "QQQ", etf_bench, all_freqs)

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
        print_table("BEAR ETFs", bear_etfs, "SPY", etf_bench_bear, all_freqs)
        print_portfolio_v3(bear_etfs, True)
    else:
        all_picks = sorted(stocks + etfs, key=lambda x: x.get('Score',0), reverse=True)
        valid_picks = [p for p in all_picks if p.get('Score',0) > 0 and not p.get('_reference')]
        print_portfolio_v3(valid_picks, False)
        
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
    
    sections = [
        (stocks, "Top Stocks"),
        (etfs, "Top ETFs")
    ]
    if is_bear_mode:
        sections.insert(0, (bear_etfs, "Top Bear Shorts"))

    html = generate_html_v3(regime, sections, mode_str, all_freqs)

    # Output V3 Dashboard
    html_path = os.path.join(ROOT, "index.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  Dashboard saved: {html_path}")
    print(f"{SEP2}\n")

if __name__ == '__main__':
    main()
