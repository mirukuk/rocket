#!/usr/bin/env python3
"""
Momentum Console Screener v3 — Academic momentum + alpha selection + risk management.

Upgrades from v2:
  Benchmark gates    : SOXL (stocks) / QQQ (ETFs) outperformance required
  Short-term confirm : 3D/5D acceleration overlay on 2W/1M/3M/6M core
  Signal system      : BUY / STRONG BUY / HOLD / SELL + 4-criteria grading
  Top Picks          : 4/4 grade + today outperformance gate
  History tracking   : Shared history/ format, frequency counting
  Overextension      : Multi-level penalty cascade
  SMH bounce         : Dip-buy detection for semiconductors
  Regime rebalance   : Forced rebalance on regime drop >= 2 levels
  HTML output        : momentum.html with regime dashboard + Top Picks cards
  Today %            : prev_close→close (standard, matches benchmark)
"""

import sys, os, json, re, argparse
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yfinance as yf
import requests
import pandas as pd
import logging

# Silence yfinance / urllib3 noise
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)

ROOT = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(ROOT, 'history')
MAX_HISTORY = 10

# ── Configuration ──────────────────────────────────────────────────────
MAX_POSITIONS = 5
POSITION_SIZE = 20.0
TRAILING_STOP_STOCK = 15.0
TRAILING_STOP_LEVERAGED = 25.0
TODAY_OUTPERFORMANCE_MARGIN = 1.0

MOM_2W = 10
MOM_1M = 21
MOM_3M = 63
MOM_6M = 126
MOM_WEIGHTS = {MOM_2W: 0.10, MOM_1M: 0.20, MOM_3M: 0.35, MOM_6M: 0.35}

OVEREXTENSION_PCT = 20.0

LEVERAGED_TICKERS = {
    'TQQQ', 'SOXL', 'UPRO', 'SPXL', 'TECL', 'FNGU', 'LABU', 'TNA',
    'UDOW', 'CURE', 'DFEN', 'DRN', 'DUSL', 'FAS', 'HIBL', 'MIDU',
    'NAIL', 'RETL', 'TPOR', 'WANT', 'WEBL',
    'SQQQ', 'SPXS', 'SDOW', 'FAZ', 'TZA', 'LABD',
}

ETF_URL = "https://finviz.com/screener.ashx?v=411&f=ind_exchangetradedfund%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&o=-volume"
STOCK_URL = "https://finviz.com/screener.ashx?v=411&f=sh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&ft=3&o=-volume"
STOCK_URL2 = "https://finviz.com/screener.ashx?v=411&f=sh_avgvol_o400%2Csh_price_o10%2Cta_change_u%2Cta_perf_1w10o%2Cta_sma20_pa&ft=3&o=-perf1w"
STOCK_URL3 = "https://finviz.com/screener.ashx?v=411&f=sh_avgvol_o400%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_1w10o%2Cta_sma20_pa&ft=3&o=-perf1w"

SEP = "-" * 72
SEP2 = "=" * 72
REBALANCE_FILE = os.path.join(ROOT, 'last_rebalance.json')


# ── Helpers ────────────────────────────────────────────────────────────

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
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
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
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        if len(tickers) == 1:
            return (data[['Close']].rename(columns={'Close': tickers[0]}),
                    data[['Volume']].rename(columns={'Volume': tickers[0]}),
                    data[['Open']].rename(columns={'Open': tickers[0]}))
        return (data.get('Close', pd.DataFrame()),
                data.get('Volume', pd.DataFrame()),
                data.get('Open', pd.DataFrame()))
    except Exception as e:
        print(f"   Download error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def enrich_name(ticker):
    try:
        return yf.Ticker(ticker).info.get('shortName', ticker)
    except Exception:
        return ticker


# ── Benchmark ─────────────────────────────────────────────────────────

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


# ── Regime Detection (4 levels) + SMH bounce ─────────────────────────

def get_regime():
    regime_score = 0
    components = {}
    smh_bounce = {'bounce': False, 'drawdown': 0, 'recovery': 0}

    # 1. SPY vs 200MA (+35) and 200MA slope (+15)
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

    # 2. VIX level (+0 to +25)
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

    # 3. Fear & Greed (+0 to +15)
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

    # 4. SPY 50MA trend (+10)
    try:
        if spy_hist is not None and len(spy_hist) >= 70:
            ma50 = float(spy_hist['Close'].rolling(50).mean().iloc[-1])
            ma50_prev = float(spy_hist['Close'].rolling(50).mean().iloc[-21])
            components['spy_ma50'] = round(ma50, 2)
            components['ma50_rising'] = ma50 > ma50_prev
            if ma50 > ma50_prev:
                regime_score += 10
    except Exception:
        pass

    # 5. SMH bounce detection
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
        level, alloc, label = 1, 0, "RISK OFF"

    # SMH bounce can override RISK OFF → CAUTIOUS
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


# ── Ticker Scoring ────────────────────────────────────────────────────

def score_ticker_v2(ticker, close_df, vol_df, open_df):
    if ticker not in close_df.columns:
        return None

    prices = close_df[ticker].dropna()
    vols = (vol_df[ticker].dropna()
            if ticker in vol_df.columns else pd.Series(dtype=float))

    if len(prices) < MOM_6M + 5:
        return None

    price = float(prices.iloc[-1])
    if price <= 0:
        return None

    # ── Momentum (2W / 1M / 3M / 6M) ──
    mom_2w = pct(prices, MOM_2W)
    mom_1m = pct(prices, MOM_1M)
    mom_3m = pct(prices, MOM_3M)
    mom_6m = pct(prices, MOM_6M)

    if mom_3m is None or mom_6m is None:
        return None

    # ── Short-term (3D / 5D / 15D) for acceleration + benchmark ──
    p3 = pct(prices, 3) or 0
    p5 = pct(prices, 5) or 0
    p15 = pct(prices, 15) or 0

    # ── Acceleration: 3D pace > 5D pace and 3D > 1% ──
    rate3 = (p3 / 3) if p3 > 0 else 0
    rate5 = (p5 / 5) if p5 > 0 else 0
    acceleration = rate3 > rate5 and p3 > 1.0

    # ── Moving Averages ──
    ma20 = float(prices.tail(20).mean())
    ma50 = float(prices.tail(50).mean())
    ma200 = (float(prices.tail(200).mean())
             if len(prices) >= 200 else None)

    ma50_prev = (float(prices.iloc[:-20].tail(50).mean())
                 if len(prices) >= 70 else ma50)

    ma50_rising = ma50 > ma50_prev
    above_200ma = price > ma200 if ma200 else False
    above_ma20 = price > ma20

    # ── Trend Quality: 50MA rising AND above 200MA ──
    trend_quality = ma50_rising and above_200ma

    # ── Entry Quality: distance above 50MA ──
    extension_pct = round((price - ma50) / ma50 * 100, 2) if ma50 > 0 else 0
    overextended = extension_pct > OVEREXTENSION_PCT

    # ── Trailing Stop: drawdown from 6M peak ──
    peak_6m = float(prices.tail(MOM_6M).max())
    drawdown_pct = (round((price - peak_6m) / peak_6m * 100, 2)
                    if peak_6m > 0 else 0)
    stop_threshold = (TRAILING_STOP_LEVERAGED
                      if is_leveraged(ticker) else TRAILING_STOP_STOCK)
    stop_triggered = drawdown_pct <= -stop_threshold

    # ── Volume ──
    avg20_vol = float(vols.tail(20).mean()) if len(vols) >= 20 else 0
    avg5_vol = float(vols.tail(5).mean()) if len(vols) >= 5 else 0
    vol_surge = round(avg5_vol / avg20_vol, 2) if avg20_vol > 0 else 1.0
    dollar_volume = price * avg20_vol

    # ── Breakout: within 2% of 6M high with volume ──
    high20 = float(prices.tail(20).max())
    near_high = ((peak_6m - price) / peak_6m < 0.02
                 if peak_6m > 0 else False)
    near_high_20 = ((high20 - price) / high20 < 0.02
                    if high20 > 0 else False)
    breakout = near_high and vol_surge > 1.0

    # ── Today %: prev_close → close (standard) ──
    today_pct = None
    if len(prices) >= 2:
        prev_close = float(prices.iloc[-2])
        if prev_close > 0:
            today_pct = round((price / prev_close - 1) * 100, 2)

    # ── Composite Momentum Score ──
    raw_mom = (
        (mom_2w or 0) * MOM_WEIGHTS[MOM_2W] +
        (mom_1m or 0) * MOM_WEIGHTS[MOM_1M] +
        (mom_3m or 0) * MOM_WEIGHTS[MOM_3M] +
        (mom_6m or 0) * MOM_WEIGHTS[MOM_6M]
    )

    score = raw_mom * 3.0

    # Trend quality multiplier
    if trend_quality:
        score *= 1.20
    elif above_200ma:
        score *= 1.05
    elif ma50_rising:
        score *= 0.90
    else:
        score *= 0.60

    # Acceleration bonus (short-term confirmation)
    if acceleration:
        score *= 1.15

    # Breakout bonus
    if breakout:
        score *= 1.10

    # Volume surge bonus
    if vol_surge > 1.5:
        score *= 1.08

    # ── Multi-level overextension penalties ──
    if overextended:
        penalty = max(0.70, 1.0 - (extension_pct - OVEREXTENSION_PCT) / 100)
        score *= penalty
    if p5 > 35:
        score *= 0.85
    if p5 > 50:
        score *= 0.80
    if p15 > 80:
        score *= 0.80
    if p15 > max(p5, 1) * 4:
        score *= 0.75
    if not acceleration and p5 > 25:
        score *= 0.75
    if not near_high_20 and p15 > 80:
        score *= 0.70

    # Fading penalty
    if p3 <= 0:
        score *= 0.55
    elif p3 < 2:
        score *= 0.85

    # Trailing stop already triggered
    if stop_triggered:
        score *= 0.15

    # All short/mid windows negative
    if (mom_2w or 0) < 0 and (mom_1m or 0) < 0 and (mom_3m or 0) < 0:
        score *= 0.30

    score = round(score, 2)

    return {
        'Ticker': ticker,
        'Price': round(price, 2),
        'Today %': today_pct,
        '3D %': p3,
        '5D %': p5,
        '15D %': p15,
        'Mom 2W': mom_2w,
        'Mom 1M': mom_1m,
        'Mom 3M': mom_3m,
        'Mom 6M': mom_6m,
        'Score': score,
        'MA50': round(ma50, 2),
        'MA200': round(ma200, 2) if ma200 else None,
        'MA50 Rising': ma50_rising,
        'Above 200MA': above_200ma,
        'Above MA20': above_ma20,
        'Trend Quality': trend_quality,
        'Extension %': extension_pct,
        'Overextended': overextended,
        'Drawdown %': drawdown_pct,
        'Stop Triggered': stop_triggered,
        'Stop Threshold': stop_threshold,
        'Vol Surge': vol_surge,
        'Acceleration': acceleration,
        'Breakout': breakout,
        'Near High': near_high_20,
        'Dollar Volume': dollar_volume,
        'Leveraged': is_leveraged(ticker),
    }


# ── Signal / Grade System ─────────────────────────────────────────────

def _signal(r, freq, rank, total, is_ref=False):
    if is_ref:
        return 'SELL'
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


def _grade(r, freq, rank, total, is_ref=False):
    hits = 0
    score = r.get('Score', 0)
    vs = r.get('Vol Surge', 1.0)
    acc = r.get('Acceleration', False)
    brk = r.get('Breakout', False)
    sig = _signal(r, freq, rank, total, is_ref)
    if score > 30:
        hits += 1
    if vs > 1.3 or acc or brk:
        hits += 1
    if freq >= 2:
        hits += 1
    if 'BUY' in sig:
        hits += 1
    return hits


# ── Screener ──────────────────────────────────────────────────────────

def run_screen(name, finviz_urls, bench_ticker, min_dv=30e6, dl_limit=200,
               top_n=15, ensure_ticker=None):
    print(f"\n[{name}]")
    bench = bench_perf(bench_ticker)
    print(
        f"   {bench_ticker} - Today: {fmt_pct(bench['perf_today'])} | "
        f"5D: {bench['perf_5d']:+.2f}% | 15D: {bench['perf_15d']:+.2f}%"
    )

    if isinstance(finviz_urls, str):
        finviz_urls = [finviz_urls]
    all_tickers = []
    seen = set()
    for url in finviz_urls:
        for t in fetch_finviz(url):
            if t not in seen:
                seen.add(t)
                all_tickers.append(t)
    print(f"   Found {len(all_tickers)} unique tickers "
          f"from {len(finviz_urls)} source(s)")

    dl = list(set(all_tickers[:dl_limit]) | ({ensure_ticker} if ensure_ticker else set()))
    print(f"   Downloading {len(dl)} tickers (period=1y)...")
    close_df, vol_df, open_df = bulk_download(dl, period="1y")

    results, ref_data = [], None
    total = len(dl)
    passed_names = []          # tickers that pass filters — enrich after loop

    for i, t in enumerate(dl):
        sys.stdout.write(f"\r   Scoring {i+1}/{total}  " + " " * 20)
        sys.stdout.flush()
        d = score_ticker_v2(t, close_df, vol_df, open_df)
        if d is None:
            continue

        # Benchmark comparison
        d['Today vs'] = round(d['Today %'] - bench['perf_today'], 2) if d.get('Today %') is not None and bench['perf_today'] is not None else None
        d['5D vs'] = round(d['5D %'] - bench['perf_5d'], 2)
        d['15D vs'] = round(d['15D %'] - bench['perf_15d'], 2)

        # Reference ticker
        if t == ensure_ticker:
            ref_data = dict(d)

        if d['Dollar Volume'] < min_dv:
            continue
        if not d.get('Above MA20') and t != ensure_ticker:
            continue
        if (d['Mom 3M'] or 0) <= 0 and (d['Mom 6M'] or 0) <= 0 and t != ensure_ticker:
            continue
        if d['3D %'] <= 0 and d['5D %'] <= 0 and t != ensure_ticker:
            continue

        # Benchmark gate
        if 'STOCK' in name:
            beats_bench = (
                _is_today_leader(d, bench['perf_today'])
                and d['5D %'] > bench['perf_5d']
                and d['15D %'] > bench['perf_15d']
            )
        else:
            beats_bench = d['5D %'] > bench['perf_5d'] or d['15D %'] > bench['perf_15d']

        if beats_bench:
            results.append(d)

    # Clear progress line
    sys.stdout.write("\r" + " " * 50 + "\r")
    sys.stdout.flush()

    # Enrich names AFTER scoring (avoids noisy API calls in progress loop)
    sys.stdout.write(f"   Enriching names for {len(results)} tickers...")
    sys.stdout.flush()
    for d in results:
        d['Name'] = enrich_name(d['Ticker'])
    if ref_data:
        ref_data['Name'] = enrich_name(ref_data['Ticker'])
    sys.stdout.write(f"\r   {len(results)} passed filters" + " " * 20 + "\n")
    sys.stdout.flush()

    if ensure_ticker and ref_data and not any(r['Ticker'] == ensure_ticker for r in results):
        ref_data['_reference'] = True
        results.append(ref_data)

    results.sort(key=lambda x: x.get('Score', 0), reverse=True)
    return results[:top_n], bench


# ── History ───────────────────────────────────────────────────────────

def save_history(stocks, etfs=None):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    if datetime.now().weekday() >= 5:
        print("   Skipping (weekend)")
        return
    path = os.path.join(HISTORY_DIR, f'{datetime.now():%Y-%m-%d}.json')
    data = {'stocks': stocks}
    if etfs:
        data['etfs'] = etfs
    with open(path, 'w') as f:
        json.dump(data, f)
    print(f"   Saved: {path}")
    files = sorted(os.listdir(HISTORY_DIR))
    for old in files[:-MAX_HISTORY]:
        os.remove(os.path.join(HISTORY_DIR, old))


def load_history():
    sh, eh = [], []
    if not os.path.exists(HISTORY_DIR):
        return sh, eh
    for fname in sorted(os.listdir(HISTORY_DIR))[-MAX_HISTORY:]:
        with open(os.path.join(HISTORY_DIR, fname)) as f:
            data = json.load(f)
        date = fname.replace('.json', '')
        if 'stocks' in data:
            sh.append({'date': date, 'stocks': data['stocks']})
        if 'etfs' in data:
            eh.append({'date': date, 'etfs': data['etfs']})
    return sh, eh


def freq_count(history, key='stocks'):
    c = Counter()
    for day in history:
        for item in day.get(key, []):
            c[item['Ticker']] += 1
    return c


# ── Rebalance ─────────────────────────────────────────────────────────

def check_rebalance(regime_level):
    today = datetime.now().date()
    if os.path.exists(REBALANCE_FILE):
        with open(REBALANCE_FILE) as f:
            data = json.load(f)
        last = datetime.strptime(data['date'], '%Y-%m-%d').date()
        days_since = (today - last).days
        prev_level = data.get('regime_level', regime_level)
        regime_drop = prev_level - regime_level
        forced = regime_drop >= 2
        return {'last': str(last), 'days_since': days_since,
                'due': days_since >= 28 or forced,
                'forced': forced, 'regime_drop': regime_drop}
    return {'last': 'Never', 'days_since': 999, 'due': True,
            'forced': False, 'regime_drop': 0}


def save_rebalance(regime_level):
    with open(REBALANCE_FILE, 'w') as f:
        json.dump({'date': datetime.now().strftime('%Y-%m-%d'),
                   'regime_level': regime_level}, f)


# ── Console Output ────────────────────────────────────────────────────

def print_regime(regime):
    print(f"\n{SEP2}")
    print(f"  MARKET REGIME: {regime['label']}  "
          f"(Score: {regime['score']}/100)")
    print(f"  Allocation: {regime['allocation_pct']}% of capital")
    print(f"{SEP2}")

    c = regime['components']
    if 'spy_price' in c:
        status = "ABOVE" if c.get('spy_above_200') else "BELOW"
        rising = "Rising" if c.get('ma200_rising') else "Falling"
        print(f"  SPY:  ${c['spy_price']}  {status} 200MA "
              f"(${c['spy_ma200']})  {rising}")
    if 'spy_ma50' in c:
        r50 = "Rising" if c.get('ma50_rising') else "Falling"
        print(f"  SPY 50MA: ${c['spy_ma50']}  {r50}")
    if 'vix' in c:
        print(f"  VIX:  {c['vix']}  ({c.get('vix_level', '')})")
    if 'fng' in c:
        print(f"  Fear & Greed: {c['fng']}  ({c.get('fng_label', '')})")

    smh = regime.get('smh_bounce', {})
    if smh.get('bounce'):
        print(f"  !! SMH BOUNCE  DD:{smh['drawdown']:.1f}% "
              f"Recovery:{smh['recovery']:.1f}%")

    filled = regime['allocation_pct'] // 10
    bars = "#" * filled + "." * (10 - filled)
    print(f"  Risk Meter: [{bars}] {regime['allocation_pct']}%")


def print_rebalance_status(rb):
    print(f"\n{SEP}")
    if rb.get('forced'):
        tag = f">>> FORCED (regime dropped {rb['regime_drop']} levels) <<<"
    elif rb['due']:
        tag = ">>> DUE NOW <<<"
    else:
        tag = "Not yet due"
    print(f"  REBALANCE: {tag}")
    print(f"  Last: {rb['last']}  |  Days since: {rb['days_since']}  "
          f"|  Cadence: Monthly (~28d)")
    print(f"{SEP}")


def print_results(title, results, bench_ticker, bench, freqs, history_dates,
                  regime_alloc):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")
    print(f"  {bench_ticker}: Today {fmt_pct(bench['perf_today'])} | "
          f"5D {bench['perf_5d']:+.2f}% | 15D {bench['perf_15d']:+.2f}%")
    print(f"  History: {', '.join(history_dates) if history_dates else 'None'}")
    print()

    if not results:
        print("  No results")
        return

    hdr = (f"  {'#':>3}  {'Ticker':<7} {'Name':<22} {'Score':>7} "
           f"{'Today':>7} {'3M':>7} {'6M':>7} "
           f"{'Trend':>5} {'Vol':>5} {'Flags':<5} "
           f"{'Freq':>6} {'Action':<12}")
    div = (f"  {'---':>3}  {'------':<7} {'----':<22} {'-----':>7} "
           f"{'-----':>7} {'-----':>7} {'-----':>7} "
           f"{'-----':>5} {'---':>5} {'-----':<5} "
           f"{'----':>6} {'------':<12}")
    print(hdr)
    print(div)

    total = len(results)
    for i, r in enumerate(results, 1):
        if r['Trend Quality']:
            trend = " Y"
        elif r['Above 200MA'] or r['MA50 Rising']:
            trend = " ~"
        else:
            trend = " N"
        lev = "L" if r.get('Leveraged') else ""
        acc = "A" if r.get('Acceleration') else ""
        brk = "B" if r.get('Breakout') else ""
        stop = "!" if r.get('Stop Triggered') else ""
        flags = f"{acc}{brk}{stop}{lev}" or "-"

        is_ref = r.get('_reference', False)
        freq = freqs.get(r['Ticker'], 0)
        sig = _signal(r, freq, i, total, is_ref)
        hits = _grade(r, freq, i, total, is_ref)
        grade = f"[{hits}/4]" if hits >= 3 else ""
        ref_tag = " (ref)" if is_ref else ""

        print(
            f"  {i:>3}  {r['Ticker']:<7} {r.get('Name', r['Ticker'])[:22]:<22} "
            f"{r['Score']:>7.1f} "
            f"{fmt_pct(r.get('Today %')):>7} "
            f"{fmt_pct(r['Mom 3M']):>7} {fmt_pct(r['Mom 6M']):>7} "
            f"{trend:>5} {r['Vol Surge']:>4.1f}x {flags:<5} "
            f"{freq:>2}/{MAX_HISTORY:<3} {sig:<10} {grade}{ref_tag}"
        )

        if i >= 25:
            remaining = len(results) - 25
            if remaining > 0:
                print(f"  ... ({remaining} more)")
            break


def print_top_picks(stocks, etfs, sf, ef, stock_bench):
    all_candidates = []

    for i, r in enumerate(stocks):
        if r.get('_reference'):
            continue
        f = sf.get(r['Ticker'], 0)
        hits = _grade(r, f, i + 1, len(stocks))
        if hits >= 4 and _is_today_leader(r, stock_bench.get('perf_today')):
            all_candidates.append((r, f, i + 1, hits))

    for i, r in enumerate(etfs):
        if r.get('_reference'):
            continue
        f = ef.get(r['Ticker'], 0)
        hits = _grade(r, f, i + 1, len(etfs))
        if hits >= 4 and _is_today_leader(r, stock_bench.get('perf_today')):
            all_candidates.append((r, f, i + 1, hits))

    threshold = 4
    if not all_candidates:
        threshold = 3
        for i, r in enumerate(stocks):
            if r.get('_reference'):
                continue
            f = sf.get(r['Ticker'], 0)
            hits = _grade(r, f, i + 1, len(stocks))
            if hits >= 3 and _is_today_leader(r, stock_bench.get('perf_today')):
                all_candidates.append((r, f, i + 1, hits))

    all_candidates.sort(key=lambda x: x[0].get('Score', 0), reverse=True)

    print(f"\n{SEP2}")
    print(f"  TOP PICKS -- All {threshold} Criteria Strong")
    print(f"  Today > SOXL by {TODAY_OUTPERFORMANCE_MARGIN:.1f}% | "
          f"Score + Vol/Acc + Freq + Action")
    print(f"{SEP2}")

    if not all_candidates:
        print("  No top picks right now")
        return

    for r, freq, rank, hits in all_candidates[:5]:
        ticker = r.get('Ticker', '?')
        name = r.get('Name', ticker)[:30]
        today = fmt_pct(r.get('Today %'))
        dollar_vol = fmt_dollar_volume(r.get('Dollar Volume'))
        score = r.get('Score', 0)
        vs = r.get('Vol Surge', 1.0)
        acc = " Accel" if r.get('Acceleration') else ""
        brk = " Breakout" if r.get('Breakout') else ""
        sig = _signal(r, freq, rank, 999)
        print(
            f"  >>> {ticker:<7} {name:<30} Today:{today:>8}  "
            f"$Vol:{dollar_vol:>10}  Score:{score:>6.0f}  "
            f"Vol:{vs:.1f}x{acc}{brk}  Freq:{freq}/{MAX_HISTORY}  "
            f"Action:{sig}  [{hits}/4]"
        )


def print_portfolio(stocks, etfs, regime):
    alloc = regime['allocation_pct']

    candidates = [r for r in stocks + etfs
                  if not r.get('Stop Triggered') and r.get('Score', 0) > 0
                  and not r.get('_reference')]
    candidates.sort(key=lambda x: x['Score'], reverse=True)

    quality = [r for r in candidates if r['Trend Quality']]
    partial = [r for r in candidates
               if r['Above 200MA'] and not r['Trend Quality']]
    pool = quality + partial
    picks = pool[:MAX_POSITIONS]

    print(f"\n{SEP2}")
    print(f"  PORTFOLIO  --  Top {MAX_POSITIONS} Picks @ "
          f"{POSITION_SIZE:.0f}% each")
    eff = POSITION_SIZE * alloc / 100
    print(f"  Regime allocation: {alloc}%  -->  "
          f"Effective position: {eff:.1f}% each")
    if alloc == 0:
        print(f"  !!  REGIME = RISK OFF  -->  "
              f"0% allocation. All cash recommended.")
    print(f"{SEP2}")

    if not picks:
        print("  No qualifying picks.")
        return

    for i, r in enumerate(picks, 1):
        eff_size = POSITION_SIZE * alloc / 100
        trend = ("Confirmed (50MA rising + >200MA)"
                 if r['Trend Quality'] else "Partial")
        ext_warn = (f"  !! OVEREXTENDED ({r['Extension %']:+.0f}%)"
                    if r['Overextended'] else "")
        stop_type = ("Leveraged 25%"
                     if r['Leveraged'] else "Stock 15%")
        stop_pct = (TRAILING_STOP_LEVERAGED
                    if r['Leveraged'] else TRAILING_STOP_STOCK)
        stop_price = r['Price'] * (1 - stop_pct / 100)

        print(f"\n  #{i}  {r['Ticker']}  --  {r.get('Name', r['Ticker'])[:30]}")
        print(f"      Price: ${r['Price']:.2f}  |  Score: {r['Score']:.1f}")
        print(f"      Mom: 2W {fmt_pct(r['Mom 2W'])} | "
              f"1M {fmt_pct(r['Mom 1M'])} | "
              f"3M {fmt_pct(r['Mom 3M'])} | "
              f"6M {fmt_pct(r['Mom 6M'])}")
        print(f"      Trend: {trend}{ext_warn}")
        print(f"      50MA: ${r['MA50']}  |  "
              f"200MA: ${r['MA200'] or 'N/A'}")
        print(f"      Size: {eff_size:.1f}%  |  "
              f"Trailing Stop: ${stop_price:.2f} ({stop_type})")
        print(f"      Vol: {r['Vol Surge']:.1f}x  |  "
              f"$Vol: {fmt_dollar_volume(r['Dollar Volume'])}  |  "
              f"DD: {r['Drawdown %']:+.1f}%")


# ── HTML Generation ───────────────────────────────────────────────────

CSS = """* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0d1117; color:#c9d1d9; min-height:100vh; padding:2rem; }
.c { max-width:1200px; margin:0 auto; }
h1 { font-size:1.5rem; margin-bottom:.5rem; color:#58a6ff; }
h2 { font-size:1.1rem; margin:1.5rem 0 .5rem; color:#8b949e; border-bottom:1px solid #30363d; padding-bottom:.5rem; }
h2.tp { color:#f0c040; border-bottom:2px solid #f0c040; font-size:1.3rem; }
.date { color:#8b949e; font-size:.875rem; margin-bottom:2rem; }
.sig { font-size:2.5rem; font-weight:bold; margin:1rem 0; padding:1rem 2rem; border-radius:.5rem; display:inline-block; }
.sig.buy { background:#238636; color:#fff; }
.sig.sell { background:#da3633; color:#fff; }
.sig.cautious { background:#d29922; color:#fff; }
.metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1rem; margin-bottom:2rem; }
.m { background:#161b22; padding:1rem; border-radius:.5rem; }
.ml { color:#8b949e; font-size:.75rem; margin-bottom:.25rem; }
.mv { font-size:1.25rem; font-weight:600; }
.ms { font-size:.75rem; margin-top:.25rem; }
.sf { color:#f85149; } .sg { color:#3fb950; } .sn { color:#d29922; }
table { width:100%; border-collapse:collapse; margin-bottom:1rem; }
th,td { text-align:left; padding:.5rem .75rem; border-bottom:1px solid #30363d; }
th { color:#8b949e; font-weight:500; font-size:.75rem; text-transform:uppercase; }
td { font-size:.85rem; } tr:hover { background:#161b22; }
.bi { color:#8b949e; font-size:.875rem; margin-bottom:1rem; }
.hi { color:#58a6ff; font-size:.875rem; margin-bottom:1rem; }
tr.champ { background:linear-gradient(90deg,#1a2a1a 0%,#0d1117 100%); border-left:3px solid #f0c040; }
tr.champ td { font-weight:600; }
.badge { display:inline-block; font-size:.65rem; padding:2px 6px; border-radius:3px; margin-left:4px; font-weight:700; vertical-align:middle; }
.badge.g4 { background:#238636; color:#fff; }
.badge.g3 { background:#1a6b2a; color:#ccc; }
.dim { opacity:.5; }
.tp-card { background:linear-gradient(135deg,#1a2a1a,#161b22); border:1px solid #f0c040; border-radius:.5rem; padding:1rem 1.5rem; margin-bottom:.75rem; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.5rem; }
.tp-card .tk { font-size:1.3rem; font-weight:700; color:#f0c040; }
.tp-card .nm { color:#8b949e; font-size:.85rem; }
.tp-card .st { display:flex; gap:1.5rem; align-items:center; flex-wrap:wrap; }
.tp-card .st div { text-align:center; }
.tp-card .st .lbl { font-size:.65rem; color:#8b949e; text-transform:uppercase; }
.tp-card .st .val { font-size:1rem; font-weight:600; }
.regime-bar { background:#161b22; border-radius:.25rem; height:1.5rem; margin:.5rem 0; position:relative; }
.regime-fill { height:100%; border-radius:.25rem; }"""


def _sig_html(sig):
    colors = {'STRONG BUY': '#00ff7f', 'BUY': '#3fb950', 'SELL': '#f85149', 'HOLD': '#d29922'}
    c = colors.get(sig, '#d29922')
    return f"<span style='color:{c};font-weight:bold'>{sig}</span>"


def _grade_badge(hits):
    if hits == 4:
        return "<span class='badge g4'>&#x2B50; 4/4</span>"
    elif hits == 3:
        return "<span class='badge g3'>3/4</span>"
    return ""


def _html_row(i, r, freq, total, is_ref=False):
    vs = r.get('Vol Surge', 1.0)
    vs_s = f"<span style='color:#3fb950'>&#x2191;{vs:.1f}x</span>" if vs > 1.3 else f"{vs:.1f}x"
    acc = "&#x1F525;" if r.get('Acceleration') else ""
    brk = "&#x1F4C8;" if r.get('Breakout') else ""
    ref = " <span style='color:#8b949e;font-size:.75rem'>(ref)</span>" if is_ref else ""
    sig = _signal(r, freq, i, total, is_ref)
    hits = _grade(r, freq, i, total, is_ref)
    badge = _grade_badge(hits)
    if is_ref:
        cls = " style='background:#1c2333;opacity:.85'"
    elif hits >= 4:
        cls = " class='champ'"
    elif hits <= 1 and not is_ref:
        cls = " class='dim'"
    else:
        cls = ""
    return (f"<tr{cls}><td>{i}</td><td>{r['Ticker']}{ref}{badge}</td>"
            f"<td>{r.get('Name', r['Ticker'])[:25]}</td>"
            f"<td>{fmt_pct(r.get('Today %'))}</td>"
            f"<td>{fmt_dollar_volume(r.get('Dollar Volume'))}</td>"
            f"<td>{r.get('Score', 0):.0f}</td>"
            f"<td>{vs_s} {acc}{brk}</td>"
            f"<td>{freq}/{MAX_HISTORY}</td>"
            f"<td>{_sig_html(sig)}</td></tr>")


def _html_table(rows_html):
    hdr = ''.join(f'<th>{h}</th>' for h in
                  ['#', 'Ticker', 'Name', 'Today', '$Vol', 'Score',
                   'Vol/Acc', 'Freq', 'Action'])
    return f"<table><thead><tr>{hdr}</tr></thead><tbody>{rows_html}</tbody></table>"


def _html_top_card(r, freq, rank, total):
    vs = r.get('Vol Surge', 1.0)
    vs_s = f"&#x2191;{vs:.1f}x" if vs > 1.3 else f"{vs:.1f}x"
    acc_s = " &#x1F525;" if r.get('Acceleration') else ""
    brk_s = " &#x1F4C8;" if r.get('Breakout') else ""
    sig = _signal(r, freq, rank, total)
    sc = r.get('Score', 0)
    sig_color = '#00ff7f' if 'STRONG' in sig else '#3fb950'
    return (f"<div class='tp-card'>"
            f"<div><span class='tk'>&#x2B50; {r['Ticker']}</span>"
            f"<span class='nm'> &mdash; {r.get('Name', r['Ticker'])[:30]}</span></div>"
            f"<div class='st'>"
            f"<div><div class='lbl'>Today</div><div class='val' style='color:#3fb950'>{fmt_pct(r.get('Today %'))}</div></div>"
            f"<div><div class='lbl'>$Vol</div><div class='val'>{fmt_dollar_volume(r.get('Dollar Volume'))}</div></div>"
            f"<div><div class='lbl'>Score</div><div class='val' style='color:#3fb950'>{sc:.0f}</div></div>"
            f"<div><div class='lbl'>Vol/Acc</div><div class='val' style='color:#3fb950'>{vs_s}{acc_s}{brk_s}</div></div>"
            f"<div><div class='lbl'>Freq</div><div class='val' style='color:#3fb950'>{freq}/{MAX_HISTORY}</div></div>"
            f"<div><div class='lbl'>Action</div><div class='val' style='color:{sig_color}'>{sig}</div></div>"
            f"</div></div>")


def generate_html(regime, stocks, sb, etfs, eb, sf, ef, sh, eh):
    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    c = regime['components']
    smh = regime.get('smh_bounce', {})

    alloc = regime['allocation_pct']
    regime_cls = 'buy' if alloc >= 60 else 'cautious' if alloc >= 30 else 'sell'
    smh_h = (f"<p style='color:#f0883e;font-weight:bold'>&#x26A0; SMH BOUNCE "
             f"(DD:{smh['drawdown']:.1f}% Rec:{smh['recovery']:.1f}%)</p>") if smh.get('bounce') else ""

    vix_val = c.get('vix', 'N/A')
    vix_lvl = c.get('vix_level', '')
    fng_val = c.get('fng', 'N/A')
    fng_lbl = c.get('fng_label', '')
    spy_p = c.get('spy_price', 'N/A')
    spy_ma = c.get('spy_ma200', 'N/A')
    spy_above = c.get('spy_above_200', False)
    fng_cls = ('sf' if isinstance(fng_val, int) and fng_val <= 40
               else 'sg' if isinstance(fng_val, int) and fng_val >= 60 else 'sn')

    fill_color = '#238636' if alloc >= 60 else '#d29922' if alloc >= 30 else '#da3633'
    regime_bar = (f"<div class='regime-bar'><div class='regime-fill' "
                  f"style='width:{alloc}%;background:{fill_color}'></div></div>")

    e_rows = "".join(_html_row(i+1, r, ef.get(r['Ticker'], 0), len(etfs),
                               r.get('_reference')) for i, r in enumerate(etfs))
    s_rows = "".join(_html_row(i+1, r, sf.get(r['Ticker'], 0), len(stocks),
                               r.get('_reference')) for i, r in enumerate(stocks))

    # Top picks HTML
    top_cards = ""
    all_items = stocks + etfs
    for i, r in enumerate(all_items):
        if r.get('_reference'):
            continue
        f = sf.get(r['Ticker'], 0) if i < len(stocks) else ef.get(r['Ticker'], 0)
        hits = _grade(r, f, i + 1, len(all_items))
        if hits >= 4 and _is_today_leader(r, sb.get('perf_today')):
            top_cards += _html_top_card(r, f, i + 1, len(all_items))

    top_section = ""
    if top_cards:
        top_section = f"<h2 class='tp'>&#x2B50; Top Picks (4/4 Criteria)</h2>{top_cards}"

    s_days = ", ".join(d['date'] for d in sh) or "None"
    e_days = ", ".join(d['date'] for d in eh) or "None"

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Momentum Screener v3</title><style>{CSS}</style></head>
<body><div class="c">
<h1>Momentum Screener v3</h1><p class="date">{now}</p>
<div class="sig {regime_cls}">{regime['label']}</div>
{smh_h}
{regime_bar}
<div class="metrics">
<div class="m"><div class="ml">Regime Score</div><div class="mv">{regime['score']}/100</div><div class="ms">{regime['label']}</div></div>
<div class="m"><div class="ml">Allocation</div><div class="mv">{alloc}%</div><div class="ms">{MAX_POSITIONS} pos @ {POSITION_SIZE:.0f}%</div></div>
<div class="m"><div class="ml">VIX</div><div class="mv">{vix_val}</div><div class="ms">{vix_lvl}</div></div>
<div class="m"><div class="ml">Fear &amp; Greed</div><div class="mv">{fng_val}</div><div class="ms {fng_cls}">{fng_lbl}</div></div>
<div class="m"><div class="ml">SPY vs 200MA</div><div class="mv">${spy_p}</div><div class="ms">{'ABOVE' if spy_above else 'BELOW'} ${spy_ma}</div></div>
<div class="m"><div class="ml">Stops</div><div class="mv">{TRAILING_STOP_STOCK:.0f}%/{TRAILING_STOP_LEVERAGED:.0f}%</div><div class="ms">Stock / Leveraged</div></div>
</div>

{top_section}

<h2>Stock Screener (Beat SOXL)</h2>
<p class="bi">SOXL: Today {fmt_pct(sb['perf_today'])} | 5D {sb['perf_5d']:+.2f}% | 15D {sb['perf_15d']:+.2f}%</p>
<p class="hi">Filter: Today &gt; SOXL by {TODAY_OUTPERFORMANCE_MARGIN:.1f}% + 5D/15D beat  |  History: {s_days}</p>
{_html_table(s_rows)}

<h2>ETF Screener (Beat QQQ)</h2>
<p class="bi">QQQ: Today {fmt_pct(eb['perf_today'])} | 5D {eb['perf_5d']:+.2f}% | 15D {eb['perf_15d']:+.2f}%</p>
<p class="hi">History: {e_days}</p>
{_html_table(e_rows)}

</div></body></html>"""


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Momentum Screener v3 (Console + HTML)")
    parser.add_argument("--mark-rebalanced", action="store_true",
                        help="Record today as rebalance date")
    parser.add_argument("--no-save-history", action="store_true",
                        help="Skip saving today's results to history/")
    parser.add_argument("--no-html", action="store_true",
                        help="Skip HTML output")
    args = parser.parse_args()

    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    print(f"\n{SEP2}")
    print(f"  Momentum Screener v3  |  {now}")
    print(f"  Windows: 2W/1M/3M/6M | Benchmark: SOXL/QQQ | "
          f"Stops: 15%/25% | Regime: 4-level")
    print(f"{SEP2}")

    # ── Regime ──
    regime = get_regime()
    print_regime(regime)

    # ── Rebalance check ──
    rb = check_rebalance(regime['allocation_pct'] // 25)
    print_rebalance_status(rb)

    if args.mark_rebalanced:
        save_rebalance(regime['allocation_pct'] // 25)
        print("  Rebalance date saved.")

    # ── Screen stocks (benchmark: SOXL) ──
    stocks, stock_bench = run_screen(
        "STOCK SCREENER (vs SOXL)",
        [STOCK_URL, STOCK_URL2, STOCK_URL3],
        bench_ticker="SOXL",
        min_dv=30e6, dl_limit=200,
    )

    # ── Screen ETFs (benchmark: QQQ) ──
    etfs, etf_bench = run_screen(
        "ETF SCREENER (vs QQQ)",
        [ETF_URL],
        bench_ticker="QQQ",
        min_dv=50e6, dl_limit=80,
    )

    # ── Load history for frequency ──
    stock_history, etf_history = load_history()
    stock_freqs = freq_count(stock_history)
    etf_freqs = freq_count(etf_history)
    s_dates = [d['date'] for d in stock_history]
    e_dates = [d['date'] for d in etf_history]

    # ── Print full tables ──
    print_results("STOCKS — Ranked by Momentum (Beat SOXL)",
                  stocks, "SOXL", stock_bench, stock_freqs, s_dates,
                  regime['allocation_pct'])
    print_results("ETFs — Ranked by Momentum (Beat QQQ)",
                  etfs, "QQQ", etf_bench, etf_freqs, e_dates,
                  regime['allocation_pct'])

    # ── Top Picks ──
    print_top_picks(stocks, etfs, stock_freqs, etf_freqs, stock_bench)

    # ── Portfolio picks ──
    print_portfolio(stocks, etfs, regime)

    # ── Save history ──
    if not args.no_save_history:
        save_history('stocks', stocks)
        save_history('etfs', etfs)

    # ── Generate HTML ──
    if not args.no_html:
        html = generate_html(regime, stocks, stock_bench, etfs, etf_bench,
                             stock_freqs, etf_freqs, stock_history, etf_history)
        html_path = os.path.join(os.path.dirname(__file__), "momentum.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n  HTML saved: {html_path}")

    # ── Summary ──
    print(f"\n{SEP2}")
    print(f"  SUMMARY")
    print(f"{SEP2}")
    print(f"  Regime: {regime['label']} ({regime['score']}/100) "
          f"--> {regime['allocation_pct']}% allocation")
    print(f"  Stocks: {len(stocks)} passed (bench SOXL)  |  "
          f"ETFs: {len(etfs)} passed (bench QQQ)")
    print(f"  History days: stocks {len(stock_history)} / "
          f"etfs {len(etf_history)}")
    if rb.get('forced'):
        print(f"  Rebalance: FORCED (regime drop)")
    elif rb['due']:
        print(f"  Rebalance: DUE")
    else:
        remaining = max(0, 28 - rb['days_since'])
        print(f"  Rebalance: Next in ~{remaining}d")
    print(f"  Trailing stops: {TRAILING_STOP_STOCK}% stocks / "
          f"{TRAILING_STOP_LEVERAGED}% leveraged")
    print(f"  Max positions: {MAX_POSITIONS} @ "
          f"{POSITION_SIZE}% each")
    print(f"\n  [DONE]\n")


if __name__ == "__main__":
    main()
