#!/usr/bin/env python3
"""
Momentum Console Screener v2 — Academic-grade momentum with risk management.

Key changes from v1 (run_all_console.py):
  Momentum windows  : 2W / 1M / 3M / 6M  (strongest empirical factor)
  Trailing stops     : 15% stocks / 25% leveraged
  Regime scaling     : 4 levels (100 / 60 / 30 / 0%)
  Positions          : 5 max @ 20% each
  Rebalance          : Monthly (~28 days)
  Trend quality      : 50MA rising + above 200MA
  Entry quality      : Penalize > 20% above 50MA
"""

import sys, os, json, re, argparse
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yfinance as yf
import requests
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))

# ── Configuration ──────────────────────────────────────────────────────
MAX_POSITIONS = 5
POSITION_SIZE = 20.0              # % per position
TRAILING_STOP_STOCK = 15.0        # %
TRAILING_STOP_LEVERAGED = 25.0    # %

# Momentum windows (trading days)
MOM_2W = 10
MOM_1M = 21
MOM_3M = 63
MOM_6M = 126

# Momentum weights (sum = 1.0) — heavier on 3M/6M per academic evidence
MOM_WEIGHTS = {MOM_2W: 0.10, MOM_1M: 0.20, MOM_3M: 0.35, MOM_6M: 0.35}

# Overextension threshold: penalize if price > 20% above 50MA
OVEREXTENSION_PCT = 20.0

# Known leveraged ETFs
LEVERAGED_TICKERS = {
    'TQQQ', 'SOXL', 'UPRO', 'SPXL', 'TECL', 'FNGU', 'LABU', 'TNA',
    'UDOW', 'CURE', 'DFEN', 'DRN', 'DUSL', 'FAS', 'HIBL', 'MIDU',
    'NAIL', 'RETL', 'TPOR', 'WANT', 'WEBL',
    'SQQQ', 'SPXS', 'SDOW', 'FAZ', 'TZA', 'LABD',
}

# Finviz URLs (same sources as existing screener)
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
        data = yf.download(tickers, period=period, auto_adjust=True,
                           threads=True, progress=False)
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


# ── Regime Detection (4 levels) ───────────────────────────────────────

def get_regime():
    """
    4-level market regime:
      100% FULL RISK ON  — SPY above rising 200MA, low VIX, greed
       60% MODERATE       — mostly bullish, some caution
       30% CAUTIOUS       — mixed signals
        0% RISK OFF       — bear market, high VIX
    """
    regime_score = 0
    components = {}

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
        components['fng'] = fng
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

    regime_score = max(0, min(100, regime_score))

    if regime_score >= 75:
        level, alloc, label = 4, 100, "FULL RISK ON"
    elif regime_score >= 55:
        level, alloc, label = 3, 60, "MODERATE"
    elif regime_score >= 35:
        level, alloc, label = 2, 30, "CAUTIOUS"
    else:
        level, alloc, label = 1, 0, "RISK OFF"

    return {
        'score': regime_score,
        'level': level,
        'allocation_pct': alloc,
        'label': label,
        'components': components,
    }


# ── Ticker Scoring ────────────────────────────────────────────────────

def score_ticker_v2(ticker, close_df, vol_df, open_df):
    """
    Score using 2W/1M/3M/6M momentum, trend quality, entry quality,
    and trailing stop detection.
    """
    if ticker not in close_df.columns:
        return None

    prices = close_df[ticker].dropna()
    vols = (vol_df[ticker].dropna()
            if ticker in vol_df.columns else pd.Series(dtype=float))

    # Need enough bars for 6M momentum
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

    # ── Moving Averages ──
    ma50 = float(prices.tail(50).mean())
    ma200 = (float(prices.tail(200).mean())
             if len(prices) >= 200 else None)

    # 50MA from 20 trading days ago
    ma50_prev = (float(prices.iloc[:-20].tail(50).mean())
                 if len(prices) >= 70 else ma50)

    ma50_rising = ma50 > ma50_prev
    above_200ma = price > ma200 if ma200 else False

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
    near_high = ((peak_6m - price) / peak_6m < 0.02
                 if peak_6m > 0 else False)
    breakout = near_high and vol_surge > 1.0

    # ── Composite Momentum Score ──
    raw_mom = (
        (mom_2w or 0) * MOM_WEIGHTS[MOM_2W] +
        (mom_1m or 0) * MOM_WEIGHTS[MOM_1M] +
        (mom_3m or 0) * MOM_WEIGHTS[MOM_3M] +
        (mom_6m or 0) * MOM_WEIGHTS[MOM_6M]
    )

    score = raw_mom * 3.0  # scale for readability

    # Trend quality multiplier
    if trend_quality:
        score *= 1.20       # confirmed uptrend: bonus
    elif above_200ma:
        score *= 1.05       # above 200MA but 50MA not rising
    elif ma50_rising:
        score *= 0.90       # 50MA rising but below 200MA
    else:
        score *= 0.60       # no trend confirmation

    # Breakout bonus
    if breakout:
        score *= 1.10

    # Volume surge bonus
    if vol_surge > 1.5:
        score *= 1.08

    # Entry quality penalty — avoids buying tops
    if overextended:
        penalty = max(0.70, 1.0 - (extension_pct - OVEREXTENSION_PCT) / 100)
        score *= penalty

    # Trailing stop already triggered → heavily penalize
    if stop_triggered:
        score *= 0.15

    # All short/mid windows negative → fading
    if (mom_2w or 0) < 0 and (mom_1m or 0) < 0 and (mom_3m or 0) < 0:
        score *= 0.30

    score = round(score, 2)

    # Today's change (open→close)
    today_pct = None
    if ticker in open_df.columns:
        opens = open_df[ticker].dropna()
        if len(opens) > 0:
            latest_open = float(opens.iloc[-1])
            if latest_open > 0:
                today_pct = round((price / latest_open - 1) * 100, 2)

    return {
        'Ticker': ticker,
        'Price': round(price, 2),
        'Today %': today_pct,
        'Mom 2W': mom_2w,
        'Mom 1M': mom_1m,
        'Mom 3M': mom_3m,
        'Mom 6M': mom_6m,
        'Score': score,
        'MA50': round(ma50, 2),
        'MA200': round(ma200, 2) if ma200 else None,
        'MA50 Rising': ma50_rising,
        'Above 200MA': above_200ma,
        'Trend Quality': trend_quality,
        'Extension %': extension_pct,
        'Overextended': overextended,
        'Drawdown %': drawdown_pct,
        'Stop Triggered': stop_triggered,
        'Stop Threshold': stop_threshold,
        'Vol Surge': vol_surge,
        'Breakout': breakout,
        'Dollar Volume': dollar_volume,
        'Leveraged': is_leveraged(ticker),
    }


# ── Screener ──────────────────────────────────────────────────────────

def run_screen(name, finviz_urls, min_dv=30e6, dl_limit=200):
    print(f"\n[{name}]")

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

    dl = all_tickers[:dl_limit]
    print(f"   Downloading {len(dl)} tickers (period=1y)...")
    close_df, vol_df, open_df = bulk_download(dl, period="1y")

    results = []
    for i, t in enumerate(dl):
        print(f"   Scoring {i+1}/{len(dl)}: {t}     ", end='\r')
        d = score_ticker_v2(t, close_df, vol_df, open_df)
        if d is None:
            continue
        if d['Dollar Volume'] < min_dv:
            continue
        # Must have positive 3M or 6M momentum
        if (d['Mom 3M'] or 0) <= 0 and (d['Mom 6M'] or 0) <= 0:
            continue
        d['Name'] = enrich_name(t)
        results.append(d)

    print(f"\n   {len(results)} passed filters")
    results.sort(key=lambda x: x['Score'], reverse=True)
    return results


# ── Rebalance ─────────────────────────────────────────────────────────

def check_rebalance():
    today = datetime.now().date()
    if os.path.exists(REBALANCE_FILE):
        with open(REBALANCE_FILE) as f:
            data = json.load(f)
        last = datetime.strptime(data['date'], '%Y-%m-%d').date()
        days_since = (today - last).days
        return {'last': str(last), 'days_since': days_since,
                'due': days_since >= 28}
    return {'last': 'Never', 'days_since': 999, 'due': True}


def save_rebalance():
    with open(REBALANCE_FILE, 'w') as f:
        json.dump({'date': datetime.now().strftime('%Y-%m-%d')}, f)


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
        print(f"  VIX:  {c['vix']}")
    if 'fng' in c:
        print(f"  Fear & Greed: {c['fng']}")

    filled = regime['allocation_pct'] // 10
    bars = "#" * filled + "." * (10 - filled)
    print(f"  Risk Meter: [{bars}] {regime['allocation_pct']}%")


def print_rebalance_status(rb):
    print(f"\n{SEP}")
    tag = ">>> DUE NOW <<<" if rb['due'] else "Not yet due"
    print(f"  REBALANCE: {tag}")
    print(f"  Last: {rb['last']}  |  Days since: {rb['days_since']}  "
          f"|  Cadence: Monthly (~28d)")
    print(f"{SEP}")


def print_results(title, results, regime_alloc):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")

    if not results:
        print("  No results")
        return

    hdr = (f"  {'#':>3}  {'Ticker':<7} {'Name':<22} {'Score':>7} "
           f"{'2W':>7} {'1M':>7} {'3M':>7} {'6M':>7} "
           f"{'Trend':>5} {'Ext%':>6} {'DD%':>6} {'Stop':>4} "
           f"{'Vol':>5} {'$Vol':>10}")
    div = (f"  {'---':>3}  {'-------':<7} {'----------------------':<22} "
           f"{'-------':>7} {'-------':>7} {'-------':>7} "
           f"{'-------':>7} {'-------':>7} {'-----':>5} "
           f"{'------':>6} {'------':>6} {'----':>4} "
           f"{'-----':>5} {'----------':>10}")
    print(hdr)
    print(div)

    for i, r in enumerate(results, 1):
        if r['Trend Quality']:
            trend = " Y"
        elif r['Above 200MA'] or r['MA50 Rising']:
            trend = " ~"
        else:
            trend = " N"
        stop = "!!!" if r['Stop Triggered'] else ""
        ext = (f"{r['Extension %']:+.0f}%"
               if r['Extension %'] > 10 else "")
        lev = " L" if r['Leveraged'] else ""

        print(
            f"  {i:>3}  {r['Ticker']:<7} {r['Name'][:22]:<22} "
            f"{r['Score']:>7.1f} "
            f"{fmt_pct(r['Mom 2W']):>7} {fmt_pct(r['Mom 1M']):>7} "
            f"{fmt_pct(r['Mom 3M']):>7} {fmt_pct(r['Mom 6M']):>7} "
            f"{trend:>5} {ext:>6} {r['Drawdown %']:>+5.1f}% "
            f"{stop:>4} {r['Vol Surge']:.1f}x "
            f"{fmt_dollar_volume(r['Dollar Volume']):>10}{lev}"
        )

        if i >= 25:
            remaining = len(results) - 25
            if remaining > 0:
                print(f"  ... ({remaining} more)")
            break


def print_portfolio(stocks, etfs, regime):
    """Build top-5 portfolio from combined candidates."""
    alloc = regime['allocation_pct']

    candidates = [r for r in stocks + etfs
                  if not r['Stop Triggered'] and r['Score'] > 0]
    candidates.sort(key=lambda x: x['Score'], reverse=True)

    # Prefer trend-quality names; fill remainder with above-200MA
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

        print(f"\n  #{i}  {r['Ticker']}  --  {r['Name'][:30]}")
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


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Momentum Screener v2 (Console)")
    parser.add_argument("--mark-rebalanced", action="store_true",
                        help="Record today as rebalance date")
    args = parser.parse_args()

    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    print(f"\n{SEP2}")
    print(f"  Momentum Screener v2  |  {now}")
    print(f"  Windows: 2W/1M/3M/6M | Stops: 15%/25% "
          f"| Regime: 4-level | Monthly rebalance")
    print(f"{SEP2}")

    # ── Regime ──
    regime = get_regime()
    print_regime(regime)

    # ── Rebalance check ──
    rb = check_rebalance()
    print_rebalance_status(rb)

    if args.mark_rebalanced:
        save_rebalance()
        print("  Rebalance date saved.")

    # ── Screen stocks ──
    stocks = run_screen(
        "STOCK SCREENER",
        [STOCK_URL, STOCK_URL2, STOCK_URL3],
        min_dv=30e6, dl_limit=200,
    )

    # ── Screen ETFs ──
    etfs = run_screen(
        "ETF SCREENER",
        [ETF_URL],
        min_dv=50e6, dl_limit=80,
    )

    # ── Print full tables ──
    print_results("STOCKS  --  Ranked by Momentum Score",
                  stocks, regime['allocation_pct'])
    print_results("ETFs  --  Ranked by Momentum Score",
                  etfs, regime['allocation_pct'])

    # ── Portfolio picks ──
    print_portfolio(stocks, etfs, regime)

    # ── Summary ──
    print(f"\n{SEP2}")
    print(f"  SUMMARY")
    print(f"{SEP2}")
    print(f"  Regime: {regime['label']} ({regime['score']}/100) "
          f"--> {regime['allocation_pct']}% allocation")
    print(f"  Stocks screened: {len(stocks)} passed  |  "
          f"ETFs screened: {len(etfs)} passed")
    if rb['due']:
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
