#!/usr/bin/env python3
"""Market screener: finds US stocks & ETFs with strong momentum vs benchmarks."""

import sys, os, json, re, argparse
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yfinance as yf
import requests
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(ROOT, 'history')
MAX_HISTORY = 10
TODAY_OUTPERFORMANCE_MARGIN = 1.0
ETF_URL = "https://finviz.com/screener.ashx?v=411&f=ind_exchangetradedfund%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&o=-volume"
STOCK_URL = "https://finviz.com/screener.ashx?v=411&f=sh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&ft=3&o=-volume"
# Extra sources: weekly gainers with volume, new highs, recent breakouts
STOCK_URL2 = "https://finviz.com/screener.ashx?v=411&f=sh_avgvol_o400%2Csh_price_o10%2Cta_change_u%2Cta_perf_1w10o%2Cta_sma20_pa&ft=3&o=-perf1w"
STOCK_URL3 = "https://finviz.com/screener.ashx?v=411&f=sh_avgvol_o400%2Csh_price_o10%2Cta_change_u%2Cta_changeopen_u%2Cta_perf_1w10o%2Cta_sma20_pa&ft=3&o=-perf1w"

# -- Helpers -----------------------------------------------------------

def pct(series, n):
    if len(series) < n:
        return 0
    return round((float(series.iloc[-1]) / float(series.iloc[-n]) - 1) * 100, 2)


def intraday_pct(open_price, close_price):
    if open_price is None or open_price <= 0:
        return None
    return round((float(close_price) / float(open_price) - 1) * 100, 2)


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


def bulk_download(tickers):
    if not tickers:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        data = yf.download(tickers, period="3mo", auto_adjust=True, threads=True, progress=False)
        if data.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        if len(tickers) == 1:
            return (data[['Close']].rename(columns={'Close': tickers[0]}),
                    data[['Volume']].rename(columns={'Volume': tickers[0]}),
                    data[['Open']].rename(columns={'Open': tickers[0]}))
        return (data.get('Close', pd.DataFrame()), data.get('Volume', pd.DataFrame()),
                data.get('Open', pd.DataFrame()))
    except Exception as e:
        print(f"   Download error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def score_ticker(ticker, close_df, vol_df, open_df, min_bars=15):
    try:
        if ticker not in close_df.columns:
            return None
        prices = close_df[ticker].dropna()
        vols = vol_df[ticker].dropna()
        if len(prices) < min_bars:
            return None
        price = float(prices.iloc[-1])
        if price <= 0:
            return None
        p3, p5, p15 = pct(prices, 3), pct(prices, 5), pct(prices, 15)
        ma20 = float(prices.tail(20).mean())
        ma50 = float(prices.tail(50).mean()) if len(prices) >= 50 else price
        avg20 = float(vols.tail(20).mean())
        avg5 = float(vols.tail(5).mean())
        high20 = float(prices.tail(20).max())
        today_pct = None
        # True acceleration: 3-day annualized pace beats 5-day pace
        rate3 = (p3 / 3) if p3 > 0 else 0
        rate5 = (p5 / 5) if p5 > 0 else 0
        acc = rate3 > rate5 and p3 > 1.0  # gaining speed AND 3D > +1%
        # Breakout: price within 2% of 20-day high with volume
        near_high = (high20 - price) / high20 < 0.02 if high20 > 0 else False
        vs = round(avg5 / avg20, 2) if avg20 > 0 else 1.0
        breakout = near_high and vs > 1.0
        # Change from open: compare latest close to latest open
        change_open_up = False
        if ticker in open_df.columns:
            opens = open_df[ticker].dropna()
            if len(opens) > 0:
                latest_open = float(opens.iloc[-1])
                change_open_up = price > latest_open
                today_pct = intraday_pct(latest_open, price)
        return {
            'Ticker': ticker, 'Price': round(price, 2),
            'Today %': today_pct,
            '3D %': p3, '5D %': p5, '15D %': p15,
            'Dollar Volume': price * avg20,
            'Above MA20': price > ma20, 'Above MA50': price > ma50,
            'Vol Surge': vs,
            'Acceleration': acc,
            'Breakout': breakout,
            'Near High': near_high,
            'ChangeOpenUp': change_open_up,
        }
    except Exception:
        return None


def enrich(data, ticker):
    try:
        data['Name'] = yf.Ticker(ticker).info.get('shortName', ticker)
    except Exception:
        data['Name'] = ticker
    return data


def bench_perf(ticker):
    try:
        h = yf.Ticker(ticker).history(period="3mo")
        latest_open = float(h['Open'].iloc[-1]) if not h.empty else None
        latest_close = float(h['Close'].iloc[-1]) if not h.empty else None
        return {
            'ticker': ticker,
            'perf_today': intraday_pct(latest_open, latest_close),
            'perf_5d': pct(h['Close'], 5),
            'perf_15d': pct(h['Close'], 15),
        }
    except Exception:
        return {'ticker': ticker, 'perf_today': None, 'perf_5d': 0, 'perf_15d': 0}


def _is_today_leader(r, bench_today, margin=TODAY_OUTPERFORMANCE_MARGIN):
    today_pct = r.get('Today %')
    if today_pct is None or bench_today is None:
        return False
    return today_pct >= bench_today + margin


def _fmt_pct(value):
    return 'N/A' if value is None else f"{value:+.2f}%"


def _fmt_dollar_volume(value):
    if value is None:
        return 'N/A'
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    if value >= 1e6:
        return f"${value / 1e6:.1f}M"
    return f"${value:,.0f}"


def calc_composite(d):
    def clamp(value, low, high):
        return max(low, min(value, high))

    # Bound raw returns so squeeze-like moves do not dominate the table.
    p3 = clamp(d['3D %'], -8, 12)
    p5 = clamp(d['5D %'], -10, 25)
    p15 = clamp(d['15D %'], -15, 45)
    abs_mom = p3 * 3.0 + p5 * 2.0 + p15 * 1.0

    # Relative outperformance is helpful, but it should be capped as a bonus.
    rel_vs = clamp((d['5D vs'] + d['15D vs']) / 2, 0, 40)
    sc = abs_mom + rel_vs * 0.6

    # Acceleration: stocks gaining speed get a measured boost.
    if d['Acceleration']:
        sc *= 1.15

    # Breakout: near 20-day high with volume.
    if d.get('Breakout'):
        sc *= 1.08

    # Penalize names that are fading or badly overextended.
    if d['3D %'] <= 0:
        sc *= 0.55
    elif d['3D %'] < 2:
        sc *= 0.85

    if d['5D %'] > 35:
        sc *= 0.85
    if d['5D %'] > 50:
        sc *= 0.8
    if d['15D %'] > 80:
        sc *= 0.8
    if d['15D %'] > max(d['5D %'], 1) * 4:
        sc *= 0.75
    if not d.get('Acceleration') and d['5D %'] > 25:
        sc *= 0.75
    if not d.get('Near High') and d['15D %'] > 80:
        sc *= 0.7

    return round(sc, 2)

# -- Market Analysis ---------------------------------------------------

def get_market():
    vix = fng = spy = None
    smh = {'bounce': False, 'drawdown': 0, 'recovery': 0}

    try:
        h = yf.Ticker('^VIX').history(period="5d")
        v = round(float(h['Close'].iloc[-1]), 2)
        level = ("LOW" if v < 15 else "NORMAL" if v < 20 else
                 "ELEVATED" if v < 25 else "HIGH" if v < 30 else "EXTREME")
        vix = {'value': v, 'level': level}
    except Exception:
        pass

    try:
        r = requests.get('https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
                         headers={'User-Agent': 'Mozilla/5.0', 'Accept': '*/*',
                                  'Origin': 'https://www.cnn.com', 'Referer': 'https://www.cnn.com/'},
                         timeout=15)
        s = int(r.json()['fear_and_greed']['score'])
        label = ("EXTREME FEAR" if s <= 20 else "FEAR" if s <= 40 else
                 "NEUTRAL" if s <= 60 else "GREED" if s <= 80 else "EXTREME GREED")
        fng = {'score': s, 'status': label}
    except Exception:
        pass

    try:
        h = yf.Ticker('SPY').history(period="1y")
        if len(h) >= 200:
            p = float(h['Close'].iloc[-1])
            ma = float(h['Close'].rolling(200).mean().iloc[-1])
            spy = {'price': round(p, 2), 'ma200': round(ma, 2), 'above': p > ma}
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
            smh = {'bounce': dd > 15 and rec > 2, 'drawdown': dd, 'recovery': rec}
    except Exception:
        pass

    signal = "SELL"
    if spy and vix:
        if spy['above'] and vix['value'] < 27:
            signal = "BUY"
        elif smh['bounce']:
            signal = "BUY THE DIP"

    score = 50
    if vix:
        score += (15 if vix['value'] < 18 else 5 if vix['value'] < 22 else
                  -15 if vix['value'] > 28 else -5 if vix['value'] > 25 else 0)
    if spy and spy['above']:
        score += 15
    score = max(0, min(100, score))
    label = "BULLISH" if score >= 70 else "BEARISH" if score < 50 else "NEUTRAL"

    return {'vix': vix, 'fng': fng, 'spy': spy, 'signal': signal,
            'market_score': {'score': score, 'label': label}, 'smh_bounce': smh}

# -- Unified Screener --------------------------------------------------

def run_screener(name, finviz_urls, bench_ticker, min_dv, dl_limit, top_n,
                 min_bars=15, ensure_ticker=None):
    print(f"\n[{name}]")
    bench = bench_perf(bench_ticker)
    print(
        f"   {bench_ticker} - Today: {_fmt_pct(bench['perf_today'])} | "
        f"5D: {bench['perf_5d']:+.2f}% | 15D: {bench['perf_15d']:+.2f}%"
    )

    # Merge tickers from multiple Finviz sources
    if isinstance(finviz_urls, str):
        finviz_urls = [finviz_urls]
    all_tickers = []
    seen = set()
    for url in finviz_urls:
        for t in fetch_finviz(url):
            if t not in seen:
                seen.add(t)
                all_tickers.append(t)
    print(f"   Found {len(all_tickers)} unique tickers from {len(finviz_urls)} source(s)")

    dl = list(set(all_tickers[:dl_limit]) | ({ensure_ticker} if ensure_ticker else set()))
    print(f"   Downloading {len(dl)}...")
    close_df, vol_df, open_df = bulk_download(dl)

    results, ref_data = [], None

    for i, t in enumerate(dl):
        print(f"   Scoring {i+1}/{len(dl)}: {t}     ", end='\r')
        d = score_ticker(t, close_df, vol_df, open_df, min_bars)
        if d is None:
            continue

        d['Today vs'] = round(d['Today %'] - bench['perf_today'], 2) if d.get('Today %') is not None and bench['perf_today'] is not None else None
        d['5D vs'] = round(d['5D %'] - bench['perf_5d'], 2)
        d['15D vs'] = round(d['15D %'] - bench['perf_15d'], 2)

        # Always compute ensure_ticker as reference
        if t == ensure_ticker:
            d['Composite Score'] = calc_composite(d)
            enrich(d, t)
            ref_data = dict(d)

        if d['Dollar Volume'] < min_dv:
            continue
        # Must be above at least MA20 (allow early breakouts without MA50)
        if not d['Above MA20'] and t != ensure_ticker:
            continue
        # ABSOLUTE PERFORMANCE GATE: must actually be going up
        if d['3D %'] <= 0 and d['5D %'] <= 0 and t != ensure_ticker:
            continue
        # Stocks must clearly lead SOXL today and keep leading on 5D/15D.
        if 'STOCK' in name:
            beats_bench = (
                _is_today_leader(d, bench['perf_today'])
                and d['5D %'] > bench['perf_5d']
                and d['15D %'] > bench['perf_15d']
            )
        else:
            beats_bench = d['5D %'] > bench['perf_5d'] or d['15D %'] > bench['perf_15d']
        if beats_bench:
            d['Composite Score'] = calc_composite(d)
            if t != ensure_ticker:
                enrich(d, t)
            results.append(d)

    print(f"\n   {len(results)} passed filters")

    if ensure_ticker and ref_data and not any(r['Ticker'] == ensure_ticker for r in results):
        ref_data['_reference'] = True
        results.append(ref_data)

    results.sort(key=lambda x: x.get('Composite Score', 0), reverse=True)
    return results[:top_n], bench

# -- History ------------------------------------------------------------

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

# -- HTML Generation ----------------------------------------------------

CSS = """* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#0d1117; color:#c9d1d9; min-height:100vh; padding:2rem; }
.c { max-width:1200px; margin:0 auto; }
h1 { font-size:1.5rem; margin-bottom:.5rem; color:#58a6ff; }
h2 { font-size:1.1rem; margin:1.5rem 0 .5rem; color:#8b949e; border-bottom:1px solid #30363d; padding-bottom:.5rem; }
h2.tp { color:#f0c040; border-bottom:2px solid #f0c040; font-size:1.3rem; }
.date { color:#8b949e; font-size:.875rem; margin-bottom:2rem; }
.sig { font-size:3rem; font-weight:bold; margin:1rem 0; padding:1rem 2rem; border-radius:.5rem; display:inline-block; }
.sig.buy { background:#238636; color:#fff; }
.sig.sell { background:#da3633; color:#fff; }
.metrics { display:grid; grid-template-columns:repeat(4,1fr); gap:1rem; margin-bottom:2rem; }
.m { background:#161b22; padding:1rem; border-radius:.5rem; }
.ml { color:#8b949e; font-size:.75rem; margin-bottom:.25rem; }
.mv { font-size:1.25rem; font-weight:600; }
.ms { font-size:.75rem; margin-top:.25rem; }
.sf { color:#f85149; } .sg { color:#3fb950; } .sn { color:#d29922; }
table { width:100%; border-collapse:collapse; margin-bottom:1rem; }
th,td { text-align:left; padding:.75rem; border-bottom:1px solid #30363d; }
th { color:#8b949e; font-weight:500; font-size:.75rem; text-transform:uppercase; }
td { font-size:.9rem; } tr:hover { background:#161b22; }
.bi { color:#8b949e; font-size:.875rem; margin-bottom:1rem; }
.hi { color:#58a6ff; font-size:.875rem; margin-bottom:1rem; }
tr.champ { background:linear-gradient(90deg,#1a2a1a 0%,#0d1117 100%); border-left:3px solid #f0c040; }
tr.champ td { font-weight:600; }
.badge { display:inline-block; font-size:.65rem; padding:2px 6px; border-radius:3px; margin-left:4px; font-weight:700; vertical-align:middle; }
.badge.g4 { background:#238636; color:#fff; }
.badge.g3 { background:#1a6b2a; color:#ccc; }
.badge.g2 { background:#30363d; color:#8b949e; }
.tp-card { background:linear-gradient(135deg,#1a2a1a,#161b22); border:1px solid #f0c040; border-radius:.5rem; padding:1rem 1.5rem; margin-bottom:.75rem; display:flex; justify-content:space-between; align-items:center; }
.tp-card .tk { font-size:1.3rem; font-weight:700; color:#f0c040; }
.tp-card .nm { color:#8b949e; font-size:.85rem; }
.tp-card .st { display:flex; gap:1.5rem; align-items:center; }
.tp-card .st div { text-align:center; }
.tp-card .st .lbl { font-size:.65rem; color:#8b949e; text-transform:uppercase; }
.tp-card .st .val { font-size:1rem; font-weight:600; }
.dim { opacity:.5; }
.none-msg { color:#8b949e; font-style:italic; padding:1rem 0; }"""


def _signal(r, freq, rank, total, is_ref=False):
    """Determine BUY/SELL/HOLD action for a screened ticker."""
    if is_ref:
        return 'SELL'
    score = r.get('Composite Score', 0)
    vs = r.get('Vol Surge', 1.0)
    acc = r.get('Acceleration', False)
    brk = r.get('Breakout', False)
    p3 = r.get('3D %', 0)
    if score <= 0:
        return 'SELL'
    # Strong conviction: high score + momentum confirmation
    top_half = rank <= max(total // 2, 1)
    has_momentum = vs > 1.3 or acc
    # STRONG BUY conditions
    if score > 100 and has_momentum and freq >= 2:
        return 'STRONG BUY'
    if score > 100 and brk and freq >= 2:
        return 'STRONG BUY'
    # BUY conditions
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
    """Return how many of the 4 criteria are strong: Score, Vol/Acc, Freq, Action."""
    hits = 0
    score = r.get('Composite Score', 0)
    vs = r.get('Vol Surge', 1.0)
    acc = r.get('Acceleration', False)
    brk = r.get('Breakout', False)
    sig = _signal(r, freq, rank, total, is_ref)
    # 1) Score: strong composite
    if score > 30:
        hits += 1
    # 2) Vol/Acc: volume surge, acceleration, or breakout
    if vs > 1.3 or acc or brk:
        hits += 1
    # 3) Freq: appeared 2+ times in history (more realistic)
    if freq >= 2:
        hits += 1
    # 4) Action: BUY or STRONG BUY
    if 'BUY' in sig:
        hits += 1
    return hits


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


def _row(i, r, freq, total, is_ref=False, is_etf=False):
    vs = r.get('Vol Surge', 1.0)
    vs_s = f"<span style='color:#3fb950'>&#x2191;{vs:.1f}x</span>" if vs > 1.3 else f"{vs:.1f}x"
    acc = "&#x1F525;" if r.get('Acceleration') else ""
    brk = "&#x1F4C8;" if r.get('Breakout') else ""
    lev = ""
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
    return (f"<tr{cls}><td>{i}</td><td>{lev}{r['Ticker']}{ref}{badge}</td>"
            f"<td>{r.get('Name', r['Ticker'])[:25]}</td>"
            f"<td>{_fmt_pct(r.get('Today %'))}</td><td>{_fmt_dollar_volume(r.get('Dollar Volume'))}</td><td>{r.get('Composite Score', 0):.0f}</td>"
            f"<td>{vs_s} {acc}{brk}</td><td>{freq}/{MAX_HISTORY}</td>"
            f"<td>{_sig_html(sig)}</td></tr>")


def _table(rows_html):
    hdr = ''.join(f'<th>{h}</th>' for h in ['#', 'Ticker', 'Name', 'Today', 'Dollar Vol', 'Score', 'Vol/Acc', 'Freq', 'Action'])
    return f"<table><thead><tr>{hdr}</tr></thead><tbody>{rows_html}</tbody></table>"


def _top_pick_card(r, freq, rank, total):
    """Generate a highlighted card for a top pick (4/4 criteria)."""
    vs = r.get('Vol Surge', 1.0)
    vs_s = f"&#x2191;{vs:.1f}x" if vs > 1.3 else f"{vs:.1f}x"
    acc_s = " &#x1F525;" if r.get('Acceleration') else ""
    brk_s = " &#x1F4C8;" if r.get('Breakout') else ""
    sig = _signal(r, freq, rank, total)
    sc = r.get('Composite Score', 0)
    sig_color = '#00ff7f' if 'STRONG' in sig else '#3fb950'
    return (f"<div class='tp-card'>"
            f"<div><span class='tk'>&#x2B50; {r['Ticker']}</span>"
            f"<span class='nm'> &mdash; {r.get('Name', r['Ticker'])[:30]}</span></div>"
            f"<div class='st'>"
            f"<div><div class='lbl'>Today</div><div class='val' style='color:#3fb950'>{_fmt_pct(r.get('Today %'))}</div></div>"
            f"<div><div class='lbl'>Dollar Vol</div><div class='val'>{_fmt_dollar_volume(r.get('Dollar Volume'))}</div></div>"
            f"<div><div class='lbl'>Score</div><div class='val' style='color:#3fb950'>{sc:.0f}</div></div>"
            f"<div><div class='lbl'>Vol/Acc</div><div class='val' style='color:#3fb950'>{vs_s}{acc_s}{brk_s}</div></div>"
            f"<div><div class='lbl'>Freq</div><div class='val' style='color:#3fb950'>{freq}/{MAX_HISTORY}</div></div>"
            f"<div><div class='lbl'>Action</div><div class='val' style='color:{sig_color}'>{sig}</div></div>"
            f"</div></div>")


def generate_html(mkt, etfs, eb, stocks, sb, sf, sh, ef, eh):
    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    v, fg, sp = mkt['vix'], mkt['fng'], mkt['spy']
    ms, sig, smh = mkt['market_score'], mkt['signal'], mkt['smh_bounce']

    sig_cls = 'buy' if 'BUY' in sig else 'sell'
    ms_cls = 'sg' if ms['score'] >= 70 else 'sf' if ms['score'] < 50 else 'sn'
    fg_cls = ('sf' if fg and fg['score'] <= 40 else 'sg' if fg and fg['score'] >= 60 else 'sn')
    smh_h = (f"<p style='color:#f0883e;font-weight:bold'>&#x26A0; SMH BOUNCE "
             f"(DD:{smh['drawdown']:.1f}% Rec:{smh['recovery']:.1f}%)</p>") if smh['bounce'] else ""

    e_rows = "".join(_row(i+1, r, ef.get(r['Ticker'], 0), len(etfs), r.get('_reference'), True) for i, r in enumerate(etfs))
    s_rows = "".join(_row(i+1, r, sf.get(r['Ticker'], 0), len(stocks)) for i, r in enumerate(stocks))

    # History score ranking
    stock_map = {}
    for day in sh:
        for s in day.get('stocks', []):
            stock_map[s['Ticker']] = s
    h_sorted = sorted(stock_map.values(), key=lambda x: x.get('Composite Score', 0), reverse=True) if stock_map else []
    h_rows = "".join(
        _row(i+1, s, sf.get(s['Ticker'], 0), len(h_sorted))
        for i, s in enumerate(h_sorted)
    ) if h_sorted else ""

    # --- TOP PICKS: stocks and ETFs with all 4 criteria strong ---
    all_candidates = []
    for i, r in enumerate(stocks):
        f = sf.get(r['Ticker'], 0)
        hits = _grade(r, f, i+1, len(stocks))
        if hits >= 4 and _is_today_leader(r, sb['perf_today']):
            all_candidates.append((r, f, i+1))
    for i, r in enumerate(etfs):
        if r.get('_reference'):
            continue
        f = ef.get(r['Ticker'], 0)
        hits = _grade(r, f, i+1, len(etfs))
        if hits >= 4 and _is_today_leader(r, sb['perf_today']):
            all_candidates.append((r, f, i+1))
    # Also scan history-ranked stocks
    for i, s in enumerate(h_sorted):
        f = sf.get(s['Ticker'], 0)
        hits = _grade(s, f, i+1, len(h_sorted))
        if hits >= 4 and _is_today_leader(s, sb['perf_today']) and not any(c[0]['Ticker'] == s['Ticker'] for c in all_candidates):
            all_candidates.append((s, f, i+1))
    all_candidates.sort(key=lambda x: x[0].get('Composite Score', 0), reverse=True)
    tp_html = "".join(_top_pick_card(r, f, rank, 999) for r, f, rank in all_candidates)
    if not tp_html:
        # Show 3/4 if no 4/4 exist
        for i, r in enumerate(stocks):
            f = sf.get(r['Ticker'], 0)
            hits = _grade(r, f, i+1, len(stocks))
            if hits >= 3 and _is_today_leader(r, sb['perf_today']):
                all_candidates.append((r, f, i+1))
        for i, s in enumerate(h_sorted):
            f = sf.get(s['Ticker'], 0)
            hits = _grade(s, f, i+1, len(h_sorted))
            if hits >= 3 and _is_today_leader(s, sb['perf_today']) and not any(c[0]['Ticker'] == s['Ticker'] for c in all_candidates):
                all_candidates.append((s, f, i+1))
        all_candidates.sort(key=lambda x: x[0].get('Composite Score', 0), reverse=True)
        tp_html = "".join(_top_pick_card(r, f, rank, 999) for r, f, rank in all_candidates[:5])

    s_days = ", ".join(d['date'] for d in sh) or "None"
    e_days = ", ".join(d['date'] for d in eh) or "None"

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Market Analysis</title><style>{CSS}</style></head>
<body><div class="c">
<h1>Market Analysis</h1><p class="date">{now}</p>
<div class="sig {sig_cls}">{sig}</div>
{smh_h}
<div class="metrics">
<div class="m"><div class="ml">Market Score</div><div class="mv">{ms['score']}/100</div><div class="ms {ms_cls}">{ms['label']}</div></div>
<div class="m"><div class="ml">VIX</div><div class="mv">{v['value'] if v else 'N/A'}</div><div class="ms">{v['level'] if v else 'N/A'}</div></div>
<div class="m"><div class="ml">Fear &amp; Greed</div><div class="mv">{fg['score'] if fg else 'N/A'}</div><div class="ms {fg_cls}">{fg['status'] if fg else 'N/A'}</div></div>
<div class="m"><div class="ml">SPY vs 200-MA</div><div class="mv">${sp['price'] if sp else 'N/A'}</div><div class="ms">{'ABOVE' if sp and sp['above'] else 'BELOW'} ${sp['ma200'] if sp else 'N/A'}</div></div>
</div>

<h2 class="tp">&#x1F3C6; TOP PICKS &mdash; All 4 Criteria Strong</h2>
<p class="hi">Today &gt; SOXL by {TODAY_OUTPERFORMANCE_MARGIN:.1f}% &bull; 5D/15D beat SOXL &bull; Vol Surge or Accel &bull; Action = BUY</p>
{tp_html if tp_html else "<p class='none-msg'>No 4/4 picks right now &mdash; showing best 3/4 above</p>"}

<h2>ETF Screener (Beat QQQ)</h2>
<p class="bi">QQQ: Today {_fmt_pct(eb['perf_today'])} | 5D {eb['perf_5d']:+.2f}% | 15D {eb['perf_15d']:+.2f}%</p>
<p class="hi">History: {e_days}</p>
{_table(e_rows)}

<h2>Stock Screener (Beat SOXL)</h2>
<p class="bi">SOXL: Today {_fmt_pct(sb['perf_today'])} | 5D {sb['perf_5d']:+.2f}% | 15D {sb['perf_15d']:+.2f}%</p>
<p class="hi">Filter: Today &gt; SOXL by {TODAY_OUTPERFORMANCE_MARGIN:.1f}% and 5D/15D must also beat SOXL</p>
<p class="hi">History: {s_days}</p>
{_table(s_rows)}

{('<h2>Stock Score Ranking</h2><p class="hi">All stocks ranked by latest composite score</p>' + _table(h_rows)) if h_rows else ''}
</div></body></html>"""

# -- Main ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-save-history', action='store_true')
    args = parser.parse_args()

    print("=" * 50)
    print("  MARKET SCREENER")
    print("=" * 50)

    mkt = get_market()
    print(f"   Score: {mkt['market_score']['score']}/100 | Signal: {mkt['signal']}")

    etfs, eb = run_screener('ETF SCREENER', ETF_URL, 'QQQ', 50e6, 80, 10, ensure_ticker='SOXL')
    stocks, sb = run_screener('STOCK SCREENER', [STOCK_URL, STOCK_URL2, STOCK_URL3],
                              'SOXL', 30e6, 200, 15, min_bars=20)

    if not args.no_save_history:
        print("\n[SAVING HISTORY]")
        save_history(stocks, etfs)

    sh, eh = load_history()
    sf, ef = freq_count(sh, 'stocks'), freq_count(eh, 'etfs')
    print(f"   History: {len(sh)}d stocks, {len(eh)}d ETFs")

    print("\n[GENERATING HTML]")
    html = generate_html(mkt, etfs, eb, stocks, sb, sf, sh, ef, eh)
    out = os.path.join(ROOT, 'index.html')
    with open(out, 'w') as f:
        f.write(html)
    print(f"   -> {out}\n[DONE]")


if __name__ == "__main__":
    main()
