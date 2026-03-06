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
LEVERAGED = {'SOXL','TQQQ','NVDL','UPRO','TECL','FAS','LABU','FNGU',
             'TNA','SPXL','QLD','ROM','BULZ','KORU','YINN','EDC'}
ETF_URL = "https://finviz.com/screener.ashx?v=411&f=ind_exchangetradedfund%2Csh_price_o10%2Cta_change_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&o=-volume"
STOCK_URL = "https://finviz.com/screener.ashx?v=411&f=sh_price_o10%2Cta_change_u%2Cta_perf_13w20o%2Cta_perf2_26w50o&ft=3&o=-volume"

# -- Helpers -----------------------------------------------------------

def pct(series, n):
    if len(series) < n:
        return 0
    return round((float(series.iloc[-1]) / float(series.iloc[-n]) - 1) * 100, 2)


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
        return pd.DataFrame(), pd.DataFrame()
    try:
        data = yf.download(tickers, period="3mo", auto_adjust=True, threads=True, progress=False)
        if data.empty:
            return pd.DataFrame(), pd.DataFrame()
        if len(tickers) == 1:
            return (data[['Close']].rename(columns={'Close': tickers[0]}),
                    data[['Volume']].rename(columns={'Volume': tickers[0]}))
        return (data.get('Close', pd.DataFrame()), data.get('Volume', pd.DataFrame()))
    except Exception as e:
        print(f"   Download error: {e}")
        return pd.DataFrame(), pd.DataFrame()


def score_ticker(ticker, close_df, vol_df, min_bars=15):
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
        return {
            'Ticker': ticker, 'Price': round(price, 2),
            '3D %': p3, '5D %': p5, '15D %': p15,
            'Dollar Volume': price * avg20,
            'Above MA20': price > ma20, 'Above MA50': price > ma50,
            'Vol Surge': round(avg5 / avg20, 2) if avg20 > 0 else 1.0,
            'Acceleration': (p3 > p5 * 0.6 and p3 > 0) if p5 != 0 else False,
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
        return {'ticker': ticker, 'perf_5d': pct(h['Close'], 5), 'perf_15d': pct(h['Close'], 15)}
    except Exception:
        return {'ticker': ticker, 'perf_5d': 0, 'perf_15d': 0}


def calc_composite(d, dv_scale, is_leveraged=False):
    avg = (d['5D vs'] + d['15D vs']) / 2
    sc = avg * (d['Dollar Volume'] / dv_scale)
    if d['Vol Surge'] > 1.2:
        sc *= min(d['Vol Surge'], 1.5)
    if is_leveraged and sc > 0:
        sc *= 2.0
    if d['Acceleration']:
        sc *= 1.3
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

def run_screener(name, finviz_url, bench_ticker, min_dv, dl_limit, top_n,
                 min_bars=15, ensure_ticker=None):
    print(f"\n[{name}]")
    bench = bench_perf(bench_ticker)
    print(f"   {bench_ticker} - 5D: {bench['perf_5d']:+.2f}% | 15D: {bench['perf_15d']:+.2f}%")

    tickers = fetch_finviz(finviz_url)
    print(f"   Found {len(tickers)} tickers")

    dl = list(set(tickers[:dl_limit]) | ({ensure_ticker} if ensure_ticker else set()))
    print(f"   Downloading {len(dl)}...")
    close_df, vol_df = bulk_download(dl)

    dv_scale = 1e6 if 'ETF' in name else 1e8
    results, ref_data = [], None

    for i, t in enumerate(dl):
        print(f"   Scoring {i+1}/{len(dl)}: {t}     ", end='\r')
        d = score_ticker(t, close_df, vol_df, min_bars)
        if d is None:
            continue

        d['5D vs'] = round(d['5D %'] - bench['perf_5d'], 2)
        d['15D vs'] = round(d['15D %'] - bench['perf_15d'], 2)

        # Always compute ensure_ticker as reference
        if t == ensure_ticker:
            avg_abs = (d['5D %'] + d['15D %']) / 2
            sc = avg_abs * (d['Dollar Volume'] / 1e9)
            if d['Vol Surge'] > 1.2:
                sc *= min(d['Vol Surge'], 1.5)
            sc *= 2.0
            if d['Acceleration']:
                sc *= 1.3
            d['Composite Score'] = round(sc, 2)
            enrich(d, t)
            ref_data = dict(d)

        if d['Dollar Volume'] < min_dv:
            continue
        if not (d['Above MA20'] and d['Above MA50']) and t != ensure_ticker:
            continue
        if d['5D %'] > bench['perf_5d'] and d['15D %'] > bench['perf_15d']:
            d['Composite Score'] = calc_composite(d, dv_scale, t in LEVERAGED)
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
.hi { color:#58a6ff; font-size:.875rem; margin-bottom:1rem; }"""


def _action(r, freq, rank, total, is_ref=False):
    """Single action signal combining score, volume, acceleration, frequency."""
    if is_ref:
        return '-', '#8b949e'
    score = r.get('Composite Score', 0)
    if score <= 0:
        return 'AVOID', '#f85149'
    vs = r.get('Vol Surge', 1.0)
    acc = r.get('Acceleration', False)
    top = rank <= max(total // 3, 1)
    # Count conviction signals
    signals = sum([vs > 1.2, acc, freq >= 3, top, score > 200])
    if signals >= 3:
        return '&#x1F525; STRONG BUY', '#3fb950'
    if signals >= 1 and score > 0:
        return 'BUY', '#3fb950'
    return 'HOLD', '#d29922'


def _row(i, r, freq, total, is_ref=False, is_etf=False):
    lev = "&#x26A1;" if is_etf and r['Ticker'] in LEVERAGED else ""
    ref = " <span style='color:#8b949e;font-size:.75rem'>(ref)</span>" if is_ref else ""
    sty = " style='background:#1c2333;opacity:.85'" if is_ref else ""
    label, color = _action(r, freq, i, total, is_ref)
    return (f"<tr{sty}><td>{i}</td><td>{lev}{r['Ticker']}{ref}</td>"
            f"<td>{r.get('Name', r['Ticker'])[:25]}</td>"
            f"<td><span style='color:{color};font-weight:bold'>{label}</span></td></tr>")


def _table(rows_html):
    hdr = ''.join(f'<th>{h}</th>' for h in ['#', 'Ticker', 'Name', 'Action'])
    return f"<table><thead><tr>{hdr}</tr></thead><tbody>{rows_html}</tbody></table>"


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

<h2>ETF Screener (Beat QQQ)</h2>
<p class="bi">QQQ: 5D {eb['perf_5d']:+.2f}% | 15D {eb['perf_15d']:+.2f}%</p>
<p class="hi">History: {e_days}</p>
{_table(e_rows)}

<h2>Stock Screener (Beat SOXL)</h2>
<p class="bi">SOXL: 5D {sb['perf_5d']:+.2f}% | 15D {sb['perf_15d']:+.2f}%</p>
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

    etfs, eb = run_screener('ETF SCREENER', ETF_URL, 'QQQ', 50e6, 50, 10, ensure_ticker='SOXL')
    stocks, sb = run_screener('STOCK SCREENER', STOCK_URL, 'SOXL', 75e6, 100, 15, min_bars=50)

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
