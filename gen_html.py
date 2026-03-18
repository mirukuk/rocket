#!/usr/bin/env python3
"""Generate HTML from existing history files."""

import os, json
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(ROOT, 'history')
INDEX_HTML = os.path.join(ROOT, 'index.html')

CSS = """body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;background:#111;color:#eee;margin:0;padding:20px}a{color:#6cf}.c{max-width:1200px;margin:0 auto}.date{color:#888;font-size:14px}.sig{font-size:28px;font-weight:700;text-align:center;padding:15px;margin:20px 0;border-radius:8px}.sig.buy{background:#0a5}.sig.sell{background:#d33}.sig.neutral{background:#b85}metrics{display:flex;gap:15px;flex-wrap:wrap;margin:20px 0}.m{background:#222;padding:15px;border-radius:8px;min-width:120px}.ml{color:#888;font-size:12px}.mv{font-size:24px;font-weight:700}.ms{font-size:12px;padding:3px 8px;border-radius:4px;display:inline-block;margin-top:5px}.sg{background:#0a5;color:#fff}.sf{background:#d33;color:#fff}.sn{background:#b85;color:#fff}h2{color:#eee;margin:30px 0 10px;border-bottom:1px solid #333;padding-bottom:10px}.tp{color:#fb0}.hi{color:#888;font-size:14px}.none-msg{color:#888;font-style:italic;padding:20px}table{width:100%;border-collapse:collapse;margin:15px 0;font-size:13px}th,td{padding:8px;text-align:left;border-bottom:1px solid #333}th{color:#888;font-weight:500}.buy{color:#0a5}.sell{color:#d33}.hold{color:#b85}.t0{width:40px}.t1{width:70px}.t2{width:180px}.t3{width:70px}.t4{width:70px}.t5{width:60px}.t6{width:60px}.t7{width:50px}.t8{width:50px}.tp-card{background:#1a1a1a;padding:15px;border-radius:8px;margin:10px 0;border-left:4px solid #fb0}.tp-rank{font-size:20px;font-weight:700;color:#fb0;margin-right:10px}.tp-ticker{font-size:18px;font-weight:700;color:#fff;margin-right:10px}.tp-name{color:#888;font-size:14px}.tp-pct{font-size:24px;font-weight:700;margin:5px 0}.tp-vol{color:#666;font-size:12px}.tp-score{background:#222;padding:3px 10px;border-radius:4px;font-size:12px;margin-left:10px}.tp-action{font-weight:700;padding:3px 10px;border-radius:4px;margin-left:10px}.tp-action.buy{background:#0a5;color:#fff}.tp-action.strong-buy{background:#fb0;color:#000}.bi{font-weight:700;color:#fff}"""

def load_history():
    files = sorted([f for f in os.listdir(HISTORY_DIR) if f.endswith('.json')])
    stocks, etfs = [], []
    for f in files[-10:]:
        with open(os.path.join(HISTORY_DIR, f)) as fp:
            d = json.load(fp)
            stocks.extend(d.get('stocks', []))
            etfs.extend(d.get('etfs', []))
    return stocks, etfs

def _fmt_pct(v):
    return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"

def main():
    stocks, etfs = load_history()
    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    
    # Sort by score
    stocks.sort(key=lambda x: x.get('Composite Score', 0), reverse=True)
    etfs.sort(key=lambda x: x.get('Composite Score', 0), reverse=True)
    
    s_rows = "".join(f"<tr><td class='t0'>{i+1}</td><td class='t1'><a href='#'>{r['Ticker']}</a></td><td class='t2'>{r.get('Name','')}</td><td class='t3'>{_fmt_pct(r.get('Today %',0))}</td><td class='t4'>{r.get('Volume',''):.0f}</td><td class='t5'>{r.get('Composite Score',0)}</td><td class='t6'>{r.get('Action','')}</td></tr>" for i, r in enumerate(stocks[:50]))
    e_rows = "".join(f"<tr><td class='t0'>{i+1}</td><td class='t1'><a href='#'>{r['Ticker']}</a></td><td class='t2'>{r.get('Name','')}</td><td class='t3'>{_fmt_pct(r.get('Today %',0))}</td><td class='t4'>{r.get('Volume',''):.0f}</td><td class='t5'>{r.get('Composite Score',0)}</td><td class='t6'>{r.get('Action','')}</td></tr>" for i, r in enumerate(etfs[:30]))
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Screener History</title><style>{CSS}</style></head>
<body><div class="c">
<h1>Stock Screener</h1><p class="date">{now}</p>
<h2>Top Stocks (by Composite Score)</h2>
<table><tr><th>#</th><th>Ticker</th><th>Name</th><th>Today</th><th>Volume</th><th>Score</th><th>Action</th></tr>{s_rows}</table>
<h2>Top ETFs (by Composite Score)</h2>
<table><tr><th>#</th><th>Ticker</th><th>Name</th><th>Today</th><th>Volume</th><th>Score</th><th>Action</th></tr>{e_rows}</table>
</div></body></html>"""
    
    with open(INDEX_HTML, 'w') as f:
        f.write(html)
    print(f"Generated {INDEX_HTML}")

if __name__ == "__main__":
    main()