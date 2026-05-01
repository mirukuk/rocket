import requests
import re

url = 'https://finviz.com/screener.ashx?v=411&f=sh_price_o10&ft=4'
r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
text = r.text

# Try various patterns
patterns = [
    r'quote\.ashx\?t=([A-Z]+)',
    r'"ticker":"([A-Z]+)"',
    r'data-ticker="([A-Z]+)"',
    r'Ticker.*?([A-Z]{2,5})',
]

for p in patterns:
    m = re.findall(p, text)
    if m:
        print(f'Pattern {p[:30]}: {len(m)} found, sample={list(set(m))[:20]}')
        if len(m) > 20:
            break
else:
    # Look for any large block of data
    idx = text.find('screener-table')
    if idx > 0:
        print('Found screener-table at', idx)
        print(text[idx:idx+500])
    # Look for any ticker references
    for ticker in ['SPY', 'QQQ', 'TQQQ', 'AAPL', 'SOXL']:
        if ticker in text:
            print(f'Found {ticker} in page')
            idx = text.find(ticker)
            print(text[max(0,idx-50):idx+50])
    
    # Try to find table data section
    idx = text.find('"ticker"')
    if idx > 0:
        print('Found "ticker" at', idx)
        print(text[idx:idx+200])
    
    idx = text.find("'ticker'")
    if idx > 0:
        print("Found 'ticker' at", idx)
        print(text[idx:idx+200])
