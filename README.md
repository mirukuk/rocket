# Market Screener

One file (`run.py`) → generates `index.html` dashboard.

## Strategy

### 1. Market Regime (BUY / SELL gate)

| Check | BUY condition |
|-------|--------------|
| SPY vs 200-MA | Price **above** 200-day moving average |
| VIX | Below **27** |
| SMH Bounce | Drawdown >15% from 50D high + 5D recovery >2% → **BUY THE DIP** |

Market Score (0-100) adds granularity: VIX < 18 → +15, SPY above 200-MA → +15.

**Rule: Only buy stocks when signal is BUY or BUY THE DIP.**

### 2. Stock Selection (How to find stocks)

Source: Finviz screener — US stocks with:
- Price > $10
- 13-week performance > +20%
- 26-week performance > +50%
- Sorted by volume (highest first)

Filters applied by the script:
- **Dollar volume > $75M/day** (liquidity)
- **Price above MA20 AND MA50** (uptrend confirmed)
- **Today % must beat SOXL by at least 1.0%** (stronger intraday leadership)
- **5D and 15D return both beat SOXL** (must outperform the benchmark)

### 3. Scoring (How to rank)

```
Composite Score = Bounded 3D/5D/15D momentum
                  + capped 5D/15D relative-strength bonus
                  × Acceleration bonus
                  × Breakout bonus
                  with penalties for fading or overextended moves
```

### 4. How to Buy & Sell

**Entry:**
- Pick from the top of the Stock Screener table (highest composite score, with Today % clearly ahead of SOXL)
- Prefer stocks with **high frequency** (appear across many days → persistent momentum)
- Prefer stocks with **Vol Surge ↑** (institutional interest) and **🔥 Acceleration** (momentum accelerating)

**Position sizing:**
- Concentrate on top 2-3 picks max
- Use the ETF screener to find the broad sector bet (e.g. ERX for energy, SOXL for semis)

**Exit (sell when):**
- Stock drops below MA20 → momentum broken
- Stock no longer beats SOXL benchmark on 5D/15D → relative momentum lost
- Market signal flips to SELL (VIX spikes or SPY drops below 200-MA)
- Stock disappears from screener for 2+ consecutive days → momentum faded

### 5. ETF Layer

Same logic but benchmarked against QQQ. SOXL is always shown as reference.
Use ETFs to identify which **sector** is leading, then pick individual stocks from that sector.

## Usage

```bash
python run.py                    # run the screener, generates index.html
```

## Files

- `run.py` — all logic in one file (market analysis, screener, HTML generation)
- `index.html` — output dashboard (auto-generated)
- `history/*.json` — daily snapshots
