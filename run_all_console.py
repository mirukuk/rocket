#!/usr/bin/env python3
"""Console-only market screener runner (no HTML output).
Shows all the same information as the HTML dashboard."""

import argparse
from collections import Counter
from datetime import datetime

from run_all import (
    ETF_URL,
    MAX_HISTORY,
    STOCK_URL,
    STOCK_URL2,
    STOCK_URL3,
    TODAY_OUTPERFORMANCE_MARGIN,
    _grade,
    _is_today_leader,
    _signal,
    get_market,
    run_screener,
    save_history,
    load_history,
    freq_count,
)

SEP = "-" * 70


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


def print_market(mkt):
    """Print market metrics: VIX, Fear & Greed, SPY vs MA200, SMH bounce."""
    v = mkt.get("vix")
    fg = mkt.get("fng")
    sp = mkt.get("spy")
    ms = mkt["market_score"]
    smh = mkt.get("smh_bounce", {})

    print(f"\n{'=' * 70}")
    print(f"  SIGNAL: {mkt['signal']}    |    Market Score: {ms['score']}/100 ({ms['label']})")
    print(f"{'=' * 70}")

    print(f"\n  VIX:           {v['value'] if v else 'N/A':<8} {v['level'] if v else ''}")
    print(f"  Fear & Greed:  {fg['score'] if fg else 'N/A':<8} {fg['status'] if fg else ''}")
    if sp:
        above = "ABOVE" if sp["above"] else "BELOW"
        print(f"  SPY:           ${sp['price']:<8} {above} MA200 (${sp['ma200']})")
    else:
        print(f"  SPY:           N/A")

    if smh.get("bounce"):
        print(f"  !! SMH BOUNCE  DD:{smh['drawdown']:.1f}% Recovery:{smh['recovery']:.1f}%")


def print_table(title, bench_ticker, bench, items, freqs, history_dates):
    """Print a full table matching the HTML columns: #, Ticker, Name, Score, Vol/Acc, Freq, Action."""
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"{SEP}")
    print(f"  {bench_ticker}: Today {fmt_pct(bench.get('perf_today'))} | 5D {bench['perf_5d']:+.2f}% | 15D {bench['perf_15d']:+.2f}%")
    print(f"  History: {', '.join(history_dates) if history_dates else 'None'}")
    print()

    if not items:
        print("  No results")
        return

    # Header
    print(f"  {'#':>3}  {'Ticker':<7} {'Name':<25} {'Today':>8} {'$Vol':>10} {'Score':>6} {'Vol':>5} {'Flags':<6} {'Freq':>6} {'Action':<12}")
    print(f"  {'---':>3}  {'------':<7} {'----':<25} {'-----':>8} {'----':>10} {'-----':>6} {'---':>5} {'-----':<6} {'----':>6} {'------':<12}")

    total = len(items)
    for i, r in enumerate(items, start=1):
        ticker = r.get("Ticker", "?")
        name = r.get("Name", ticker)[:25]
        today = fmt_pct(r.get("Today %"))
        dollar_vol = fmt_dollar_volume(r.get("Dollar Volume"))
        score = r.get("Composite Score", 0)
        vs = r.get("Vol Surge", 1.0)
        acc = "A" if r.get("Acceleration") else ""
        brk = "B" if r.get("Breakout") else ""
        flags = f"{acc}{brk}" or "-"
        is_ref = r.get("_reference", False)
        freq = freqs.get(ticker, 0)
        sig = _signal(r, freq, i, total, is_ref)
        hits = _grade(r, freq, i, total, is_ref)
        grade = f"[{hits}/4]" if hits >= 3 else ""
        ref_tag = " (ref)" if is_ref else ""

        vs_str = f"{vs:.1f}x"
        print(
            f"  {i:>3}  {ticker:<7} {name:<25} {today:>8} {dollar_vol:>10} {score:>6.0f} {vs_str:>5} {flags:<6} "
            f"{freq:>2}/{MAX_HISTORY:<3} {sig:<10} {grade}{ref_tag}"
        )


def print_top_picks(stocks, etfs, sf, ef, sh, stock_bench):
    """Print TOP PICKS: tickers with all 4 criteria strong."""
    # Build history-ranked stocks
    stock_map = {}
    for day in sh:
        for s in day.get("stocks", []):
            stock_map[s["Ticker"]] = s
    h_sorted = sorted(
        stock_map.values(),
        key=lambda x: x.get("Composite Score", 0),
        reverse=True,
    ) if stock_map else []

    all_candidates = []

    # Stocks with 4/4
    for i, r in enumerate(stocks):
        f = sf.get(r["Ticker"], 0)
        hits = _grade(r, f, i + 1, len(stocks))
        if hits >= 4 and _is_today_leader(r, stock_bench.get("perf_today")):
            all_candidates.append((r, f, i + 1, hits))

    # ETFs with 4/4
    for i, r in enumerate(etfs):
        if r.get("_reference"):
            continue
        f = ef.get(r["Ticker"], 0)
        hits = _grade(r, f, i + 1, len(etfs))
        if hits >= 4 and _is_today_leader(r, stock_bench.get("perf_today")):
            all_candidates.append((r, f, i + 1, hits))

    # History stocks with 4/4
    for i, s in enumerate(h_sorted):
        f = sf.get(s["Ticker"], 0)
        hits = _grade(s, f, i + 1, len(h_sorted))
        if hits >= 4 and _is_today_leader(s, stock_bench.get("perf_today")) and not any(c[0]["Ticker"] == s["Ticker"] for c in all_candidates):
            all_candidates.append((s, f, i + 1, hits))

    # Fall back to 3/4 if no 4/4
    threshold = 4
    if not all_candidates:
        threshold = 3
        for i, r in enumerate(stocks):
            f = sf.get(r["Ticker"], 0)
            hits = _grade(r, f, i + 1, len(stocks))
            if hits >= 3 and _is_today_leader(r, stock_bench.get("perf_today")):
                all_candidates.append((r, f, i + 1, hits))
        for i, s in enumerate(h_sorted):
            f = sf.get(s["Ticker"], 0)
            hits = _grade(s, f, i + 1, len(h_sorted))
            if hits >= 3 and _is_today_leader(s, stock_bench.get("perf_today")) and not any(c[0]["Ticker"] == s["Ticker"] for c in all_candidates):
                all_candidates.append((s, f, i + 1, hits))

    all_candidates.sort(key=lambda x: x[0].get("Composite Score", 0), reverse=True)

    print(f"\n{'=' * 70}")
    print(f"  TOP PICKS -- All {threshold} Criteria Strong")
    print(f"  Today > SOXL by {TODAY_OUTPERFORMANCE_MARGIN:.1f}% | 5D/15D beat SOXL | Vol Surge or Accel | Action = BUY")
    print(f"{'=' * 70}")

    if not all_candidates:
        print("  No top picks right now")
        return

    for r, freq, rank, hits in all_candidates[:5]:
        ticker = r.get("Ticker", "?")
        name = r.get("Name", ticker)[:30]
        today = fmt_pct(r.get("Today %"))
        dollar_vol = fmt_dollar_volume(r.get("Dollar Volume"))
        score = r.get("Composite Score", 0)
        vs = r.get("Vol Surge", 1.0)
        acc = " Accel" if r.get("Acceleration") else ""
        brk = " Breakout" if r.get("Breakout") else ""
        sig = _signal(r, freq, rank, 999)
        print(
            f"  >>> {ticker:<7} {name:<30} Today:{today:>8}  $Vol:{dollar_vol:>10}  Score:{score:>6.0f}  "
            f"Vol:{vs:.1f}x{acc}{brk}  Freq:{freq}/{MAX_HISTORY}  Action:{sig}  [{hits}/4]"
        )


def print_history_ranking(sh, sf):
    """Print Stock Score Ranking from history data."""
    stock_map = {}
    for day in sh:
        for s in day.get("stocks", []):
            stock_map[s["Ticker"]] = s
    if not stock_map:
        return

    h_sorted = sorted(
        stock_map.values(),
        key=lambda x: x.get("Composite Score", 0),
        reverse=True,
    )

    print(f"\n{SEP}")
    print(f"  STOCK SCORE RANKING (from history)")
    print(f"{SEP}")
    print(f"  {'#':>3}  {'Ticker':<7} {'Name':<25} {'Today':>8} {'$Vol':>10} {'Score':>6} {'Vol':>5} {'Flags':<6} {'Freq':>6} {'Action':<12}")
    print(f"  {'---':>3}  {'------':<7} {'----':<25} {'-----':>8} {'----':>10} {'-----':>6} {'---':>5} {'-----':<6} {'----':>6} {'------':<12}")

    total = len(h_sorted)
    for i, r in enumerate(h_sorted, start=1):
        ticker = r.get("Ticker", "?")
        name = r.get("Name", ticker)[:25]
        today = fmt_pct(r.get("Today %"))
        dollar_vol = fmt_dollar_volume(r.get("Dollar Volume"))
        score = r.get("Composite Score", 0)
        vs = r.get("Vol Surge", 1.0)
        acc = "A" if r.get("Acceleration") else ""
        brk = "B" if r.get("Breakout") else ""
        flags = f"{acc}{brk}" or "-"
        freq = sf.get(ticker, 0)
        sig = _signal(r, freq, i, total)
        hits = _grade(r, freq, i, total)
        grade = f"[{hits}/4]" if hits >= 3 else ""

        vs_str = f"{vs:.1f}x"
        print(
            f"  {i:>3}  {ticker:<7} {name:<25} {today:>8} {dollar_vol:>10} {score:>6.0f} {vs_str:>5} {flags:<6} "
            f"{freq:>2}/{MAX_HISTORY:<3} {sig:<10} {grade}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-save-history", action="store_true")
    args = parser.parse_args()

    now = f"{datetime.now():%Y-%m-%d %H:%M}"
    print(f"\n  Market Analysis  |  {now}")

    # --- Market metrics ---
    mkt = get_market()
    print_market(mkt)

    # --- Screeners ---
    etfs, eb = run_screener("ETF SCREENER", ETF_URL, "QQQ", 50e6, 80, 10, ensure_ticker="SOXL")
    stocks, sb = run_screener(
        "STOCK SCREENER",
        [STOCK_URL, STOCK_URL2, STOCK_URL3],
        "SOXL",
        30e6,
        200,
        15,
        min_bars=20,
    )

    # --- History ---
    if not args.no_save_history:
        print("\n[SAVING HISTORY]")
        save_history(stocks, etfs)

    sh, eh = load_history()
    sf, ef = freq_count(sh, "stocks"), freq_count(eh, "etfs")
    s_days = [d["date"] for d in sh]
    e_days = [d["date"] for d in eh]
    print(f"   History: {len(sh)}d stocks, {len(eh)}d ETFs")

    # --- Top Picks ---
    print_top_picks(stocks, etfs, sf, ef, sh, sb)

    # --- ETF table ---
    print_table("ETF Screener (Beat QQQ)", "QQQ", eb, etfs, ef, e_days)

    # --- Stock table ---
    print_table("Stock Screener (Beat SOXL)", "SOXL", sb, stocks, sf, s_days)

    # --- History ranking ---
    print_history_ranking(sh, sf)

    print(f"\n{'=' * 70}")
    print("  [DONE] Console output only (no HTML generated)")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
