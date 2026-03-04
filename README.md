# Stock Screener Files

## screener.py
- `get_us_stocks(limit)` - Fetches stock tickers from Finviz
- `calc_performance(hist)` - Calculates 5D and 15D performance % 
- `get_benchmark_performance(benchmark)` - Gets SOXL benchmark performance
- `get_stock_data(ticker)` - Fetches stock data (price, MA, volume, etc.)
- `screen_stocks()` - Main screening logic with filters (min $75M volume, above MA20 & MA50)
- `display_results(df, soxl_perf, top_n)` - Displays results (default top_n=20)
- `main()` - Entry point

## etf_screener.py
- `get_etfs(limit)` - Fetches ETF tickers from Finviz
- `calc_performance(hist)` - Calculates 5D and 15D performance %
- `get_benchmark_performance(benchmark)` - Gets QQQ benchmark performance
- `get_etf_data(ticker)` - Fetches ETF data
- `screen_etfs()` - Main ETF screening logic (min $50M volume)
- `compare_custom_tickers()` - Compares user tickers vs screener results
- `display_results(df, qqq_perf, top_n)` - Displays results (default top_n=30, display top_n=10)
- `get_tickers_from_input()` - Gets tickers from CLI args or input
- `main()` - Entry point (hardcoded custom_tickers=['SOXL'])

## screener_compare.py
- Same core functions as screener.py
- `compare_custom_tickers()` - Compares user tickers vs screener results
- `get_tickers_from_input()` - Gets tickers from CLI args or interactive input
- `display_results()` - Displays results (default top_n=30)
- `main()` - Entry point requiring tickers as CLI argument

## run_market_analysis.py
- `get_vix()` - Fetches VIX data with level classification (LOW/NORMAL/ELEVATED/HIGH/EXTREME)
- `get_fear_greed()` - Fetches CNN Fear & Greed index
- `get_spy_ma()` - Gets SPY price and 200-day moving average
- `main()` - Entry point with BUY/SELL signal based on SPY > 200-MA and VIX < 27
