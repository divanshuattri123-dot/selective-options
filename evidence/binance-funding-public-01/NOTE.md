# Public BTC settled-funding archive

The finite run fetched an explicit 48-hour window using public discovery and four history pages, including a final empty page: five requests accepted, six observations stored. All reported decimal strings, funding timestamps and actual response receipts are retained. No credentials, private requests, orders or paper run were used. The probe stopped after completion.

`verify_saved.py` independently checks every saved response against the database without importing the collector or parser. It reconstructs all six canonical identities, checks inclusive bounds and ascending pagination, verifies 12 SQL receipt/source visibility boundaries, checks the pending outbox and exact Parquet equality, and confirms the source database hash is unchanged. Replaying this verifier makes zero network requests.

The official [Binance funding-history reference](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#get-funding-rate-history) specifies inclusive time bounds and ascending results. The next request begins one millisecond after the last returned funding timestamp. Reported timestamps include millisecond offsets; they are preserved exactly and never rounded to an inferred eight-hour grid. The API's optional rate type is preserved; no missing type is inferred as Regular.

The accepted scope is active USDT perpetuals. Malformed, duplicate/nonascending, out-of-window/future, wrong-symbol or unsupported discovery data fails before staged history publication. Distinct downloads preserve their receipts in identity; exact poll replay deduplicates. Multiple observations of a settlement must be selected as observed revisions before any later aggregation. No funding aggregation or historical-interval inference is implemented.

An empty page establishes API exhaustion within this request, not a verified gap-free settlement schedule. BTC is the only asset publicly verified by this run. Historical intervals, finality, first publication and other venues/assets remain unverified. This bounded archive is M2 ingestion evidence, not backtest-ready history, signals or trading approval.
