# FRED DFII10 bounded public probe, version 0.3.39

The finite probe attempted exactly one official FRED DFII10 CSV HTTP request with a 90-calendar-day inclusive window. The configured timeout was 15 seconds. It ended with `TimeoutError` before an HTTP response entity or status was received. No request was retried. `report.json` records `SOURCE_UNAVAILABLE_OR_INVALID`, and the production quality view is `BLOCKED` with `TIPS_FEED_UNAVAILABLE`. This evidence does not establish public yield data availability.

The sole retained event is canonical failed feed health. Both point-in-time boundary checks passed; the event becomes available only at its actual receipt. The database and sealed Parquet match exactly after closed reload. The pending outbox contains one event after reopening. The bounded store quality query equals the pure quality view before and after reopening. Redis delivery was not attempted.

If the CSV source becomes available during a separately authorized future capture, the collector retains exact decoded CSV entity bytes and hash, native date strings, native yield-percent strings, and distinct dot/empty missing-value markers. Entire bounded windows are retained as new receipt revisions. Daily observation dates are logical dates; the system does not invent a publication instant, historical vintage, or continuous history. The HTTP endpoint, series and inclusive date/byte budgets are fixed; redirects are refused and TLS verification uses the default settings.

Validation performed before this public request:

- 69 targeted tests passed: 54 collector/parser/quality tests and 15 finite-verifier tests. All HTTP verifier test transport is explicitly mocked, and fixture results are labeled accordingly.
- The mocked transport tests exercised successful native values, HTTP failure, malformed CSV, transport failure, exact receipt boundaries, duplicate insertion, sealed archive, closed reload, reopened outbox, bounded store view, overwrite refusal, independent native-field corruption, and the one-request budget.
- Ruff checks and formatting passed for the four owned source/verifier/test files. Strict mypy passed for the collector and verifier.

Collector source SHA-256 before and after the actual request: `168d17bf73dd0e3e345f564ea6849ee2335b00f284af9ab88ec1a69778723919`. `verification_source.py` preserves the finite verifier used for this attempt. The sealed Parquet checksum is recorded in `report.json` and its archive manifest.

Primary source references: [FRED DFII10 series](https://fred.stlouisfed.org/series/DFII10), [FRED CSV downloading documentation](https://fredhelp.stlouisfed.org/fred/data/downloading/using-the-download-data-link/), and [Federal Reserve H.15 release](https://www.federalreserve.gov/releases/h15/).

Milestone 2 remains incomplete. The probe neither approves trading nor submits orders.
