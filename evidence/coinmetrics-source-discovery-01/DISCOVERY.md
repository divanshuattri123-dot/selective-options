# Bounded Coin Metrics Community discovery — 7 October 2026

Five sequential HTTPS GET requests were sent with no API key or account. Each was bounded to 20 seconds and 2,000,000 response bytes, without redirects or retries. All native bytes, status, request parameters, local final response receipts, body SHA-256 and selected response headers are retained beside this file. No files from older evidence were changed.

| Request | HTTP | Result |
| --- | --- | --- |
| reference | 200 | Native reference definitions for CapMrktCurUSD, CapRealUSD, CapMVRVCur and SOPR |
| catalog | 200 | Daily community:true CapMrktCurUSD and CapMVRVCur for btc and eth; no entries for CapRealUSD or SOPR |
| series | 403 | Combined four-metric request refused: CapRealUSD for btc is unavailable with supplied credentials |
| supported-series | 200 | Two exact native daily rows for each btc and eth, market cap and MVRV decimal strings preserved |
| sopr-series | 403 | SOPR-only request refused for btc with supplied credentials |

The two changed series probes refine the requested metric set; neither repeats an unchanged access failure. The response rate-limit-plan header was `download`, despite the community hostname and absence of authentication. It is retained as observed; it does not establish paid entitlement or replace the documented community limits.

Supported implementation scope is only provider-supplied daily CapMrktCurUSD (USD) and CapMVRVCur (Dimensionless) for BTC and ETH. CapRealUSD/SOPR are unavailable in this observed community catalog; the HTTP refusal explicitly names BTC, so no separate successful or failed ETH-only timeseries probe is claimed. No realized cap is inferred by dividing market cap by MVRV. No exchange flows, whale attribution, SOL data, original publication time, provider finality or complete historical coverage are established.

Official primary references consulted:

- [API v4 documentation](https://docs.coinmetrics.io/api/v4/): community HTTPS origin, no-key access, pagination, decimal string/null/absent-field semantics and rate limits.
- [Market capitalization definitions](https://docs.coinmetrics.io/network-data/network-data-overview/market/market-capitalization): daily current market cap and native MVRV units and definitions.
- [Valuation definitions](https://docs.coinmetrics.io/network-data/network-data-overview/economics/valuation): SOPR description.

The native daily timestamp is a provider period label, not an availability timestamp. The collector retains it exactly and uses the actual local response receipt for point-in-time availability. Its additional requirement that label plus one day is at or before poll start is an explicit conservative collection policy; it does not prove the provider's final revision or first publication.
