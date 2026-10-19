# Next M2 increment after 0.3.53

The explicit SOL network checkpoint resume and Keltner research increment are complete within their documented saved-data scope. Do not repeat them. Full M2 and later model acceptance remain incomplete; see VALIDATION_REPORT.md for the completed check scope and current results.

## Recommendation: preserve crypto-news first receipts across explicit capture resume

CryptoNewsCollector currently starts without retained headline metadata. A new capture directory can therefore give repeated saved headlines new first-observation receipts. Reopening the same store deduplicates existing rows, but that does not establish an explicit production handoff to a new output. The existing finite crypto-news public sample has 25 native metadata rows plus bound health, providing offline acceptance material without new provider calls.

Implement an opt-in `capture --resume-crypto-news-from DATABASE` that requires crypto-news capture enabled. Add a bounded read-only `crypto_news_checkpoint.py` loader for the exact `coindesk_rss/public_reference/GLOBAL/CRYPTO_NEWS` scope, both source and receipt clocks, row/raw stored-value byte budgets, canonical metadata/health validation and latest health freshness. Audit all eligible native bindings. Preserve every retained headline identity and first receipt, including identities absent from the latest response that can reappear later. Successful repeated polls can legitimately reference the same headline; do not impose SOL's exactly-one-health rule on this source.

Copy exact retained metadata and the required health evidence into a fresh destination with deliveries pending. Determine the minimal health set that keeps every copied headline fully bound after reload; copying only latest health must not leave older retained identities unbound. Close/reload/revalidate before the news producer is constructed or started. The collector must install/revalidate the retained metadata before accepting another response, preserve first receipts for unchanged identities, and handle revisions without rewriting earlier observations. Report the exact seed scope, receipts, bounds and explicit incomplete-history/transport/acknowledgement boundaries. Defaults remain unchanged.

## Concrete files and acceptance

- Add `src/selective_options/crypto_news_checkpoint.py`, `tests/test_crypto_news_checkpoint.py`, `tests/test_crypto_news_capture_resume.py`, and `scripts/verify_crypto_news_resume.py`; wire only explicit CLI/capture options and any necessary collector initial-state validation.
- Use the original `crypto-news-public-02` XML, metadata, receipt mappings and health. Preserve source hashes and native payload/ID/first-receipt values. Verify closed SQL/Parquet/archive/outbox equality. Run the actual finite capture pipeline with every transport mocked and observe destination revalidation before construction/start.
- Replay the same original XML at explicitly labelled later fixture clocks and prove original headline receipts stay unchanged while the new health observation is retained. Keep synthetic revision, disappearance/reappearance and later outage cases separate from public provenance.
- Refuse failed/tied/stale latest health, missing/conflicting bindings, partial sink metadata, future either-clock evidence, malformed payload/provenance, row/byte excess and failed destination validation. Invalid preflight must not create output or start a producer; failed copy must close handles and retain honest failure evidence.
- Add focused meaningful tests, independent saved-native verification and a fresh `evidence/crypto-news-resume-saved-native-01/`. Preserve historical .53/source proof bytes. No network poll is needed for this increment.

## Independent research and real blockers

Independent closed-bar feature/backtester work remains authorized using saved data and clearly labelled fixtures. Additional spec features such as Stochastic RSI, ATR percentiles and swing structure can be implemented independently; do not label them an accepted signal/regime model before their prerequisite gates pass.

The news handoff cannot establish original publication time, missing historical headlines, complete crypto/financial coverage, sentiment or directional labels. Specialist flow/exchange attribution and missing on-chain/macroeconomic/history inputs remain coverage/access gaps. Persistent hosting, deployed Redis consumers, whole-host recovery and seven actual days of configured-feed operation require real infrastructure and elapsed evidence. Paper-day/trade credit remains zero and trading remains disabled. Do not create accounts, buy services, resume a scheduler or place orders.
