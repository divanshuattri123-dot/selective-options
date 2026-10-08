# Offline revision proof

This proof reads the existing taker and global account-ratio public databases in read-only mode. All 24 original observations retain their actual source/receipt clocks, values and event IDs. The report records unchanged source database hashes. It makes zero network requests.

The six rows in `synthetic.duckdb`, `synthetic.parquet` and `restored/events.duckdb` are explicitly synthetic revisions. They use saved BTC payloads to test A → B → A, with simulated later receipts and a changed reported field. They are not new public observations or evidence of actual venue revisions. Exact poll replay inserts no extra row; each new receipt remains distinct. Restore preserves all six rows and selects one observed revision per period at each receipt, independently checked by SQL.

Before 0.3.22, identical values for a period shared an identity across receipts. A later return to the original value could therefore disappear through deduplication. New flow payloads include `observation_received_at_ms` in their canonical identity. This retains each poll, while replaying the same poll deduplicates. `flow_at` selects one latest observed revision per period, filtering both clocks before validating canonical rows. Ambiguous same-receipt revisions and malformed eligible observations fail explicitly. Capture exposes these summaries only for enabled flow feeds.

Legacy rows remain accepted under their old identities, but lost historical receipts and reversions cannot be reconstructed. Raw poll observations must not be summed as distinct periods. Projection integrity does not establish source freshness/health, field semantics, volume units, finality, publication time, continuous history or trading eligibility.

To repeat the proof, copy `verify_saved.py` to a new evidence sibling directory and run it with the project environment. Its generated output paths must be fresh; the restore directory refuses overwrite. Preserve this completed evidence. Unit tests exercise isolated temporary databases without modifying these saved artifacts.
