# Independent read-only Coin Metrics integration review

Date: 8 October 2026. Reviewer: delegated archive_rotation agent. This is a local engineering review, not a public-data collection or independent trading validation.

Reviewed the root-owned coinmetrics_quality.py, DuckStore.coinmetrics_quality and its scoped SQL helper, new configuration policies, ingestion producer/supervision/report wiring, and scripts/verify_coinmetrics.py. No source files or existing evidence were modified. The frozen collector was only read. Before/after hashes are retained beside this note.

No confirmed causal, quality or policy defect remains in this reviewed scope.

- Healthy quality requires canonical per-asset health and its exact observation IDs, original observation receipts, native period labels and poll-start bindings. Duplicate periods, ambiguous latest health, unfinished/current or later data and absent cohort members refuse a healthy view. Latest failed health and latest missing metrics cannot borrow older numeric values or success.
- Explicit native-label windows remain inclusive, aligned daily bounds. They do not establish complete historical returns. Daily labels, completed response receipts and poll freshness remain separate. Unsupported realized cap/SOPR and exchange attribution remain unavailable, and no derived substitutes are introduced.
- DuckStore applies source, public_reference environment, asset, symbol, kinds and BOTH clocks before LIMIT(max_rows+1). Overflow is rejected instead of truncating quality history. This limits application row materialization; database scans, SQL execution time, engine memory and arbitrary corrupted payload byte lengths are not bounded by that row limit.
- Constructor/configuration bounds agree. Optional capture is disabled by default, scopes the collector to configured BTC/ETH, explicitly reports unsupported configured assets, supervises collector failures, drains writer/store lifecycle, and keeps readiness/trading flags false.
- The finite proof uses the actual bus/consumer/store, validates exact pending outbox membership, both-clock boundaries, Parquet/native typed rows and restore, preserves native entities, refuses pre-existing or ambiguous output paths, and refuses executed-code drift before a final success report. Explicit mocked transport remains labelled as mocked; source access failures remain unavailable rather than public success.

The local labelled fixture batch passed 95 tests in 30.44 seconds:

    tests/test_coinmetrics_quality.py
    tests/test_coinmetrics_validation.py
    tests/test_coinmetrics_verifier.py
    tests/test_coinmetrics_audit.py

The full console result is local-regressions.log. These tests mock HTTP or replay explicitly mocked saved bodies; this review made ZERO public HTTP calls and submitted ZERO orders. A separate root-owned actual public collector poll and its saved-native audit are still required for new release evidence. This review does not claim M2 completion, continuous history, provider publication/finality, live readiness or a validated strategy edge.
