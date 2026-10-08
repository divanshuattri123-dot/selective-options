# Exact current-manifest metadata proof

`verify_saved.py` replays 109,845 unchanged native rows from the two preserved captures using exact-conflict insert and duplicate replay. Four saved asset views and 20 PIT boundaries match the original full-row canonical oracle, including stale legs and empty gold. All original native SQL subsets, baseline/fixture archives, pending outbox, closed reload and twelve source hashes match.

A labelled fixture clones one native metadata payload into 110,000 obsolete receipt generations, all strictly before the earliest original receipt. They are synthetic, not historical downloads. The hash-verified actual 0.3.35 wheel exceeds its unchanged 100,000-row budget. Current BTC materialization is [3 controls, 1,010 exact manifest metadata, 2,020 latest tied legs] with 172,089 retained rows/outbox entries. Original freshness and timestamps are unchanged. Complete derived artifacts are in `../metadata-projection-replay-01`.

Proof requires fresh report/output and `work/baseline_0335`, plus the unchanged previous wheel. It makes no requests or orders and does not delete history. Every eligible control remains budgeted; SQL scan/time/memory, database growth and sustained capacity are not certified. M2 remains partial.
