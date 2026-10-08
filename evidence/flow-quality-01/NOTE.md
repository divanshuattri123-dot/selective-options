# Flow window quality evidence

This verifier makes zero network requests and reads the two saved public databases without modification. It checks the actual 24 flow observations and eight source-health observations for BTC/ETH/SOL/PAXG. SQL independently confirms three consecutive five-minute source period ends for each asset/feed. Availability is assessed at the original recorded health receipt, not at today's clock.

All later evaluation clocks, global discovery failures, per-asset health recovery and removal of a middle period are explicitly simulated. The four rows in `synthetic-health.json` are synthetic and do not record a real venue outage or recovery. A future failure does not alter an earlier assessment; a global failure blocks every asset, and later BTC health alone does not recover ETH. A fresh newest observation cannot hide the removed period. Saved source database hashes remain unchanged.

`OBSERVED_WINDOW_AVAILABLE` proves only a bounded recent sampled window, canonical rows, receipt/source age limits, and appropriate source health. It does not prove older continuity, original publication time, finality, semantics, normalized units, signals or trading eligibility. Legacy repeated receipts discarded before 0.3.22 remain unrecoverable.

The saved results are reproducible with the project environment and `python evidence/flow-quality-01/verify_saved.py`. The public databases stay read-only; only this verifier's deterministic report and synthetic-health files are regenerated. Prefer copying the script to a new evidence sibling to preserve the completed checkpoint unchanged.
