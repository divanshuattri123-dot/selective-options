# Exact event replay after compiled-validator reuse

`verify_saved.py` loads the immutable 0.3.32 wheel only after matching its release hash, extracts its 52 source modules into `work/baseline_0332` and imports it under a separate package name. It reconstructs 23,519 saved native stream envelopes with original receipts from `receive-timing-public-01` and compares every event field/payload JSON/identity/clock against both versions and the original row.

Full replay took 59.0932726 seconds in the baseline and 4.3066127 seconds in current code. Three alternating 250-row sample medians were 607.6198 and 42.5112 ms. These local measurements are not a threshold, original wire order, restored dropped duplicates or sustained pipeline proof. All parsed outputs match and original source bytes remain unchanged. No network calls occur. Its report is immutable and the saved baseline wheel is required for rerun in a separate output artifact.
