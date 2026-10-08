# Saved stablecoin poll restart proof

`report.json` records an offline replay of the seven original public HTTP response entities in `../stablecoin-supply-public-01`. The production request parser reproduces the original paired USDC/USDT snapshot and its bound health record exactly, including every native response receipt. The original source files and their hashes remain unchanged.

The repeated poll, RPC failure and recovery are explicitly synthetic fixtures. Their later receipt offsets are 10,000 ms and 30,000 ms; the failure occurs at the recorded synthetic health clock. No new network session or public request is created. This proves local restart behavior, not an actual provider outage, recovery, independent finality, first publication or continuous supply history.

Three exports contain 2, 5 and 7 records. Each checksummed archive matches the closed database at its corresponding phase. The failed checkpoint refuses to return an older healthy revision at the failure time. An explicit historical query before that failure supplies a restart anchor for the deliberate recovery fixture; it does not imply current feed health. Failure stays visible until the later complete successful poll and bound health are both eligible. Fourteen exact point-in-time boundaries, seven pending outbox records and original receipt preservation pass.

`verifier_snapshot.py` preserves the verifier used. The report distinguishes its current verification source hashes from the historical 0.3.39 public capture code hash. There is no Redis delivery, trading approval or complete-ingestion claim.
