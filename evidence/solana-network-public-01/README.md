# Finite SOL public network check

Version 0.3.28 made four read-only RPC requests to the official public mainnet endpoint, verified its genesis identity, and stored one recent performance sample, one returned vote-account stake snapshot, and one health observation. All three records remain pending in the local outbox. No order, private call or recurring scheduler was started.

The source returned 672 current and 6,118 delinquent vote accounts, totaling exactly 442013190171137211 lamports of visible activated stake. This is the returned vote-account aggregate, not total SOL supply, all stake accounts, stake flows, active fee payers or DEX volume. The performance clock is the estimated production time of the sample-end slot, independently obtained after the sample response. The stake snapshot has no context slot/time and uses its actual receipt.

`results.json` preserves the validated RPC results and each actual receipt. `report.json` records six visibility boundaries, exact replay deduplication, sealed Parquet equality and closed database/quality reload. `independent-proof.json` checks SQL visibility, every accepted raw quantity/record and all receipt mappings separately. It replays the saved public responses through the collector exactly, then adds a clearly synthetic 429 failure to a separate four-row fixture. That failure blocks the later quality view while the original view and original database bytes remain unchanged. Replay made zero new HTTP requests.

`fixture.duckdb` and `fixture.parquet` contain the original three saved-response replay rows plus one synthetic outage row. They are not evidence that the public source actually failed. `verify_saved.py` refuses existing fixture outputs; copy the original database, archive, results/report and verifier to a fresh sibling folder for replay. The public verifier also requires a fresh output directory.

This finite check does not certify historical continuity, restart/rotation, sustained collection, independent finality, source authenticity, complete on-chain coverage or trading eligibility.

Source definitions: [public RPC](https://solana.com/docs/rpc), [performance samples](https://solana.com/docs/rpc/http/getrecentperformancesamples), [estimated block time](https://solana.com/docs/rpc/http/getblocktime), [vote accounts](https://solana.com/docs/rpc/http/getvoteaccounts), [cluster identity](https://namespaces.chainagnostic.org/solana/caip2).
