# Next M2 increment after 0.3.52

## Recommendation: explicit SOL network checkpoint resume

Implement an opt-in, bounded startup handoff for the existing Solana performance and visible vote-stake snapshots. The checklist marks historical continuity and whole-source recovery pending; the source already has a finite saved public sample and canonical quality logic, so this can be built and proven offline without accounts, purchases, or provider calls. The .52 periodic open-bar generation work is complete as a distinct increment and should not be repeated here.

## Why this increment

`SolanaNetworkCollector.collect` stages the RPC responses, then publishes two native events followed by a separate health event. A failure during sink publication can leave a durable partial prefix. `network_quality` verifies the latest successful health and its referenced pair, but it does not certify that all older eligible native rows are bound. There is no SOL startup checkpoint/resume path. The saved public replay has three original rows, exact receipt mappings, archive/reload verification, and a separate clearly synthetic outage case (`outputs/selective-options/evidence/solana-network-public-01/`).

## Exact implementation scope

- Add `outputs/selective-options/src/selective_options/solana_checkpoint.py`: a read-only DuckDB loader with fixed `solana_public` / `public_reference` / `SOL` scope, both source and observation clocks, row and stored-event-value byte budgets, deterministic output order, and exact event preservation. Reuse `network_quality` for latest health, source-time and receipt freshness, and canonical pair checks. Additionally bind every selected native row to exactly one successful health reference across retained eligible history, so partial publication is not hidden by an older healthy view. Return only the latest validated native pair and its exact health row as the minimal restart seed; never create a fallback from older health.
- Wire a new explicit `capture --resume-solana-from DATABASE` option through `outputs/selective-options/src/selective_options/cli.py` and `ingestion.py`. Require SOL configured and `solana_network_enabled`. Validate before output creation, copy exact rows with deliveries pending, close and reopen the destination, revalidate the seed and outbox count before any producer starts. Keep normal capture defaults unchanged and report the seed scope, selected health receipt, copied rows, and explicit false values for complete history, recovered transport, trading readiness, and acknowledgements restored.
- Add `outputs/selective-options/tests/test_solana_checkpoint.py` and `tests/test_solana_capture_resume.py`; add `scripts/verify_solana_resume.py` plus a new private evidence directory for the saved-data proof. Do not edit the frozen .52 files while its run is pending.

## Acceptance evidence

1. Offline saved-native replay selects the exact 3-row public sample (2 native rows + bound health), preserves all payload bytes, event IDs and receipts, leaves the source DB unchanged, and copies into a new DB whose closed reload, exact DuckDB/Parquet rows, and pending outbox match.
2. Startup-order proof observes that the destination has closed and revalidated before the mocked SOL collector is constructed or started; invalid seeds fail before creating output or invoking any producer.
3. Refusal cases cover latest failed health, tied latest health, missing or duplicate references, an unbound native prefix after a sink interruption, mismatched event IDs/receipt/symbol, malformed canonical payload, future either-clock evidence, stale health/native receipts/sample time, wrong source/environment/genesis, and row/byte overflow. Check PIT visibility immediately before and at both source/receipt boundaries.
4. A labelled synthetic later outage blocks the new as-of view while preserving the prior saved-public view and source bytes. Fixture provenance stays explicit; no synthetic row is described as public.
5. No HTTP requests are made for this increment. Any future public poll, if separately authorized by the existing run workflow, remains a finite read-only check and cannot establish continuous history, complete staking coverage, DEX/fee-payer data, independent finality, sustained hosting, or seven-day acceptance.

## Real blockers versus engineering work

Independent engineering can validate and hand off the finite saved pair now. Exchange flows, DEX volume, fee payers, stake flows, broad chain coverage, sustained history, whole-host restart recovery, deployed Redis, and seven-day operation remain real coverage/deployment gaps. This checkpoint alone does not satisfy M2 or authorize later acceptance.