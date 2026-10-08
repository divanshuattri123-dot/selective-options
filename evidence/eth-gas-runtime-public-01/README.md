# Finite actual public gas anchor restart

Run 0.3.38 made exactly four public dRPC read-only RPC requests through the production collector, two polls separated by closed-store anchor copy/reload. Default TLS remained enabled. Both returned the same native finalized block; first observation receipt stays unchanged while each poll health binds its own actual response receipt. Native bytes/hex hashes, quantities, PIT, archive, pending outbox and closed checkpoint passed independent verification.

See report.json, first/ and restarted/, and ../restart-parent-validation-0338.json. source_hashes_before seals all 54 module files plus the actual runtime verifier, and those bytes still match final release source. worker-handoff.json separately records offline worker test/source handoff.

This is a finite explicit gas anchor restart, not whole-source resumed ingestion, continuous history, independently verified finality, actual successor progression, Redis delivery, public outage recovery or trading approval. Existing PublicNode failures were not retried. The default disabled opt-in/provider policy did not change.
