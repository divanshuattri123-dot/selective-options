# Offline funding revision evidence

The verifier makes zero network requests. It reads the six original public BTC funding observations read-only and proves progressive visibility at their three actual response receipts. The original source database hash remains unchanged. Its payloads, source timestamps, values, identities and observation clocks are preserved exactly.

The mixed fixture databases and Parquet combine those six original rows with four explicitly synthetic observations of the earliest charge: A → B → A, followed by an unchanged later poll. Changed rates/receipts are fixtures, not evidence of actual venue revisions. Their four event IDs are listed in `report.json`. Replaying all observations inserts no extra rows. All ten rows survive seal/export/restore with exact identities and clocks; the as-of view selects six distinct charge timestamps at each synthetic receipt, with one newest observed revision for the revised charge. Independent SQL checks the complete selected ID list.

`funding_history_at` filters source/environment/asset/symbol/kind, inclusive requested funding-time bounds and both availability clocks before canonical validation. Corrupt eligible observations, same-receipt conflicting revisions and eligible-row budget overflow refuse selection. Results are ordered by original funding timestamp; no eight-hour grid is inferred. Reported rate types and unknown historical intervals remain unchanged. The finite public verifier now includes `funding_revision_view`; earlier public evidence remains unchanged.

This is scoped observation selection, not rate summation, source authenticity, gap-free schedule proof, historical interval verification, finality, first-publication reconstruction or trading readiness. New downloads cannot become available to earlier decisions. No private calls or orders occur.

To reproduce while preserving this evidence, copy `verify_saved.py` to a new evidence sibling and run it with the project environment. The generated restore output must be fresh; completed directories refuse overwrite. Unit tests use independent temporary databases.
