# Offline saved-native SOL checkpoint handoff

`verification.json` is the accepted final 0.3.53 proof. The verifier binds the exact
source hashes before execution and checks them again after the proof. The initial
run made before adding that fingerprint is preserved separately under
`work/solana-resume-before-source-hashes-01/`; it is not the accepted final proof.

The source is the existing finite public sample under
`evidence/solana-network-public-01/`. Its historical selection clock is
`1791028544400`, the original successful health receipt. This offline replay does
not assert that the saved sample is fresh on the current wall clock. Every file in
that historical source tree retains its original SHA-256, including the original
database hash `b3c2b76352daffbdc18a26b17b6b8540fe37c1dbc88acf87d093e12a0e8a3221`.

The independent audit compares the saved RPC responses with all accepted native
sample fields and all 672 current plus 6,118 delinquent vote-account records. It
checks integer quantities, the exact visible aggregate
`442013190171137211` lamports, individual response receipts, sample slot/block time,
canonical payload bytes and independently calculated event IDs. Twelve SQL checks
cover immediately before and at both source and receipt boundaries for all three
rows. SQL and Parquet stored values match exactly.

`capture/` is an actual finite capture pipeline invocation with Deribit, chain and
SOL producers mocked to remain idle. Socket resolution and connection are denied.
The actual read-only destination loader and a second independent SQL reader both
observe the three exact committed rows and three exact pending deliveries after
the writer closes. Only then may the mocked SOL collector be constructed or any
mocked producer start. The final closed database and Parquet contain precisely
the original three events, payload bytes and receipts. There were zero network
attempts and no new public or private requests.

`restored/` is a real checksummed archive restore into a fresh database. All three
rows exactly match the source and remain pending. Delivery acknowledgements are
not copied or restored. The focused verifier tests also check a synthetic source
with acknowledged rows: the new capture and archive restore still contain three
pending deliveries.

`synthetic-later-outage.duckdb` contains the saved three-row seed plus a locally
constructed later failed-health row. It preserves the earlier saved view and
refuses the later resume as `SOL_FEED_UNAVAILABLE`. No actual public outage is
asserted. `synthetic-unbound-prefix.duckdb` contains the saved seed plus only the
first canonical performance row of a new, locally constructed poll. The later
resume refuses it as `SOL_CHECKPOINT_UNBOUND_SNAPSHOT`. Both invalid capture
preflights create no output and invoke no producer. These two fixture databases
are labelled synthetic and are separate from the original public source.

To reproduce, use the locked project environment and a fresh output directory:

```powershell
$env:PYTHONPATH='src'
.venv\Scripts\python.exe scripts/verify_solana_resume.py --output evidence/solana-resume-saved-native-02
```

This proof certifies a bounded saved seed handoff. It does not certify complete
history, recovered transports, independent provider finality, whole-host restart
recovery, sustained hosting, seven-day operation, M2 completion, paper validation,
signals, strategy performance or trading readiness. No account, credential,
purchase, live order or paper-trading operation was used.
