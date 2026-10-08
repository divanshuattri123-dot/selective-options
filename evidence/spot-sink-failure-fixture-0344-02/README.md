# Synthetic capture failure validation

This evidence deliberately fails the second of two synthetic trade publications. Actual collector supervision, EventBus, consumer, DuckStore, shutdown drain and archive code execute; the socket and failure are fixtures. No public requests occur.

The collector opens one fixture connection, retains its prior local frontier and refuses all later trade, flush and run calls before opening another connection. The capture supervisor records the expected FAILED result and raises SPOT_EMISSION_FAILED with the original synthetic OSError as its cause. The first valid trade, which already reached the queue, is drained to storage and archived. It remains pending in the delivery outbox. The other two rows are derived subscription and halt statuses. No bars or second trade appear. All typed records match the sealed Parquet archive, and both source and receipt clocks gate availability.

The FAILED capture is an expected test outcome. It is not a failed live collector, an atomic sink transaction, a real Redis test or a trading session. Source and verifier hashes in report.json bind this finite proof. The earlier collector frontier is local state, not proof that no valid prefix reached storage. A fresh explicit committed-sequence resume still starts new bar accumulators as partial.
