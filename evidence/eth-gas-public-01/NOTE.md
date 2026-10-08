# Superseded probe-report fields

This first attempt returned SOURCE_UNAVAILABLE and accepted no RPC results. Its `public_requests: 0` counted accepted results, not attempted requests; one chain-ID request was attempted. Its `repeat_deduplication_verified: true` was vacuous because no gas event existed. Neither field establishes successful ingestion or gas-event deduplication.

The verifier now separates request attempts from accepted results and marks gas deduplication unverified when no gas event exists. The second preserved attempt (`../eth-gas-public-02/report.json`) records one attempted request, zero results, no PIT checks, and `ClientConnectorCertificateError`. The health-only archive equality check did pass. Tests cover successful and unavailable verifier paths with clearly synthetic RPC fixtures.
