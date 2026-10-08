# Original testnet key: verified settings

Latest check: 2026-09-20 18:38:32 UTC (21 September 00:08:32 IST).

| Check | Result |
| --- | --- |
| Testnet authentication / key enabled | Passed |
| Actual key maximum permissions | Trade read/write, Account read, Wallet none |
| Custody, Block Trade, Block RFQ and alternate interfaces | None |
| Effective token bounded by maximum key permissions | Passed |
| Account-default and fresh-connection cancel-on-disconnect | Both enabled |
| Native authenticated session and full discovery | Passed |
| Real refresh grant | Passed using advanced injectable scheduling clock |

The refresh test sent a real refresh grant to testnet early. It was not a natural token-expiry or endurance test. No account settings were changed by the agent and no orders were submitted. The production key was not used. This successful check supersedes older diagnostics retained in evidence.

Evidence: [settings-start-check.json](evidence/settings-start-check.json), [authenticated-probe.json](evidence/authenticated-probe.json).
