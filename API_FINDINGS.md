# Verified API findings and specification conflicts

Checked 20 September 2026. Venue snapshots are point-in-time evidence, not promises about future listings or liquidity.

## Public testnet evidence

The real WebSocket probe in `evidence/public-testnet-probe.json` completed on `2026-09-20T13:47:27Z`. Full captured summary chains are in `evidence/chains/`.

| Underlying | Active options | Summary rows | Positive, non-crossed two-sided summaries |
| --- | ---: | ---: | ---: |
| BTC | 1,050 | 1,050 | 885 |
| ETH | 854 | 854 | 604 |
| SOL | 294 | 294 | 0 |
| PAXG | 0 | 0 | 0 |

All captured summaries passed the configured age check. Six detailed tickers were fetched. The observed local/server offset was approximately 1.87 seconds with 0.275 seconds round-trip uncertainty; this is within the configured 5-second bound, not proof of an NTP-synchronised production clock.

SOL was listed but had no two-sided summary quotes in this sample. PAXG returned an empty option list, so the probe reports `GOLD_UNTRADEABLE`. It does not claim a liquid synthetic or external route exists.

## Currency and contract metadata

The [published get_instruments reference](https://docs.deribit.com/api-reference/market-data/public-get_instruments) lists a narrower currency enum than the live testnet accepts. Direct SOL and PAXG discovery succeeded in this session. Code first queries the requested underlying; only an explicit unsupported-currency parameter error permits retrying `currency=any`, followed by a `base_currency` filter. All results retain base/quote/settlement currency and contract/tick metadata. No naming pattern is used to infer contract economics.

## Rates and authentication

The [rate-limit documentation](https://docs.deribit.com/articles/rate-limits) assigns separate costs to instrument discovery and subscriptions. The client has separate token buckets and conservative ordinary-call defaults. These are local ceilings; actual public per-IP allowance may differ. Reconnect does not reset the local buckets.

The [authentication API](https://docs.deribit.com/api-reference/authentication/public-auth) supports client credentials and refresh tokens. Reduced token permissions do not prove the source key lacks withdrawal rights. The implementation also inspects the matching key's maximum scope using [list_api_keys](https://docs.deribit.com/api-reference/account-management/private-list_api_keys). The latter is documented as potentially requiring TFA; that remains an account-dependent readiness blocker. No private response payload is logged or persisted.

## Cancel-on-disconnect conflict

[enable_cancel_on_disconnect](https://docs.deribit.com/api-reference/session-management/private-enable_cancel_on_disconnect) is documented as requiring `account:read_write`. It also states that an enabled account default applies to subsequently opened connections. To retain a restricted runtime key, the current design requires this default to be set through account administration and verifies the actual connection using [get_cancel_on_disconnect](https://docs.deribit.com/api-reference/session-management/private-get_cancel_on_disconnect), which requires `account:read`. A disabled connection halts readiness. This is an explicit alternative to enabling the setting from the runtime key, pending real authenticated verification.

At the initial public probe no testnet account or key was available. The authenticated follow-up below supersedes that initial limitation.

## Authenticated follow-up at 18:03:38 UTC

Testnet authentication with the user-supplied credentials succeeded. The returned scope contained `connection mainaccount` only. The [scope reference](https://docs.deribit.com/articles/access-scope) defines mainaccount as a server-added identity marker; our earlier rejection of this marker was corrected. Identity markers alone do not prove wallet or trade permissions. `private/list_api_keys` returned 13021, documented as insufficient permission/forbidden in the [complete error table](https://docs.deribit.com/articles/errors). This was not a confirmed TFA challenge. `private/get_cancel_on_disconnect` reported the current connection disabled. No orders or account-setting changes were made. See `evidence/authenticated-diagnostics.json`; it contains no key identifiers, secrets or tokens.

## Replacement credential follow-up at 18:25:22 UTC

Testnet rejected the second user-supplied credential pair with code 13004, listed as invalid_credentials in the [complete RPC error table](https://docs.deribit.com/articles/errors). This does not establish whether the key was created on production, entered incorrectly, disabled, or otherwise mismatched. No production fallback was attempted and no orders were placed. The prior key authenticated but had separate unresolved permission/COD checks.
