# Operator and accountant review checklist

This build produces software configuration and public market-data records, not tax advice. Transaction-ledger and tax-CSV implementation is milestone 13 and is not present yet. The configured rates are scenarios supplied in the build prompt; no tax classification is inferred for an offshore option, cash settlement, collateral transfer, or tokenised gold product.

Reviewed against official sources on 20 September 2026:

- The [Income Tax Department's ITR-2 FAQ](https://www.incometax.gov.in/iec/foportal/help/FileITR-2Online-FAQ) describes 30% VDA taxation with applicable surcharge and 4% cess. The prompt's 31.2% scenario omits any applicable surcharge; it must not be presented as everyone's effective rate.
- The department's [tax payment guidance](https://www.incometax.gov.in/iec/foportal/help/all-topics/e-filing-services/tax-payments) distinguishes pre-April-2026 transactions from transactions under the Income-tax Act, 2025, with updated section references. Do not hard-code the prompt's 1961 Act section numbers as the only current reporting identifiers. Maintain transaction date and legal-rule version.
- The [RBI LRS master direction](https://www.rbi.org.in/scripts/notificationuser.aspx?id=10192) addresses restrictions on remittances for margins or margin calls to overseas exchanges/counterparties. The exact proposed funding route needs qualified review; venue access alone is not proof that remitting funds is permissible.

The operator must verify with their chartered accountant and, where needed, a FEMA specialist:

1. Current venue availability, Indian residency/KYC acceptance, terms of service and permitted instrument types.
2. FEMA/LRS treatment of the proposed funding route and offshore derivatives account. Do not infer permission from the broad phrase “crypto is legal.”
3. The applicable statute, tax year, current CBDT guidance, surcharge, cess, cost basis and treatment of fees, funding, option premia, exercise and cash settlement.
4. Whether each transaction falls within VDA transfer rules, applicable withholding provisions, thresholds, responsibility to deduct, and return/form identifiers under the relevant Act.
5. Loss set-off and carry-forward treatment at the correct transaction/category level. Do not automatically net all platform P&L and label it taxable income.
6. Schedule VDA and any foreign asset/account disclosures such as Schedule FA, based on actual residency and account/product classification.
7. A documented INR FX and asset-pricing source, time convention, immutable trade identifiers, and reconciliation against venue statements and withholding credits.
8. At least seven years of records as requested, or a longer period where applicable. Specify encryption, restricted access, backups and restore verification.

Planned ledger fields: execution timestamp, original publication/observation timestamps where relevant, venue, instrument and contract metadata, side, quantity and unit, native consideration/currency, INR FX rate/source/as-of time, INR consideration, acquisition cost, fees, funding, withholding, withholding-credit status, cash-flow impact and tax-rule version.

TDS must be separately modelled as withheld cash and a potential tax credit/receivable, with any financing cost of locked cash shown explicitly. Do not subtract it both as a permanent trading expense and again in the final tax liability. Reports should distinguish pre-tax expectancy, scenario post-tax expectancy, and available-cash impact. No such reports can be validated until real classified transaction records and FX data exist.
