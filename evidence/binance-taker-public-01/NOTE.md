The first finite probe received discovery plus three BTC five-minute aggregates, but published no flow rows. Its original report, response receipts and health-only database/archive are preserved.

The parser initially applied a twenty-minute maximum age to every sample. The oldest returned sample began about 21 minutes before receipt; the newest period had ended about six minutes before receipt. The corrected parser allows a bounded thirty-minute three-sample window and separately requires the newest ended period to be at most ten minutes old. Unended periods are still withheld, timestamps/receipts are unchanged, and no first-publication time or finality is inferred. A regression replays this exact saved response.

After that concrete correction, the second finite probe in `../binance-taker-public-02` passed for BTC, ETH, SOL and PAXG. This first run remains failed diagnostic evidence, not a working feed claim.
