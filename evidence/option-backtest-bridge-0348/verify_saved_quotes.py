"""Read-only saved-native option adapter proof; performs no simulation or order call."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

from selective_options.option_backtest import OptionExecutionProfile, load_saved_option_quotes

ROOT = Path(__file__).resolve().parents[2]
DATABASE = ROOT / "evidence/reused-validation-public-01/events.duckdb"
EXPECTED_SHA256 = "97c10f3c2f59a67edff83104fceca3b0b195b1498432597abde62e509f4cabdc"
AS_OF_MS = 1_791_050_702_961
SYMBOL = "BTC-25DEC26-140000-C"


def main() -> None:
    if hashlib.sha256(DATABASE.read_bytes()).hexdigest() != EXPECTED_SHA256:
        raise SystemExit("saved public archive digest differs from the reviewed input")
    profile = OptionExecutionProfile(
        profile_id="saved-native-read-only-unit-validation",
        asset="BTC",
        quote_currency="BTC",
        settlement_currency="BTC",
        instrument_type="reversed",
        expected_contract_size=Decimal("1"),
        contract_multiplier=Decimal("1"),
        displayed_amount_unit="base_currency_amount",
        quote_price_unit="currency_per_underlying_unit",
    )
    loaded = load_saved_option_quotes(
        DATABASE,
        asset="BTC",
        symbol=SYMBOL,
        as_of_ms=AS_OF_MS,
        profile=profile,
        max_discovery_age_ms=100_000,
        max_clock_skew_ms=5_000,
        max_rows=100_000,
    )
    report = {
        "status": "SAVED_NATIVE_OPTION_BRIDGE_VALIDATED",
        "data_kind": "existing_saved_deribit_testnet_evidence",
        "source": "evidence/reused-validation-public-01/events.duckdb",
        "source_sha256": loaded.source_sha256,
        "as_of_ms": loaded.as_of_ms,
        "symbol": loaded.instrument.instrument_name,
        "discovery_event_id": loaded.manifest_event_id,
        "metadata_event_id": loaded.metadata_event_id,
        "materialized_quote_count": len(loaded.quotes),
        "quote_with_both_sides": sum(
            quote.bid is not None and quote.ask is not None for quote in loaded.quotes
        ),
        "quote_with_missing_side": sum(
            quote.bid is None or quote.ask is None for quote in loaded.quotes
        ),
        "max_clock_skew_ms": loaded.max_clock_skew_ms,
        "orders_submitted": 0,
        "simulation_run": False,
        "gate2_complete": False,
        "trading_enabled": False,
        "limitation": "read-only adapter validation; caller did not validate venue economics",
    }
    output = Path(__file__).with_name("report.json")
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
