"""Independent native membership/clock/Decimal-sum audit; no market requests."""

import argparse
import hashlib
import json
from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot
from selective_options.models import Instrument
from selective_options.option_coverage import Universe
from selective_options.option_flow_research import research_option_flow
from selective_options.options_flow_features import NativeAmount, PutCallRatio, PutCallReport
from selective_options.options_research import load_options_snapshots

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence/reused-validation-public-01/events.duckdb"
SOURCE_SHA256 = "97c10f3c2f59a67edff83104fceca3b0b195b1498432597abde62e509f4cabdc"
CUTOFF = 1791050702961
POLICY = {
    "max_quote_age_ms": 10_000,
    "max_discovery_age_ms": 100_000,
    "max_clock_skew_ms": 10_000,
    "max_rows": 100_000,
    "max_expiries": 128,
}


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise IntegrityError(reason)


def amount(value: NativeAmount | None, expected: Decimal | Fraction) -> None:
    exact = Fraction(expected)
    require(
        value is not None
        and value.numerator == str(exact.numerator)
        and value.denominator == str(exact.denominator),
        "FLOW_PROOF_NATIVE_AMOUNT_OR_RATIO",
    )


def metric(result: PutCallRatio, calls: Decimal, puts: Decimal) -> None:
    require(
        result.input_complete
        and result.unit == "native_base_currency"
        and result.base_currency == "BTC",
        "FLOW_PROOF_METRIC_UNIT_OR_COMPLETENESS",
    )
    amount(result.call_total, calls)
    amount(result.put_total, puts)
    if calls == 0:
        require(
            result.state == "UNAVAILABLE"
            and result.reason == "CALL_DENOMINATOR_ZERO"
            and result.value is None
            and result.ratio_exact is None,
            "FLOW_PROOF_ZERO_DENOMINATOR",
        )
    else:
        exact = Fraction(puts) / Fraction(calls)
        amount(result.ratio_exact, exact)
        require(
            result.state == "AVAILABLE" and result.reason is None and result.value == float(exact),
            "FLOW_PROOF_RATIO_VALUE",
        )


def check(
    report: PutCallReport, snapshot: PointInTimeSnapshot, discovery: PointInTimeSnapshot
) -> dict[str, object]:
    require(
        report.formula_version == "options-put-call-v1"
        and report.asset == "BTC"
        and report.environment == "testnet"
        and report.as_of_ms == CUTOFF
        and report.amount_unit_policy == "deribit_option_base_currency"
        and report.volume_field == "stats.volume"
        and report.volume_window == "native_rolling_24h"
        and all(getattr(report, name) == value for name, value in POLICY.items()),
        "FLOW_PROOF_SCOPE_OR_POLICY",
    )
    require(
        not any(
            (
                report.full_chain_verified,
                report.current_feed_health_verified,
                report.executable_quotes_verified,
                report.simultaneous_quote_snapshot_verified,
                report.contract_count_ratio,
                report.interpolated,
                report.ready_for_signals,
                report.trading_enabled,
            )
        ),
        "FLOW_PROOF_UNSUPPORTED_APPROVAL",
    )
    rows = {row.event_id: row for row in (*snapshot.rows(), *discovery.rows())}
    identities = tuple(sorted(rows))
    digest = hashlib.sha256(
        json.dumps(
            [rows[key].model_dump(mode="json") for key in identities],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    require(
        report.input_event_ids == identities and report.input_sha256 == digest,
        "FLOW_PROOF_INPUT_IDENTITY",
    )
    manifests = [
        row
        for row in discovery.rows()
        if row.kind == "status"
        and row.payload().get("reason") == "UNIVERSE_DISCOVERED"
        and row.payload().get("asset") == "BTC"
    ]
    manifest = max(manifests, key=lambda row: row.observed_at_ms)
    universe = Universe.model_validate(manifest.payload())
    require(
        report.manifest_event_id == manifest.event_id
        and report.manifest_observed_at_ms == manifest.observed_at_ms
        and report.discovery_coverage == universe.coverage == "FULL"
        and report.discovered_members == universe.available == 1010,
        "FLOW_PROOF_DISCOVERY",
    )
    metadata = {
        identity: Instrument.model_validate_json(rows[identity].payload_json, strict=True)
        for identity in universe.metadata_event_ids
    }
    require(
        {item.expiration_timestamp for item in metadata.values()}
        == {item.expiration_timestamp for item in report.expiries}
        and len(report.expiries) == 13,
        "FLOW_PROOF_EXPIRY_MEMBERSHIP",
    )
    latest: dict[str, MarketEvent] = {}
    for row in snapshot.rows(kind="ticker"):
        if row.symbol not in latest or row.event_time_ms > latest[row.symbol].event_time_ms:
            latest[row.symbol] = row
        elif row.event_time_ms == latest[row.symbol].event_time_ms:
            require(row.event_id == latest[row.symbol].event_id, "FLOW_PROOF_AMBIGUOUS_QUOTE")
    totals = [Decimal(0)] * 4
    checked = []
    with localcontext() as context:
        context.prec = 160
        for expiry in report.expiries:
            members = {
                identity: item
                for identity, item in metadata.items()
                if item.expiration_timestamp == expiry.expiration_timestamp
            }
            require(
                set(members) == {member.metadata_event_id for member in expiry.members}
                and len(members) == len(expiry.members) == expiry.quote_coverage,
                "FLOW_PROOF_MEMBER_SET",
            )
            sums = [Decimal(0)] * 4
            for member in expiry.members:
                native = metadata[member.metadata_event_id]
                quote = latest[native.instrument_name]
                body = quote.payload()
                require(
                    member.symbol == native.instrument_name
                    and member.option_type == native.option_type
                    and native.base_currency == "BTC"
                    and member.metadata_observed_at_ms
                    == rows[member.metadata_event_id].observed_at_ms
                    and member.contract_size == native.contract_size
                    and (
                        member.quote_currency,
                        member.settlement_currency,
                        member.instrument_type,
                        member.price_index,
                    )
                    == (
                        native.quote_currency,
                        native.settlement_currency,
                        native.instrument_type,
                        native.price_index,
                    ),
                    "FLOW_PROOF_NATIVE_METADATA",
                )
                require(
                    member.quote_event_id == quote.event_id
                    and member.quote_source_time_ms == quote.event_time_ms
                    and member.quote_observed_at_ms == quote.observed_at_ms
                    and member.quote_reason is None
                    and member.volume_reason is None
                    and max(quote.event_time_ms, quote.observed_at_ms) <= CUTOFF
                    and max(CUTOFF - quote.event_time_ms, CUTOFF - quote.observed_at_ms)
                    <= POLICY["max_quote_age_ms"]
                    and abs(quote.event_time_ms - quote.observed_at_ms)
                    <= POLICY["max_clock_skew_ms"]
                    and quote.observed_at_ms >= manifest.observed_at_ms,
                    "FLOW_PROOF_NATIVE_CLOCKS_OR_QUALITY",
                )
                stats = body["stats"]
                if not isinstance(stats, dict) or "volume" not in stats:
                    raise IntegrityError("FLOW_PROOF_NATIVE_VOLUME_MISSING")
                oi, volume = Decimal(str(body["open_interest"])), Decimal(str(stats["volume"]))
                amount(member.open_interest, oi)
                amount(member.volume_24h, volume)
                side = 0 if member.option_type == "call" else 1
                sums[side] += oi
                sums[side + 2] += volume
            metric(expiry.open_interest, sums[0], sums[1])
            metric(expiry.volume, sums[2], sums[3])
            expected = () if sums[2] else ("CALL_DENOMINATOR_ZERO",)
            require(expiry.blockers == expected, "FLOW_PROOF_EXPIRY_BLOCKERS")
            totals = [left + right for left, right in zip(totals, sums, strict=True)]
            checked.append(
                {
                    "expiration_timestamp": expiry.expiration_timestamp,
                    "members": len(members),
                    "oi_state": expiry.open_interest.state,
                    "volume_state": expiry.volume.state,
                }
            )
        metric(report.aggregate_open_interest, totals[0], totals[1])
        metric(report.aggregate_volume, totals[2], totals[3])
    require(
        report.state == "PARTIAL" and report.blockers == ("INCOMPLETE_EXPIRY_VOLUME_RATIOS",),
        "FLOW_PROOF_PARTIAL_STATUS",
    )
    return {
        "checked_members": universe.available,
        "expiries": checked,
        "exact_native_amount_and_ratio_checks": universe.available * 2 + 13 * 6 + 6,
        "native_totals": [str(value) for value in totals],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    output = parser.parse_args().output
    require(file_digest(DATABASE) == SOURCE_SHA256, "FLOW_PROOF_SOURCE_CHANGED")
    value = research_option_flow(
        DATABASE,
        output,
        asset="BTC",
        as_of_ms=CUTOFF,
        amount_unit_policy="deribit_option_base_currency",
        volume_field="stats.volume",
        **POLICY,
    )
    snapshot, discovery, digest = load_options_snapshots(DATABASE, asset="BTC", as_of_ms=CUTOFF)
    report = PutCallReport.model_validate_json(json.dumps(value["put_call_report"]))
    checked = check(report, snapshot, discovery)
    require(file_digest(DATABASE) == digest == SOURCE_SHA256, "FLOW_PROOF_SOURCE_CHANGED")
    verification = {
        "status": "SAVED_NATIVE_PUT_CALL_AUDIT_PASSED",
        "source_database_sha256": digest,
        "report_sha256": file_digest(output / "put-call.json"),
        "verifier_sha256": file_digest(Path(__file__)),
        "checks": checked,
        "source_mutated": False,
        "new_public_requests": 0,
        "historical_strategy_trials": 0,
        "ready_for_signals": False,
        "trading_enabled": False,
    }
    with (output / "verification.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(verification, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": verification["status"],
                "checked_members": checked["checked_members"],
                "trading_enabled": False,
            }
        )
    )


if __name__ == "__main__":
    main()
