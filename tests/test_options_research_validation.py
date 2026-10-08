"""Independent finite analytics checks; synthetic fixtures are not market history."""

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from selective_options.cli import main
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.models import Instrument
from selective_options.options_features import compute_options_features
from selective_options.options_research import load_options_snapshots, research_options
from selective_options.store import DuckStore

POLICY = {
    "option_type": "call",
    "moneyness_method": "native_underlying_log_distance",
    "tie_policy": "exact_distance_refuse",
    "max_quote_age_ms": 500,
    "max_discovery_age_ms": 2000,
    "max_atm_log_moneyness": 0.1,
    "max_underlying_mismatch_fraction": 0.01,
    "flat_threshold_iv_points": 1.0,
}


def fixture_rows(instrument, ticker):
    """Two invented expiries with explicit native-format marks and receipt clocks."""
    metadata, quotes = [], []
    for index, iv in enumerate((40.0, 50.0)):
        symbol = f"BTC-SYNTHETIC-EXPIRY-{index}-C"
        model = Instrument.model_validate(
            instrument
            | {
                "instrument_name": symbol,
                "strike": 100000.0,
                "expiration_timestamp": 100000 + index * 100000,
            }
        )
        metadata.append(
            make_event(
                source="deribit",
                environment="testnet",
                asset="BTC",
                symbol=symbol,
                kind="instrument",
                timestamp_kind="receipt",
                event_time_ms=1000,
                observed_at_ms=1000,
                payload=model.model_dump(mode="json"),
            )
        )
        quotes.append(
            make_event(
                source="deribit",
                environment="testnet",
                asset="BTC",
                symbol=symbol,
                kind="ticker",
                event_time_ms=1800,
                observed_at_ms=1810,
                payload=ticker
                | {
                    "instrument_name": symbol,
                    "timestamp": 1800,
                    "mark_iv": iv,
                    "underlying_price": 100000.0,
                    "underlying_index": "SYNTHETIC-INDEX",
                },
            )
        )
    manifest = make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        timestamp_kind="receipt",
        event_time_ms=1100,
        observed_at_ms=1100,
        payload={
            "reason": "UNIVERSE_DISCOVERED",
            "asset": "BTC",
            "available": 2,
            "selected": 2,
            "coverage": "FULL",
            "instruments": [r.symbol for r in metadata],
            "selected_instruments": [r.symbol for r in metadata],
            "metadata_event_ids": [r.event_id for r in metadata],
        },
    )
    return [*metadata, *quotes, manifest]


async def closed_database(path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(rows, exact=True)
    finally:
        await store.close()
    return path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def quote_variant(row, *, source=None, receipt=None, **body):
    source = row.event_time_ms if source is None else source
    return make_event(
        source=row.source,
        environment=row.environment,
        asset=row.asset,
        symbol=row.symbol,
        kind="ticker",
        event_time_ms=source,
        observed_at_ms=row.observed_at_ms if receipt is None else receipt,
        payload=row.payload() | body | {"timestamp": source},
    )


async def test_sql_filters_both_clocks_before_shared_overflow(instrument, ticker, tmp_path):
    rows = fixture_rows(instrument, ticker)
    extras = [
        quote_variant(rows[2], source=2001, receipt=1900, mark_iv=99.0),
        quote_variant(rows[3], source=1900, receipt=2001, mark_iv=99.0),
    ]
    # Future-only rows must not consume the five-row historical materialization budget.
    database = await closed_database(tmp_path / "source.duckdb", rows + extras)
    before = digest(database)
    quotes, discovery, hashed = load_options_snapshots(
        database, asset="BTC", as_of_ms=2000, max_rows=5
    )
    assert set(quotes.rows()) == set(rows[:4])
    assert discovery.rows() == (rows[4],)
    assert hashed == before == digest(database)
    report = compute_options_features(quotes, discovery, **POLICY)
    assert report.adjacent_iv_differences_points == (10.0,)
    assert [r.mark_iv_points for r in report.expiries] == [40.0, 50.0]
    assert set(report.input_event_ids) == {r.event_id for r in rows}
    assert not report.ready_for_signals and not report.trading_enabled


async def test_shared_asset_and_system_overflow_refuses_without_output(
    instrument, ticker, tmp_path
):
    database = await closed_database(tmp_path / "source.duckdb", fixture_rows(instrument, ticker))
    before = digest(database)
    output = tmp_path / "result"
    with pytest.raises(IntegrityError, match="HISTORY_OVER_BUDGET"):
        research_options(database, output, asset="BTC", as_of_ms=2000, max_rows=4, **POLICY)
    assert not output.exists() and digest(database) == before


async def test_receipts_ids_and_native_metadata_are_kept_exact(instrument, ticker, tmp_path):
    rows = fixture_rows(instrument, ticker)
    database = await closed_database(tmp_path / "source.duckdb", rows)
    result = research_options(database, tmp_path / "result", asset="BTC", as_of_ms=2000, **POLICY)
    payload = json.loads((tmp_path / "result/options-features.json").read_text())
    assert payload == result
    report = payload["options_report"]
    assert report["manifest_event_id"] == rows[4].event_id
    for index, expiry in enumerate(report["expiries"]):
        assert expiry["metadata_event_id"] == rows[index].event_id
        assert expiry["quote_event_id"] == rows[index + 2].event_id
        assert expiry["quote_source_time_ms"] == 1800
        assert expiry["quote_observed_at_ms"] == 1810
        assert expiry["metadata_observed_at_ms"] == 1000
    assert payload["source_database_sha256"] == digest(database)
    assert payload["source_scope_and_cutoff"]["materialized_rows"] == 5
    assert not payload["ready_for_signals"]


async def test_source_mutation_during_calculation_refused_before_publication(
    instrument, ticker, tmp_path
):
    database = await closed_database(tmp_path / "source.duckdb", fixture_rows(instrument, ticker))
    output = tmp_path / "result"
    original = compute_options_features

    def mutate(*args, **kwargs):
        report = original(*args, **kwargs)
        with database.open("ab") as stream:
            stream.write(b"EXPLICIT_TEST_ONLY_MUTATION")
        return report

    with patch("selective_options.options_research.compute_options_features", mutate):
        with pytest.raises(IntegrityError, match="RESEARCH_SOURCE_CHANGED"):
            research_options(database, output, asset="BTC", as_of_ms=2000, **POLICY)
    assert not output.exists()


async def test_source_ties_do_not_follow_later_receipt_or_favorable_iv(
    instrument, ticker, tmp_path
):
    rows = fixture_rows(instrument, ticker)
    conflict = quote_variant(rows[2], receipt=1820, mark_iv=1.0)
    database = await closed_database(tmp_path / "source.duckdb", rows + [conflict])
    snapshots = load_options_snapshots(database, asset="BTC", as_of_ms=2000)
    report = compute_options_features(*snapshots[:2], **POLICY)
    assert report.state == "UNAVAILABLE" and report.term_shape == "UNAVAILABLE"
    assert report.expiries[0].state == "UNAVAILABLE"
    assert report.expiries[0].quote_event_id is None


async def test_new_unreferenced_metadata_cannot_replace_manifest_generation(
    instrument, ticker, tmp_path
):
    rows = fixture_rows(instrument, ticker)
    newer = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=rows[0].symbol,
        kind="instrument",
        timestamp_kind="receipt",
        event_time_ms=1500,
        observed_at_ms=1500,
        payload=rows[0].payload() | {"strike": 130000.0},
    )
    database = await closed_database(tmp_path / "source.duckdb", rows + [newer])
    quote, discovery, _ = load_options_snapshots(database, asset="BTC", as_of_ms=2000)
    report = compute_options_features(quote, discovery, **POLICY)
    assert report.expiries[0].metadata_event_id == rows[0].event_id
    assert report.expiries[0].strike == 100000


async def test_closed_database_requirement_refuses_wal(instrument, ticker, tmp_path):
    database = await closed_database(tmp_path / "source.duckdb", fixture_rows(instrument, ticker))
    database.with_suffix(".duckdb.wal").write_bytes(b"EXPLICIT_FAKE_WAL_FOR_REFUSAL")
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        load_options_snapshots(database, asset="BTC", as_of_ms=2000)


@pytest.mark.parametrize(
    "policy", [{"max_rows": True}, {"max_rows": 0}, {"as_of_ms": True}, {"asset": "DOGE"}]
)
def test_bad_read_policies_refuse_before_open(tmp_path, policy):
    with pytest.raises(ValueError):
        load_options_snapshots(
            tmp_path / "missing", **({"asset": "BTC", "as_of_ms": 2000} | policy)
        )


async def test_actual_cli_requires_policies_and_publishes_analytics_only(
    instrument, ticker, tmp_path, monkeypatch
):
    database = await closed_database(tmp_path / "source.duckdb", fixture_rows(instrument, ticker))
    output = tmp_path / "result"
    args = [
        "research-options",
        "--database",
        str(database),
        "--output",
        str(output),
        "--asset",
        "BTC",
        "--as-of-ms",
        "2000",
    ]
    monkeypatch.setattr("sys.argv", ["selective-options", *args])
    with pytest.raises(SystemExit):
        main()
    assert not output.exists()
    for key, value in POLICY.items():
        args.extend(["--" + key.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", ["selective-options", *args])
    assert main() == 0
    result = json.loads((output / "options-features.json").read_text())
    assert result["options_report"]["term_shape"] == "CONTANGO"
    assert not result["trading_enabled"]
    assert not list(tmp_path.rglob("trials.jsonl"))


def test_saved_native_btc_front_expiry_exact_receipts_and_percentage_points():
    """Read one finite old public testnet cohort; never claim current feed health."""
    database = (
        Path(__file__).resolve().parents[1] / "evidence/reused-validation-public-01/events.duckdb"
    )
    expected = "97c10f3c2f59a67edff83104fceca3b0b195b1498432597abde62e509f4cabdc"
    assert digest(database) == expected
    quote, discovery, hashed = load_options_snapshots(database, asset="BTC", as_of_ms=1791050702961)
    report = compute_options_features(
        quote,
        discovery,
        **(
            POLICY
            | {
                "max_quote_age_ms": 10000,
                "max_discovery_age_ms": 100000,
                "max_atm_log_moneyness": 0.2,
                "max_underlying_mismatch_fraction": 0.1,
            }
        ),
    )
    front = report.expiries[0]
    assert front.symbol == "BTC-4OCT26-85000-C" and front.mark_iv_points == 12.28
    assert front.strike == 85000 and front.underlying_price == 84976.58
    assert (
        front.quote_event_id == "900fd8c9f5868a1707af12a5aad85b7bcc467d60ab6f28d978d7449156ffb2c5"
    )
    assert (
        front.metadata_event_id
        == "9d68dba1c62b16fc05a4b995424b074adafe05a7055291d299ec0a5ca34fca18"
    )
    assert front.quote_source_time_ms == 1791050701197
    assert front.quote_observed_at_ms == 1791050699929
    assert front.metadata_observed_at_ms == 1791050626654
    assert report.discovered_members == 1010 and len(report.expiries) == 13
    # These literal references were independently read from saved SQL/JSON, not
    # copied from the feature implementation or generated proof report.
    names = (
        "4OCT26-85000",
        "5OCT26-85000",
        "6OCT26-85000",
        "7OCT26-85000",
        "9OCT26-85000",
        "16OCT26-85000",
        "23OCT26-85000",
        "30OCT26-85000",
        "27NOV26-86000",
        "25DEC26-86000",
        "26MAR27-88000",
        "25JUN27-88000",
        "24SEP27-90000",
    )
    marks = (
        12.28,
        20.53,
        24.05,
        25.29,
        28.71,
        30.8,
        31.87,
        32.83,
        35.44,
        36.52,
        37.5,
        38.54,
        38.91,
    )
    assert tuple(expiry.symbol for expiry in report.expiries) == tuple(
        f"BTC-{name}-C" for name in names
    )
    assert tuple(expiry.mark_iv_points for expiry in report.expiries) == marks
    assert report.term_shape == "MIXED"
    put = compute_options_features(
        quote,
        discovery,
        **(
            POLICY
            | {
                "option_type": "put",
                "max_quote_age_ms": 10000,
                "max_discovery_age_ms": 100000,
                "max_atm_log_moneyness": 0.2,
                "max_underlying_mismatch_fraction": 0.1,
            }
        ),
    )
    assert tuple(expiry.symbol for expiry in put.expiries) == tuple(
        f"BTC-{name}-P" for name in names
    )
    assert tuple(expiry.mark_iv_points for expiry in put.expiries) == marks
    assert put.term_shape == "MIXED"
    assert put.adjacent_iv_differences_points == pytest.approx(
        tuple(b - a for a, b in zip(marks, marks[1:], strict=False))
    )
    assert not report.full_chain_verified and not report.current_feed_health_verified
    assert hashed == digest(database) == expected
