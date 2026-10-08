"""Fixed-hash saved-native audit and forged-report refusal; no market calls."""

import pytest

from scripts import verify_options_flow as proof
from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.options_flow_features import NativeAmount, compute_put_call_ratios
from selective_options.options_research import load_options_snapshots


@pytest.fixture(scope="module")
def native():
    assert file_digest(proof.DATABASE) == proof.SOURCE_SHA256
    snapshot, controls, _ = load_options_snapshots(
        proof.DATABASE, asset="BTC", as_of_ms=proof.CUTOFF
    )
    report = compute_put_call_ratios(
        snapshot,
        controls,
        amount_unit_policy="deribit_option_base_currency",
        volume_field="stats.volume",
        **proof.POLICY,
    )
    return report, snapshot, controls


def audit(native, report=None):
    return proof.check(native[0] if report is None else report, native[1], native[2])


def test_all_saved_members_native_amounts_clocks_and_ratios_match(native):
    checked = audit(native)
    assert checked["checked_members"] == 1010
    assert len(checked["expiries"]) == 13
    assert native[0].aggregate_open_interest.value == 0.7753088307760276
    assert native[0].aggregate_volume.value == 0.2829367350165104
    assert native[0].expiries[-1].volume.reason == "CALL_DENOMINATOR_ZERO"


@pytest.mark.parametrize(
    "field,value",
    [
        ("trading_enabled", True),
        ("current_feed_health_verified", True),
        ("contract_count_ratio", True),
        ("executable_quotes_verified", True),
    ],
)
def test_unsupported_readiness_or_unit_claim_refuses(native, field, value):
    with pytest.raises(IntegrityError, match="FLOW_PROOF_UNSUPPORTED_APPROVAL"):
        audit(native, native[0].model_copy(update={field: value}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("quote_source_time_ms", proof.CUTOFF + 1),
        ("quote_observed_at_ms", 1),
        ("quote_reason", "invented-health"),
        ("metadata_observed_at_ms", 1),
        ("contract_size", 999.0),
        ("symbol", "invented"),
    ],
)
def test_changed_native_member_identity_or_clocks_refuse(native, field, value):
    report = native[0]
    expiry = report.expiries[0]
    member = expiry.members[0].model_copy(update={field: value})
    changed = expiry.model_copy(update={"members": (member, *expiry.members[1:])})
    forged = report.model_copy(update={"expiries": (changed, *report.expiries[1:])})
    with pytest.raises(IntegrityError, match="FLOW_PROOF_NATIVE_"):
        audit(native, forged)


def test_changed_native_amount_cannot_preserve_report(native):
    report = native[0]
    expiry = report.expiries[0]
    member = expiry.members[0].model_copy(
        update={"open_interest": NativeAmount(numerator="1", denominator="1")}
    )
    changed = expiry.model_copy(update={"members": (member, *expiry.members[1:])})
    with pytest.raises(IntegrityError, match="FLOW_PROOF_NATIVE_AMOUNT_OR_RATIO"):
        audit(native, report.model_copy(update={"expiries": (changed, *report.expiries[1:])}))


def test_aggregate_ratio_and_partial_status_tampering_refuse(native):
    report = native[0]
    metric = report.aggregate_open_interest.model_copy(update={"value": 1.0})
    with pytest.raises(IntegrityError, match="FLOW_PROOF_RATIO_VALUE"):
        audit(native, report.model_copy(update={"aggregate_open_interest": metric}))
    with pytest.raises(IntegrityError, match="FLOW_PROOF_PARTIAL_STATUS"):
        audit(
            native, report.model_copy(update={"state": "PUT_CALL_RATIOS_COMPUTED", "blockers": ()})
        )


def test_member_omission_and_input_digest_tampering_refuse(native):
    report = native[0]
    expiry = report.expiries[0].model_copy(update={"members": report.expiries[0].members[1:]})
    with pytest.raises(IntegrityError, match="FLOW_PROOF_MEMBER_SET"):
        audit(native, report.model_copy(update={"expiries": (expiry, *report.expiries[1:])}))
    with pytest.raises(IntegrityError, match="FLOW_PROOF_INPUT_IDENTITY"):
        audit(native, report.model_copy(update={"input_sha256": "0" * 64}))
