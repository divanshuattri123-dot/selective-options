import json
import logging

import pytest
from pydantic import SecretStr

from selective_options.auth import AuthSession
from selective_options.errors import SecurityError
from selective_options.logging import JsonFormatter, configure_logging, redact, register_secret
from selective_options.security import (
    Credentials,
    load_credentials,
    verify_key_scope,
    verify_token_scope,
)

SAFE = "account:read trade:read_write wallet:none"


def test_separate_credentials(monkeypatch):
    monkeypatch.setenv("DERIBIT_PRODUCTION_CLIENT_ID", "production-id")
    monkeypatch.setenv("DERIBIT_PRODUCTION_CLIENT_SECRET", "production-secret")
    with pytest.raises(SecurityError, match="DERIBIT_TESTNET"):
        load_credentials("testnet")
    assert load_credentials("production").profile == "production"
    with pytest.raises(SecurityError):
        load_credentials("bogus")


@pytest.mark.parametrize(
    "scope",
    [
        "account:read trade:read_write wallet:read_write",
        "account:read_write trade:read_write wallet:none",
        "account:read trade:read_write",
        "account:read trade:read wallet:none",
        SAFE + " unknown_identity_marker",
        SAFE + " withdrawal:enabled",
        SAFE + " wallet:read",
        SAFE + " custody:admin",
        SAFE + " custody:read_write",
    ],
)
def test_unsafe_or_unknown_scope_rejected(scope):
    with pytest.raises(SecurityError):
        verify_key_scope(scope)


def test_known_safe_scope_and_metadata():
    verify_key_scope(
        "connection mainaccount " + SAFE + " expires:300 ip:127.0.0.1 session:probe custody:read"
    )


def test_identity_markers_do_not_prove_functional_permissions():
    with pytest.raises(SecurityError, match="absence of withdrawal permission"):
        verify_key_scope("connection mainaccount")


def test_denied_venue_permission_areas_are_recognised():
    verify_key_scope(
        SAFE
        + " block_rfq:none sbe_order_entry:none fix_drop_copy:none sbe_market_data:none rest_order_entry:none"
    )


def test_withdrawal_permission_is_reported_before_other_write_permissions():
    scope = "block_rfq:read_write block_trade:read_write custody:read_write account:read_write trade:read_write wallet:read_write"
    with pytest.raises(SecurityError, match="withdrawal access"):
        verify_key_scope(scope)


def test_token_may_omit_wallet_none_only_with_verified_safe_maximum():
    verify_token_scope("connection mainaccount account:read trade:read_write", SAFE)


@pytest.mark.parametrize(
    "scope,maximum",
    [
        (
            "connection mainaccount account:read trade:read_write",
            "account:read trade:read_write wallet:read_write",
        ),
        (SAFE + " custody:read", SAFE),
        ("connection mainaccount", SAFE),
        (SAFE + " block_rfq:read_write", SAFE + " block_rfq:none"),
        ("account:read trade:read_write wallet:read", SAFE),
    ],
)
def test_missing_denied_token_scope_cannot_hide_elevated_or_unproven_access(scope, maximum):
    with pytest.raises(SecurityError):
        verify_token_scope(scope, maximum)


def test_redaction_including_exception_payload():
    register_secret("test-sensitive-value")
    assert "test-sensitive-value" not in redact("prefix test-sensitive-value")
    assert "abc123" not in redact("client_secret=abc123")
    record = logging.LogRecord(
        "test",
        logging.ERROR,
        "",
        1,
        "test-sensitive-value",
        (),
        (ValueError, ValueError("private"), None),
    )
    record.signal_id = "signal-test"
    output = json.loads(JsonFormatter().format(record))
    assert output["event"] == "[REDACTED]"
    assert output["error_type"] == "ValueError"
    assert output["signal_id"] == "signal-test"
    configure_logging()


def credentials(profile="testnet"):
    return Credentials(
        profile=profile,
        client_id=SecretStr("fixture-id"),
        client_secret=SecretStr("fixture-secret"),
    )


def token(scope=SAFE):
    return {
        "access_token": "fixture-access",
        "refresh_token": "fixture-refresh",
        "scope": scope,
        "expires_in": 100,
        "token_type": "bearer",
    }


def key(scope=SAFE, enabled=True):
    return [{"client_id": "fixture-id", "max_scope": scope, "enabled": enabled}]


async def test_auth_refresh_rechecks_actual_key(fake_rpc):
    now = [0.0]
    rpc = fake_rpc(
        [
            token(),
            key(),
            {"enabled": True, "scope": "connection"},
            token(),
            key(),
            {"enabled": True, "scope": "connection"},
        ]
    )
    auth = AuthSession(rpc, credentials(), 60, clock=lambda: now[0])
    with pytest.raises(SecurityError):
        await auth.ensure_fresh()
    await auth.authenticate()
    await auth.ensure_fresh()
    assert len(rpc.calls) == 3 and auth.ready
    now[0] = 51
    await auth.ensure_fresh()
    assert rpc.calls[3][1]["grant_type"] == "refresh_token"
    assert auth.ready


async def test_auth_accepts_omitted_wallet_only_after_metadata_verification(fake_rpc):
    rpc = fake_rpc(
        [
            token("connection mainaccount account:read trade:read_write"),
            key(SAFE + " block_rfq:none sbe_market_data:none"),
            {"enabled": True, "scope": "connection"},
        ]
    )
    auth = AuthSession(rpc, credentials(), 10)
    await auth.authenticate()
    assert auth.ready


async def test_auth_never_becomes_ready_without_key_metadata(fake_rpc):
    auth = AuthSession(
        fake_rpc(
            [
                token("connection mainaccount account:read trade:read_write"),
                SecurityError("key metadata unavailable"),
            ]
        ),
        credentials(),
        10,
    )
    with pytest.raises(SecurityError, match="metadata unavailable"):
        await auth.authenticate()
    assert not auth.ready


@pytest.mark.parametrize(
    "keys,cod",
    [
        (
            key("account:read trade:read_write wallet:read_write"),
            {"enabled": True, "scope": "connection"},
        ),
        (key(enabled=False), {"enabled": True, "scope": "connection"}),
        ([], {"enabled": True, "scope": "connection"}),
        (key(), {"enabled": False, "scope": "connection"}),
        (key(), {"enabled": True, "scope": "account"}),
    ],
)
async def test_reduced_token_cannot_hide_unsafe_key_or_cod(fake_rpc, keys, cod):
    auth = AuthSession(fake_rpc([token(), keys, cod]), credentials(), 10)
    with pytest.raises(SecurityError):
        await auth.authenticate()
    assert not auth.ready


async def test_refresh_failure_invalidates_readiness(fake_rpc):
    now = [0]
    auth = AuthSession(
        fake_rpc(
            [
                token(),
                key(),
                {"enabled": True, "scope": "connection"},
                SecurityError("refresh denied"),
            ]
        ),
        credentials(),
        10,
        clock=lambda: now[0],
    )
    await auth.authenticate()
    now[0] = 99
    with pytest.raises(SecurityError):
        await auth.ensure_fresh()
    assert not auth.ready


def test_production_credentials_refused(fake_rpc):
    with pytest.raises(SecurityError):
        AuthSession(fake_rpc([]), credentials("production"), 10)
