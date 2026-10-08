import os
from typing import Literal

from pydantic import BaseModel, ConfigDict, SecretStr

from .errors import SecurityError
from .logging import register_secret


class Credentials(BaseModel):
    model_config = ConfigDict(frozen=True)
    profile: Literal["testnet", "production"]
    client_id: SecretStr
    client_secret: SecretStr


def load_credentials(profile: str) -> Credentials:
    if profile not in {"testnet", "production"}:
        raise SecurityError("unknown key profile")
    prefix = f"DERIBIT_{profile.upper()}_"
    client_id, secret = os.getenv(prefix + "CLIENT_ID"), os.getenv(prefix + "CLIENT_SECRET")
    if not client_id or not secret:
        raise SecurityError(f"set {prefix}CLIENT_ID and {prefix}CLIENT_SECRET locally")
    register_secret(client_id)
    register_secret(secret)
    return Credentials.model_validate(
        {"profile": profile, "client_id": client_id, "client_secret": secret}
    )


PERMISSION_AREAS = frozenset(
    {
        "account",
        "wallet",
        "trade",
        "block_trade",
        "block_rfq",
        "custody",
        "sbe_order_entry",
        "fix_drop_copy",
        "sbe_market_data",
        "rest_order_entry",
    }
)


def _parse_scope(scope: str) -> dict[str, str]:
    permissions: dict[str, str] = {}
    for token in scope.split():
        if ":" not in token:
            # Deribit adds mainaccount as an identity marker, not a write permission.
            if token not in {"connection", "mainaccount"}:
                raise SecurityError("unrecognised or privileged key scope")
            continue
        name, value = token.split(":", 1)
        if name in permissions:
            raise SecurityError("duplicate key scope")
        permissions[name] = value
        if name in {"expires", "ip", "session"}:
            continue
        if name not in PERMISSION_AREAS:
            raise SecurityError("unrecognised key permission")
        if value not in {"none", "read", "read_write"}:
            raise SecurityError("unrecognised permission level")
    return permissions


def verify_key_scope(scope: str) -> None:
    permissions = _parse_scope(scope)
    if permissions.get("wallet") == "read_write":
        raise SecurityError("wallet:read_write enables withdrawal access and is prohibited")
    for name, value in permissions.items():
        if name in PERMISSION_AREAS and name != "trade" and value == "read_write":
            raise SecurityError(f"{name}:read_write is a prohibited non-trading write permission")
    if permissions.get("wallet") not in {"none", "read"}:
        raise SecurityError("cannot establish absence of withdrawal permission")
    if permissions.get("trade") != "read_write" or permissions.get("account") != "read":
        raise SecurityError("trade:read_write and account:read required for readiness checks")


def verify_token_scope(scope: str, verified_max_scope: str) -> None:
    """A token cannot exceed a verified safe key, even when denied areas are omitted."""
    verify_key_scope(verified_max_scope)
    maximum = _parse_scope(verified_max_scope)
    token = _parse_scope(scope)
    # The actual server omits wallet:none from the returned token. Only a verified
    # key maximum can establish the upper bound; never infer safety from omission alone.
    scope_to_check = scope
    if "wallet" not in token:
        scope_to_check += " wallet:" + maximum["wallet"]
    verify_key_scope(scope_to_check)
    levels = {"none": 0, "read": 1, "read_write": 2}
    for name, value in token.items():
        if name in PERMISSION_AREAS and levels[value] > levels[maximum.get(name, "none")]:
            raise SecurityError(f"token {name} permission exceeds verified key maximum")
