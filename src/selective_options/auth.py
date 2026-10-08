import asyncio
import time
from collections.abc import Callable

from pydantic import TypeAdapter

from .errors import SecurityError
from .logging import register_secret
from .models import ApiKeyMetadata, AuthToken, CancelOnDisconnect
from .rpc import RpcClient
from .security import Credentials, verify_key_scope, verify_token_scope


class AuthSession:
    """Bound to one connection; recreate after reconnect. Failed checks clear readiness."""

    def __init__(
        self,
        rpc: RpcClient,
        credentials: Credentials,
        refresh_margin_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if credentials.profile != "testnet":
            raise SecurityError("only testnet credentials supported in milestone 1")
        self._rpc, self._credentials = rpc, credentials
        self._margin, self._clock = refresh_margin_s, clock
        self._token: AuthToken | None = None
        self._refresh_at = 0.0
        self._lock = asyncio.Lock()
        self.ready = False

    async def authenticate(self) -> None:
        async with self._lock:
            self.ready = False
            raw = await self._rpc.call(
                "public/auth",
                {
                    "grant_type": "client_credentials",
                    "client_id": self._credentials.client_id.get_secret_value(),
                    "client_secret": self._credentials.client_secret.get_secret_value(),
                    "scope": "connection account:read trade:read_write wallet:none",
                },
            )
            await self._accept_token(raw)

    async def _accept_token(self, raw: object) -> None:
        token = AuthToken.model_validate(raw)
        register_secret(token.access_token.get_secret_value())
        register_secret(token.refresh_token.get_secret_value())
        # Token scope can be narrowed by the server. Inspect the actual key's maximum.
        keys = TypeAdapter(list[ApiKeyMetadata]).validate_python(
            await self._rpc.call("private/list_api_keys", {})
        )
        matching = [key for key in keys if key.client_id == self._credentials.client_id]
        if len(matching) != 1 or not matching[0].enabled:
            raise SecurityError("cannot establish enabled key maximum permissions")
        verify_key_scope(matching[0].max_scope)
        verify_token_scope(token.scope, matching[0].max_scope)
        # Enabling COD requires account:read_write per current docs, conflicting with
        # least-privilege keys. Require an operator-enabled account default instead.
        cod = CancelOnDisconnect.model_validate(
            await self._rpc.call("private/get_cancel_on_disconnect", {"scope": "connection"})
        )
        if not cod.enabled or cod.scope != "connection":
            raise SecurityError(
                "connection cancel-on-disconnect not enabled; configure account default before reconnecting"
            )
        self._token = token
        self._refresh_at = (
            self._clock() + token.expires_in - min(self._margin, token.expires_in / 2)
        )
        self.ready = True

    async def ensure_fresh(self) -> None:
        async with self._lock:
            if not self.ready or self._token is None:
                raise SecurityError("authentication readiness is not established")
            if self._clock() < self._refresh_at:
                return
            self.ready = False
            raw = await self._rpc.call(
                "public/auth",
                {
                    "grant_type": "refresh_token",
                    "refresh_token": self._token.refresh_token.get_secret_value(),
                },
            )
            await self._accept_token(raw)
