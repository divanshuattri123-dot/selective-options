"""Serialized asynchronous RPC. No trading or arbitrary private RPC methods exposed."""

import asyncio
import json
from types import TracebackType
from typing import Protocol, Self

from pydantic import JsonValue, ValidationError
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .config import VenueConfig
from .errors import IntegrityError, RpcError, TradingUnavailable, TransportError
from .models import RpcEnvelope
from .rate_limit import EndpointLimiter

ALLOWED_METHODS = frozenset(
    {
        "public/get_time",
        "public/test",
        "public/get_instruments",
        "public/get_book_summary_by_currency",
        "public/ticker",
        "public/get_volatility_index_data",
        "public/auth",
        "private/list_api_keys",
        "private/get_cancel_on_disconnect",
    }
)


class RpcClient(Protocol):
    async def call(self, method: str, params: dict[str, JsonValue]) -> JsonValue: ...


class WebSocketRpc:
    def __init__(self, config: VenueConfig, limiter: EndpointLimiter) -> None:
        self.config, self._limiter = config, limiter
        self._socket: ClientConnection | None = None
        self._lock = asyncio.Lock()
        self._next_id = 0

    async def __aenter__(self) -> Self:
        if self.config.ws_url != "wss://test.deribit.com/ws/api/v2":
            raise TradingUnavailable("milestone 1 only connects to testnet")
        try:
            self._socket = await connect(
                self.config.ws_url,
                open_timeout=self.config.connect_timeout_s,
                close_timeout=2,
                max_size=self.config.max_message_bytes,
                ping_interval=20,
                ping_timeout=20,
            )
        except (OSError, TimeoutError, WebSocketException) as exc:
            raise TransportError("WebSocket handshake failed") from exc
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        socket, self._socket = self._socket, None
        if socket is not None:
            await socket.close()

    async def call(self, method: str, params: dict[str, JsonValue]) -> JsonValue:
        if method not in ALLOWED_METHODS:
            raise TradingUnavailable("RPC method unavailable in milestone 1")
        await self._limiter.acquire(method)
        async with self._lock:
            if self._socket is None:
                raise TransportError("socket is not connected; resync required")
            self._next_id += 1
            request_id = self._next_id
            try:
                async with asyncio.timeout(self.config.request_timeout_s):
                    await self._socket.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "method": method,
                                "params": params,
                            },
                            allow_nan=False,
                        )
                    )
                    raw = await self._socket.recv()
                    response = RpcEnvelope.model_validate_json(raw)
                    if response.id != request_id:
                        raise IntegrityError("RPC response id mismatch")
                    if response.error is not None:
                        data = response.error.data
                        parameter = data.get("param") if isinstance(data, dict) else None
                        raise RpcError(
                            method,
                            response.error.code,
                            parameter if isinstance(parameter, str) else None,
                        )
                    return response.result
            except RpcError as exc:
                if exc.code == 10028:
                    await self.close()
                raise
            except asyncio.CancelledError:
                await self.close()
                raise
            except (OSError, TimeoutError, WebSocketException) as exc:
                await self.close()
                raise TransportError("RPC transport failed; socket invalidated") from exc
            except (ValidationError, IntegrityError) as exc:
                await self.close()
                raise IntegrityError("malformed or mismatched RPC response") from exc
