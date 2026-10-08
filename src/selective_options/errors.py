class TradingUnavailable(RuntimeError):
    """No execution path is available in this milestone."""


class SecurityError(RuntimeError):
    """A credential, environment or safety property could not be verified."""


class IntegrityError(RuntimeError):
    """A required data integrity check failed."""


class TradeGap(IntegrityError):
    def __init__(self, symbol: str, expected_id: int, received_id: int) -> None:
        self.symbol, self.expected_id, self.received_id = symbol, expected_id, received_id
        super().__init__("SPOT_TRADE_SEQUENCE_GAP")


class TransportError(RuntimeError):
    """Request outcome is unknown; reconnect and resync before further use."""


class RpcError(RuntimeError):
    def __init__(self, method: str, code: int, parameter: str | None = None) -> None:
        # Server messages may echo credentials. Never retain or log them.
        self.code = code
        self.parameter = parameter
        super().__init__(f"RPC rejected {method}, code={code}")
