from .errors import TradingUnavailable


class LiveEngine:
    def arm(self) -> None:
        raise TradingUnavailable(
            "MILESTONE_INCOMPLETE: execution, risk, validation and paper evidence unavailable; live trading disabled"
        )
