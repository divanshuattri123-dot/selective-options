"""Structured event logging with secret redaction and no protocol-body logging."""

import json
import logging
import re
from datetime import UTC, datetime

_secrets: set[str] = set()
_sensitive = re.compile(
    r"(?i)(client_secret|access_token|refresh_token|authorization|api_key)([\s\"':=]+)([^\s,}\"]+)"
)


def register_secret(value: str) -> None:
    if value:
        _secrets.add(value)


def redact(value: str) -> str:
    for secret in sorted(_secrets, key=len, reverse=True):
        value = value.replace(secret, "[REDACTED]")
    return _sensitive.sub(r"\1\2[REDACTED]", value)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "event": redact(record.getMessage()),
                "signal_id": getattr(record, "signal_id", None),
                "error_type": record.exc_info[0].__name__
                if record.exc_info and record.exc_info[0]
                else None,
            }
        )


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.getLogger().handlers = [handler]
    logging.getLogger().setLevel(level)
    # Frame logging can contain client secrets and tokens, even during errors.
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
