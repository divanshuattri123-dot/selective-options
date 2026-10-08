"""Reject common secret assignments without printing their values."""

import re
import sys
from pathlib import Path

PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(
        r"""(?i)(?:DERIBIT_\w+_CLIENT_(?:ID|SECRET)|client_secret|access_token|refresh_token)["']?[ \t]*[=:][ \t]*(["'])([a-zA-Z0-9_./+=-]{16,})\1"""
    ),
    re.compile(
        r"""(?im)^[ \t]*(?:DERIBIT_\w+_CLIENT_(?:ID|SECRET)|client_secret|access_token|refresh_token)[ \t]*=[ \t]*[a-zA-Z0-9_./+=-]{16,}[ \t]*$"""
    ),
)


def main() -> int:
    failures = []
    for filename in sys.argv[1:]:
        path = Path(filename)
        if path.name == "check_secrets.py":
            continue
        content = path.read_text(encoding="utf-8", errors="replace")
        if any(pattern.search(content) for pattern in PATTERNS):
            failures.append(str(path))
    if failures:
        print("Potential secrets found (values omitted): " + ", ".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
