import subprocess
import sys
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / "scripts" / "check_secrets.py"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("DERIBIT_TESTNET_CLIENT_ID=\nDERIBIT_TESTNET_CLIENT_SECRET=\n", 0),
        ('{"client_secret": "' + "synthetic" * 4 + '"}', 1),
        ("access_token=" + "synthetic" * 4, 1),
        ("-----BEGIN " + "PRIVATE KEY-----", 1),
        ('{"client_secret": self._credentials.client_secret.get_secret_value()}', 0),
    ],
    ids=["empty-values", "quoted-json", "plain-env", "private-key", "code-expression"],
)
def test_secret_hook_detects_values_without_echoing_them(tmp_path, text, expected):
    sample = tmp_path / "sample.txt"
    sample.write_text(text, encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(HOOK), str(sample)], capture_output=True, text=True
    )
    assert result.returncode == expected
    assert "synthetic" * 4 not in result.stderr
