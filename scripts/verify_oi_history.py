"""Fetch and verify a bounded public open-interest archive; no orders or credentials."""

import argparse
import asyncio
from pathlib import Path

from verify_funding_history import verify

from selective_options.oi_history import INTERVALS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--asset", choices=("BTC", "ETH", "SOL", "PAXG"), default="BTC")
    parser.add_argument("--hours", type=int, default=1)
    parser.add_argument("--interval", choices=tuple(INTERVALS), default="5min")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    args = parser.parse_args()
    asyncio.run(
        verify(
            args.output,
            args.asset,
            args.hours,
            args.page_size,
            args.max_pages,
            kind="open_interest",
            interval=args.interval,
        )
    )
