"""Re-fetch past days' Polymarket trades into raw, so compaction sees every trade.

Polymarket's trade history (data-api) publishes trades minutes after they happen, and the live poll
re-read only its last minute, so it missed about two thirds of trades (measured 2026-11-18; TASKS.md
3.E.1). Hours later the history is complete. This fetches every trade executed on the given UTC days
for the recorded markets and writes them to each day's own raw partition, as the day's last file:

    raw/stream=trades/date=D/hour=23/refetch-<D 23:55 window ms>-<fetch ms>.jsonl.gz

so that compacting day D times them against day D's WebSocket prints by transaction hash. Lines are
trades.FIELDS rows plus recv_ms (the fetch time, not when the trade became public). Compaction
de-duplicates on trade id, so a re-run only adds a file. Re-process the days afterwards
(python -m recorder.compact --date D) to rebuild their tables.

    python -m recorder.refetch --store s3://bucket --pairs s3://bucket/ref/pairs/current.json \\
        --date 2026-11-18 --days 3
"""
import argparse
import gzip
import json
import sys
import threading
import time
from collections import defaultdict
from datetime import date as Date, datetime, timedelta, timezone

import requests

from recorder import pairs as pairs_mod
from recorder import rest_trades
from recorder.sink import raw_path
from recorder.store import open_store
from recorder.trades import FIELDS

DAY_MS = 86_400_000
LAST_WINDOW_MS = 300_000          # the raw sink's 5-minute window


def day_start_ms(d):
    return int(datetime.fromisoformat(d).replace(tzinfo=timezone.utc).timestamp() * 1000)


def refetch_poly(store, days, conditions, session=None, now_ms=None, log=print):
    """Write the Polymarket trades executed on each UTC day in days (YYYY-MM-DD) to raw.

    conditions maps Polymarket condition id to YES token. Returns {day: trades written}."""
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "market-book-recorder/0.2"
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    since = min(day_start_ms(d) for d in days)
    rows = []
    for batch in rest_trades.chunks(sorted(conditions.items()), rest_trades.POLY_TRADE_BATCH):
        rows += rest_trades.poly_trades(session, dict(batch), since, threading.Event())
    by_day = defaultdict(list)
    for r in rows:
        by_day[datetime.fromtimestamp(r[3] / 1000, timezone.utc).date().isoformat()].append(r)
    written = {}
    for d in days:
        got = sorted(by_day.get(d, ()), key=lambda r: (r[3], r[1]))
        written[d] = len(got)
        if not got:
            log(f"{d}: no Polymarket trades")
            continue
        key = raw_path("trades", day_start_ms(d) + DAY_MS - LAST_WINDOW_MS, "refetch", now).replace("\\", "/")
        lines = "".join(json.dumps({"recv_ms": now, **dict(zip(FIELDS, r))}, separators=(",", ":")) + "\n"
                        for r in got)
        store.write(key, gzip.compress(lines.encode()))
        log(f"{d}: {len(got)} Polymarket trades re-fetched to {key}")
    return written


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store", required=True, help="local directory or s3://bucket[/prefix]")
    ap.add_argument("--pairs", required=True, help="pairs file (path or s3://bucket/key)")
    ap.add_argument("--date", required=True, help="last UTC day to re-fetch, YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=1, help="re-fetch this many days ending at --date (default 1)")
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args(argv)
    end = Date.fromisoformat(args.date)
    days = [(end - timedelta(days=i)).isoformat() for i in range(args.days - 1, -1, -1)]
    _, _, conditions = pairs_mod.markets(pairs_mod.load(args.pairs, args.region))
    refetch_poly(open_store(args.store, args.region), days, conditions, log=lambda s: print(s, flush=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
