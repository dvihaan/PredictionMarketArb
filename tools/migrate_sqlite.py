"""One-off: convert the old recorder's SQLite database into the curated Parquet tables.

    .venv/bin/python tools/migrate_sqlite.py --db data/books.sqlite --store data/raw-local
    .venv/bin/python tools/migrate_sqlite.py --db data/books.sqlite --store s3://<bucket>

Reads the database read-only. Writes, per day:

    curated/books/venue=<v>/date=D/part-legacy.parquet   source = "rest_poll" (5 s REST polling era)
    curated/coverage/date=D/part-legacy.parquet          one "poll_cycle" event per polling cycle
    curated/trades/venue=<v>/date=D/part-0.parquet       merged with any existing trades, de-duplicated

The "part-legacy" names keep these files apart from the nightly job's part-0 files, so neither can
overwrite the other. Book rows keep the old format: yes_bids/no_bids, top 25 levels, best first.
Gaps in the old recording stay visible as gaps between poll_cycle events.
"""
import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.compact import BOOKS_SCHEMA, COVERAGE_SCHEMA, ParquetOut, levels, upsert_trades, utc_date  # noqa: E402
from recorder.store import open_store  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.path.join(ROOT, "data", "books.sqlite"))
    ap.add_argument("--store", required=True, help="local directory or s3://bucket[/prefix]")
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args(argv)

    store = open_store(args.store, args.region)
    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    # Books: one output file per venue and day, written in recording order.
    outs, counts = {}, defaultdict(int)
    for venue, market, recv, server, yes, no in db.execute(
            "SELECT venue, market, recv_ms, server_ms, yes_bids, no_bids FROM books ORDER BY rowid"):
        key = (venue, utc_date(recv))
        if key not in outs:
            outs[key] = ParquetOut(BOOKS_SCHEMA)
        outs[key].add(ts_ms=recv, server_ms=server, market=market, yes_bids=levels(json.loads(yes)),
                      no_bids=levels(json.loads(no)), source="rest_poll", suspect=False, suspect_levels=None)
        counts[key] += 1
    for (venue, d), out in sorted(outs.items()):
        store.write(f"curated/books/venue={venue}/date={d}/part-legacy.parquet", out.finish())
        print(f"books {venue} {d}: {counts[(venue, d)]} rows")

    # Coverage: each polling cycle, with what it fetched and any failures.
    cov = defaultdict(lambda: ParquetOut(COVERAGE_SCHEMA))
    n_cycles = 0
    for cid, started, finished, k_ok, p_ok, missing, errors in db.execute(
            "SELECT id, started_ms, finished_ms, kalshi_ok, poly_ok, missing, errors FROM cycles ORDER BY id"):
        detail = {"cycle_id": cid, "finished_ms": finished, "kalshi_ok": k_ok, "poly_ok": p_ok}
        if missing:
            detail["missing"] = json.loads(missing)
        if errors:
            detail["errors"] = json.loads(errors)
        cov[utc_date(started)].add(recv_ms=started, venue="legacy", event="poll_cycle",
                                   detail=json.dumps(detail, separators=(",", ":")))
        n_cycles += 1
    for d, out in sorted(cov.items()):
        store.write(f"curated/coverage/date={d}/part-legacy.parquet", out.finish())
    print(f"coverage: {n_cycles} poll cycles over {len(cov)} days")

    # Trades: same normalized format as the new recorder's REST backfill.
    trades = defaultdict(list)
    has_trades = db.execute("SELECT 1 FROM sqlite_master WHERE name = 'trades'").fetchone()
    if has_trades:
        for venue, tid, market, ts, price, size, taker in db.execute(
                "SELECT venue, trade_id, market, ts_ms, yes_price, size, taker FROM trades"):
            trades[venue].append(dict(trade_id=tid, market=market, ts_ms=ts, yes_price=price, size=size,
                                      taker=taker, source="rest"))
    for venue, rows in trades.items():
        print(f"trades {venue}: {len(rows)} migrated, {upsert_trades(store, venue, rows)} in their partitions")
    db.close()


if __name__ == "__main__":
    main()
