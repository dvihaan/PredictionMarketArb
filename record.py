"""Record public order books for every pair in watchlist.json. Read-only: no keys, no orders.

Each polling cycle is logged, including failures, so analysis can tell "no change"
apart from "no data". A book row is written only when that book changed.

    python record.py                 # run until Ctrl+C
    python record.py --duration 120  # run for two minutes
"""
import argparse
import json
import os
import signal
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
KALSHI_BOOKS = "https://api.elections.kalshi.com/trade-api/v2/markets/orderbooks"
POLY_BOOKS = "https://clob.polymarket.com/books"
BATCH = 50
MAX_LEVELS = 25
TIMEOUT = 15

SCHEMA = """
CREATE TABLE IF NOT EXISTS cycles(
  id INTEGER PRIMARY KEY, started_ms INTEGER, finished_ms INTEGER,
  kalshi_ok INTEGER, poly_ok INTEGER, missing TEXT, errors TEXT);
CREATE TABLE IF NOT EXISTS books(
  cycle_id INTEGER, venue TEXT, market TEXT, recv_ms INTEGER, server_ms INTEGER,
  yes_bids TEXT, no_bids TEXT);
CREATE INDEX IF NOT EXISTS books_cycle ON books(cycle_id);
"""


def now_ms():
    return int(time.time() * 1000)


def levels(pairs, reverse=True):
    """Convert to floats, drop empty levels, sort best first and keep the top of the book."""
    out = [(round(float(p), 5), round(float(s), 4)) for p, s in pairs if float(s) > 0]
    out.sort(key=lambda z: z[0], reverse=reverse)
    return out[:MAX_LEVELS]


def request_json(session, method, url, **kw):
    """One retry after a rate limit or server error; a slow cycle should stay visible."""
    for attempt in range(2):
        r = session.request(method, url, timeout=TIMEOUT, **kw)
        if r.status_code in (429, 500, 502, 503, 504) and attempt == 0:
            time.sleep(1.0)
            continue
        r.raise_for_status()
        return r.json()


def chunks(items, n):
    return [items[i:i + n] for i in range(0, len(items), n)]


def fetch_kalshi(session, tickers):
    got, errors = {}, []
    for batch in chunks(tickers, BATCH):
        try:
            data = request_json(session, "GET", KALSHI_BOOKS, params=[("tickers", t) for t in batch])
        except Exception as e:  # keep recording the other batches and log the failure
            errors.append(f"kalshi: {type(e).__name__}: {e}"[:300])
            continue
        recv = now_ms()
        for row in data.get("orderbooks", []):
            b = row.get("orderbook_fp") or {}
            got[row["ticker"]] = (recv, None, levels(b.get("yes_dollars") or []), levels(b.get("no_dollars") or []))
    return got, errors


def fetch_poly(session, tokens):
    got, errors = {}, []
    for batch in chunks(tokens, BATCH):
        try:
            data = request_json(session, "POST", POLY_BOOKS, json=[{"token_id": t} for t in batch])
        except Exception as e:
            errors.append(f"poly: {type(e).__name__}: {e}"[:300])
            continue
        recv = now_ms()
        for b in data:
            yes_bids = levels([(x["price"], x["size"]) for x in b.get("bids") or []])
            # A YES ask at p is the same order as a NO bid at 1 - p.
            no_bids = levels([(round(1 - float(x["price"]), 5), x["size"]) for x in b.get("asks") or []])
            server = int(b["timestamp"]) if b.get("timestamp") else None
            got[b["asset_id"]] = (recv, server, yes_bids, no_bids)
    return got, errors


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watchlist", default=os.path.join(HERE, "watchlist.json"))
    ap.add_argument("--db", default=os.path.join(HERE, "data", "books.sqlite"))
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between polling cycles (default 5)")
    ap.add_argument("--duration", type=float, default=0, help="stop after this many seconds (default: run until Ctrl+C)")
    args = ap.parse_args()

    pairs = json.load(open(args.watchlist, encoding="utf-8"))["pairs"]
    tickers = sorted({p["kalshi_ticker"] for p in pairs})
    tokens = sorted({p["poly_yes_token"] for p in pairs})
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    db = sqlite3.connect(args.db)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(SCHEMA)

    # Resume change detection from what is already stored, so a restart adds no duplicates.
    last = {}
    latest = ("SELECT venue, market, yes_bids, no_bids FROM books WHERE rowid IN "
              "(SELECT MAX(rowid) FROM books GROUP BY venue, market)")
    for venue, market, yes, no in db.execute(latest):
        last[(venue, market)] = (yes, no)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    k_session, p_session = requests.Session(), requests.Session()
    for s in (k_session, p_session):
        s.headers["User-Agent"] = "market-book-recorder/0.1"

    print(f"Recording {len(pairs)} pairs every {args.interval:g}s to {args.db}. Press Ctrl+C to stop.", flush=True)
    t_start = time.monotonic()
    next_at = t_start
    n_cycles = n_rows = 0
    with ThreadPoolExecutor(2) as pool:
        while not stop.is_set():
            started = now_ms()
            fk = pool.submit(fetch_kalshi, k_session, tickers)
            fp = pool.submit(fetch_poly, p_session, tokens)
            kbooks, kerr = fk.result()
            pbooks, perr = fp.result()
            finished = now_ms()

            missing = [["kalshi", t] for t in tickers if t not in kbooks]
            missing += [["poly", t] for t in tokens if t not in pbooks]
            errors = kerr + perr
            cur = db.execute(
                "INSERT INTO cycles(started_ms, finished_ms, kalshi_ok, poly_ok, missing, errors) VALUES(?,?,?,?,?,?)",
                (started, finished, len(kbooks), len(pbooks),
                 json.dumps(missing) if missing else None, json.dumps(errors) if errors else None))
            cycle_id = cur.lastrowid
            rows = []
            for venue, books in (("kalshi", kbooks), ("poly", pbooks)):
                for market, (recv, server, yes, no) in books.items():
                    y, n = json.dumps(yes, separators=(",", ":")), json.dumps(no, separators=(",", ":"))
                    if last.get((venue, market)) != (y, n):
                        last[(venue, market)] = (y, n)
                        rows.append((cycle_id, venue, market, recv, server, y, n))
            db.executemany("INSERT INTO books VALUES(?,?,?,?,?,?,?)", rows)
            db.commit()
            n_cycles += 1
            n_rows += len(rows)

            if errors:
                print(f"cycle {cycle_id}: {errors[0]}", flush=True)
            if n_cycles % 12 == 1:
                print(f"cycles={n_cycles} changed_books={n_rows} kalshi={len(kbooks)}/{len(tickers)} "
                      f"poly={len(pbooks)}/{len(tokens)} last_cycle_ms={finished - started}", flush=True)

            if args.duration and time.monotonic() - t_start >= args.duration:
                break
            next_at += args.interval
            wait = next_at - time.monotonic()
            if wait > 0:
                stop.wait(wait)
            else:
                next_at = time.monotonic()
    db.close()
    print(f"Stopped after {n_cycles} cycles and {n_rows} changed books.", flush=True)


if __name__ == "__main__":
    main()
