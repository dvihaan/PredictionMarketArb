"""Record public order books for every pair in watchlist.json. Read-only: no keys, no orders.

Each polling cycle is logged, including failures, so analysis can tell "no change"
apart from "no data". A book row is written only when that book changed.

Public trades are recorded too, on a slower background poll, so analyze_maker.py can
estimate how often a resting order would have been filled. Trades carry their own
timestamps, so a restart backfills whatever was missed while the recorder was down.

    python record.py                 # run until Ctrl+C
    python record.py --duration 120  # run for two minutes
    python record.py --trade-interval 0   # books only
"""
import argparse
import calendar
import hashlib
import json
import os
import queue
import signal
import sqlite3
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
KALSHI_BOOKS = "https://api.elections.kalshi.com/trade-api/v2/markets/orderbooks"
POLY_BOOKS = "https://clob.polymarket.com/books"
KALSHI_TRADES = "https://api.elections.kalshi.com/trade-api/v2/markets/trades"
POLY_TRADES = "https://data-api.polymarket.com/v2/trades"
POLY_TRADE_BATCH = 20      # the API accepts at most 20 condition ids per request
TRADE_PAGE = 1000
TRADE_MAX_PAGES = 200
TRADE_LAG_MS = 60_000      # trades can be published late, so each poll re-reads the last minute
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
CREATE TABLE IF NOT EXISTS trades(
  venue TEXT, trade_id TEXT, market TEXT, ts_ms INTEGER, yes_price REAL, size REAL, taker TEXT,
  PRIMARY KEY(venue, trade_id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS trades_ts ON trades(ts_ms);
CREATE TABLE IF NOT EXISTS trade_marks(
  venue TEXT, market TEXT, covered_ms INTEGER, PRIMARY KEY(venue, market)) WITHOUT ROWID;
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


def iso_ms(text):
    """UTC timestamp like 2026-11-10T17:03:16.710012Z to epoch ms, with any number of decimals."""
    whole, _, frac = text.rstrip("Z").partition(".")
    seconds = calendar.timegm(datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").timetuple())
    return seconds * 1000 + int((frac + "000")[:3])


def kalshi_trades(session, ticker, since_ms, stop):
    """Every trade on one market since since_ms. taker is the side the taker took in YES terms."""
    rows, cursor = [], None
    for _ in range(TRADE_MAX_PAGES):
        params = {"ticker": ticker, "limit": TRADE_PAGE, "min_ts": since_ms // 1000}
        if cursor:
            params["cursor"] = cursor
        data = request_json(session, "GET", KALSHI_TRADES, params=params)
        for t in data.get("trades") or []:
            ts = iso_ms(t["created_time"])
            book_side = t.get("taker_book_side") or {"yes": "bid", "no": "ask"}[t["taker_side"]]
            rows.append(("kalshi", t["trade_id"], ticker, ts, float(t["yes_price_dollars"]), float(t["count_fp"]),
                         "buy_yes" if book_side == "bid" else "sell_yes"))
        cursor = data.get("cursor")
        if not cursor or stop.is_set():
            return rows
    raise RuntimeError(f"more than {TRADE_MAX_PAGES} pages of trades for {ticker}")


def poly_trades(session, by_condition, since_ms, stop):
    """Every trade on a batch of markets since since_ms. by_condition maps condition id to YES token.

    The API returns newest first with no time filter, so page back until since_ms is passed.
    A trade in the NO token is the mirror trade in YES: buying NO at p is selling YES at 1 - p.
    """
    rows, cursor = [], None
    for _ in range(TRADE_MAX_PAGES):
        params = {"condition": ",".join(by_condition), "limit": TRADE_PAGE}
        if cursor:
            params["cursor"] = cursor
        data = request_json(session, "GET", POLY_TRADES, params=params)
        page = data.get("data") or []
        for t in page:
            ts = int(t["timestamp"]) * 1000
            if ts < since_ms:
                continue
            yes_token = by_condition[t["condition_id"]]
            is_yes = t["token_id"] == yes_token
            price = float(t["price"])
            buys_yes = (t["side"] == "BUY") == is_yes
            # No trade id is published, so build one. Wallet and size keep fills in one transaction apart.
            raw = "|".join(str(t[k]) for k in ("transaction_hash", "token_id", "side", "price", "size", "proxy_wallet"))
            rows.append(("poly", hashlib.sha1(raw.encode()).hexdigest()[:24], yes_token, ts,
                         round(price if is_yes else 1 - price, 6), float(t["size"]),
                         "buy_yes" if buys_yes else "sell_yes"))
        pg = data.get("pagination") or {}
        cursor = pg.get("next_cursor")
        reached = page and int(page[-1]["timestamp"]) * 1000 < since_ms
        if reached or not page or not pg.get("has_more") or not cursor or stop.is_set():
            return rows
    raise RuntimeError(f"more than {TRADE_MAX_PAGES} pages of Polymarket trades")


def poll_trades(pairs, marks, default_ms, interval, out, stop):
    """Background loop: fetch new trades and hand them to the main thread, which owns the database.

    marks[(venue, market)] is the time up to which every trade is already stored. It only
    advances after a successful fetch, so a failed poll is simply retried from the same point.
    """
    k_session, p_session = requests.Session(), requests.Session()
    for s in (k_session, p_session):
        s.headers["User-Agent"] = "market-book-recorder/0.1"
    tickers = sorted({p["kalshi_ticker"] for p in pairs})
    conditions = sorted({(p["poly_condition_id"], p["poly_yes_token"]) for p in pairs if p.get("poly_condition_id")})
    while not stop.is_set():
        rows, new_marks, errors = [], {}, []
        for ticker in tickers:
            if stop.is_set():
                break
            begun = now_ms()
            try:
                rows += kalshi_trades(k_session, ticker, marks.get(("kalshi", ticker), default_ms), stop)
                new_marks[("kalshi", ticker)] = begun - TRADE_LAG_MS
            except Exception as e:
                errors.append(f"kalshi trades {ticker}: {type(e).__name__}: {e}"[:300])
            stop.wait(0.15)  # stay well under the public rate limit
        for batch in chunks(conditions, POLY_TRADE_BATCH):
            if stop.is_set():
                break
            begun = now_ms()
            since = min(marks.get(("poly", tok), default_ms) for _, tok in batch)
            try:
                rows += poly_trades(p_session, dict(batch), since, stop)
                for _, tok in batch:
                    new_marks[("poly", tok)] = begun - TRADE_LAG_MS
            except Exception as e:
                errors.append(f"poly trades: {type(e).__name__}: {e}"[:300])
        if not stop.is_set():
            # An interrupted fetch may be incomplete, so its marks are dropped with it.
            for key, ms in new_marks.items():
                marks[key] = max(ms, marks.get(key, 0))
            out.put((rows, dict(marks), errors))
        stop.wait(interval)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watchlist", default=os.path.join(HERE, "watchlist.json"))
    ap.add_argument("--db", default=os.path.join(HERE, "data", "books.sqlite"))
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between polling cycles (default 5)")
    ap.add_argument("--duration", type=float, default=0, help="stop after this many seconds (default: run until Ctrl+C)")
    ap.add_argument("--trade-interval", type=float, default=60.0,
                    help="seconds between trade polls (default 60, 0 to record books only)")
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

    # Trades are fetched from where the last run stopped, or from the first recorded book.
    trade_queue = queue.Queue()
    n_trades = 0
    if args.trade_interval > 0:
        marks = {(v, m): ms for v, m, ms in db.execute("SELECT venue, market, covered_ms FROM trade_marks")}
        first_cycle = db.execute("SELECT MIN(started_ms) FROM cycles").fetchone()[0]
        skipped = sum(1 for p in pairs if not p.get("poly_condition_id"))
        if skipped:
            print(f"{skipped} pairs have no poly_condition_id, so their Polymarket trades are not recorded.", flush=True)
        threading.Thread(target=poll_trades, daemon=True,
                         args=(pairs, marks, first_cycle or now_ms(), args.trade_interval, trade_queue, stop)).start()

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
            while not trade_queue.empty():
                t_rows, t_marks, t_errors = trade_queue.get()
                before = db.total_changes
                db.executemany("INSERT OR IGNORE INTO trades VALUES(?,?,?,?,?,?,?)", t_rows)
                n_trades += db.total_changes - before
                db.executemany("INSERT OR REPLACE INTO trade_marks VALUES(?,?,?)",
                               [(v, m, ms) for (v, m), ms in t_marks.items()])
                if t_errors:
                    print(f"trade poll: {len(t_errors)} errors, first: {t_errors[0]}", flush=True)
            db.commit()
            n_cycles += 1
            n_rows += len(rows)

            if errors:
                print(f"cycle {cycle_id}: {errors[0]}", flush=True)
            if n_cycles % 12 == 1:
                print(f"cycles={n_cycles} changed_books={n_rows} kalshi={len(kbooks)}/{len(tickers)} "
                      f"poly={len(pbooks)}/{len(tokens)} trades={n_trades} last_cycle_ms={finished - started}", flush=True)

            if args.duration and time.monotonic() - t_start >= args.duration:
                break
            next_at += args.interval
            wait = next_at - time.monotonic()
            if wait > 0:
                stop.wait(wait)
            else:
                next_at = time.monotonic()
    db.close()
    print(f"Stopped after {n_cycles} cycles, {n_rows} changed books and {n_trades} trades.", flush=True)


if __name__ == "__main__":
    main()
