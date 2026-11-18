"""Nightly processing: one UTC day of raw capture files -> curated Parquet tables.

    .venv/bin/python -m recorder.compact --store data/raw-local                 # yesterday (UTC)
    .venv/bin/python -m recorder.compact --store s3://<bucket> --date 2026-11-16
    .venv/bin/python -m recorder.compact --store data/raw-local --date 2026-11-16 --days 3
    .venv/bin/python -m recorder.compact --store s3://<bucket> --today        # 3-hourly refresh of today so far
    .venv/bin/python -m recorder.compact --store s3://<bucket> --pairs <pairs file> --refetch-poly   # the nightly

--refetch-poly first re-fetches each day's Polymarket trades into raw (recorder/refetch.py): the Data API
publishes trades minutes late, so the live poll can miss some. A failed re-fetch still processes the day
but skips the NightlyOK metric, so the nightly-job alarm emails.

Reads raw/stream=*/date=D/ and writes, overwriting any earlier output for the same day:

    curated/books/venue=<kalshi|poly>/date=D/part-0.parquet   one row per book change, top 25 levels
    curated/trades/venue=<kalshi|poly>/date=T/part-0.parquet  de-duplicated on trade_id; T is the trade's own date
    curated/coverage/date=D/part-0.parquet                    the recorder's connection and check events
    curated/_state/books/date=D.json                          end-of-day books, the starting point for day D+1

Books replay the raw messages in arrival order through recorder/books.py, the same code the recorder
uses live. Kalshi only sends snapshots on (re)subscribe, so day D starts from day D-1's end-of-day
books; a missing day D-1 is processed first, up to MAX_CATCH_UP days back.

Book rows carry:
    source          snapshot | delta | rest_repair
    suspect         the book may be wrong: Kalshi between a seq gap and the market's fresh snapshot;
                    Polymarket between the last REST check that matched and the check that found a mismatch.
    suspect_levels  which levels were wrong, as [{side: "yes"|"no", price}] in yes_bids/no_bids terms.
                    Polymarket: the levels where our book differed from Polymarket's REST book at the repair.
                    NULL on a suspect row means "unknown, treat the whole book as uncertain" (Kalshi resyncs,
                    or a Polymarket window with no REST book to compare).
At each confirmed Polymarket mismatch the REST book is applied as a "rest_repair" row (Binoy's
"flag and repair" choice), so later rows build on Polymarket's own book. Suspect Polymarket rows are held
back until their repair is seen, so a file is not strictly in time order: sort by ts_ms when reading.

Trade rows carry recv_ms, our receive time, where known: Kalshi WebSocket trades, and Polymarket REST
trades whose settlement transaction (tx_hash) printed on the WebSocket feed (last_trade_price, same YES
token): the earliest such print. Only YES tokens are subscribed and NO-token trades never print there, so
those stay NULL; ts_ms (the venue's time; one-second resolution for Polymarket) is then the only timing.
REST rows recorded before tx_hash was kept fall back to matching a print by YES price within 2 s, which
times far fewer trades (REST prices are averaged per taker order, prints are per fill).
"""
import argparse
import heapq
import io
import json
import re
import sys
import time
from collections import defaultdict
from datetime import date as Date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from recorder.books import KalshiBook, PolyBook
from recorder.sink import read_lines
from recorder.store import open_store

MAX_CATCH_UP = 7
ROW_GROUP = 20_000
REPAIR_LOOKBACK_MS = 15_000     # the confirming REST fetch is a few seconds before the mismatch event

PRINT_MATCH_MS = 2_000          # price fallback: a REST trade without tx_hash matches a print within this many ms

LEVELS = pa.list_(pa.struct([("price", pa.float64()), ("size", pa.float64())]))
SUSPECT_LEVELS = pa.list_(pa.struct([("side", pa.string()), ("price", pa.float64())]))
BOOKS_SCHEMA = pa.schema([("ts_ms", pa.int64()), ("server_ms", pa.int64()), ("market", pa.string()),
                          ("yes_bids", LEVELS), ("no_bids", LEVELS), ("source", pa.string()),
                          ("suspect", pa.bool_()), ("suspect_levels", SUSPECT_LEVELS)])
TRADES_SCHEMA = pa.schema([("trade_id", pa.string()), ("market", pa.string()), ("ts_ms", pa.int64()),
                           ("yes_price", pa.float64()), ("size", pa.float64()), ("taker", pa.string()),
                           ("source", pa.string()), ("recv_ms", pa.int64()), ("tx_hash", pa.string())])
COVERAGE_SCHEMA = pa.schema([("recv_ms", pa.int64()), ("venue", pa.string()), ("event", pa.string()),
                             ("detail", pa.string())])


def utc_date(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()


def day_before(d):
    return (Date.fromisoformat(d) - timedelta(days=1)).isoformat()


# ---- raw input ----------------------------------------------------------------------------------

def raw_keys(store, stream, d):
    """Raw files for one stream and day, in time order. Names are <host>-<window ms>-<run ms>.jsonl.gz
    (or <host>-<window ms>.jsonl.gz before run ms existed): ordered by window, then run, so a run that
    restarted inside a window comes after the run that stopped."""
    def order(key):
        m = re.search(r"/([^/]*?)-(\d{13})(?:-(\d{13}))?\.jsonl\.gz$", key)
        return (int(m.group(2)), int(m.group(3) or 0), m.group(1)) if m else (0, 0, key)
    return sorted((k for k in store.list(f"raw/stream={stream}/date={d}/") if k.endswith(".jsonl.gz")), key=order)


def raw_records(store, stream, d):
    for key in raw_keys(store, stream, d):
        yield from read_lines(store.read(key))


def has_raw(store, d):
    return any(store.list(f"raw/stream={s}/date={d}/") for s in ("kalshi_ws", "poly_ws"))


# ---- output -------------------------------------------------------------------------------------

class ParquetOut:
    """Collects rows column by column and writes row groups as it goes, to keep memory flat."""

    def __init__(self, schema):
        self.schema = schema
        self.buf = io.BytesIO()
        self.writer = pq.ParquetWriter(self.buf, schema, compression="zstd")
        self.cols = {name: [] for name in schema.names}
        self.rows = 0

    def add(self, **row):
        for name in self.schema.names:
            self.cols[name].append(row[name])
        self.rows += 1
        if len(self.cols[self.schema.names[0]]) >= ROW_GROUP:
            self._flush()

    def _flush(self):
        if self.cols[self.schema.names[0]]:
            self.writer.write_table(pa.table(self.cols, schema=self.schema))
            self.cols = {name: [] for name in self.schema.names}

    def finish(self):
        self._flush()
        self.writer.close()
        return self.buf.getvalue()


def levels(pairs):
    return [{"price": p, "size": s} for p, s in pairs]


# ---- end-of-day state ---------------------------------------------------------------------------

def state_key(d):
    return f"curated/_state/books/date={d}.json"


def load_state(store, d):
    """Books at the end of day d, or None if that day hasn't been processed."""
    key = state_key(d)
    if not store.exists(key):
        return None
    s = json.loads(store.read(key))
    kalshi = {}
    for t, b in s["kalshi"].items():
        kb = KalshiBook()
        kb.yes = {float(p): v for p, v in b["yes"].items()}
        kb.no = {float(p): v for p, v in b["no"].items()}
        kalshi[t] = kb
    poly = {}
    for t, b in s["poly"].items():
        pb = PolyBook()
        pb.bid = {float(p): v for p, v in b["bid"].items()}
        pb.ask = {float(p): v for p, v in b["ask"].items()}
        pb.timestamp_ms = b.get("ts")
        poly[t] = pb
    return {"kalshi": kalshi, "poly": poly}


def dump_state(kalshi, poly):
    return json.dumps({
        "kalshi": {t: {"yes": {str(p): s for p, s in b.yes.items()}, "no": {str(p): s for p, s in b.no.items()}}
                   for t, b in kalshi.items()},
        "poly": {t: {"bid": {str(p): s for p, s in b.bid.items()}, "ask": {str(p): s for p, s in b.ask.items()},
                     "ts": b.timestamp_ms} for t, b in poly.items()},
    }, separators=(",", ":")).encode()


# ---- Kalshi -------------------------------------------------------------------------------------

def kalshi_trade(msg, recv_ms=None):
    """A Kalshi WebSocket trade, normalized like the REST fetcher in rest_trades.kalshi_trades()."""
    book_side = msg.get("taker_book_side") or {"yes": "bid", "no": "ask"}[msg["taker_side"]]
    return dict(trade_id=msg["trade_id"], market=msg["market_ticker"], ts_ms=int(msg["ts_ms"]),
                yes_price=float(msg["yes_price_dollars"]), size=float(msg["count_fp"]),
                taker="buy_yes" if book_side == "bid" else "sell_yes", source="ws", recv_ms=recv_ms, tx_hash=None)


def process_kalshi(store, d, books, coverage, out, trades):
    """Replay one day of Kalshi messages. books (ticker -> KalshiBook) is updated in place."""
    resyncs = [(r["recv_ms"], 0, "resync", r) for r in coverage if r["venue"] == "kalshi" and r["event"] == "resync"]
    ws = ((r["recv_ms"], 1, "ws", r) for r in raw_records(store, "kalshi_ws", d))
    suspect = set()
    for recv, _, kind, rec in heapq.merge(resyncs, ws, key=lambda x: (x[0], x[1])):
        if kind == "resync":
            suspect = set(books)               # every book may be missing deltas until its fresh snapshot
            continue
        m = json.loads(rec["raw"])
        t = m.get("type")
        if t == "trade":
            trades["kalshi"].append(kalshi_trade(m["msg"], recv))
            continue
        if t not in ("orderbook_snapshot", "orderbook_delta"):
            continue
        msg = m["msg"]
        ticker = msg["market_ticker"]
        if t == "orderbook_snapshot":
            books[ticker] = KalshiBook.from_snapshot(msg)
            suspect.discard(ticker)
            source, server = "snapshot", m.get("sending_ts_ms")
        else:
            if ticker not in books:
                continue                       # no snapshot yet: the book is unknown
            books[ticker].apply_delta(msg)
            source, server = "delta", msg.get("ts_ms")
        b = books[ticker]
        # A resync means deltas were missed, so which levels are wrong is unknown: suspect_levels stays NULL.
        out.add(ts_ms=recv, server_ms=server, market=ticker, yes_bids=levels(b.yes_bids()),
                no_bids=levels(b.no_bids()), source=source, suspect=ticker in suspect, suspect_levels=None)


# ---- Polymarket ---------------------------------------------------------------------------------

def poly_mismatches(store, d, coverage):
    """Pass 1: suspect windows and repair books for every confirmed Polymarket mismatch.

    Returns ({token: sorted [(start_ms, end_ms)]}, [(event_ms, token, rest_book)]). The window runs
    from the last REST check before the one that first flagged the token, to the confirming event.
    """
    events = [(r["recv_ms"], tok) for r in coverage
              if r["venue"] == "poly" and r["event"] == "book_mismatch" for tok in r.get("tokens", [])]
    if not events:
        return {}, []
    wanted = defaultdict(list)                  # token -> event times
    for ms, tok in events:
        wanted[tok].append(ms)
    checks = defaultdict(list)                  # token -> REST check times
    repair = {}                                 # (event ms, token) -> latest REST book at or before it
    for rec in raw_records(store, "poly_rest", d):
        for b in rec["books"]:
            tok = b["asset_id"]
            if tok not in wanted:
                continue
            checks[tok].append(rec["recv_ms"])
            for ms in wanted[tok]:
                if ms - REPAIR_LOOKBACK_MS <= rec["recv_ms"] <= ms:
                    repair[(ms, tok)] = b
    windows, repairs = defaultdict(list), []
    for ms, tok in events:
        times = [t for t in checks[tok] if t <= ms]
        # times[-1] is the confirming re-check, times[-2] the check that first flagged it,
        # times[-3] the last check that matched. Without it, the window starts at the day's first data.
        start = times[-3] if len(times) >= 3 else 0
        windows[tok].append((start, ms))
        if (ms, tok) in repair:
            repairs.append((ms, tok, repair[(ms, tok)]))
    for tok in windows:
        windows[tok].sort()
    return windows, repairs


def in_windows(windows, tok, ms):
    """True if ms falls in one of the token's suspect windows (start, end]. A day has only a few per token."""
    return any(s < ms <= e for s, e in windows.get(tok, ()))


def wrong_levels(local, rest):
    """Levels where our rebuilt book differs from Polymarket's REST book, in yes_bids/no_bids terms:
    a token bid at p is ("yes", p); a token ask at p is ("no", 1 - p), as PolyBook.no_bids() shows it."""
    good = PolyBook.from_book(rest)
    out = []
    for side, mine, theirs, flip in (("yes", local.bid, good.bid, False), ("no", local.ask, good.ask, True)):
        for p in sorted(set(mine) | set(theirs)):
            if mine.get(p) != theirs.get(p):
                out.append({"side": side, "price": round(1 - p, 5) if flip else p})
    return out


def process_poly(store, d, books, coverage, out):
    """Replay one day of Polymarket messages. books (YES token -> PolyBook) is updated in place.

    Returns the day's WebSocket trade prints as [(recv_ms, token, price, ts_ms, tx_hash)], for timing REST trades.
    Rows inside a token's suspect window are held until that token's repair, so they can be stamped with
    the levels that turned out to be wrong; rows still held at the end of the day get suspect_levels NULL.
    """
    windows, repairs = poly_mismatches(store, d, coverage)
    fixes = [(ms, 0, "repair", (tok, b)) for ms, tok, b in sorted(repairs, key=lambda x: x[0])]
    ws = ((r["recv_ms"], 1, "ws", r) for r in raw_records(store, "poly_ws", d))
    held = defaultdict(list)                   # token -> suspect rows waiting for their repair
    prints = []

    def emit(recv, tok, server, source):
        b = books[tok]
        row = dict(ts_ms=recv, server_ms=server, market=tok, yes_bids=levels(b.yes_bids()),
                   no_bids=levels(b.no_bids()), source=source, suspect=False, suspect_levels=None)
        if source != "rest_repair" and in_windows(windows, tok, recv):
            row["suspect"] = True
            held[tok].append(row)
        else:
            out.add(**row)

    def release(tok, upto_ms, wrong):
        keep = []
        for row in held.pop(tok, []):
            if row["ts_ms"] <= upto_ms:
                out.add(**{**row, "suspect_levels": wrong})
            else:
                keep.append(row)
        if keep:
            held[tok] = keep

    for recv, _, kind, rec in heapq.merge(fixes, ws, key=lambda x: (x[0], x[1])):
        if kind == "repair":
            tok, rest = rec
            wrong = wrong_levels(books[tok], rest) if tok in books else None
            release(tok, recv, wrong)
            books[tok] = PolyBook.from_book(rest)
            emit(recv, tok, books[tok].timestamp_ms, "rest_repair")
            continue
        m = json.loads(rec["raw"])
        for ev in m if isinstance(m, list) else [m]:
            kind = ev.get("event_type")
            if kind == "book":
                tok = ev["asset_id"]
                books[tok] = PolyBook.from_book(ev)
                emit(recv, tok, books[tok].timestamp_ms, "snapshot")
            elif kind == "price_change":
                ts = int(ev["timestamp"]) if ev.get("timestamp") else None
                changed = []
                for c in ev.get("price_changes") or []:
                    b = books.get(c["asset_id"])
                    if b is not None:             # only YES tokens have books; NO entries are the mirror
                        b.apply_change(c, ts)
                        if c["asset_id"] not in changed:
                            changed.append(c["asset_id"])
                for tok in changed:
                    emit(recv, tok, ts, "delta")
            elif kind == "last_trade_price" and ev.get("timestamp"):
                prints.append((recv, ev["asset_id"], float(ev["price"]), int(ev["timestamp"]),
                               ev.get("transaction_hash")))
    for tok in list(held):
        release(tok, float("inf"), None)      # no repair seen today: which levels were wrong is unknown
    return prints


def time_poly_trades(rows, prints):
    """Give Polymarket REST trades our receive time from the WebSocket prints. Returns (by_hash, by_price).

    A row with tx_hash gets the earliest print with the same YES token and transaction (one transaction
    can hold several fills and several REST rows; all became public with its first print). No print, no
    time: a NO-token trade, or a trade while the feed was down. A row without tx_hash (recorded before the
    hash was kept) falls back to a print with the same token and YES price within PRINT_MATCH_MS, each
    print used once; REST prices are averaged per taker order and prints are per fill, so few match."""
    first = {}
    by_token = defaultdict(list)
    for p in sorted(prints, key=lambda x: (x[3], x[0])):
        if p[4]:
            key = (p[1], p[4])
            first[key] = min(first.get(key, p[0]), p[0])
        by_token[p[1]].append(p)
    used, by_hash, by_price = set(), 0, 0
    for r in rows:
        if r.get("recv_ms") is not None:
            continue
        if r.get("tx_hash"):
            recv = first.get((r["market"], r["tx_hash"]))
            if recv is not None:
                r["recv_ms"] = recv
                by_hash += 1
            continue
        for p in by_token.get(r["market"], ()):
            if id(p) in used or abs(p[3] - r["ts_ms"]) > PRINT_MATCH_MS or abs(p[2] - r["yes_price"]) > 1e-6:
                continue
            r["recv_ms"] = p[0]
            used.add(id(p))
            by_price += 1
            break
    return by_hash, by_price


# ---- trades and coverage -----------------------------------------------------------------------

def rest_trades(store, d):
    """The day's REST trade rows per venue, one per trade id. Polls overlap and re-fetches repeat trades,
    so a trade can be in raw several times; a row with a transaction hash is kept over one without it,
    so the hash decides the trade's timing and the row without it can't use up another trade's print."""
    rows = {}
    for r in raw_records(store, "trades", d):
        key = (r["venue"], r["trade_id"])
        if key in rows and (rows[key]["tx_hash"] or not r.get("tx_hash")):
            continue
        rows[key] = dict(trade_id=r["trade_id"], market=r["market"], ts_ms=int(r["ts_ms"]),
                         yes_price=float(r["yes_price"]), size=float(r["size"]), taker=r["taker"],
                         source="rest", recv_ms=None, tx_hash=r.get("tx_hash"))
    out = defaultdict(list)
    for (venue, _), row in rows.items():
        out[venue].append(row)
    return out


def merged_timing(winner, other):
    """recv_ms and tx_hash for two rows of one trade. When only one row has the transaction hash, that row
    decides both: it was timed by its own print (or left untimed when there is none), while a row without
    the hash may hold a price-matched time from another transaction. Otherwise a known value is kept
    whichever row has it."""
    hashed = [x for x in (winner, other) if x.get("tx_hash")]
    if len(hashed) == 1:
        return {"recv_ms": hashed[0].get("recv_ms"), "tx_hash": hashed[0]["tx_hash"]}
    return {k: winner.get(k) if winner.get(k) is not None else other.get(k) for k in ("recv_ms", "tx_hash")}


def upsert_trades(store, venue, rows):
    """Merge trades into their trade-date partitions, de-duplicated on trade_id. Returns rows written."""
    by_date = defaultdict(list)
    for r in rows:
        by_date[utc_date(r["ts_ms"])].append(r)
    written = 0
    for d, new in sorted(by_date.items()):
        key = f"curated/trades/venue={venue}/date={d}/part-0.parquet"
        merged = {}
        if store.exists(key):
            for r in pq.read_table(io.BytesIO(store.read(key))).to_pylist():
                merged[r["trade_id"]] = r
        for r in new:
            r = {**r, "recv_ms": r.get("recv_ms"), "tx_hash": r.get("tx_hash")}
            old = merged.get(r["trade_id"])
            # REST rows win over WebSocket rows for the same trade; otherwise first seen wins. Receive time
            # and transaction hash come from merged_timing() (WebSocket rows have the receive time, REST rows
            # usually don't; rows recorded before the hash was kept don't have it).
            winner = r if old is None or (r["source"] == "rest" and old["source"] != "rest") else old
            other = (old if winner is r else r) or {}
            merged[r["trade_id"]] = {**winner, **merged_timing(winner, other)}
        rows_sorted = sorted(merged.values(), key=lambda r: (r["ts_ms"], r["trade_id"]))
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows_sorted, schema=TRADES_SCHEMA), buf, compression="zstd")
        store.write(key, buf.getvalue())
        written += len(rows_sorted)
    return written


def coverage_table(coverage):
    out = ParquetOut(COVERAGE_SCHEMA)
    for r in coverage:
        detail = {k: v for k, v in r.items() if k not in ("recv_ms", "venue", "event")}
        out.add(recv_ms=r["recv_ms"], venue=r["venue"], event=r["event"],
                detail=json.dumps(detail, separators=(",", ":")) if detail else None)
    return out.finish(), out.rows


# ---- one day ------------------------------------------------------------------------------------

def utc_today():
    return datetime.now(timezone.utc).date().isoformat()


def process_day(store, d, log=print, depth=0, today=None):
    """Rebuild one day's curated tables. A past day also saves its end-of-day books, the starting
    point for the next day. Today (a refresh of a day still in progress) never does: a mid-day
    snapshot saved as "end of day" would silently corrupt the next day if the nightly run failed."""
    started = time.monotonic()
    today = today or utc_today()
    prev = day_before(d)
    state = load_state(store, prev)
    if state is None and has_raw(store, prev) and depth < MAX_CATCH_UP:
        log(f"{d}: {prev} has not been processed yet; processing it first")
        process_day(store, prev, log, depth + 1, today)
        state = load_state(store, prev)
    if state is None:
        state = {"kalshi": {}, "poly": {}}
        log(f"{d}: no earlier books; starting empty (books appear once each market sends a snapshot)")

    coverage = list(raw_records(store, "coverage", d))
    cov_bytes, n_cov = coverage_table(coverage)
    store.write(f"curated/coverage/date={d}/part-0.parquet", cov_bytes)

    trades = rest_trades(store, d)
    summary, prints = {}, []
    for venue, fn in (("kalshi", process_kalshi), ("poly", process_poly)):
        out = ParquetOut(BOOKS_SCHEMA)
        if venue == "kalshi":
            fn(store, d, state["kalshi"], coverage, out, trades)
        else:
            prints = fn(store, d, state["poly"], coverage, out)
        data = out.finish()
        store.write(f"curated/books/venue={venue}/date={d}/part-0.parquet", data)
        summary[venue] = (out.rows, len(data))
    by_hash, by_price = time_poly_trades(trades.get("poly", []), prints)
    n_trades = {v: upsert_trades(store, v, rows) for v, rows in trades.items()}
    complete = d < today
    if complete:
        store.write(state_key(d), dump_state(state["kalshi"], state["poly"]))
    log(f"{d}: kalshi {summary['kalshi'][0]} book rows ({summary['kalshi'][1] / 1e6:.1f} MB), "
        f"poly {summary['poly'][0]} book rows ({summary['poly'][1] / 1e6:.1f} MB), "
        f"{n_cov} coverage events, trades written {n_trades} "
        f"(Polymarket REST trades timed by a print: {by_hash + by_price} of {len(trades.get('poly', []))}"
        + (f", {by_price} by the price fallback" if by_price else "") + "), "
        f"{time.monotonic() - started:.0f} s"
        + ("" if complete else " (day in progress: end-of-day books not saved)"))


MANIFEST_KEY = "curated/manifest.json"


def write_manifest(store):
    """Index of the curated and reference files, so tools reading over HTTPS can find them without
    listing the bucket. {"generated_ms", "tables": {"books": [{"key", "venue", "date", "bytes", "modified"}],
    ...}}. bytes and modified (epoch ms) let a local cache tell when a file has changed and needs re-downloading."""
    tables = defaultdict(list)
    for key, size, modified in store.list_meta("curated/"):
        if key.startswith("curated/_state/") or not key.endswith(".parquet"):
            continue
        table = key.split("/")[1]
        parts = dict(p.split("=", 1) for p in key.split("/")[2:-1] if "=" in p)
        tables[table].append({"key": key, **parts, "bytes": size, "modified": modified})
    tables["ref"] = [{"key": k, "bytes": size, "modified": modified} for k, size, modified in store.list_meta("ref/")]
    body = {"generated_ms": int(time.time() * 1000), "tables": dict(tables)}
    store.write(MANIFEST_KEY, json.dumps(body, indent=1).encode())
    return {t: len(v) for t, v in tables.items()}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store", required=True, help="local directory or s3://bucket[/prefix] holding raw/")
    ap.add_argument("--date", help="UTC day to process, YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--days", type=int, default=1, help="process this many days ending at --date (default 1)")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--pairs", help="pairs file (path or s3://); if given, also snapshot today's reference data")
    ap.add_argument("--metrics-namespace", help="report NightlyOK=1 to CloudWatch after a successful run")
    ap.add_argument("--manifest-only", action="store_true", help="only rebuild curated/manifest.json")
    ap.add_argument("--today", action="store_true",
                    help="refresh today's tables so far (the 3-hourly refresh); never saves end-of-day books")
    ap.add_argument("--refetch-poly", action="store_true",
                    help="before processing each day, re-fetch its Polymarket trades into raw (needs --pairs): "
                         "the live poll misses trades the Data API publishes late (recorder/refetch.py)")
    args = ap.parse_args(argv)
    if args.refetch_poly and not args.pairs:
        ap.error("--refetch-poly needs --pairs")
    if args.manifest_only:
        print(f"manifest: {write_manifest(open_store(args.store, args.region))}", flush=True)
        return 0
    if args.today:
        args.date, args.days = utc_today(), 1
    end = Date.fromisoformat(args.date) if args.date else datetime.now(timezone.utc).date() - timedelta(days=1)
    store = open_store(args.store, args.region)
    if args.pairs:
        from recorder import refdata
        today = datetime.now(timezone.utc).date().isoformat()
        n_pairs, n_k, n_p, errors, tbill = refdata.snapshot(store, today, args.pairs, args.region)
        rate = (f"T-bill {tbill['yield_pct']}% ({tbill['rate_date']})" if tbill
                else "T-bill rate NOT fetched (analysis falls back to the latest stored rate)")
        print(f"reference data {today}: {n_pairs} pairs, {n_k} Kalshi and {n_p} Polymarket markets, "
              f"{errors} fetch errors, {rate}", flush=True)
    conditions, refetch_failed = None, []
    if args.refetch_poly:
        from recorder import pairs as pairs_mod
        _, _, conditions = pairs_mod.markets(pairs_mod.load(args.pairs, args.region))
    for i in range(args.days - 1, -1, -1):
        d = (end - timedelta(days=i)).isoformat()
        if not has_raw(store, d):
            print(f"{d}: no raw data; skipped", flush=True)
            continue
        if conditions is not None:
            from recorder.refetch import refetch_poly
            try:
                refetch_poly(store, [d], conditions, log=lambda s: print(s, flush=True))
            except Exception as e:   # the day is still worth processing with the trades recorded live
                refetch_failed.append(d)
                print(f"{d}: Polymarket trade re-fetch FAILED ({type(e).__name__}: {e}); processing with the "
                      f"trades recorded live, which miss late-published ones", flush=True)
        process_day(store, d, log=lambda s: print(s, flush=True))
    print(f"manifest: {write_manifest(store)}", flush=True)
    if refetch_failed:
        # No NightlyOK, so the nightly-job alarm emails; re-run the night's job once the Data API answers.
        print(f"re-fetch failed for {', '.join(refetch_failed)}: not reporting success", flush=True)
        return 1
    if args.metrics_namespace:
        from recorder.metrics import Metrics
        Metrics(args.metrics_namespace, args.region).put({"NightlyOK": 1})
    return 0


if __name__ == "__main__":
    sys.exit(main())
