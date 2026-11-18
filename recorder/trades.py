"""Periodic REST trade backfill for the recorded pairs, using rest_trades.py's fetchers.

Both venues publish full trade history, so trades don't depend on a live connection: each poll
fetches everything since the last covered time per market. Marks are kept in a small state file;
if it is lost, the poll goes back backfill_hours. Overlapping fetches are harmless, because
compaction de-duplicates on (venue, trade_id).

Polymarket's Data API publishes trades minutes after they happen (measured 2026-11-18, TASKS.md 3.E.1),
so each Polymarket poll re-reads the last POLY_LOOKBACK_MS, not just the last minute. To keep raw small,
a poll returns only trades this process hasn't returned before. The nightly job re-fetches the whole
previous day anyway (recorder/refetch.py), which catches anything published even later.
"""
import json
import os
import threading
import time

import requests

from recorder import rest_trades as record   # fetchers moved unchanged from deprecated/record.py

FIELDS = ("venue", "trade_id", "market", "ts_ms", "yes_price", "size", "taker", "tx_hash")
# Probe of 40 trades, 2026-11-18: published p50 177 s, p90 255 s, max 301 s after the trade (batches about
# every 5 min); earlier recorded trades occasionally took 26-38 min. One hour covers both; nightly catches the rest.
POLY_LOOKBACK_MS = 60 * 60_000


def now_ms():
    return int(time.time() * 1000)


class TradePoller:
    def __init__(self, state_path, backfill_hours=24.0, log=print):
        self.state_path, self.log = state_path, log
        self.default_ms = now_ms() - int(backfill_hours * 3600 * 1000)
        self.marks = self._load()
        self.returned = {}                     # (venue, trade_id) -> ts_ms of trades already returned
        self.stop = threading.Event()          # record.py's fetchers check a threading.Event
        self.k_session, self.p_session = requests.Session(), requests.Session()
        for s in (self.k_session, self.p_session):
            s.headers["User-Agent"] = "market-book-recorder/0.2"

    def _load(self):
        try:
            with open(self.state_path, encoding="utf-8") as f:
                return {tuple(k.split("|", 1)): v for k, v in json.load(f).items()}
        except (OSError, ValueError):
            return {}

    def _save(self):
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({f"{v}|{m}": ms for (v, m), ms in self.marks.items()}, f)
        os.replace(tmp, self.state_path)

    def poll(self, tickers, conditions):
        """Blocking; run in a worker thread. Returns (trade dicts, error strings).

        conditions maps Polymarket condition id to YES token. Marks advance only after a successful
        fetch, so a failed market is retried from the same point next time.
        """
        rows, errors = [], []
        for ticker in tickers:
            if self.stop.is_set():
                break
            begun = now_ms()
            try:
                got = record.kalshi_trades(self.k_session, ticker, self.marks.get(("kalshi", ticker), self.default_ms),
                                           self.stop)
                rows += got
                if not self.stop.is_set():     # an interrupted fetch may be incomplete
                    self.marks[("kalshi", ticker)] = begun - record.TRADE_LAG_MS
            except Exception as e:
                errors.append(f"kalshi trades {ticker}: {type(e).__name__}: {e}"[:300])
            self.stop.wait(0.15)               # stay well under the public rate limit
        for batch in record.chunks(sorted(conditions.items()), record.POLY_TRADE_BATCH):
            if self.stop.is_set():
                break
            begun = now_ms()
            since = min(self.marks.get(("poly", tok), self.default_ms) for _, tok in batch)
            try:
                rows += record.poly_trades(self.p_session, dict(batch), since, self.stop)
                if not self.stop.is_set():
                    for _, tok in batch:
                        self.marks[("poly", tok)] = begun - POLY_LOOKBACK_MS
            except Exception as e:
                errors.append(f"poly trades: {type(e).__name__}: {e}"[:300])
        if not self.stop.is_set():
            self._save()
        return [dict(zip(FIELDS, r)) for r in self._unreturned(rows)], errors

    def _unreturned(self, rows):
        """Rows not returned before, so overlapping look-backs don't repeat trades in raw. Forgets trades
        older than any look-back can reach again."""
        fresh = []
        for r in rows:
            key = (r[0], r[1])
            if key not in self.returned:
                self.returned[key] = r[3]
                fresh.append(r)
        horizon = now_ms() - POLY_LOOKBACK_MS - 2 * record.TRADE_LAG_MS
        self.returned = {k: ts for k, ts in self.returned.items() if ts >= horizon}
        return fresh
