"""Tests for recorder/compact.py, using a synthetic raw store in a temp directory.

    .venv/bin/python -m unittest discover -s tests
"""
import io
import json
import os
import re
import sys
import tempfile
import unittest

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.compact import process_day  # noqa: E402
from recorder.sink import RawSink  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

D1, D2 = "2026-11-16", "2026-11-17"
T1 = 1_794_787_200_000            # 2026-11-16 00:00:00 UTC
T2 = T1 + 86_400_000              # 2026-11-17 00:00:00 UTC


class Clock:
    def __init__(self):
        self.ms = T1

    def __call__(self):
        return self.ms


class Raw:
    """Writes raw records the way the recorder does, at controlled times."""

    def __init__(self, root):
        self.clock = Clock()
        self.sink = RawSink(root, window_s=300, host="h", clock=self.clock)

    def at(self, ms):
        self.clock.ms = ms
        return self

    def kalshi(self, ms, **m):
        self.at(ms).sink.write("kalshi_ws", {"recv_ms": ms, "raw": json.dumps(m)})

    def poly(self, ms, m):
        self.at(ms).sink.write("poly_ws", {"recv_ms": ms, "raw": json.dumps(m)})

    def rest(self, ms, books):
        self.at(ms).sink.write("poly_rest", {"recv_ms": ms, "books": books})

    def cov(self, ms, venue, event, **kw):
        self.at(ms).sink.write("coverage", {"recv_ms": ms, "venue": venue, "event": event, **kw})

    def trade(self, ms, **t):
        self.at(ms).sink.write("trades", {"recv_ms": ms, **t})

    def close(self):
        self.sink.close_all()


def snap(ticker, yes, no=()):
    return dict(type="orderbook_snapshot", sid=1, seq=1,
                msg={"market_ticker": ticker, "yes_dollars_fp": [[f"{p:.4f}", f"{s:.2f}"] for p, s in yes],
                     "no_dollars_fp": [[f"{p:.4f}", f"{s:.2f}"] for p, s in no]})


def delta(ticker, side, price, d, ts):
    return dict(type="orderbook_delta", sid=1, seq=2,
                msg={"market_ticker": ticker, "side": side, "price_dollars": f"{price:.4f}", "delta_fp": f"{d:.2f}",
                     "ts_ms": ts})


def pbook(tok, bids, asks, ts):
    return {"event_type": "book", "asset_id": tok, "market": "m", "timestamp": str(ts), "hash": "h",
            "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
            "asks": [{"price": str(p), "size": str(s)} for p, s in asks]}


def pchange(tok, price, size, side, ts):
    return {"event_type": "price_change", "market": "m", "timestamp": str(ts),
            "price_changes": [{"asset_id": tok, "price": str(price), "size": str(size), "side": side},
                              {"asset_id": "NO-" + tok, "price": str(round(1 - price, 2)), "size": str(size),
                               "side": "SELL" if side == "BUY" else "BUY"}]}


class CompactTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(self.tmp.name)
        self.raw = Raw(self.tmp.name)
        self.logs = []

    def tearDown(self):
        self.tmp.cleanup()

    def run_day(self, d, today="2026-12-31"):
        process_day(self.store, d, log=self.logs.append, today=today)

    def books(self, venue, d):
        key = f"curated/books/venue={venue}/date={d}/part-0.parquet"
        return pq.read_table(io.BytesIO(self.store.read(key))).to_pylist()

    def trades(self, venue, d):
        key = f"curated/trades/venue={venue}/date={d}/part-0.parquet"
        return pq.read_table(io.BytesIO(self.store.read(key))).to_pylist()

    # ---- Kalshi ----

    def test_kalshi_rows_and_day_carry_over(self):
        r = self.raw
        r.kalshi(T1 + 1000, **snap("K", [(0.40, 10)], [(0.55, 5)]))
        r.kalshi(T1 + 2000, **delta("K", "yes", 0.40, 5, T1 + 1990))
        # Day 2: no new snapshot, only a delta, which must apply on top of day 1's end state.
        r.kalshi(T2 + 1000, **delta("K", "no", 0.55, -5, T2 + 990))
        r.close()
        self.run_day(D1)
        self.run_day(D2)
        d1 = self.books("kalshi", D1)
        self.assertEqual([x["source"] for x in d1], ["snapshot", "delta"])
        self.assertEqual(d1[1]["yes_bids"], [{"price": 0.4, "size": 15.0}])
        self.assertEqual(d1[1]["server_ms"], T1 + 1990)
        d2 = self.books("kalshi", D2)
        self.assertEqual(len(d2), 1)
        self.assertEqual(d2[0]["yes_bids"], [{"price": 0.4, "size": 15.0}])
        self.assertEqual(d2[0]["no_bids"], [])

    def test_refresh_of_today_never_saves_end_of_day_books(self):
        r = self.raw
        r.kalshi(T1 + 1000, **snap("K", [(0.40, 10)]))
        r.kalshi(T2 + 1000, **delta("K", "yes", 0.40, 1, T2))
        r.close()
        self.run_day(D2, today=D2)            # 3-hourly refresh of a day in progress; D1 caught up first
        self.assertTrue(self.store.exists(f"curated/_state/books/date={D1}.json"))    # complete day: saved
        self.assertFalse(self.store.exists(f"curated/_state/books/date={D2}.json"))   # today: not saved
        self.assertTrue(self.store.exists(f"curated/books/venue=kalshi/date={D2}/part-0.parquet"))
        self.assertEqual(self.books("kalshi", D2)[0]["yes_bids"], [{"price": 0.4, "size": 11.0}])
        self.assertIn("day in progress", self.logs[-1])

    def test_catch_up_processes_missing_previous_day(self):
        r = self.raw
        r.kalshi(T1 + 1000, **snap("K", [(0.40, 10)]))
        r.kalshi(T2 + 1000, **delta("K", "yes", 0.40, 1, T2))
        r.close()
        self.run_day(D2)                      # day 1 was never processed
        self.assertTrue(self.store.exists(f"curated/books/venue=kalshi/date={D1}/part-0.parquet"))
        self.assertEqual(self.books("kalshi", D2)[0]["yes_bids"], [{"price": 0.4, "size": 11.0}])

    def test_kalshi_resync_marks_suspect_until_snapshot(self):
        r = self.raw
        r.kalshi(T1 + 1000, **snap("K", [(0.40, 10)]))
        r.kalshi(T1 + 1500, **snap("J", [(0.30, 10)]))
        r.cov(T1 + 2000, "kalshi", "resync", reason="seq gap")
        r.kalshi(T1 + 2001, **delta("K", "yes", 0.40, 1, T1 + 2001))     # still suspect
        r.kalshi(T1 + 2100, **snap("K", [(0.40, 12)]))                   # fresh snapshot clears K
        r.kalshi(T1 + 2200, **delta("K", "yes", 0.40, 1, T1 + 2200))
        r.kalshi(T1 + 2300, **delta("J", "yes", 0.30, 1, T1 + 2300))     # J has no snapshot yet: suspect
        r.close()
        self.run_day(D1)
        rows = [(x["market"], x["source"], x["suspect"]) for x in self.books("kalshi", D1)]
        self.assertEqual(rows, [("K", "snapshot", False), ("J", "snapshot", False), ("K", "delta", True),
                                ("K", "snapshot", False), ("K", "delta", False), ("J", "delta", True)])
        # Missed Kalshi deltas: which levels are wrong is unknown, so suspect_levels is NULL (whole book).
        self.assertTrue(all(x["suspect_levels"] is None for x in self.books("kalshi", D1)))

    # ---- Polymarket ----

    def test_poly_flag_and_repair(self):
        r = self.raw
        rest_ok = pbook("Y", [(0.5, 100)], [(0.52, 10)], T1 + 1000)
        r.poly(T1 + 1000, [pbook("Y", [(0.5, 100)], [(0.52, 10)], T1 + 1000)])
        r.rest(T1 + 30_000, [rest_ok])                                   # last check that matched
        r.poly(T1 + 40_000, pchange("Y", 0.5, 160, "BUY", T1 + 40_000))  # the change Polymarket never applies
        r.rest(T1 + 60_000, [rest_ok])                                   # first check flags it
        r.rest(T1 + 63_000, [rest_ok])                                   # re-check confirms it
        r.cov(T1 + 63_001, "poly", "book_mismatch", tokens=["Y"])
        r.poly(T1 + 63_100, pbook("Y", [(0.5, 100)], [(0.52, 10)], T1 + 1000))   # fresh book after re-subscribe
        r.poly(T1 + 70_000, pchange("Y", 0.52, 0, "SELL", T1 + 70_000))
        r.close()
        self.run_day(D1)
        rows = [(x["ts_ms"] - T1, x["source"], x["suspect"], x["yes_bids"]) for x in self.books("poly", D1)]
        self.assertEqual(rows[0][:3], (1000, "snapshot", False))
        self.assertEqual(rows[1][:3], (40_000, "delta", True))           # inside (30 000, 63 001]
        self.assertEqual(rows[1][3], [{"price": 0.5, "size": 160.0}])
        self.assertEqual(rows[2][:3], (63_001, "rest_repair", False))
        self.assertEqual(rows[2][3], [{"price": 0.5, "size": 100.0}])    # repaired to Polymarket's book
        self.assertEqual(rows[3][:3], (63_100, "snapshot", False))
        self.assertEqual(rows[4][:3], (70_000, "delta", False))
        self.assertEqual(self.books("poly", D1)[4]["no_bids"], [])        # the only ask was removed
        # Per-level marker: only the YES bid at 0.50 was wrong (160 vs Polymarket's 100); the ask was fine.
        levels = [x["suspect_levels"] for x in self.books("poly", D1)]
        self.assertEqual(levels[1], [{"side": "yes", "price": 0.5}])
        self.assertEqual([levels[i] for i in (0, 2, 3, 4)], [None] * 4)

    def test_poly_wrong_ask_marked_in_no_bid_terms(self):
        r = self.raw
        rest_ok = pbook("Y", [(0.5, 100)], [(0.52, 10)], T1)
        r.poly(T1 + 1000, [rest_ok])
        r.rest(T1 + 30_000, [rest_ok])
        r.poly(T1 + 40_000, pchange("Y", 0.53, 7, "SELL", T1 + 40_000))  # an ask Polymarket never shows
        r.rest(T1 + 60_000, [rest_ok])
        r.rest(T1 + 63_000, [rest_ok])
        r.cov(T1 + 63_001, "poly", "book_mismatch", tokens=["Y"])
        r.close()
        self.run_day(D1)
        suspect = [x for x in self.books("poly", D1) if x["suspect"]]
        # A YES ask at 0.53 appears in no_bids at 1 - 0.53 = 0.47.
        self.assertEqual([x["suspect_levels"] for x in suspect], [[{"side": "no", "price": 0.47}]])

    def test_poly_window_without_repair_book_is_whole_book_uncertain(self):
        r = self.raw
        r.poly(T1 + 1000, [pbook("Y", [(0.5, 100)], [], T1)])
        r.poly(T1 + 2000, pchange("Y", 0.5, 160, "BUY", T1 + 2000))
        r.cov(T1 + 3000, "poly", "book_mismatch", tokens=["Y"])           # no REST books recorded at all
        r.close()
        self.run_day(D1)
        rows = self.books("poly", D1)
        self.assertEqual([(x["suspect"], x["suspect_levels"]) for x in rows], [(True, None), (True, None)])

    def test_poly_rest_trade_timed_by_matching_print(self):
        r = self.raw
        r.poly(T1 + 1000, [pbook("Y", [(0.5, 100)], [], T1)])
        r.poly(T1 + 5047, {"event_type": "last_trade_price", "asset_id": "Y", "market": "m", "price": "0.5",
                           "size": "3", "side": "SELL", "timestamp": str(T1 + 5000), "transaction_hash": "0xab"})
        r.trade(T1 + 60_000, venue="poly", trade_id="p1", market="Y", ts_ms=T1 + 5000, yes_price=0.5, size=3.0,
                taker="sell_yes")
        r.trade(T1 + 60_000, venue="poly", trade_id="p2", market="Y", ts_ms=T1 + 9000, yes_price=0.48, size=1.0,
                taker="sell_yes")                                            # no print at this price
        r.close()
        self.run_day(D1)
        got = {t["trade_id"]: t["recv_ms"] for t in self.trades("poly", D1)}
        self.assertEqual(got, {"p1": T1 + 5047, "p2": None})
        self.assertIn("timed by a print: 1 of 2", self.logs[-1])

    def test_poly_no_entries_are_ignored(self):
        r = self.raw
        r.poly(T1 + 1000, [pbook("Y", [(0.4, 1)], [], T1)])
        r.poly(T1 + 2000, pchange("Y", 0.41, 2, "BUY", T1 + 2000))       # carries a NO-Y mirror entry too
        r.close()
        self.run_day(D1)
        self.assertEqual([x["market"] for x in self.books("poly", D1)], ["Y", "Y"])

    # ---- trades, coverage, idempotency ----

    def test_trades_deduplicated_by_trade_date_rest_preferred(self):
        r = self.raw
        r.kalshi(T1 + 5000, type="trade", sid=2, seq=1,
                 msg={"trade_id": "t1", "market_ticker": "K", "yes_price_dollars": "0.4000", "count_fp": "3.00",
                      "taker_side": "yes", "taker_book_side": "bid", "ts_ms": T1 + 4000})
        r.trade(T1 + 60_000, venue="kalshi", trade_id="t1", market="K", ts_ms=T1 + 4000, yes_price=0.4, size=3.0,
                taker="buy_yes")
        r.trade(T1 + 60_000, venue="poly", trade_id="p1", market="Y", ts_ms=T1 + 50_000, yes_price=0.5, size=1.0,
                taker="sell_yes")
        # Day 2's REST poll backfills a day-1 trade: it must land in day 1's partition.
        r.trade(T2 + 1000, venue="poly", trade_id="p0", market="Y", ts_ms=T1 + 10_000, yes_price=0.5, size=2.0,
                taker="buy_yes")
        r.trade(T2 + 1000, venue="poly", trade_id="p1", market="Y", ts_ms=T1 + 50_000, yes_price=0.5, size=1.0,
                taker="sell_yes")
        r.kalshi(T2 + 2000, **snap("K", [(0.4, 1)]))                     # day 2 needs raw book data to run
        r.close()
        self.run_day(D1)
        self.run_day(D2)
        k = self.trades("kalshi", D1)
        self.assertEqual([(t["trade_id"], t["source"], t["taker"]) for t in k], [("t1", "rest", "buy_yes")])
        # The REST row wins, but keeps the WebSocket row's receive time.
        self.assertEqual(k[0]["recv_ms"], T1 + 5000)
        p = self.trades("poly", D1)
        self.assertEqual([t["trade_id"] for t in p], ["p0", "p1"])       # sorted by time, no duplicate p1

    def test_runs_in_same_window_are_read_in_order(self):
        """Two recorder runs in one 5-minute window: the stopped run's rows come before the restarted run's,
        also when the older file has the pre-2026-11-17 name without a run id."""
        from recorder.compact import raw_keys
        old = RawSink(self.tmp.name, window_s=300, host="ip-10-42-0-86", clock=lambda: T1 + 1000)
        old.write("kalshi_ws", {"recv_ms": T1 + 1000, "raw": json.dumps(snap("K", [(0.40, 10)]))})
        old.close_all()
        # Rename the first file to the old naming scheme (no run id).
        k_old = raw_keys(self.store, "kalshi_ws", D1)[0]
        legacy = re.sub(r"-\d{13}\.jsonl\.gz$", ".jsonl.gz", k_old)
        os.replace(os.path.join(self.tmp.name, *k_old.split("/")), os.path.join(self.tmp.name, *legacy.split("/")))
        new = RawSink(self.tmp.name, window_s=300, host="ip-10-42-0-86", clock=lambda: T1 + 2000)
        new.write("kalshi_ws", {"recv_ms": T1 + 2000, "raw": json.dumps(delta("K", "yes", 0.40, 5, T1 + 2000))})
        new.close_all()
        keys = raw_keys(self.store, "kalshi_ws", D1)
        self.assertEqual(keys, [legacy, keys[1]])
        self.run_day(D1)
        self.assertEqual([x["source"] for x in self.books("kalshi", D1)], ["snapshot", "delta"])
        self.assertEqual(self.books("kalshi", D1)[1]["yes_bids"], [{"price": 0.4, "size": 15.0}])

    def test_rerun_is_identical(self):
        r = self.raw
        r.kalshi(T1 + 1000, **snap("K", [(0.40, 10)]))
        r.poly(T1 + 1000, [pbook("Y", [(0.5, 1)], [], T1)])
        r.cov(T1 + 1, "recorder", "started", pid=1)
        r.close()
        self.run_day(D1)
        first = {k: self.store.read(k) for k in self.store.list("curated/")}
        self.run_day(D1)
        second = {k: self.store.read(k) for k in self.store.list("curated/")}
        self.assertEqual(first.keys(), second.keys())
        for k in first:
            if k.endswith(".parquet"):
                a = pq.read_table(io.BytesIO(first[k])).to_pylist()
                b = pq.read_table(io.BytesIO(second[k])).to_pylist()
                self.assertEqual(a, b, k)
            else:
                self.assertEqual(first[k], second[k], k)

    def test_manifest_lists_curated_and_ref_files(self):
        from recorder.compact import MANIFEST_KEY, write_manifest
        r = self.raw
        r.kalshi(T1 + 1000, **snap("K", [(0.40, 10)]))
        r.close()
        self.run_day(D1)
        self.store.write("ref/pairs/date=2026-11-16/pairs.json", b"{}")
        counts = write_manifest(self.store)
        m = json.loads(self.store.read(MANIFEST_KEY))
        books = {b["key"]: b for b in m["tables"]["books"]}
        key = f"curated/books/venue=kalshi/date={D1}/part-0.parquet"
        entry = books[key]
        self.assertEqual((entry["venue"], entry["date"]), ("kalshi", D1))
        # bytes and modified let the laptop cache detect changed files
        self.assertEqual(entry["bytes"], len(self.store.read(key)))
        self.assertGreater(entry["modified"], 1_700_000_000_000)
        self.assertNotIn("_state", json.dumps(m))
        self.assertEqual([r["key"] for r in m["tables"]["ref"]], ["ref/pairs/date=2026-11-16/pairs.json"])
        self.assertEqual(m["tables"]["ref"][0]["bytes"], 2)
        self.assertEqual(counts["coverage"], 1)

    def test_coverage_detail_is_json(self):
        r = self.raw
        r.cov(T1 + 1, "poly", "book_mismatch", tokens=["Y"])
        r.kalshi(T1 + 2, **snap("K", []))
        r.close()
        self.run_day(D1)
        c = pq.read_table(io.BytesIO(self.store.read(f"curated/coverage/date={D1}/part-0.parquet"))).to_pylist()
        self.assertEqual(c[0]["event"], "book_mismatch")
        self.assertEqual(json.loads(c[0]["detail"]), {"tokens": ["Y"]})


if __name__ == "__main__":
    unittest.main()
