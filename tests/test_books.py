"""Tests for recorder/books.py: rebuilding order books from WebSocket messages.

    .venv/bin/python -m unittest discover -s tests
"""
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.books import KalshiBook, PolyBook, SeqTracker  # noqa: E402

FIXTURES = os.path.join(ROOT, "tests", "fixtures")


def fixture(name):
    out = []
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        for line in f:
            raw = json.loads(line)["raw"]
            if raw[:1] not in "[{":
                continue                      # PING / PONG text frames
            m = json.loads(raw)
            out.extend(m if isinstance(m, list) else [m])
    return out


class KalshiBookTest(unittest.TestCase):
    def snapshot(self):
        return {"market_ticker": "T", "clock": 10,
                "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
                "no_dollars_fp": [["0.5400", "20.00"], ["0.5600", "146.00"]]}

    def test_snapshot_best_first(self):
        b = KalshiBook.from_snapshot(self.snapshot())
        self.assertEqual(b.yes_bids(), [(0.22, 333.0), (0.08, 300.0)])
        self.assertEqual(b.no_bids(), [(0.56, 146.0), (0.54, 20.0)])

    def test_missing_side_is_empty(self):
        b = KalshiBook.from_snapshot({"market_ticker": "T", "yes_dollars_fp": [["0.1000", "5.00"]]})
        self.assertEqual(b.no_bids(), [])

    def test_delta_adds_reduces_and_removes(self):
        b = KalshiBook.from_snapshot(self.snapshot())
        b.apply_delta({"side": "yes", "price_dollars": "0.2300", "delta_fp": "10.00"})
        b.apply_delta({"side": "yes", "price_dollars": "0.2200", "delta_fp": "-33.00"})
        b.apply_delta({"side": "no", "price_dollars": "0.5600", "delta_fp": "-146.00"})
        self.assertEqual(b.yes_bids(), [(0.23, 10.0), (0.22, 300.0), (0.08, 300.0)])
        self.assertEqual(b.no_bids(), [(0.54, 20.0)])

    def test_depth_limit(self):
        snap = {"yes_dollars_fp": [[f"{i / 100:.4f}", "1.00"] for i in range(1, 40)]}
        b = KalshiBook.from_snapshot(snap)
        self.assertEqual(len(b.yes_bids(25)), 25)
        self.assertEqual(b.yes_bids(25)[0], (0.39, 1.0))

    def test_fixture_replay_has_no_negative_levels(self):
        books, seq = {}, SeqTracker()
        for m in fixture("kalshi_ws_sample.jsonl"):
            if m.get("type") not in ("orderbook_snapshot", "orderbook_delta"):
                continue
            self.assertFalse(seq.gap(m["sid"], m["seq"]), "fixture has contiguous seq numbers")
            msg = m["msg"]
            if m["type"] == "orderbook_snapshot":
                books[msg["market_ticker"]] = KalshiBook.from_snapshot(msg)
            else:
                books[msg["market_ticker"]].apply_delta(msg)
        self.assertEqual(len(books), 3)
        for b in books.values():
            for p, s in b.yes_bids(1000) + b.no_bids(1000):
                self.assertGreater(s, 0)
                self.assertTrue(0 < p < 1)


class SeqTrackerTest(unittest.TestCase):
    def test_contiguous_and_gap(self):
        t = SeqTracker()
        self.assertFalse(t.gap(1, 1))
        self.assertFalse(t.gap(1, 2))
        self.assertTrue(t.gap(1, 4))           # 3 was skipped
        self.assertFalse(t.gap(1, 5))          # tracking continues from the new position
        self.assertFalse(t.gap(2, 1))          # sids are independent

    def test_reset(self):
        t = SeqTracker()
        t.gap(1, 7)
        t.reset()
        self.assertFalse(t.gap(1, 1))


class PolyBookTest(unittest.TestCase):
    def book(self):
        return {"asset_id": "A", "timestamp": "100", "hash": "h",
                "bids": [{"price": "0.01", "size": "50"}, {"price": "0.20", "size": "10"}],
                "asks": [{"price": "0.99", "size": "5"}, {"price": "0.25", "size": "7"}]}

    def test_book_best_first(self):
        b = PolyBook.from_book(self.book())
        self.assertEqual(b.bids(), [(0.2, 10.0), (0.01, 50.0)])
        self.assertEqual(b.asks(), [(0.25, 7.0), (0.99, 5.0)])
        self.assertEqual(b.timestamp_ms, 100)

    def test_price_change_sets_absolute_size(self):
        b = PolyBook.from_book(self.book())
        b.apply_change({"asset_id": "A", "price": "0.20", "size": "3", "side": "BUY"}, 200)
        b.apply_change({"asset_id": "A", "price": "0.21", "size": "4", "side": "BUY"}, 200)
        b.apply_change({"asset_id": "A", "price": "0.25", "size": "0", "side": "SELL"}, 201)
        self.assertEqual(b.bids(), [(0.21, 4.0), (0.2, 3.0), (0.01, 50.0)])
        self.assertEqual(b.asks(), [(0.99, 5.0)])
        self.assertEqual(b.timestamp_ms, 201)

    def test_yes_no_view_matches_recorder_format(self):
        """yes_bids are the token's bids; no_bids are its asks at 1 - p, as record.py stored them."""
        b = PolyBook.from_book(self.book())
        self.assertEqual(b.yes_bids(), [(0.2, 10.0), (0.01, 50.0)])
        self.assertEqual(b.no_bids(), [(0.75, 7.0), (0.01, 5.0)])

    def test_fixture_replay_keeps_yes_and_no_mirrored(self):
        """Polymarket's NO book is the mirror of YES: every YES bid at p is a NO ask at 1 - p."""
        books = {}
        for m in fixture("poly_ws_sample.jsonl"):
            if m.get("event_type") == "book":
                books[m["asset_id"]] = PolyBook.from_book(m)
            elif m.get("event_type") == "price_change":
                for c in m["price_changes"]:
                    if c["asset_id"] in books:
                        books[c["asset_id"]].apply_change(c, int(m["timestamp"]))
        by_market = {}
        for m in fixture("poly_ws_sample.jsonl"):
            if m.get("event_type") == "book":
                by_market.setdefault(m["market"], set()).add(m["asset_id"])
        self.assertEqual(len(by_market), 3)
        for a, b in (sorted(ids) for ids in by_market.values()):
            mirror = [(round(1 - p, 4), s) for p, s in books[b].bids(1000)]
            self.assertEqual(sorted(books[a].asks(1000)), sorted(mirror))


if __name__ == "__main__":
    unittest.main()
