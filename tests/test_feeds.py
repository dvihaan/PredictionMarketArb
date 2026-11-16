"""Message-handling tests for the Kalshi and Polymarket feeds, without network access.

    .venv/bin/python -m unittest discover -s tests
"""
import asyncio
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.books import PolyBook  # noqa: E402
from recorder.kalshi_ws import KalshiFeed  # noqa: E402
from recorder.poly_ws import PolyFeed, same_book  # noqa: E402


class FakeSink:
    def __init__(self):
        self.rows = []

    def write(self, stream, record):
        self.rows.append((stream, record))


class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, text):
        self.sent.append(json.loads(text) if text[:1] == "{" else text)


def run(coro):
    return asyncio.run(coro)


class KalshiFeedTest(unittest.TestCase):
    def setUp(self):
        self.sink, self.events = FakeSink(), []
        self.feed = KalshiFeed(key=None, sink=self.sink, log=lambda *_: None,
                               coverage=lambda venue, event, **kw: self.events.append(event))
        self.feed.ws = FakeWS()
        self.feed.sids = {"orderbook_delta": 1, "trade": 2}
        self.feed.subscribed = {"A"}

    def msg(self, **m):
        run(self.feed._handle(json.dumps(m)))

    def test_ok_reply_counts_toward_seq(self):
        """Regression: an "ok" reply with sid 1 / seq 58 must not make seq 59 look like a gap."""
        self.msg(type="orderbook_snapshot", sid=1, seq=57,
                 msg={"market_ticker": "A", "yes_dollars_fp": [["0.4000", "10.00"]]})
        self.msg(type="ok", sid=1, seq=58, msg={"market_tickers": ["A"]})
        self.msg(type="orderbook_delta", sid=1, seq=59,
                 msg={"market_ticker": "A", "side": "yes", "price_dollars": "0.4000", "delta_fp": "5.00"})
        self.assertEqual(self.feed.resyncs, 0)
        self.assertEqual(self.feed.books["A"].yes_bids(), [(0.4, 15.0)])

    def test_book_gap_requests_snapshots(self):
        self.msg(type="orderbook_snapshot", sid=1, seq=1, msg={"market_ticker": "A", "yes_dollars_fp": []})
        self.msg(type="orderbook_delta", sid=1, seq=3,
                 msg={"market_ticker": "A", "side": "no", "price_dollars": "0.5000", "delta_fp": "1.00"})
        self.assertEqual(self.feed.resyncs, 1)
        cmd = self.feed.ws.sent[-1]
        self.assertEqual(cmd["cmd"], "update_subscription")
        self.assertEqual(cmd["params"]["action"], "get_snapshot")

    def test_trade_gap_is_logged_not_resynced(self):
        self.msg(type="trade", sid=2, seq=1, msg={})
        self.msg(type="trade", sid=2, seq=5, msg={})
        self.assertEqual(self.feed.resyncs, 0)
        self.assertIn("trade_seq_gap", self.events)

    def test_every_message_is_recorded_raw(self):
        self.msg(type="trade", sid=2, seq=1, msg={})
        self.assertEqual([s for s, _ in self.sink.rows], ["kalshi_ws"])
        self.assertIn("recv_ms", self.sink.rows[0][1])


class PolyFeedTest(unittest.TestCase):
    def setUp(self):
        self.sink = FakeSink()
        self.feed = PolyFeed(self.sink, coverage=lambda *a, **k: None, log=lambda *_: None)
        self.feed.subscribed = {"Y"}

    def test_book_list_and_changes_for_subscribed_token_only(self):
        self.feed._handle(json.dumps([{"event_type": "book", "asset_id": "Y", "timestamp": "1",
                                       "bids": [{"price": "0.4", "size": "10"}], "asks": []}]))
        self.feed._handle(json.dumps({"event_type": "price_change", "timestamp": "2", "price_changes": [
            {"asset_id": "Y", "price": "0.4", "size": "12", "side": "BUY"},
            {"asset_id": "N", "price": "0.6", "size": "12", "side": "SELL"}]}))
        self.assertEqual(list(self.feed.books), ["Y"])
        self.assertEqual(self.feed.books["Y"].bids(), [(0.4, 12.0)])

    def test_pong_not_recorded(self):
        self.feed._handle("PONG")
        self.assertEqual(self.sink.rows, [])

    def test_bookless_token_closed_or_resubscribed(self):
        """A token with no book is unsubscribed if its market is closed, re-subscribed if REST has a book."""
        import recorder.poly_ws as pw
        self.feed.ws = FakeWS()
        self.feed.tokens = {"Y", "C", "M"}
        self.feed.subscribed = {"Y", "C", "M"}
        events = []
        self.feed.coverage = lambda venue, event, **kw: events.append((event, kw))
        original = pw.market_closed
        pw.market_closed = lambda session, token: token == "C"
        try:
            run(self.feed._handle_bookless(["C", "M"], rest={"M": {"bids": [], "asks": []}}))
        finally:
            pw.market_closed = original
        self.assertEqual(self.feed.closed, {"C"})
        self.assertEqual(self.feed.subscribed, {"Y", "M"})
        self.assertIn(("book_missing", {"tokens": ["M"]}), events)
        self.assertIn({"assets_ids": ["C"], "operation": "unsubscribe"}, self.feed.ws.sent)
        self.assertIn({"assets_ids": ["M"], "operation": "subscribe"}, self.feed.ws.sent)

    def test_same_book(self):
        local = PolyBook.from_book({"bids": [{"price": "0.51", "size": "1478.39"}],
                                    "asks": [{"price": "0.53", "size": "5"}]})
        rest = {"bids": [{"price": "0.51", "size": "1478.39"}], "asks": [{"price": "0.53", "size": "5"}]}
        self.assertTrue(same_book(local, rest))
        # The divergence seen live on 2026-11-16: one level 60 contracts off.
        rest["bids"][0]["size"] = "1418.39"
        self.assertFalse(same_book(local, rest))

    def test_same_book_ignores_level_order_and_zero_sizes(self):
        local = PolyBook.from_book({"bids": [{"price": "0.1", "size": "1"}, {"price": "0.2", "size": "2"}], "asks": []})
        rest = {"bids": [{"price": "0.20", "size": "2"}, {"price": "0.10", "size": "1"},
                         {"price": "0.05", "size": "0"}], "asks": []}
        self.assertTrue(same_book(local, rest))


if __name__ == "__main__":
    unittest.main()
