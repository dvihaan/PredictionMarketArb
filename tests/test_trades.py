"""Tests for recorder/trades.py: Polymarket polls look back POLY_LOOKBACK_MS (the Data API publishes
trades late), and overlapping polls don't return a trade twice.

    .venv/bin/python -m unittest discover -s tests
"""
import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder import trades  # noqa: E402

NOW = 1_795_000_000_000


def poly_row(tid, ts):
    return ("poly", tid, "Y", ts, 0.5, 1.0, "buy_yes", "0x" + tid)


def kalshi_row(tid, ts):
    return ("kalshi", tid, "K", ts, 0.5, 1.0, "buy_yes", None)


class TradePollerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.poller = trades.TradePoller(os.path.join(self.tmp.name, "marks.json"), log=lambda s: None)
        self.since = []

    def tearDown(self):
        self.tmp.cleanup()

    def poll(self, now, poly, kalshi=()):
        def fake_poly(session, batch, since, stop):
            self.since.append(("poly", since))
            return list(poly)

        def fake_kalshi(session, ticker, since, stop):
            self.since.append(("kalshi", since))
            return list(kalshi)

        with mock.patch.object(trades, "now_ms", return_value=now), \
                mock.patch.object(trades.record, "poly_trades", side_effect=fake_poly), \
                mock.patch.object(trades.record, "kalshi_trades", side_effect=fake_kalshi), \
                mock.patch.object(self.poller.stop, "wait"):
            rows, errors = self.poller.poll(["K"], {"C1": "Y"})
        self.assertEqual(errors, [])
        return [r["trade_id"] for r in rows]

    def test_poly_looks_back_and_overlaps_are_returned_once(self):
        self.assertEqual(self.poll(NOW, [poly_row("a", NOW - 5000)], [kalshi_row("k1", NOW - 1000)]), ["k1", "a"])
        # Next minute: "b" was executed before "a" but published after the first poll.
        got = self.poll(NOW + 60_000, [poly_row("a", NOW - 5000), poly_row("b", NOW - 30 * 60_000)],
                        [kalshi_row("k1", NOW - 1000), kalshi_row("k2", NOW + 30_000)])
        self.assertEqual(got, ["k2", "b"])
        self.assertIn(("poly", NOW - trades.POLY_LOOKBACK_MS), self.since)            # Polymarket: the look-back
        self.assertIn(("kalshi", NOW - trades.record.TRADE_LAG_MS), self.since)      # Kalshi: unchanged
        self.assertEqual(self.poller.marks[("poly", "Y")], NOW + 60_000 - trades.POLY_LOOKBACK_MS)

    def test_old_trades_are_forgotten(self):
        self.poll(NOW, [poly_row("a", NOW - 5000)])
        later = NOW + trades.POLY_LOOKBACK_MS + 3 * trades.record.TRADE_LAG_MS
        self.poll(later, [])
        self.assertEqual(self.poller.returned, {})


if __name__ == "__main__":
    unittest.main()
