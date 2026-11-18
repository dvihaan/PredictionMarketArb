"""Tests for recorder/refetch.py: re-fetched Polymarket trades land in their own day's raw partition,
and compaction then times them by transaction hash.

    .venv/bin/python -m unittest discover -s tests
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from recorder.compact import main as compact_main, process_day, rest_trades  # noqa: E402
from recorder.refetch import refetch_poly  # noqa: E402
from recorder.store import LocalStore  # noqa: E402
from test_compact import D1, D2, T1, T2, Raw, pbook  # noqa: E402


class Response:
    def __init__(self, body):
        self.status_code, self.body = 200, body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    """Polymarket's /v2/trades: newest first, no time filter, one page here."""

    def __init__(self, trades):
        self.trades, self.headers, self.calls = trades, {}, 0

    def request(self, method, url, **kw):
        self.calls += 1
        conds = kw["params"]["condition"].split(",")
        page = sorted((t for t in self.trades if t["condition_id"] in conds), key=lambda t: -t["timestamp"])
        return Response({"data": page, "pagination": {"has_more": False}})


def trade(ts_ms, tx, token="Y", side="BUY", price=0.5):
    return {"timestamp": ts_ms // 1000, "condition_id": "C1", "token_id": token, "side": side, "price": price,
            "size": 2.0, "transaction_hash": tx, "proxy_wallet": "w"}


class RefetchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(self.tmp.name)
        self.logs = []

    def tearDown(self):
        self.tmp.cleanup()

    def test_rows_land_in_their_trade_day_and_are_timed_by_hash(self):
        raw = Raw(self.tmp.name)
        raw.poly(T1 + 1000, [pbook("Y", [(0.5, 100)], [], T1)])
        raw.poly(T1 + 5047, {"event_type": "last_trade_price", "asset_id": "Y", "market": "m", "price": "0.5",
                             "size": "2", "side": "BUY", "timestamp": str(T1 + 5000), "transaction_hash": "0xab"})
        raw.close()
        session = FakeSession([trade(T1 - 10_000, "0xold"),          # the day before: not fetched
                               trade(T1 + 5000, "0xab"),
                               trade(T1 + 6000, "0xno", token="N"),   # a NO-token trade, no YES print
                               trade(T2 + 9000, "0xd2")])
        now = T2 + 3_600_000
        got = refetch_poly(self.store, [D1, D2], {"C1": "Y"}, session=session, now_ms=now, log=self.logs.append)
        self.assertEqual(got, {D1: 2, D2: 1})
        self.assertEqual(session.calls, 1)                            # one batch, one page
        self.assertEqual(self.store.list(f"raw/stream=trades/date={D1}/"),
                         [f"raw/stream=trades/date={D1}/hour=23/refetch-{T2 - 300_000}-{now}.jsonl.gz"])
        rows = rest_trades(self.store, D1)["poly"]
        self.assertEqual([(r["ts_ms"], r["taker"], r["yes_price"], r["tx_hash"]) for r in rows],
                         [(T1 + 5000, "buy_yes", 0.5, "0xab"), (T1 + 6000, "sell_yes", 0.5, "0xno")])
        process_day(self.store, D1, log=self.logs.append, today="2026-12-31")
        self.assertIn("timed by a print: 1 of 2)", self.logs[-1])

    def test_nightly_refetches_each_day_and_fails_loudly(self):
        """compact --refetch-poly re-fetches before processing; a failed re-fetch still processes the day
        but returns 1 and skips NightlyOK, so the nightly-job alarm emails."""
        raw = Raw(self.tmp.name)
        raw.poly(T1 + 1000, [pbook("Y", [(0.5, 100)], [], T1)])
        raw.close()
        pairs = os.path.join(self.tmp.name, "pairs.json")
        with open(pairs, "w") as f:
            json.dump({"pairs": [{"pair_id": "P", "kalshi_ticker": "K", "poly_yes_token": "Y",
                                  "poly_condition_id": "C1"}]}, f)
        argv = ["--store", self.tmp.name, "--pairs", pairs, "--refetch-poly", "--date", D1]
        calls = []
        with mock.patch("recorder.refdata.snapshot", return_value=(1, 1, 1, 0, None)), \
                mock.patch("recorder.refetch.refetch_poly", side_effect=lambda *a, **k: calls.append(a[1:3])), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(compact_main(argv), 0)
        self.assertEqual(calls, [([D1], {"C1": "Y"})])
        os.remove(os.path.join(self.tmp.name, *f"curated/books/venue=poly/date={D1}/part-0.parquet".split("/")))
        with mock.patch("recorder.refdata.snapshot", return_value=(1, 1, 1, 0, None)), \
                mock.patch("recorder.refetch.refetch_poly", side_effect=OSError("down")), \
                mock.patch("recorder.metrics.Metrics") as metrics, contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(compact_main(argv + ["--metrics-namespace", "NS"]), 1)
        self.assertTrue(self.store.exists(f"curated/books/venue=poly/date={D1}/part-0.parquet"))
        self.assertIn("re-fetch failed for 2026-11-16", out.getvalue())
        metrics.assert_not_called()

    def test_no_trades_writes_nothing(self):
        got = refetch_poly(self.store, [D1], {"C1": "Y"}, session=FakeSession([]), now_ms=T2, log=self.logs.append)
        self.assertEqual(got, {D1: 0})
        self.assertEqual(self.store.list("raw/"), [])


if __name__ == "__main__":
    unittest.main()
