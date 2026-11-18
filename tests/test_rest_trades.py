"""Tests for recorder/rest_trades.py with a fake HTTP session (no network).

    .venv/bin/python -m unittest discover -s tests
"""
import os
import sys
import threading
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.rest_trades import poly_trades  # noqa: E402


class FakeResponse:
    status_code = 200

    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, pages):
        self.pages = list(pages)

    def request(self, method, url, **kw):
        return FakeResponse(self.pages.pop(0))


def fill(token, side, price, ts, tx, size="2"):
    return {"condition_id": "C", "token_id": token, "side": side, "price": price, "size": size, "timestamp": ts,
            "transaction_hash": tx, "proxy_wallet": "0xw"}


class PolyTradesTest(unittest.TestCase):
    def test_rows_in_yes_terms_with_transaction_hash(self):
        page = {"data": [fill("NO", "BUY", "0.3", 1_794_960_002, "0xb"), fill("YES", "SELL", "0.6", 1_794_960_001, "0xa"),
                         fill("YES", "BUY", "0.6", 1_794_959_000, "0xold")],
                "pagination": {"has_more": False}}
        rows = poly_trades(FakeSession([page]), {"C": "YES"}, 1_794_960_000_000, threading.Event())
        self.assertEqual([(r[2], r[3], r[4], r[6], r[7]) for r in rows],
                         [("YES", 1_794_960_002_000, 0.7, "sell_yes", "0xb"),     # buying NO at 0.3 = selling YES at 0.7
                          ("YES", 1_794_960_001_000, 0.6, "sell_yes", "0xa")])
        self.assertEqual(len({r[1] for r in rows}), 2)                            # distinct generated trade ids


if __name__ == "__main__":
    unittest.main()
