"""Tests for recorder/refdata.py: pairs.jsonl and the T-bill rate, without network access.

    .venv/bin/python -m unittest discover -s tests
"""
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder import refdata  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

# The first lines of the real file (fetched 2026-11-17), trimmed to the columns that matter.
CSV = """Date,"4 WEEKS COUPON EQUIVALENT","13 WEEKS BANK DISCOUNT","13 WEEKS COUPON EQUIVALENT"
11/13/2026,4.02,4.13,4.23
11/16/2026,4.02,4.16,4.26
11/12/2026,4.01,4.12,N/A
"""


class Resp:
    def __init__(self, text, status=200):
        self.text, self.status = text, status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return json.loads(self.text)


class TbillTest(unittest.TestCase):
    def test_parse_takes_latest_13_week_coupon_equivalent(self):
        self.assertEqual(refdata.parse_tbill_csv(CSV), ("2026-11-16", 4.26))

    def test_parse_empty(self):
        self.assertIsNone(refdata.parse_tbill_csv('Date,"13 WEEKS COUPON EQUIVALENT"\n'))

    def test_fetch_falls_back_to_previous_year(self):
        calls = []

        class Session:
            def get(self, url, timeout):
                calls.append(url)
                return Resp('Date,"13 WEEKS COUPON EQUIVALENT"\n') if "/2027/" in url else Resp(CSV)

        got = refdata.fetch_tbill(Session(), "2027-01-02")
        self.assertEqual((got["rate_date"], got["yield_pct"]), ("2026-11-16", 4.26))
        self.assertEqual(len(calls), 2)

    def test_fetch_unreachable_returns_none(self):
        class Session:
            def get(self, url, timeout):
                return Resp("", status=503)

        self.assertIsNone(refdata.fetch_tbill(Session(), "2026-11-17"))


class SnapshotTest(unittest.TestCase):
    def test_writes_pairs_jsonl_and_tbill(self):
        pairs = [{"pair_id": "A", "kalshi_ticker": "KA", "poly_yes_token": "1", "poly_market_id": "11"},
                 {"pair_id": "B", "kalshi_ticker": "KB", "poly_yes_token": "2", "poly_market_id": "22"}]

        class Session:
            headers = {}

            def get(self, url, timeout):
                if "treasury" in url:
                    return Resp(CSV)
                return Resp(json.dumps({"market": {"ticker": url.rsplit("/", 1)[1]}}))

        with tempfile.TemporaryDirectory() as d:
            src = os.path.join(d, "pairs.json")
            with open(src, "w", encoding="utf-8") as f:
                json.dump({"pairs": pairs}, f)
            store = LocalStore(os.path.join(d, "store"))
            with mock.patch.object(refdata.requests, "Session", Session):
                n_pairs, n_k, n_p, errors, tbill = refdata.snapshot(store, "2026-11-17", src, pause_s=0)
            lines = store.read("ref/pairs/date=2026-11-17/pairs.jsonl").decode().splitlines()
            self.assertEqual([json.loads(x)["pair_id"] for x in lines], ["A", "B"])
            rate = json.loads(store.read("ref/rates/date=2026-11-17/tbill.json"))
            self.assertEqual((rate["rate_date"], rate["yield_pct"]), ("2026-11-16", 4.26))
            self.assertEqual((n_pairs, n_k, n_p, errors), (2, 2, 2, 0))


if __name__ == "__main__":
    unittest.main()
