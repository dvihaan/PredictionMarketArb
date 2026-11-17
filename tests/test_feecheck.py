"""Tests for analysis/feecheck.py against a synthetic daily snapshot (no network).

    .venv/bin/python -m unittest discover -s tests
"""
import gzip
import json
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from analysis.data import open_data  # noqa: E402
from analysis.feecheck import check  # noqa: E402
from recorder.compact import write_manifest  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

D = "2026-11-18"


def jsonl_gz(rows):
    return gzip.compress("".join(json.dumps(r) + "\n" for r in rows).encode())


class FeeCheckTest(unittest.TestCase):
    def test_matches_differences_and_missing(self):
        pairs = [{"pair_id": "A", "kalshi_ticker": "KXA-1", "poly_yes_token": "1", "poly_market_id": "11",
                  "kalshi_fee_rate": 0.07, "poly_fee_rate": 0.04, "poly_maker_rebate_share": 0.25},
                 {"pair_id": "B", "kalshi_ticker": "KXB-1", "poly_yes_token": "2", "poly_market_id": "22",
                  "kalshi_fee_rate": 0.07, "poly_fee_rate": 0.04}]
        with tempfile.TemporaryDirectory() as root:
            st = LocalStore(root)
            st.write(f"ref/pairs/date={D}/pairs.json", json.dumps({"pairs": pairs}).encode())
            st.write(f"ref/markets/date={D}/kalshi.jsonl.gz", jsonl_gz([
                {"ticker": "KXA-1", "event_ticker": "KXA-26"}, {"ticker": "KXB-1", "event_ticker": "KXB-26"}]))
            st.write(f"ref/markets/date={D}/kalshi_series.jsonl.gz", jsonl_gz([
                {"ticker": "KXA", "fee_type": "quadratic", "fee_multiplier": 1},
                {"ticker": "KXB", "fee_type": "quadratic", "fee_multiplier": 0.5}]))
            st.write(f"ref/markets/date={D}/poly.jsonl.gz", jsonl_gz([
                {"id": "11", "feesEnabled": True, "feeSchedule": {"rate": 0.04, "rebateRate": 0.25}},
                {"id": "22", "feesEnabled": True, "feeSchedule": {"rate": 0.05, "rebateRate": 0.25}}]))
            write_manifest(st)
            rows = check(open_data(root), D)
        got = {(r["pair_id"], r["venue"], r["item"].split(" (")[0]): r["ok"] for r in rows}
        self.assertEqual(got[("A", "poly", "taker fee rate")], True)
        self.assertEqual(got[("A", "poly", "maker rebate share")], True)
        self.assertEqual(got[("A", "kalshi", "taker fee rate")], True)
        self.assertEqual(got[("B", "poly", "taker fee rate")], False)          # 0.04 vs published 0.05
        self.assertEqual(got[("B", "poly", "maker rebate share")], False)      # missing from the pairs file
        self.assertEqual(got[("B", "kalshi", "taker fee rate")], False)        # 0.07 vs 0.07 x 0.5


if __name__ == "__main__":
    unittest.main()
