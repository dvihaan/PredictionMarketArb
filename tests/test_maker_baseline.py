"""Tests for analysis/maker_baseline.py: Vihaan's unchanged QuoteSim driven over the curated data.

    .venv/bin/python -m unittest discover -s tests
"""
import contextlib
import csv
import io
import json
import os
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from analysis.data import open_data  # noqa: E402
from analysis.maker_baseline import FIELDS, run_baseline  # noqa: E402
from recorder.compact import BOOKS_SCHEMA, COVERAGE_SCHEMA, TRADES_SCHEMA, write_manifest  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

D = "2026-11-18"
T = 1_794_960_000_000          # 2026-11-18 00:00:00 UTC
PAIR = {"pair_id": "P", "family": "fam", "rule_status": "read-close", "est_resolution": "2027-11-18",
        "kalshi_ticker": "K", "kalshi_question": "Will X?", "kalshi_fee_rate": 0.0, "kalshi_maker_fee_rate": 0.0,
        "poly_market_id": "1", "poly_question": "Will X?", "poly_yes_token": "Y", "poly_condition_id": "c",
        "poly_fee_rate": 0.0, "poly_maker_fee_rate": 0.0, "poly_min_order": 0.0}


def write(store, key, rows, schema):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buf)
    store.write(key, buf.getvalue())


def book(ms, market, yes=(), no=()):
    return dict(ts_ms=T + ms, server_ms=None, market=market, source="delta", suspect=False, suspect_levels=None,
                yes_bids=[{"price": p, "size": s} for p, s in yes], no_bids=[{"price": p, "size": s} for p, s in no])


class LiveBaselineTest(unittest.TestCase):
    def test_kalshi_quote_filled_and_hedged(self):
        with tempfile.TemporaryDirectory() as root:
            store = LocalStore(root)
            store.write(f"ref/pairs/date={D}/pairs.json", json.dumps({"pairs": [PAIR]}).encode())
            # Kalshi best YES bid 0.40 (100 shown); Polymarket NO bids at 0.50 -> YES asks at 0.50: a YES bid at
            # 0.40 on Kalshi hedged by NO on Polymarket at 1 - 0.50... edge 1 - 0.40 - 0.50 = 0.10 per pair.
            write(store, f"curated/books/venue=kalshi/date={D}/part-0.parquet",
                  [book(1000, "K", yes=[(0.40, 100)]), book(60_000, "K", yes=[(0.40, 100)])], BOOKS_SCHEMA)
            write(store, f"curated/books/venue=poly/date={D}/part-0.parquet",
                  [book(1000, "Y", yes=[(0.50, 500)]), book(30_000, "Y", yes=[(0.50, 500)])], BOOKS_SCHEMA)
            write(store, f"curated/coverage/date={D}/part-0.parquet",
                  [dict(recv_ms=T, venue="recorder", event="started", detail=None)], COVERAGE_SCHEMA)
            # 20 s later (past the 10 s minimum rest), a taker sells YES at 0.40 on Kalshi: 150 contracts, of which
            # 100 are ahead of us in the queue, so our quote gets 50.
            write(store, f"curated/trades/venue=kalshi/date={D}/part-0.parquet",
                  [dict(trade_id="t1", market="K", ts_ms=T + 21_000, yes_price=0.40, size=150.0, taker="sell_yes",
                        source="ws", recv_ms=T + 21_000)], TRADES_SCHEMA)
            write_manifest(store)
            rows, stats, b = run_baseline(open_data(root), log=lambda *_: None)
        queue = [r for r in rows if r["model"] == "queue"]
        self.assertEqual(len(queue), 1)
        f = queue[0]
        self.assertEqual((f["maker_venue"], f["side"], f["kind"], f["price"], f["size"], f["hedged"]),
                         ("kalshi", "yes", "touch", 0.4, 50.0, 50.0))
        self.assertEqual(f["hedge_delay_s"], 9.0)            # hedged at the next book update (Polymarket, +30 s)
        self.assertEqual((f["profit"], f["realized_edge_c"]), (5.0, 10.0))
        self.assertEqual([r for r in rows if r["model"] == "through"], [])   # no trade printed below 0.40


class LegacyMatchTest(unittest.TestCase):
    """On Vihaan's SQLite recording, the baseline produces exactly his analyze_maker.py fills (his trade-coverage
    marks turned out not to affect any fill)."""

    DB = os.path.join(ROOT, "data", "books.sqlite")

    @unittest.skipUnless(os.path.exists(DB), "data/books.sqlite not present")
    def test_matches_original_maker_fills(self):
        for sub in ("analysis", "tools"):
            if os.path.join(ROOT, sub) not in sys.path:
                sys.path.insert(0, os.path.join(ROOT, sub))
        import analyze_maker
        import migrate_sqlite
        with tempfile.TemporaryDirectory() as tmp:
            ref_csv, store = os.path.join(tmp, "ref.csv"), os.path.join(tmp, "store")
            saved = sys.argv
            try:
                sys.argv = ["analyze_maker.py", "--db", self.DB, "--csv", ref_csv]
                with contextlib.redirect_stdout(io.StringIO()):
                    analyze_maker.main()
                    migrate_sqlite.main(["--db", self.DB, "--store", store])
            finally:
                sys.argv = saved
            write_manifest(LocalStore(store))
            # size=100 (an int), exactly as his CLI default arrives, so the CSV formatting matches too.
            rows, _, _ = run_baseline(open_data(store), log=lambda *_: None, size=100)
            with open(ref_csv, encoding="utf-8") as f:
                ref = list(csv.DictReader(f))
        self.assertEqual(len(rows), len(ref))
        for a, b in zip(ref, rows):
            self.assertEqual(a, {k: "" if b[k] is None else str(b[k]) for k in FIELDS})


if __name__ == "__main__":
    unittest.main()
