"""Tests for analysis/episodes.py: live-day episode rules on synthetic curated data, and the legacy
reproduction gate (the port must reproduce Vihaan's original analyzer exactly).

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
from analysis.episodes import LEGACY_FIELDS, run_episodes  # noqa: E402
from recorder.compact import BOOKS_SCHEMA, COVERAGE_SCHEMA, write_manifest  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

D = "2026-11-18"
T = 1_794_960_000_000          # 2026-11-18 00:00:00 UTC
PAIR = {"pair_id": "P", "family": "fam", "rule_status": "read-close", "est_resolution": "2027-11-18",
        "kalshi_ticker": "K", "kalshi_question": "Will X?", "kalshi_fee_rate": 0.0,
        "poly_market_id": "1", "poly_question": "Will X?", "poly_yes_token": "Y", "poly_condition_id": "c",
        "poly_fee_rate": 0.0, "poly_min_order": 0.0}


def lv(*pairs):
    return [{"price": p, "size": s} for p, s in pairs]


def book(ms, venue, market, yes=(), no=(), suspect=False, levels=None, source="delta"):
    return dict(ts_ms=T + ms, server_ms=None, market=market, yes_bids=lv(*yes), no_bids=lv(*no), source=source,
                suspect=suspect, suspect_levels=levels)


# Profitable P_YES+K_NO: buy YES on Polymarket at 1 - 0.6 = 0.40 and NO on Kalshi at 1 - 0.5 = 0.50 -> 0.90 a pair.
EDGE_POLY = dict(no=[(0.6, 10)])
EDGE_KALSHI = dict(yes=[(0.5, 10)])
NO_EDGE_POLY = dict(no=[(0.4, 10)])           # 0.60 + 0.50 = 1.10: no profit


class Store:
    def __init__(self, root):
        self.store = LocalStore(root)
        self.books = {"kalshi": [], "poly": []}
        self.coverage = []
        self.store.write(f"ref/pairs/date={D}/pairs.json", json.dumps({"pairs": [PAIR]}).encode())
        self.store.write(f"ref/rates/date={D}/tbill.json", json.dumps({"rate_date": D, "yield_pct": 4.0}).encode())

    def k(self, ms, **kw):
        self.books["kalshi"].append(book(ms, "kalshi", "K", **kw))

    def p(self, ms, **kw):
        self.books["poly"].append(book(ms, "poly", "Y", **kw))

    def cov(self, ms, venue, event, **detail):
        self.coverage.append(dict(recv_ms=T + ms, venue=venue, event=event,
                                  detail=json.dumps(detail) if detail else None))

    def finish(self):
        for venue, rows in self.books.items():
            buf = io.BytesIO()
            pq.write_table(pa.Table.from_pylist(rows, schema=BOOKS_SCHEMA), buf)
            self.store.write(f"curated/books/venue={venue}/date={D}/part-0.parquet", buf.getvalue())
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(self.coverage, schema=COVERAGE_SCHEMA), buf)
        self.store.write(f"curated/coverage/date={D}/part-0.parquet", buf.getvalue())
        write_manifest(self.store)


class LiveEpisodesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(self.tmp.name)
        self.s.cov(0, "recorder", "started")

    def tearDown(self):
        self.tmp.cleanup()

    def run_it(self):
        self.s.finish()
        rows, run = run_episodes(open_data(self.tmp.name), log=lambda *_: None)
        return rows, run

    def test_edge_appears_persists_and_goes(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)                   # same millisecond: evaluated once, after both apply
        self.s.p(3000, no=[(0.6, 20)])                # still profitable, more size
        self.s.p(9000, **NO_EDGE_POLY)                # gone
        rows, _ = self.run_it()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["basket"], r["end_reason"], r["duration_s"], r["cycles"]), ("P_YES+K_NO", "edge_gone", 8.0, 2))
        self.assertEqual((r["peak_size"], r["peak_profit"]), (10.0, 1.0))     # Kalshi depth limits it to 10
        self.assertEqual(r["tbill_pct"], 4.0)
        self.assertFalse(r["uncertain"])

    def test_latency_checks_use_books_as_they_stood(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)
        self.s.p(1200, no=[(0.6, 4)])                 # 200 ms later only 4 pairs are left
        self.s.p(1800, **NO_EDGE_POLY)                # gone before the 1 s check
        self.s.k(9000, **EDGE_KALSHI)                 # data continues past the 5 s check
        rows, _ = self.run_it()
        r = next(x for x in rows if x["start_utc"].endswith("00:00:01"))
        self.assertEqual((r["profit_50ms"], r["size_50ms"]), (1.0, 10.0))
        self.assertEqual((r["profit_250ms"], r["size_250ms"]), (0.4, 4.0))
        self.assertEqual((r["profit_1000ms"], r["size_1000ms"]), (0.0, 0.0))
        self.assertEqual(r["profit_5000ms"], 0.0)

    def test_latency_after_end_of_data_is_unknown(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)
        self.s.p(1100, no=[(0.6, 10)])
        rows, _ = self.run_it()
        r = rows[0]
        self.assertEqual(r["profit_50ms"], 1.0)
        self.assertIsNone(r["profit_250ms"])          # data ended at +100 ms: nothing known at +250 ms
        self.assertEqual(r["end_reason"], "still_open")

    def test_recorder_stop_cuts_episode_and_gap_is_counted(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)
        self.s.cov(5000, "recorder", "stopped")
        self.s.cov(65000, "recorder", "started")
        self.s.p(66000, **EDGE_POLY)                  # only Polymarket refreshed: Kalshi still unknown
        self.s.k(70000, **EDGE_KALSHI)                # now both known again: a new episode
        rows, run = self.run_it()
        reasons = sorted((r["start_utc"][-8:], r["end_reason"]) for r in rows)
        self.assertEqual(reasons, [("00:00:01", "coverage_gap"), ("00:01:10", "still_open")])
        self.assertEqual(run.gaps, [(T + 5000, T + 65000)])

    def test_crash_without_stop_cuts_at_last_data(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)
        self.s.p(2000, no=[(0.6, 12)])
        self.s.cov(30000, "recorder", "started")      # no "stopped": the recorder died
        rows, run = self.run_it()
        self.assertEqual((rows[0]["end_reason"], rows[0]["duration_s"]), ("coverage_gap", 1.0))
        self.assertEqual(run.gaps, [(T + 2000, T + 30000)])

    def test_market_closed_ends_episode(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)
        self.s.cov(4000, "kalshi", "market_closed", market="K")
        rows, _ = self.run_it()
        self.assertEqual(rows[0]["end_reason"], "market_closed")

    def test_uncertain_only_when_walk_reaches_marked_level(self):
        self.s.k(1000, **EDGE_KALSHI)
        # Wrong level deep in Polymarket's NO bids (price 0.2 -> YES ask 0.8): the walk stops at 0.40 / next level.
        self.s.p(1000, no=[(0.6, 10), (0.55, 5), (0.2, 5)], suspect=True, levels=[{"side": "no", "price": 0.2}])
        self.s.p(2000, **NO_EDGE_POLY)
        # Wrong level at the top of the walked side: uncertain.
        self.s.p(3000, no=[(0.6, 10)], suspect=True, levels=[{"side": "no", "price": 0.6}])
        self.s.p(4000, **NO_EDGE_POLY)
        # Wrong level only on the other side (YES bids): not used by this basket.
        self.s.p(5000, no=[(0.6, 10)], yes=[(0.3, 5)], suspect=True, levels=[{"side": "yes", "price": 0.3}])
        self.s.p(6000, **NO_EDGE_POLY)
        # Suspect with unknown levels: the whole book is uncertain.
        self.s.p(7000, no=[(0.6, 10)], suspect=True, levels=None)
        self.s.p(8000, **NO_EDGE_POLY)
        rows, _ = self.run_it()
        got = sorted((r["start_utc"][-2:], r["uncertain"]) for r in rows if r["basket"] == "P_YES+K_NO")
        self.assertEqual(got, [("01", False), ("03", True), ("05", False), ("07", True)])

    def test_start_metrics_capital_carry_and_excess(self):
        self.s.k(1000, **EDGE_KALSHI)                 # Kalshi NO at 0.50 x 10 = $5.00
        self.s.p(1000, **EDGE_POLY)                   # Polymarket YES at 0.40 x 10 = $4.00
        self.s.p(3000, no=[(0.6, 20)])
        rows, _ = self.run_it()
        r = rows[0]
        self.assertEqual((r["start_profit"], r["start_size"], r["start_capital"]), (1.0, 10.0, 9.0))
        self.assertEqual((r["start_capital_kalshi"], r["start_capital_poly"]), (5.0, 4.0))
        # Carry: Kalshi's 3.50% APY on the $5 Kalshi leg; no Polymarket holding rewards -> 5 * 3.5 / 9.
        self.assertEqual(r["carry_pct"], round(5 * 3.5 / 9, 2))
        # The total is computed from unrounded parts, so allow for the parts' rounding.
        self.assertLessEqual(abs(r["start_total_annualized_pct"] - (r["start_annualized_pct"] + r["carry_pct"])), 0.15)
        self.assertEqual(r["start_excess_pct"], round(r["start_total_annualized_pct"] - 4.0, 1))
        self.assertFalse(r["est_resolution_passed"])

    def test_capital_budgets(self):
        self.s.k(1000, yes=[(0.5, 100_000)])
        self.s.p(1000, no=[(0.6, 100_000)])          # $0.90 a pair, $0.10 edge, lots of depth
        rows, _ = self.run_it()
        r = rows[0]
        self.assertAlmostEqual(r["size_1k"], (1000 - 0.01) / 0.9, places=1)
        self.assertAlmostEqual(r["profit_1k"], r["size_1k"] * 0.1, places=1)
        self.assertAlmostEqual(r["profit_50k"], (50_000 - 0.01) / 0.9 * 0.1, places=0)

    def test_restart_split_is_one_opportunity(self):
        self.s.k(1000, **EDGE_KALSHI)
        self.s.p(1000, **EDGE_POLY)
        self.s.cov(5000, "recorder", "stopped")
        self.s.cov(65000, "recorder", "started")      # back within 120 s
        self.s.k(66000, **EDGE_KALSHI)
        self.s.p(66000, **EDGE_POLY)
        self.s.p(70000, **NO_EDGE_POLY)
        self.s.p(400_000, **EDGE_POLY)                # a separate opportunity much later
        rows, _ = self.run_it()
        ids = [r["opportunity_id"] for r in sorted(rows, key=lambda r: r["start_utc"])]
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids[0], ids[1])
        self.assertNotEqual(ids[1], ids[2])

    def test_actionable_needs_rules_budget_excess_and_latency(self):
        self.s.k(1000, yes=[(0.5, 100_000)])
        self.s.p(1000, no=[(0.6, 100_000)])          # 11% return to a resolution a year out: beats the T-bill
        self.s.p(10_000, no=[(0.6, 100_000)])
        rows, _ = self.run_it()
        r = rows[0]
        self.assertTrue(r["actionable"])
        self.assertGreater(r["profit_1000ms"], 0)
        # Same opportunity, but the rules were only read on a sibling market: not actionable.
        global PAIR
        saved = PAIR
        try:
            PAIR = {**PAIR, "rule_status": "template-read"}
            self.tearDown()
            self.setUp()
            self.s.k(1000, yes=[(0.5, 100_000)])
            self.s.p(1000, no=[(0.6, 100_000)])
            self.s.p(10_000, no=[(0.6, 100_000)])
            rows, _ = self.run_it()
            self.assertFalse(rows[0]["actionable"])
        finally:
            PAIR = saved


class LegacyReproductionTest(unittest.TestCase):
    """The acceptance gate: on Vihaan's SQLite recording, the port reproduces analyze_stream.py exactly
    (every original column of every episode, in the same order)."""

    DB = os.path.join(ROOT, "data", "books.sqlite")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    @unittest.skipUnless(os.path.exists(DB), "data/books.sqlite not present")
    def test_reproduces_original_episodes(self):
        for sub in ("analysis", "tools"):
            if os.path.join(ROOT, sub) not in sys.path:
                sys.path.insert(0, os.path.join(ROOT, sub))
        import analyze_stream
        import migrate_sqlite
        ref_csv = os.path.join(self.tmp.name, "ref.csv")
        store = os.path.join(self.tmp.name, "store")
        saved_argv = sys.argv
        try:
            sys.argv = ["analyze_stream.py", "--db", self.DB, "--csv", ref_csv]
            with contextlib.redirect_stdout(io.StringIO()):
                analyze_stream.main()
                migrate_sqlite.main(["--db", self.DB, "--store", store])
        finally:
            sys.argv = saved_argv
        write_manifest(LocalStore(store))
        rows, _ = run_episodes(open_data(store), log=lambda *_: None)
        with open(ref_csv, encoding="utf-8") as f:
            ref = list(csv.DictReader(f))
        self.assertEqual(len(rows), len(ref))
        for a, b in zip(ref, rows):
            self.assertEqual({k: a[k] for k in LEGACY_FIELDS},
                             {k: "" if b[k] is None else str(b[k]) for k in LEGACY_FIELDS})


if __name__ == "__main__":
    unittest.main()
