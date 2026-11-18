"""Tests for analysis/maker_live.py: queue tracking, latency, stale quotes, untimed trades, caps, gaps.

    .venv/bin/python -m unittest discover -s tests
"""
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
from analysis.maker_live import Order, run_live, summary  # noqa: E402
from recorder.compact import BOOKS_SCHEMA, COVERAGE_SCHEMA, TRADES_SCHEMA, write_manifest  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

D = "2026-11-18"
T = 1_794_960_000_000          # 2026-11-18 00:00:00 UTC
# One year to resolution and no fees: the 10%/yr hurdle on $0.90 of capital is 9 c, so a 10 c edge is quoted.
PAIR = {"pair_id": "P", "family": "fam", "rule_status": "read-close", "est_resolution": "2027-11-18",
        "kalshi_ticker": "K", "kalshi_question": "Will X?", "kalshi_fee_rate": 0.0, "kalshi_maker_fee_rate": 0.0,
        "poly_market_id": "1", "poly_question": "Will X?", "poly_yes_token": "Y", "poly_condition_id": "c",
        "poly_fee_rate": 0.0, "poly_maker_fee_rate": 0.0, "poly_min_order": 5.0}
W, MODEL = 2000, "pessimistic"


class QueueTest(unittest.TestCase):
    def order(self, shown=100.0, latency=0):
        return Order(0.40, 100.0, 0, latency, 0.10, 0.90, shown)

    def test_trade_then_book_counts_once(self):
        o = self.order()
        self.assertEqual(o.on_trade_at_price(10, 30, True, W, MODEL), 0)      # all 30 were ahead of us
        o.on_book(11, 70, W, MODEL)
        self.assertEqual((o.ahead, o.pending), (70, []))

    def test_book_then_trade_counts_once(self):
        """The book shows the drop first: it waits for its trade instead of also counting as a cancellation."""
        for model in ("pessimistic", "pro-rata"):
            o = self.order()
            o.on_book(10, 70, W, model)
            self.assertEqual(o.ahead, 100)                                     # maybe a trade not seen yet
            self.assertEqual(o.on_trade_at_price(11, 30, True, W, model), 0)
            o.on_book(5000, 70, W, model)
            self.assertEqual(o.ahead, 70, model)

    def test_cancellations_pessimistic_and_pro_rata(self):
        for model, want in (("pessimistic", 100), ("pro-rata", 80)):
            o = self.order()
            o.on_book(10, 150, W, model)                                       # 50 joined behind us
            o.on_book(20, 120, W, model)                                       # 30 cancelled, no trade
            o.on_book(20 + W, 120, W, model)
            self.assertAlmostEqual(o.ahead, want, msg=model)                   # pro-rata: 30 x 100/150 from ahead

    def test_cancellations_beyond_those_behind_move_us_up(self):
        o = self.order()
        o.on_book(10, 110, W, MODEL)
        o.on_book(20, 60, W, MODEL)
        o.on_book(20 + W, 60, W, MODEL)
        self.assertEqual(o.ahead, 60)

    def test_size_added_before_the_order_arrives_is_ahead(self):
        o = self.order(latency=1000)
        o.on_book(500, 150, W, MODEL)                                          # before it reaches the venue
        o.on_book(1500, 200, W, MODEL)                                         # after: behind it
        self.assertEqual(o.ahead, 150)

    def test_trade_beyond_the_queue_fills_and_keeps_place(self):
        o = self.order()
        self.assertEqual(o.on_trade_at_price(10, 130, True, W, MODEL), 30)
        self.assertEqual(o.ahead, 0)


def write(store, key, rows, schema):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buf)
    store.write(key, buf.getvalue())


def book(ms, market, yes=(), no=()):
    return dict(ts_ms=T + ms, server_ms=None, market=market, source="delta", suspect=False, suspect_levels=None,
                yes_bids=[{"price": p, "size": s} for p, s in yes], no_bids=[{"price": p, "size": s} for p, s in no])


def trade(tid, ms, market, price, size, taker="sell_yes", timed=True):
    return dict(trade_id=tid, market=market, ts_ms=T + ms, yes_price=price, size=size, taker=taker, source="rest",
                recv_ms=T + ms if timed else None, tx_hash=None)


def cov(ms, event, venue="recorder"):
    return dict(recv_ms=T + ms, venue=venue, event=event, detail=None)


class LiveMakerTest(unittest.TestCase):
    def run_case(self, kalshi, poly, trades, coverage=(), **kw):
        with tempfile.TemporaryDirectory() as root:
            store = LocalStore(root)
            store.write(f"ref/pairs/date={D}/pairs.json", json.dumps({"pairs": [PAIR]}).encode())
            write(store, f"curated/books/venue=kalshi/date={D}/part-0.parquet", kalshi, BOOKS_SCHEMA)
            write(store, f"curated/books/venue=poly/date={D}/part-0.parquet", poly, BOOKS_SCHEMA)
            write(store, f"curated/coverage/date={D}/part-0.parquet", [cov(0, "started"), *coverage], COVERAGE_SCHEMA)
            by_venue = {}
            for venue, t in trades:
                by_venue.setdefault(venue, []).append(t)
            for venue, rows in by_venue.items():
                write(store, f"curated/trades/venue={venue}/date={D}/part-0.parquet", rows, TRADES_SCHEMA)
            write_manifest(store)
            kw.setdefault("cancel_models", ["pessimistic"])
            rows, m = run_live(open_data(root), log=lambda *_: None, **kw)
            self.assertTrue(summary(rows, m))                                  # renders
            return rows, m

    # Kalshi: YES bid 0.40 (100 shown), YES ask 0.45. Polymarket YES bids 0.50 -> NO asks 0.50. Quote: bid YES on
    # Kalshi, hedge by buying NO on Polymarket: edge 1 - 0.40 - 0.50 = 10 c.
    K0 = book(1000, "K", yes=[(0.40, 100)], no=[(0.55, 100)])
    P0 = book(1000, "Y", yes=[(0.50, 500)])

    def kalshi_fills(self, rows):
        return [r for r in rows if r["maker_venue"] == "kalshi"]

    def test_fill_hedge_after_latency_and_markouts(self):
        kalshi = [self.K0, book(50_000, "K", yes=[(0.38, 100)], no=[(0.55, 100)]),
                  book(100_000, "K", yes=[(0.38, 100)], no=[(0.55, 100)])]
        poly = [self.P0, book(23_000, "Y", yes=[(0.45, 500)])]               # hedge gets dearer 2 s after the fill
        trades = [("kalshi", trade("t1", 21_000, "K", 0.40, 150))]           # 100 ahead of us, 50 fill us
        rows, m = self.run_case(kalshi, poly, trades, latencies=[250, 5000])
        got = {r["latency_ms"]: r for r in self.kalshi_fills(rows)}
        self.assertEqual(sorted(got), [250, 5000])
        fast, slow = got[250], got[5000]
        self.assertEqual((fast["kind"], fast["size"], fast["hedged"], fast["hedge_status"], fast["stale"]),
                         ("touch", 50.0, 50.0, "full", False))
        self.assertEqual((fast["profit"], fast["realized_edge_c"]), (5.0, 10.0))     # hedged at 0.50 before the move
        self.assertEqual((slow["profit"], slow["realized_edge_c"]), (2.5, 5.0))      # hedged at 0.55 after it
        # Kalshi mid (0.40 + 0.45) / 2 = 0.425 at 1 s and 10 s; (0.38 + 0.45) / 2 = 0.415 at 60 s.
        self.assertEqual((fast["markout_0s_c"], fast["markout_1s_c"], fast["markout_10s_c"], fast["markout_60s_c"]),
                         (2.5, 2.5, 2.5, 1.5))                               # the mid moved 1 c against us by 60 s
        self.assertEqual(fast["rule_status"], "read-close")

    def test_cancel_takes_latency_to_land_and_stale_quote_is_hit(self):
        kalshi = [self.K0, book(30_000, "K", yes=[(0.40, 100)], no=[(0.55, 100)])]
        poly = [self.P0, book(10_000, "Y", yes=[(0.45, 500)])]               # edge gone at 10 s: cancel
        trades = [("kalshi", trade("t1", 12_000, "K", 0.40, 150))]
        rows, _ = self.run_case(kalshi, poly, trades, latencies=[250, 5000])
        got = {r["latency_ms"]: r for r in self.kalshi_fills(rows)}
        self.assertNotIn(250, got)                                           # cancel landed at 10.25 s
        self.assertEqual((got[5000]["size"], got[5000]["stale"]), (50.0, True))   # still resting until 15 s

    def test_order_not_fillable_before_it_reaches_the_venue(self):
        kalshi = [self.K0, book(30_000, "K", yes=[(0.40, 100)], no=[(0.55, 100)])]
        trades = [("kalshi", trade("t1", 3_000, "K", 0.40, 150))]            # placed at 1 s, live from 6 s
        rows, _ = self.run_case(kalshi, [self.P0], trades, latencies=[5000])
        self.assertEqual(self.kalshi_fills(rows), [])

    def test_untimed_polymarket_trade(self):
        """Quote on Polymarket (YES bid 0.40), hedged on Kalshi (NO asks 0.50). A trade without a receive time is
        known at the end of its second and fills only an order that was live at the start of that second."""
        kalshi = [book(1000, "K", yes=[(0.50, 500)]), book(30_000, "K", yes=[(0.50, 500)])]
        poly = [book(1000, "Y", yes=[(0.40, 10)], no=[(0.55, 10)])]
        trades = [("poly", trade("p1", 1000, "Y", 0.40, 20, timed=False)),   # its second began before 1.25 s
                  ("poly", trade("p2", 5000, "Y", 0.40, 20, timed=False))]
        rows, m = self.run_case(kalshi, poly, trades, latencies=[250])
        fills = [r for r in rows if r["maker_venue"] == "poly"]
        self.assertEqual(len(fills), 1)
        f = fills[0]
        # p1 may have happened before our order arrived, so it neither fills us nor moves us up (only a book
        # update showing the drop would). p2 (known at 5.999 s) takes the 10 shown ahead, then fills 10.
        self.assertEqual((f["size"], f["trade_timed"], f["fill_utc"]), (10.0, False, "2026-11-18 00:00:05"))
        self.assertEqual(m.n_untimed, 2)

    def test_pessimistic_and_pro_rata_cancellations(self):
        kalshi = [self.K0, book(2000, "K", yes=[(0.40, 200)], no=[(0.55, 100)]),    # 100 join behind us
                  book(3000, "K", yes=[(0.40, 100)], no=[(0.55, 100)]),             # 100 cancelled
                  book(30_000, "K", yes=[(0.40, 100)], no=[(0.55, 100)])]
        trades = [("kalshi", trade("t1", 10_000, "K", 0.40, 80))]
        rows, _ = self.run_case(kalshi, [self.P0], trades, latencies=[250],
                                cancel_models=["pessimistic", "pro-rata"])
        got = {r["cancel_model"]: r["size"] for r in self.kalshi_fills(rows)}
        self.assertEqual(got, {"pro-rata": 30.0})                            # pro-rata: 50 ahead left; pessimistic: 100

    def test_cap_sizes_orders_and_stops_quoting(self):
        kalshi = [self.K0, book(30_000, "K", yes=[(0.40, 100)], no=[(0.55, 100)]),
                  book(40_000, "K", yes=[(0.40, 100)], no=[(0.55, 100)])]
        trades = [("kalshi", trade("t1", 21_000, "K", 0.40, 300)),
                  ("kalshi", trade("t2", 45_000, "K", 0.40, 300))]
        rows, m = self.run_case(kalshi, [self.P0], trades, latencies=[250], cap_pair=45.0)
        fills = self.kalshi_fills(rows)
        self.assertEqual([r["size"] for r in fills], [50.0])                 # $45 / $0.90 per hedged pair
        self.assertAlmostEqual(m.worlds[0].peak_total, 45.0)
        self.assertGreater(m.worlds[0].counts["capped_quotes"], 0)

    def test_hedge_in_a_gap_is_unknown(self):
        kalshi = [self.K0, book(45_000, "K", yes=[(0.40, 100)], no=[(0.55, 100)])]
        poly = [self.P0, book(45_000, "Y", yes=[(0.50, 500)])]
        trades = [("kalshi", trade("t1", 21_000, "K", 0.40, 150))]
        rows, _ = self.run_case(kalshi, poly, trades, coverage=[cov(22_000, "stopped"), cov(40_000, "started")],
                                latencies=[5000])
        f = self.kalshi_fills(rows)[0]
        self.assertEqual((f["hedge_status"], f["hedged"], f["profit"]), ("unknown", 0.0, 0.0))
        rows, m = self.run_case(kalshi, poly, trades, coverage=[cov(22_000, "stopped"), cov(40_000, "started")],
                                latencies=[5000])
        self.assertEqual(m.covered_ms, 0)                  # one tick before the stop, one after: the gap isn't covered


if __name__ == "__main__":
    unittest.main()
