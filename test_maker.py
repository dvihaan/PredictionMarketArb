import unittest

from maker import QuoteSim, buy

# Maker venue: YES bid 0.40 x 50. Hedge venue: YES bid 0.42, so NO can be bought at 0.58.
MAKER = {"yes_bids": [(0.40, 50)], "no_bids": [(0.58, 50)]}
HEDGE = {"yes_bids": [(0.42, 500), (0.41, 500)], "no_bids": [(0.56, 500)]}


def sim(**kw):
    kw.setdefault("min_rest_ms", 0)
    return QuoteSim("yes", 0.0, 0.0, False, size=100, **kw)


class QuoteTests(unittest.TestCase):
    def test_quotes_at_best_bid_with_expected_edge(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        self.assertEqual(s.price, 0.40)
        self.assertEqual(s.queue_ahead, 50)
        self.assertAlmostEqual(s.quoted_edge, 0.02)

    def test_no_quote_below_hurdle(self):
        s = sim(min_edge=0.03)
        s.on_books(0, MAKER, HEDGE)
        self.assertIsNone(s.price)

    def test_annualized_hurdle_scales_with_time_to_resolution(self):
        year = 365 * 86400 * 1000
        # 2c on 98c of capital is about 2%: enough for 10% a year over 30 days, not over a year.
        near = sim(min_ann=0.10, resolve_ms=year // 12)
        far = sim(min_ann=0.10, resolve_ms=year)
        near.on_books(0, MAKER, HEDGE)
        far.on_books(0, MAKER, HEDGE)
        self.assertIsNotNone(near.price)
        self.assertIsNone(far.price)

    def test_fees_reduce_the_quoted_edge(self):
        s = QuoteSim("yes", 0.0175, 0.04, False, min_rest_ms=0)
        s.on_books(0, MAKER, HEDGE)
        self.assertAlmostEqual(s.quoted_edge, 0.02 - 0.0175 * 0.4 * 0.6 - 0.04 * 0.58 * 0.42)


class FillTests(unittest.TestCase):
    def test_touch_fills_only_after_the_queue_ahead(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.40, 30, "sell_yes")
        self.assertEqual(s.fills, [])
        self.assertEqual(s.queue_ahead, 20)
        s.on_trade(2000, 0.40, 60, "sell_yes")
        self.assertEqual(len(s.fills), 1)
        self.assertEqual(s.fills[0].size, 40)
        self.assertEqual(s.fills[0].kind, "touch")

    def test_trade_through_fills_regardless_of_queue(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.39, 10, "sell_yes")
        self.assertEqual(s.fills[0].size, 10)
        self.assertEqual(s.fills[0].kind, "through")

    def test_through_only_ignores_touches(self):
        s = sim(through_only=True)
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.40, 500, "sell_yes")
        self.assertEqual(s.fills, [])

    def test_wrong_direction_or_worse_price_does_not_fill(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.40, 500, "buy_yes")
        s.on_trade(1000, 0.41, 500, "sell_yes")
        self.assertEqual(s.fills, [])

    def test_no_bid_is_filled_by_yes_buyers(self):
        s = QuoteSim("no", 0.0, 0.0, False, min_rest_ms=0)
        s.on_books(0, MAKER, HEDGE)          # NO bid 0.58, hedge buys YES at 1 - 0.56 = 0.44: edge -2c
        self.assertIsNone(s.price)
        s = QuoteSim("no", 0.0, 0.0, False, min_rest_ms=0, min_edge=-1)
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.42, 80, "buy_yes")   # a YES buyer at 0.42 hits NO bids at 0.58
        self.assertEqual(s.fills[0].size, 30)
        s.on_books(2000, MAKER, HEDGE)
        s.on_trade(3000, 0.42, 80, "sell_yes")
        self.assertEqual(len(s.fills), 1)

    def test_quote_must_rest_before_it_can_fill(self):
        s = sim(min_rest_ms=10_000)
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(5000, 0.39, 100, "sell_yes")
        self.assertEqual(s.fills, [])
        s.on_trade(10_000, 0.39, 100, "sell_yes")
        self.assertEqual(len(s.fills), 1)

    def test_queue_shrinks_when_displayed_size_drops(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_books(5000, {"yes_bids": [(0.40, 10)], "no_bids": []}, HEDGE)
        self.assertEqual(s.queue_ahead, 10)
        s.on_books(10_000, {"yes_bids": [(0.40, 90)], "no_bids": []}, HEDGE)
        self.assertEqual(s.queue_ahead, 10)

    def test_price_change_goes_to_the_back_of_the_new_queue(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.40, 30, "sell_yes")
        s.on_books(5000, {"yes_bids": [(0.41, 70)], "no_bids": []}, HEDGE)
        self.assertEqual((s.price, s.queue_ahead, s.posted_ms), (0.41, 70, 5000))


class HedgeTests(unittest.TestCase):
    def test_fill_is_hedged_at_the_next_book_not_the_quoted_one(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.39, 100, "sell_yes")
        moved = {"yes_bids": [(0.41, 500)], "no_bids": [(0.56, 500)]}   # NO now costs 0.59
        s.on_books(5000, MAKER, moved)
        f = s.fills[0]
        self.assertEqual(f.hedged, 100)
        self.assertAlmostEqual(f.quoted_edge, 0.02)
        self.assertAlmostEqual(f.profit, 100 * (1 - 0.40 - 0.59))
        self.assertAlmostEqual(f.capital, 99.0)

    def test_thin_hedge_book_leaves_part_unhedged(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_trade(1000, 0.39, 100, "sell_yes")
        s.on_books(5000, MAKER, {"yes_bids": [(0.42, 60)], "no_bids": []})
        f = s.fills[0]
        self.assertEqual(f.hedged, 60)
        self.assertAlmostEqual(f.profit, 60 * 0.02)

    def test_hedge_walks_levels_and_rounds_kalshi_fee_up(self):
        filled, cost, fee = buy([(0.50, 1), (0.60, 5)], 3, 0.07, True)
        self.assertEqual(filled, 3)
        self.assertAlmostEqual(cost, 1.70)
        self.assertAlmostEqual(fee, 0.06)     # 1.75c + 2 * 1.68c = 5.11c, rounded up

    def test_live_time_counts_only_while_quoting(self):
        s = sim()
        s.on_books(0, MAKER, HEDGE)
        s.on_books(5000, MAKER, HEDGE)
        s.cancel(8000)
        s.on_books(20_000, MAKER, HEDGE)
        s.on_books(25_000, MAKER, HEDGE)
        self.assertEqual(s.live_ms, 13_000)


if __name__ == "__main__":
    unittest.main()
