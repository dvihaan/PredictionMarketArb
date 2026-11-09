import unittest

from edge import asks_from_opposite_bids, baskets, walk

# Worked example: edges of 2.25c for 300 pairs, 1.25c for 100, 0.25c for 400.
POLY = [(0.46, 300), (0.47, 500), (0.49, 1000)]
KALSHI = [(0.50, 400), (0.51, 600), (0.53, 1000)]


class WalkTests(unittest.TestCase):
    def test_walks_to_zero_marginal_edge(self):
        f = walk(POLY, KALSHI, 0.0, 0.07)
        self.assertEqual(f.size, 800)
        self.assertAlmostEqual(f.profit, 9.00, places=2)
        self.assertAlmostEqual(f.capital, 791.00, places=2)
        self.assertAlmostEqual(f.top_edge, 0.0225, places=6)

    def test_hurdle_stops_early(self):
        f = walk(POLY, KALSHI, 0.0, 0.07, min_edge=0.016)
        self.assertEqual(f.size, 300)
        self.assertAlmostEqual(f.profit, 6.75, places=2)

    def test_no_edge_has_no_fill_but_reports_top_edge(self):
        f = walk([(0.55, 100)], [(0.50, 100)], 0.0, 0.07)
        self.assertEqual(f.size, 0)
        self.assertLess(f.top_edge, 0)

    def test_kalshi_fee_rounds_up_on_small_orders(self):
        # One contract at 50c has a 1.75c fee, rounded up to 2c.
        f = walk([(0.40, 1)], [(0.50, 1)], 0.0, 0.07)
        self.assertAlmostEqual(f.kalshi_fee, 0.02, places=6)
        self.assertAlmostEqual(f.profit, 0.08, places=6)

    def test_poly_fee_is_charged(self):
        f = walk([(0.40, 100)], [(0.50, 100)], 0.04, 0.0)
        self.assertAlmostEqual(f.poly_fee, 100 * 0.04 * 0.4 * 0.6, places=6)

    def test_below_polymarket_minimum_is_not_fillable(self):
        f = walk([(0.40, 3)], [(0.50, 100)], 0.0, 0.0, poly_min_size=5)
        self.assertEqual(f.size, 0)
        self.assertAlmostEqual(f.top_edge, 0.10, places=6)

    def test_empty_book(self):
        self.assertEqual(walk([], KALSHI, 0.0, 0.07).size, 0)


class BookTests(unittest.TestCase):
    def test_opposite_bids_become_asks(self):
        self.assertEqual(asks_from_opposite_bids([(0.95, 10), (0.94, 20)]), [(0.05, 10), (0.06, 20)])

    def test_baskets_use_the_correct_sides(self):
        poly = {"yes_bids": [(0.40, 5)], "no_bids": [(0.58, 7)]}   # YES ask 0.42
        kalshi = {"yes_bids": [(0.45, 9)], "no_bids": [(0.53, 11)]}  # YES ask 0.47
        out = {name: (p, k) for name, p, k in baskets(poly, kalshi)}
        self.assertEqual(out["P_YES+K_NO"], ([(0.42, 7)], [(0.55, 9)]))
        self.assertEqual(out["P_NO+K_YES"], ([(0.60, 5)], [(0.47, 11)]))


if __name__ == "__main__":
    unittest.main()
