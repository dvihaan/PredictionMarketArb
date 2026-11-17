"""Tests for analysis/economics.py: capital per venue, budget-capped walks, carry, dates.

    .venv/bin/python -m unittest discover -s tests
"""
import os
import random
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from analysis.economics import (Carry, annualize, budget_walk, leg_capital,  # noqa: E402
                                worst_case_end)
from edge import walk  # noqa: E402

T = 1_794_960_000_000          # 2026-11-18 00:00 UTC


def random_asks(rng):
    p, out = rng.uniform(0.05, 0.6), []
    for _ in range(rng.randint(1, 8)):
        out.append((round(p, 3), round(rng.uniform(1, 500), 2)))
        p += rng.uniform(0.001, 0.05)
    return out


class BudgetWalkTest(unittest.TestCase):
    def test_no_cap_matches_edge_walk(self):
        rng = random.Random(7)
        for _ in range(500):
            pa, ka = random_asks(rng), random_asks(rng)
            pr, kr = rng.choice([0, 0.04, 0.05]), rng.choice([0, 0.07])
            minsz = rng.choice([0, 5])
            f = walk(pa, ka, pr, kr, poly_min_size=minsz)
            size, cap, profit = budget_walk(pa, ka, pr, kr, float("inf"), poly_min_size=minsz)
            self.assertAlmostEqual(size, f.size, places=6)
            self.assertAlmostEqual(cap, f.capital, places=6)
            self.assertAlmostEqual(profit, f.profit, places=6)

    def test_budget_caps_capital(self):
        pa, ka = [(0.40, 10_000)], [(0.50, 10_000)]          # 0.90 a pair, no fees
        size, cap, profit = budget_walk(pa, ka, 0, 0, 1_000)
        self.assertLessEqual(cap, 1_000)
        self.assertAlmostEqual(size, (1_000 - 0.01) / 0.9, places=4)
        self.assertAlmostEqual(profit, size * 0.1, places=4)

    def test_budget_never_exceeded_with_fees(self):
        rng = random.Random(11)
        for _ in range(300):
            pa, ka = random_asks(rng), random_asks(rng)
            budget = rng.choice([50, 1_000, 10_000])
            size, cap, profit = budget_walk(pa, ka, 0.04, 0.07, budget)
            self.assertLessEqual(cap, budget + 1e-9)

    def test_min_size(self):
        self.assertEqual(budget_walk([(0.40, 100)], [(0.50, 100)], 0, 0, 2, poly_min_size=5), (0.0, 0.0, 0.0))


class LegCapitalTest(unittest.TestCase):
    def test_legs_add_up_to_capital(self):
        rng = random.Random(3)
        for _ in range(200):
            pa, ka = random_asks(rng), random_asks(rng)
            f = walk(pa, ka, 0.04, 0.07)
            p_cap, k_cap = leg_capital(f, pa, ka)
            self.assertAlmostEqual(p_cap + k_cap, f.capital, places=6)


class CarryTest(unittest.TestCase):
    RATES = {"kalshi_apy_pct": [{"from": "2025-10-01", "pct": 4.05}, {"from": "2026-11-01", "pct": 3.5}],
             "polymarket_holding_rewards_pct": [{"from": "2025-09-01", "pct": 4.0}]}

    def test_kalshi_rate_by_date(self):
        c = Carry(self.RATES)
        self.assertEqual((c.kalshi_pct("2026-10-15"), c.kalshi_pct("2026-11-18")), (4.05, 3.5))

    def test_blended_only_counts_eligible_polymarket(self):
        c = Carry(self.RATES)
        # $600 on Kalshi at 3.5%, $400 on Polymarket: not eligible -> 2.1%; eligible at 4% -> 2.1 + 1.6 = 3.7%
        self.assertAlmostEqual(c.blended_pct("2026-11-18", 400, 600, {"holdingRewardsEnabled": False}), 2.1)
        self.assertAlmostEqual(c.blended_pct("2026-11-18", 400, 600, {"holdingRewardsEnabled": True}), 3.7)


class DatesTest(unittest.TestCase):
    def test_worst_case_is_the_later_future_venue_date(self):
        k = {"expected_expiration_time": "2027-11-03T15:00:00Z"}
        p = {"endDate": "2026-11-04T12:00:00Z"}                   # already past at T: ignored
        self.assertEqual(worst_case_end(T, k, p).date().isoformat(), "2027-11-03")
        self.assertIsNone(worst_case_end(T, {}, p))

    def test_annualize(self):
        self.assertAlmostEqual(annualize(0.01, T, "2027-11-18"), 0.01)
        self.assertIsNone(annualize(0.01, T, "2026-11-18"))       # within a day: undefined


if __name__ == "__main__":
    unittest.main()
