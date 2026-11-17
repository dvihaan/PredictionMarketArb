"""Money questions about a hedge, beyond edge.walk(): capital per venue, profit within a capital budget,
carry (interest the venues pay on capital in positions), and resolution dates for annualizing.

Decisions behind this module: docs/TAKER_REVIEW.md (A1 carry, A2 dates, A3 capital per venue, B2 budgets).
edge.py is unchanged; budget_walk() follows edge.walk()'s rules exactly and only adds a capital cap, and with
no cap it returns the same size, capital and profit as edge.walk() (tested).
"""
import json
import math
import os
from datetime import datetime, timezone

from edge import taker_fee  # noqa: E402  (Vihaan's fee formula, unchanged)

HERE = os.path.dirname(os.path.abspath(__file__))
BUDGETS = (1_000, 10_000, 50_000)


def leg_capital(fill, poly_asks, kalshi_asks):
    """(Polymarket leg, Kalshi leg) capital for a fill: each leg's contract cost for fill.size, plus its fee.
    The two add up to fill.capital."""
    def cost(asks, qty):
        total = 0.0
        for price, size in asks:
            take = min(size, qty)
            if take <= 1e-12:
                break
            total += take * price
            qty -= take
        return total
    if fill.size <= 0:
        return 0.0, 0.0
    return cost(poly_asks, fill.size) + fill.poly_fee, cost(kalshi_asks, fill.size) + fill.kalshi_fee


def budget_walk(poly_asks, kalshi_asks, poly_rate, kalshi_rate, budget, min_edge=0.0, poly_min_size=0.0):
    """edge.walk() with a cap on capital: buy matched pairs, cheapest first, while the marginal edge is above
    min_edge and the capital (contracts + fees, Kalshi's fee rounded up to the cent per order) stays within
    budget. Returns (size, capital, profit); (0, 0, 0) when nothing profitable fits."""
    if not poly_asks or not kalshi_asks:
        return 0.0, 0.0, 0.0
    p_i = k_i = 0
    p_left, k_left = poly_asks[0][1], kalshi_asks[0][1]
    size = contract_cost = poly_fee = kalshi_fee = 0.0
    while p_i < len(poly_asks) and k_i < len(kalshi_asks):
        pp, kp = poly_asks[p_i][0], kalshi_asks[k_i][0]
        pf, kf = taker_fee(poly_rate, pp), taker_fee(kalshi_rate, kp)
        if 1 - pp - kp - pf - kf <= min_edge:
            break
        per_pair = pp + kp + pf + kf
        # Leave a cent of headroom for Kalshi's round-up on the whole order.
        room = budget - 0.01 - (contract_cost + poly_fee + kalshi_fee)
        level_qty = min(p_left, k_left)
        qty = min(level_qty, max(room, 0.0) / per_pair)
        if qty <= 1e-9:
            break
        budget_bound = qty < level_qty - 1e-9
        size += qty
        contract_cost += qty * (pp + kp)
        poly_fee += qty * pf
        kalshi_fee += qty * kf
        p_left -= qty
        k_left -= qty
        if p_left <= 1e-9:
            p_i += 1
            p_left = poly_asks[p_i][1] if p_i < len(poly_asks) else 0
        if k_left <= 1e-9:
            k_i += 1
            k_left = kalshi_asks[k_i][1] if k_i < len(kalshi_asks) else 0
        if budget_bound:
            break                               # the budget ran out inside this level
    if size <= 0 or size < poly_min_size:
        return 0.0, 0.0, 0.0
    capital = contract_cost + poly_fee + math.ceil(round(kalshi_fee * 100, 6)) / 100
    return size, capital, size - capital


# ---- carry --------------------------------------------------------------------------------------------

def _load_rates(path=os.path.join(HERE, "carry_rates.json")):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _rate_on(entries, d):
    """The entry in force on date d (latest 'from' <= d, else the earliest)."""
    before = [e for e in entries if e["from"] <= d]
    return (max(before, key=lambda e: e["from"]) if before else min(entries, key=lambda e: e["from"]))["pct"]


class Carry:
    """Annual interest on each leg's capital, in percent: Kalshi's APY on cash and positions, and Polymarket
    holding rewards only for markets with holdingRewardsEnabled in that day's snapshot."""

    def __init__(self, rates=None):
        self.rates = rates or _load_rates()

    def kalshi_pct(self, d):
        return _rate_on(self.rates["kalshi_apy_pct"], d)

    def poly_pct(self, d, poly_market):
        if not poly_market or not poly_market.get("holdingRewardsEnabled"):
            return 0.0
        return _rate_on(self.rates["polymarket_holding_rewards_pct"], d)

    def blended_pct(self, d, poly_cap, kalshi_cap, poly_market):
        """Carry on the whole position, as a percent per year of total capital."""
        total = poly_cap + kalshi_cap
        if total <= 0:
            return 0.0
        return (kalshi_cap * self.kalshi_pct(d) + poly_cap * self.poly_pct(d, poly_market)) / total


# ---- dates --------------------------------------------------------------------------------------------

def _date(text):
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def worst_case_end(start_ms, kalshi_market, poly_market):
    """The later of the venues' own end dates (Kalshi expected expiration, Polymarket endDate), ignoring any that
    are already past at start_ms. These are often cautious backstops or trading end dates, so this is a
    worst case for how long capital is tied up, not the expected resolution (that stays est_resolution)."""
    start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    dates = [d for d in (_date((kalshi_market or {}).get("expected_expiration_time")),
                         _date((poly_market or {}).get("endDate"))) if d is not None and d > start]
    return max(dates) if dates else None


def annualize(ret, start_ms, end):
    """Annualized simple return to an end datetime (or ISO date string); None within a day of the end."""
    if isinstance(end, str):
        end = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    if end is None:
        return None
    days = (end - datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)).total_seconds() / 86400
    return ret * 365 / days if days > 1 else None
