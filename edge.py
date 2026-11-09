"""Depth-aware cross-venue edge math. No I/O, so it can be unit tested directly.

Book format used everywhere: yes_bids and no_bids, each a list of [price, size],
best price first. Polymarket's YES asks are converted to NO bids when recorded,
so both venues share this format.
"""
import math
from dataclasses import dataclass

PRICE_DP = 5


def asks_from_opposite_bids(bids):
    """Buying one side means hitting the bids on the other side, at 1 - bid."""
    return [(round(1 - p, PRICE_DP), s) for p, s in bids]


def taker_fee(rate, price):
    """Per-contract taker fee: rate * p * (1 - p)."""
    return rate * price * (1 - price)


@dataclass
class Fill:
    size: float = 0.0          # matched pairs
    capital: float = 0.0       # contract cost plus fees, dollars
    profit: float = 0.0        # guaranteed profit if both legs pay out as one set
    top_edge: float = 0.0      # after-fee edge per pair at the best prices
    kalshi_fee: float = 0.0
    poly_fee: float = 0.0


def walk(poly_asks, kalshi_asks, poly_rate, kalshi_rate, min_edge=0.0, poly_min_size=0.0):
    """Buy matched pairs across both books until the marginal edge reaches min_edge.

    Kalshi fees are rounded up to the next cent on the whole order. Returns a
    Fill with size 0 when no fillable, profitable size exists.
    """
    fill = Fill()
    if not poly_asks or not kalshi_asks:
        return fill
    p_i = k_i = 0
    p_left, k_left = poly_asks[0][1], kalshi_asks[0][1]
    contract_cost = 0.0
    kalshi_fee = poly_fee = 0.0
    first = True
    while p_i < len(poly_asks) and k_i < len(kalshi_asks):
        pp, kp = poly_asks[p_i][0], kalshi_asks[k_i][0]
        pf, kf = taker_fee(poly_rate, pp), taker_fee(kalshi_rate, kp)
        edge = 1 - pp - kp - pf - kf
        if first:
            fill.top_edge = edge
            first = False
        if edge <= min_edge:
            break
        qty = min(p_left, k_left)
        fill.size += qty
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

    if fill.size < poly_min_size or fill.size <= 0:
        top = fill.top_edge
        return Fill(top_edge=top)
    fill.kalshi_fee = math.ceil(round(kalshi_fee * 100, 6)) / 100
    fill.poly_fee = poly_fee
    fill.capital = contract_cost + fill.kalshi_fee + fill.poly_fee
    fill.profit = fill.size - fill.capital
    return fill


def baskets(poly_book, kalshi_book):
    """Yield (basket_name, poly_asks, kalshi_asks) for the two opposing hedges."""
    yield ("P_YES+K_NO",
           asks_from_opposite_bids(poly_book["no_bids"]),
           asks_from_opposite_bids(kalshi_book["yes_bids"]))
    yield ("P_NO+K_YES",
           asks_from_opposite_bids(poly_book["yes_bids"]),
           asks_from_opposite_bids(kalshi_book["no_bids"]))
