"""Simulate resting a bid on one venue and hedging each fill as a taker on the other.

No I/O, so it can be unit tested directly. One QuoteSim is one standing quote: it joins the
best bid for one side on the maker venue whenever the hedged edge clears the hurdle, is
filled by recorded trades, and buys the opposite side on the hedge venue at the next book.

The fill model is deliberately cautious:
  - the quote joins the back of the queue and never improves the price;
  - cancellations ahead of it only count when the displayed size drops below its queue position;
  - a trade can only fill a quote that has rested for min_rest_ms, because trade timestamps
    and book snapshots come from different clocks;
  - the hedge is bought from the first book recorded after the fill, not the one seen before it.
"""
import math
from dataclasses import dataclass

from edge import asks_from_opposite_bids, taker_fee

EPS = 1e-9
YEAR_MS = 365 * 86400 * 1000


@dataclass
class MakerFill:
    ts_ms: int
    price: float               # what the resting bid paid per contract
    size: float
    kind: str                  # "touch": traded at our price; "through": traded past it
    quoted_edge: float         # edge per contract expected when the quote was last refreshed
    maker_fee: float = 0.0
    hedged: float = 0.0        # contracts hedged; less than size if the hedge book was too thin
    hedge_cost: float = 0.0
    hedge_fee: float = 0.0
    hedge_ms: int = 0

    @property
    def capital(self):
        return self.hedged * self.price + self.maker_fee + self.hedge_cost + self.hedge_fee

    @property
    def profit(self):
        """Locked-in profit on the hedged part. Each hedged pair pays out $1."""
        return self.hedged - self.capital


def buy(asks, qty, rate, round_up_cent):
    """Take qty contracts from asks. Returns (filled, cost, fee)."""
    filled = cost = fee = 0.0
    for price, size in asks:
        take = min(size, qty - filled)
        if take <= EPS:
            break
        filled += take
        cost += take * price
        fee += take * taker_fee(rate, price)
    if round_up_cent:
        fee = math.ceil(round(fee * 100, 6)) / 100
    return filled, cost, fee


class QuoteSim:
    def __init__(self, side, maker_rate, hedge_rate, hedge_fee_round_up, size=100.0, min_edge=0.0,
                 min_ann=0.0, resolve_ms=None, min_rest_ms=10_000, through_only=False):
        self.side = side                       # "yes" or "no": the side bid for on the maker venue
        self.bid_key = "yes_bids" if side == "yes" else "no_bids"
        self.maker_rate, self.hedge_rate = maker_rate, hedge_rate
        self.hedge_fee_round_up = hedge_fee_round_up
        self.size, self.min_edge, self.min_ann = size, min_edge, min_ann
        self.resolve_ms, self.min_rest_ms, self.through_only = resolve_ms, min_rest_ms, through_only
        self.price = None                      # None while no quote is resting
        self.posted_ms = self.last_ms = None
        self.queue_ahead = self.quoted_edge = 0.0
        self.live_ms = 0                       # total time a quote was resting
        self.pending, self.fills = [], []

    def hurdle(self, ms, capital):
        """Smallest edge worth quoting for: the flat minimum, or the annualized one if larger."""
        if not self.min_ann or self.resolve_ms is None:
            return self.min_edge
        return max(self.min_edge, self.min_ann * capital * max(self.resolve_ms - ms, 0) / YEAR_MS)

    def cancel(self, ms=None):
        """Pull the quote. Pass the last time it was known to be resting, if any."""
        if self.price is not None and self.last_ms is not None and ms is not None:
            self.live_ms += max(ms - self.last_ms, 0)
        self.price = self.posted_ms = self.last_ms = None

    def on_trade(self, ms, yes_price, size, taker):
        """A trade printed on the maker venue. taker is "buy_yes" or "sell_yes"."""
        if self.price is None or ms - self.posted_ms < self.min_rest_ms:
            return
        if self.side == "yes":
            if taker != "sell_yes":
                return
            traded = yes_price
        else:
            if taker != "buy_yes":
                return
            traded = 1 - yes_price             # a taker buying YES at y hits NO bids at 1 - y
        if traded > self.price + EPS:
            return
        if traded < self.price - EPS:
            kind, qty = "through", min(self.size, size)
        elif self.through_only:
            return
        else:
            ahead = min(self.queue_ahead, size)
            self.queue_ahead -= ahead
            kind, qty = "touch", min(self.size, size - ahead)
            if qty <= EPS:
                return
        fill = MakerFill(ms, self.price, qty, kind, self.quoted_edge,
                         maker_fee=qty * taker_fee(self.maker_rate, self.price))
        self.fills.append(fill)
        self.pending.append(fill)
        self.cancel(ms)                        # requote from the back of the queue at the next book

    def on_books(self, ms, maker_book, hedge_book):
        """New books at ms: hedge anything filled since the last call, then refresh the quote."""
        hedge_asks = asks_from_opposite_bids(hedge_book[self.bid_key])
        for fill in self.pending:
            fill.hedged, fill.hedge_cost, fill.hedge_fee = buy(
                hedge_asks, fill.size, self.hedge_rate, self.hedge_fee_round_up)
            fill.maker_fee *= fill.hedged / fill.size
            fill.hedge_ms = ms
        self.pending = []

        if self.price is not None and self.last_ms is not None:
            self.live_ms += ms - self.last_ms
        bids = maker_book[self.bid_key]
        if not bids or not hedge_asks:
            self.price = self.posted_ms = self.last_ms = None
            return
        bid, shown = bids[0]
        ask = hedge_asks[0][0]
        edge = 1 - bid - ask - taker_fee(self.maker_rate, bid) - taker_fee(self.hedge_rate, ask)
        if edge <= self.hurdle(ms, bid + ask) + EPS:
            self.price = self.posted_ms = self.last_ms = None
            return
        if self.price is None or abs(bid - self.price) > EPS:
            self.price, self.posted_ms, self.queue_ahead = bid, ms, shown
        else:
            self.queue_ahead = min(self.queue_ahead, shown)
        self.quoted_edge = edge
        self.last_ms = ms
