"""Rebuild order books from WebSocket messages. No I/O, so it can be unit tested directly.

Both the live recorder (gap checks) and the compaction job (curated books) use this module,
so there is one implementation of the book-building rules.

Output format matches record.py and edge.py: lists of (price, size), best price first.
Prices are rounded to 5 decimals and sizes to 4, as record.levels() did.

Kalshi sends bids only, per side: a YES bid at p is the same order as a NO ask at 1 - p.
Polymarket sends each token's bids and asks; for a token, "yes" means that token.
"""
PRICE_DP = 5
SIZE_DP = 4
DEPTH = 25
EPS = 1e-9


def _price(text):
    return round(float(text), PRICE_DP)


def _size(text):
    return round(float(text), SIZE_DP)


def _best_first(levels, n, reverse):
    out = sorted(((p, s) for p, s in levels.items() if s > EPS), key=lambda z: z[0], reverse=reverse)
    return out[:n]


class KalshiBook:
    """One Kalshi market: YES bids and NO bids, keyed by price."""

    def __init__(self):
        self.yes, self.no = {}, {}

    @classmethod
    def from_snapshot(cls, msg):
        """msg is the "msg" body of an orderbook_snapshot. A side with no orders is absent."""
        b = cls()
        for side, key in ((b.yes, "yes_dollars_fp"), (b.no, "no_dollars_fp")):
            for p, s in msg.get(key) or []:
                if float(s) > 0:
                    side[_price(p)] = _size(s)
        return b

    def apply_delta(self, msg):
        """msg is the "msg" body of an orderbook_delta: a signed size change at one price."""
        side = self.yes if msg["side"] == "yes" else self.no
        p = _price(msg["price_dollars"])
        s = round(side.get(p, 0.0) + float(msg["delta_fp"]), SIZE_DP)
        if s > EPS:
            side[p] = s
        else:
            side.pop(p, None)

    def yes_bids(self, n=DEPTH):
        return _best_first(self.yes, n, reverse=True)

    def no_bids(self, n=DEPTH):
        return _best_first(self.no, n, reverse=True)


class SeqTracker:
    """Detects skipped Kalshi messages: seq counts up by one per subscription (sid)."""

    def __init__(self):
        self.last = {}

    def gap(self, sid, seq):
        """True if messages were skipped before this one. Tracking continues from seq either way."""
        prev = self.last.get(sid)
        self.last[sid] = seq
        return prev is not None and seq != prev + 1

    def reset(self):
        """Call after reconnecting: sids and seq numbers start again."""
        self.last.clear()


class PolyBook:
    """One Polymarket token: its bids and asks, keyed by price."""

    def __init__(self):
        self.bid, self.ask = {}, {}
        self.timestamp_ms = None
        self.hash = None

    @classmethod
    def from_book(cls, msg):
        """msg is a "book" event. Levels can arrive in any order."""
        b = cls()
        for side, key in ((b.bid, "bids"), (b.ask, "asks")):
            for lvl in msg.get(key) or []:
                if float(lvl["size"]) > 0:
                    side[_price(lvl["price"])] = _size(lvl["size"])
        b.timestamp_ms = int(msg["timestamp"]) if msg.get("timestamp") else None
        b.hash = msg.get("hash")
        return b

    def apply_change(self, change, timestamp_ms):
        """change is one entry of a price_change event. size is the new total at that price; 0 removes it."""
        side = self.bid if change["side"] == "BUY" else self.ask
        p, s = _price(change["price"]), _size(change["size"])
        if s > EPS:
            side[p] = s
        else:
            side.pop(p, None)
        self.timestamp_ms = timestamp_ms

    def bids(self, n=DEPTH):
        return _best_first(self.bid, n, reverse=True)

    def asks(self, n=DEPTH):
        return _best_first(self.ask, n, reverse=False)

    def yes_bids(self, n=DEPTH):
        """The token's bids, in the yes_bids/no_bids format record.py used."""
        return self.bids(n)

    def no_bids(self, n=DEPTH):
        """The token's asks seen as bids for the other outcome: an ask at p is a bid at 1 - p."""
        return [(round(1 - p, PRICE_DP), s) for p, s in self.asks(n)]
