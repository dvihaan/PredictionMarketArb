"""Live maker model (Stage 3.E): rest a bid on one venue, hedge each fill on the other, on the live data.

The quoting policy is Vihaan's (maker.py): for every pair, four standing quotes (bid YES or NO, on Kalshi or
Polymarket), each joining the maker venue's best bid whenever the hedged edge at the top of both books clears
the hurdle (10%/yr on the capital until resolution, by default), and each fill hedged by buying the opposite
side on the other venue. What this model changes is how fills, timing and capital are simulated (decisions by
Binoy, 2026-11-17; see TASKS.md):

  Queue tracking. A new order joins the back of the queue: everything displayed at its price when it reaches
      the venue is ahead of it. Trades at our price consume the queue ahead first, and the rest fills us.
      A trade through our price (a seller reaching lower) fills us fully. A size drop at our price that no
      trade explains is a cancellation: --cancel-model pessimistic puts every cancellation behind us (we move
      up only when the displayed size falls below our place), pro-rata takes cancellations from ahead of and
      behind us in proportion. A trade message and the book update it causes can arrive in either order, so
      a drop waits up to --reconcile-ms for its trade before it counts as a cancellation.
  Latency. One round-trip latency L for everything we do: an order can be filled only by trades received at
      least L after we decided to place it; a cancel or reprice lands L after we decide, so meanwhile the old
      order can still be hit (a stale quote); the hedge buys from the other venue's book as it stood L after
      we learnt of the fill. This replaces Vihaan's 10 s minimum rest.
  Partial fills. The unfilled remainder keeps its place in the queue; a new order joins the back only after a
      full fill, a reprice or a cancel.
  Position caps. Capital committed (maker leg, hedge leg and fees; unhedged legs too) is capped per pair and
      in total, separately in each run. Orders are sized to fit and fills are clipped at the cap. Capital stays
      committed until the market resolves: unwinding early is not modelled.
  Markouts. The maker venue's mid 1 s, 10 s and 60 s after each fill, minus our price, in cents per contract.
      Negative means the market moved against the side we bought (adverse selection).
  No rebates and no liquidity rewards (Binoy, 2026-11-17): profit is fills and fees only.

Trade timing: our receive time where known. A Polymarket trade without one (NO-token trades never print on the
YES-only feed) is known at the end of its second and can fill only an order that was live at its start. A
Kalshi trade without one (a REST row the feed missed) uses the venue's millisecond timestamp.

Each (cancel model, latency) combination is a separate run with its own orders, fills and caps. Live days only:
5 s polling (the legacy days) can't support queue tracking. While a market's book is unknown (a recorder gap,
a disconnect, a closed market) its quotes are pulled at once, and a hedge or markout that falls in a gap is
recorded as unknown.
"""
import collections
import heapq

from analysis import replay
from analysis.maker_baseline import annualized, resolve_ms, utc
from edge import asks_from_opposite_bids, taker_fee  # noqa: E402  (Vihaan's unchanged fee and book helpers)
from maker import buy  # noqa: E402  (his depth walk for the hedge)

EPS = 1e-9
YEAR_MS = 365 * 86400 * 1000
LATENCIES_MS = (250, 1000, 5000)
CANCEL_MODELS = ("pessimistic", "pro-rata")
MARKOUTS_S = (1, 10, 60)
RECONCILE_MS = 2000
CAP_PAIR, CAP_TOTAL = 1_000.0, 10_000.0
KALSHI_MIN_ORDER = 1.0

FIELDS = (["cancel_model", "latency_ms", "pair_id", "family", "maker_venue", "side", "placed_utc", "fill_utc",
           "trade_timed", "kind", "stale", "price", "size", "clipped", "queue_wait_s", "hedged", "hedge_status",
           "quoted_edge_c", "realized_edge_c", "profit", "capital", "annualized_pct"]
          + [f"markout_{s}s_c" for s in MARKOUTS_S] + ["uncertain", "est_resolution", "kalshi_question"])


def _sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def trade_events(data, dates):
    """The days' trades in the order we learnt of them: (known_ms, start_ms, venue, market, yes_price, size,
    taker, timed). start_ms is the earliest the trade can have happened (see the module notes)."""
    parts = []
    for d in dates:
        for e, p in data.files("trades", d):
            cols = {c[0] for c in data.con.execute(f"SELECT name FROM parquet_schema({_sql_str(p)})").fetchall()}
            recv = "recv_ms" if "recv_ms" in cols else "NULL::BIGINT"
            end_of_second = 999 if e["venue"] == "poly" else 0
            parts.append(f"SELECT coalesce({recv}, ts_ms + {end_of_second}) AS known, coalesce({recv}, ts_ms) AS start, "
                         f"{_sql_str(e['venue'])} AS venue, market, yes_price, size, taker, {recv} IS NOT NULL AS timed, "
                         f"trade_id FROM read_parquet({_sql_str(p)})")
    if not parts:
        return []
    q = " UNION ALL ".join(parts) + " ORDER BY known, venue, trade_id"
    return [r[:8] for r in data.con.execute(q).fetchall()]


def size_at(levels, price):
    for p, s in levels:
        if abs(p - price) <= EPS:
            return s
    return 0.0


def row_uncertain(row, side, from_price):
    """True if a suspect book row may be wrong at or better than from_price on side ("yes"/"no" bids)."""
    if row is None or not row.get("suspect"):
        return False
    marks = row.get("suspect_levels")
    return marks is None or any(s == side and p >= from_price - EPS for s, p in marks)


def mid(row):
    """YES mid of a book row, or None if either side is empty."""
    if not row or not row["yes_bids"] or not row["no_bids"]:
        return None
    return (row["yes_bids"][0][0] + 1 - row["no_bids"][0][0]) / 2


# ---- one order and its place in the queue --------------------------------------------------------

class Order:
    __slots__ = ("price", "size", "placed_ms", "live_from", "dead_at", "quoted_edge", "cost_per", "ahead",
                 "shown", "pending", "credit", "known")

    def __init__(self, price, size, placed_ms, latency, quoted_edge, cost_per, shown):
        self.price, self.size, self.quoted_edge, self.cost_per = price, size, quoted_edge, cost_per
        self.placed_ms, self.live_from, self.dead_at = placed_ms, placed_ms + latency, None
        self.ahead = self.shown = shown        # everything displayed is ahead of a new order
        self.pending = []                      # [(ms, amount, shown_before)] size drops no trade explains yet
        self.credit = []                       # [(ms, amount)] trade volume at our price the book hasn't shown
        self.known = False                     # reached the venue (from live_from on)

    def live_at(self, ms):
        return self.live_from <= ms and (self.dead_at is None or ms < self.dead_at)

    def _pending_total(self):
        return sum(a for _, a, _ in self.pending)

    def expire(self, ms, window, model):
        """Drops older than the window are cancellations; unmatched trade credit is dropped."""
        keep = []
        for t, amount, before in self.pending:
            if ms - t < window:
                keep.append((t, amount, before))
            elif model == "pro-rata" and before > EPS:
                self.ahead -= amount * min(self.ahead, before) / before
        self.pending = keep
        self.credit = [(t, a) for t, a in self.credit if ms - t < window]
        self.ahead = max(0.0, min(self.ahead, self.shown + self._pending_total()))

    def on_book(self, ms, shown, window, model):
        if ms < self.live_from:                # not at the venue yet: all that is displayed will be ahead
            self.ahead = self.shown = shown
            self.pending, self.credit = [], []
            return
        self.expire(ms, window, model)
        drop = self.shown - shown
        while drop > EPS and self.credit:      # volume already seen as trades
            t, a = self.credit[0]
            use = min(a, drop)
            drop -= use
            if a - use > EPS:
                self.credit[0] = (t, a - use)
            else:
                self.credit.pop(0)
        if drop > EPS:
            self.pending.append((ms, drop, shown + drop))
        self.shown = shown                     # increases join behind us
        self.ahead = max(0.0, min(self.ahead, self.shown + self._pending_total()))

    def on_trade_at_price(self, ms, qty, live, window, model):
        """A trade at our price. Returns the volume left for us after the queue ahead."""
        used = 0.0
        if live:
            self.expire(ms, window, model)
            used = min(self.ahead, qty)
            self.ahead -= used
        left = qty
        while left > EPS and self.pending:     # the drop the book already showed was this trade
            t, a, before = self.pending[0]
            use = min(a, left)
            left -= use
            if a - use > EPS:
                self.pending[0] = (t, a - use, before)
            else:
                self.pending.pop(0)
        if left > EPS:
            self.credit.append((ms, left))
        return qty - used if live else 0.0


# ---- one run: a cancel model and a latency -------------------------------------------------------

class World:
    def __init__(self, cancel_model, latency, cap_pair, cap_total):
        self.cancel_model, self.latency = cancel_model, latency
        self.cap_pair, self.cap_total = cap_pair, cap_total
        self.committed = collections.defaultdict(float)
        self.total = self.peak_total = 0.0
        self.fills = []
        self.quote_ms = collections.defaultdict(float)
        self.counts = collections.Counter()

    def room(self, pid, cost_per):
        if cost_per <= EPS:
            return 0.0
        return max(0.0, min(self.cap_pair - self.committed[pid], self.cap_total - self.total)) / cost_per

    def commit(self, pid, amount):
        self.committed[pid] += amount
        self.total += amount
        self.peak_total = max(self.peak_total, self.total)


class Spec:
    """A standing quote's fixed parts: pair, maker venue and side."""

    def __init__(self, pair, maker_venue, side):
        self.pair, self.pid = pair, pair["pair_id"]
        self.maker_venue, self.side = maker_venue, side
        self.hedge_venue = "poly" if maker_venue == "kalshi" else "kalshi"
        markets = {"kalshi": pair["kalshi_ticker"], "poly": pair["poly_yes_token"]}
        self.m_key = (maker_venue, markets[maker_venue])
        self.h_key = (self.hedge_venue, markets[self.hedge_venue])
        self.bid_key = "yes_bids" if side == "yes" else "no_bids"
        rates = {"kalshi": (pair["kalshi_fee_rate"], pair.get("kalshi_maker_fee_rate", 0.0)),
                 "poly": (pair["poly_fee_rate"], pair.get("poly_maker_fee_rate", 0.0))}
        self.maker_rate = rates[maker_venue][1]
        self.hedge_rate = rates[self.hedge_venue][0]
        self.min_order = float(pair.get("poly_min_order") or 0) if maker_venue == "poly" else KALSHI_MIN_ORDER
        self.resolve_ms = resolve_ms(pair)


class Fill:
    __slots__ = ("spec", "world", "order_placed", "ms", "timed", "kind", "stale", "price", "size", "clipped",
                 "wait_ms", "quoted_edge", "maker_fee", "est", "hedged", "hedge_cost", "hedge_fee",
                 "hedge_status", "markouts", "uncertain")

    @property
    def capital(self):
        """Capital of the hedged part (each hedged pair pays out $1)."""
        share = self.hedged / self.size if self.size else 0.0
        return self.hedged * self.price + self.maker_fee * share + self.hedge_cost + self.hedge_fee

    @property
    def profit(self):
        return self.hedged - self.capital


class LiveMaker:
    def __init__(self, data, latencies=LATENCIES_MS, cancel_models=CANCEL_MODELS, size=100.0, min_edge_c=0.0,
                 min_ann_pct=10.0, cap_pair=CAP_PAIR, cap_total=CAP_TOTAL, reconcile_ms=RECONCILE_MS, log=print):
        self.data, self.log = data, log
        self.size, self.min_edge, self.min_ann = size, min_edge_c / 100, min_ann_pct / 100
        self.window = reconcile_ms
        self.worlds = [World(c, int(lat), cap_pair, cap_total) for c in cancel_models for lat in latencies]
        self.probes = replay.Probes()
        self.state, self.valid = {}, set()
        self.specs, self.pairs_from = {}, None
        self.by_market = collections.defaultdict(list)    # (venue, market) -> specs that use it (either leg)
        self.by_maker = collections.defaultdict(list)     # (venue, market) -> specs quoting on it
        self.orders = {}                                  # (world index, spec id) -> [Order]
        self.first_ms = self.last_ms = None
        self.covered_ms = 0
        self._prev_tick = None                            # last book tick since the last gap (for covered time)
        self.n_trades = self.n_untimed = 0
        self.skipped_days = []

    # -- setup --

    def _set_pairs(self, d):
        pairs, used = self.data.pairs(d)
        if used == self.pairs_from:
            return
        self.pairs_from = used
        for p in pairs:
            if any(s.pid == p["pair_id"] for s in self.specs.values()):
                continue
            for maker_venue in ("kalshi", "poly"):
                for side in ("yes", "no"):
                    s = Spec(p, maker_venue, side)
                    self.specs[id(s)] = s
                    self.by_market[s.m_key].append(s)
                    self.by_market[s.h_key].append(s)
                    self.by_maker[s.m_key].append(s)
        self.log(f"live maker pairs: {len(self.specs) // 4} from {used}")

    # -- order bookkeeping --

    def _end(self, w, spec, order, ms):
        if ms > order.live_from:
            w.quote_ms[spec.maker_venue] += ms - order.live_from

    def _orders(self, wi, spec):
        return self.orders.get((wi, id(spec)), ())

    def _prune(self, wi, spec, ms):
        """Drop orders whose cancel has landed by ms."""
        key = (wi, id(spec))
        lst = self.orders.get(key)
        if not lst:
            return
        w = self.worlds[wi]
        keep = []
        for o in lst:
            if o.dead_at is not None and o.dead_at <= ms:
                self._end(w, spec, o, o.dead_at)
            else:
                keep.append(o)
        if keep:
            self.orders[key] = keep
        else:
            del self.orders[key]

    def _pull_now(self, spec, ms):
        """Pull a spec's orders at once (its books became unknown)."""
        for wi, w in enumerate(self.worlds):
            for o in self.orders.pop((wi, id(spec)), ()):
                end = ms if o.dead_at is None else min(ms, o.dead_at)
                self._end(w, spec, o, end)

    # -- quoting decisions --

    def _signal(self, spec, ms):
        mb, hb = self.state.get(spec.m_key), self.state.get(spec.h_key)
        if spec.m_key not in self.valid or spec.h_key not in self.valid or mb is None or hb is None:
            return None
        bids = mb[spec.bid_key]
        hedge_asks = asks_from_opposite_bids(hb[spec.bid_key])
        if not bids or not hedge_asks:
            return False
        bid, shown = bids[0]
        ask = hedge_asks[0][0]
        fees = taker_fee(spec.maker_rate, bid) + taker_fee(spec.hedge_rate, ask)
        edge = 1 - bid - ask - fees
        hurdle = self.min_edge
        if self.min_ann:
            hurdle = max(hurdle, self.min_ann * (bid + ask) * max(spec.resolve_ms - ms, 0) / YEAR_MS)
        if edge <= hurdle + EPS:
            return False
        return bid, shown, edge, bid + ask + fees

    def _decide(self, spec, ms):
        sig = self._signal(spec, ms)
        if sig is None:
            self._pull_now(spec, ms)
            return
        for wi, w in enumerate(self.worlds):
            self._prune(wi, spec, ms)
            lst = self.orders.get((wi, id(spec)), [])
            open_ = next((o for o in lst if o.dead_at is None), None)
            if sig is False:
                if open_ is not None:
                    open_.dead_at = ms + w.latency
                    w.counts["cancels"] += 1
                continue
            bid, shown, edge, cost_per = sig
            if open_ is not None and abs(open_.price - bid) <= EPS:
                open_.quoted_edge = edge
                continue
            if open_ is not None:
                open_.dead_at = ms + w.latency
                w.counts["reprices"] += 1
            size = min(self.size, w.room(spec.pid, cost_per))
            if size + EPS < max(spec.min_order, EPS):
                if size < self.size:
                    w.counts["capped_quotes"] += 1
                continue
            lst.append(Order(bid, size, ms, w.latency, edge, cost_per, shown))
            self.orders[(wi, id(spec))] = lst
            w.counts["orders"] += 1

    # -- events --

    def _on_book(self, row, ms):
        key = (row["venue"], row["market"])
        for spec in self.by_maker.get(key, ()):
            levels = row[spec.bid_key]
            for wi, w in enumerate(self.worlds):
                for o in self._orders(wi, spec):
                    if o.dead_at is None or ms < o.dead_at:
                        o.on_book(ms, size_at(levels, o.price), self.window, w.cancel_model)

    def _on_trade(self, t):
        known, start, venue, market, yes_price, qty, taker, timed = t
        self.n_trades += 1
        self.n_untimed += not timed
        for spec in self.by_maker.get((venue, market), ()):
            if spec.side == "yes":
                if taker != "sell_yes":
                    continue
                traded = yes_price
            else:
                if taker != "buy_yes":
                    continue
                traded = 1 - yes_price         # a taker buying YES at y hits NO bids at 1 - y
            for wi, w in enumerate(self.worlds):
                lst = self._orders(wi, spec)
                if not lst:
                    continue
                left = qty
                for o in sorted(lst, key=lambda o: -o.price):     # a seller reaches the higher bid first
                    if left <= EPS or traded > o.price + EPS:
                        continue
                    live = o.live_at(start)
                    if traded < o.price - EPS:
                        got, kind = (min(o.size, left) if live else 0.0), "through"
                    else:
                        got, kind = min(o.size, o.on_trade_at_price(known, left, live, self.window,
                                                                     w.cancel_model)), "touch"
                    if got > EPS:
                        filled = self._fill(wi, w, spec, o, known, got, kind, timed)
                        left -= filled
                self._prune(wi, spec, known)
                lst = self.orders.get((wi, id(spec)))
                if lst:
                    done = [o for o in lst if o.size <= EPS]
                    for o in done:
                        self._end(w, spec, o, known)
                    rest = [o for o in lst if o.size > EPS]
                    if rest:
                        self.orders[(wi, id(spec))] = rest
                    else:
                        del self.orders[(wi, id(spec))]

    def _fill(self, wi, w, spec, o, ms, qty, kind, timed):
        room = w.room(spec.pid, o.cost_per)
        clipped = qty > room + EPS
        qty = min(qty, room)
        if qty <= EPS:
            w.counts["fills_blocked_by_cap"] += 1
            return 0.0
        f = Fill()
        f.spec, f.world, f.order_placed, f.ms, f.timed, f.kind = spec, w, o.placed_ms, ms, timed, kind
        f.stale = o.dead_at is not None
        f.price, f.size, f.clipped, f.wait_ms, f.quoted_edge = o.price, qty, clipped, ms - o.live_from, o.quoted_edge
        f.maker_fee = qty * taker_fee(spec.maker_rate, o.price)
        f.est = qty * o.cost_per
        f.hedged = f.hedge_cost = f.hedge_fee = 0.0
        f.hedge_status, f.markouts = None, {}
        f.uncertain = row_uncertain(self.state.get(spec.m_key), spec.side, o.price)
        o.size -= qty
        w.commit(spec.pid, f.est)
        w.fills.append(f)
        self.probes.schedule(ms + w.latency, lambda ok, f=f: self._hedge(f, ok))
        for s in MARKOUTS_S:
            self.probes.schedule(ms + s * 1000, lambda ok, f=f, s=s: self._markout(f, s, ok))
        return qty

    def _hedge(self, f, ok):
        spec, w = f.spec, f.world
        book = self.state.get(spec.h_key) if ok and spec.h_key in self.valid else None
        if book is None:
            f.hedge_status = "unknown"
        else:
            asks = asks_from_opposite_bids(book[spec.bid_key])
            f.hedged, f.hedge_cost, f.hedge_fee = buy(asks, f.size, spec.hedge_rate, spec.hedge_venue == "kalshi")
            f.hedge_status = "full" if f.hedged >= f.size - EPS else ("partial" if f.hedged > EPS else "none")
            if f.hedged > EPS:
                walked, cum = asks[0][0], 0.0
                for price, size in asks:
                    walked = price
                    cum += size
                    if cum >= f.hedged - EPS:
                        break
                f.uncertain = f.uncertain or row_uncertain(book, spec.side, round(1 - walked, 6))
        # Replace the estimate made at the fill with what was spent: the whole maker leg (hedged or not) and the hedge.
        w.commit(spec.pid, f.size * f.price + f.maker_fee + f.hedge_cost + f.hedge_fee - f.est)

    def _markout(self, f, s, ok):
        row = self.state.get(f.spec.m_key) if ok and f.spec.m_key in self.valid else None
        m = mid(row)
        if m is not None:
            side_mid = m if f.spec.side == "yes" else 1 - m
            f.markouts[s] = side_mid - f.price

    # -- the replay --

    def _advance(self, trades, ti, ms):
        """Apply trades known by ms and run look-ahead checks due before ms, in time order."""
        while ti < len(trades) and trades[ti][0] <= ms:
            self.probes.due(trades[ti][0])
            self._on_trade(trades[ti])
            ti += 1
        self.probes.due(ms)
        return ti

    def _gap(self, ms, venue=None):
        for spec in self.specs.values():
            if venue is None or venue in (spec.m_key[0], spec.h_key[0]):
                self._pull_now(spec, ms)
        self.valid = set() if venue is None else {k for k in self.valid if k[0] != venue}
        self._prev_tick = None                 # time in a gap isn't covered

    def run_live_day(self, d, trades, ti):
        events = [(e["recv_ms"], 0, e) for e in replay.coverage(self.data, d)
                  if e["event"] in ("started", "stopped", "disconnected", "market_closed")]
        ticks = ((ms, 1, group) for ms, group in replay.grouped(replay.book_rows(self.data, d)))
        for ms, kind, item in heapq.merge(events, ticks, key=lambda x: (x[0], x[1])):
            ti = self._advance(trades, ti, ms)
            if kind == 0:
                e = item
                if e["venue"] == "recorder" and e["event"] == "started" and self.last_ms is not None:
                    self._gap(self.last_ms)
                elif e["venue"] == "recorder":
                    self._gap(ms)
                elif e["event"] == "disconnected":
                    self._gap(ms, "kalshi" if e["venue"] == "kalshi" else "poly")
                elif e["event"] == "market_closed":
                    key = ("kalshi" if e["venue"] == "kalshi" else "poly", e.get("market") or e.get("token"))
                    self.valid.discard(key)
                    for spec in self.by_market.get(key, ()):
                        self._pull_now(spec, ms)
                continue
            self.first_ms = ms if self.first_ms is None else self.first_ms
            if self._prev_tick is not None:
                self.covered_ms += ms - self._prev_tick
            self._prev_tick = self.last_ms = ms
            dirty = {}
            for r in item:
                key = (r["venue"], r["market"])
                self.state[key] = r
                self.valid.add(key)
                self._on_book(r, ms)
                for spec in self.by_market.get(key, ()):
                    dirty[id(spec)] = spec
            for spec in dirty.values():
                self._decide(spec, ms)
        return ti

    def run(self, dates):
        live = [d for d in dates if replay.era(self.data, d) == "live"]
        self.skipped_days = [d for d in dates if d not in live]
        for d in self.skipped_days:
            self.log(f"{d}: legacy polling data, skipped (queue tracking needs every book change)")
        trades = trade_events(self.data, live)
        ti = 0
        for d in live:
            self._set_pairs(d)
            self.log(f"{d}: live data")
            ti = self.run_live_day(d, trades, ti)
        self.probes.flush()
        if self.last_ms is not None:
            for spec in self.specs.values():
                self._pull_now(spec, self.last_ms)
        return self._rows()

    # -- results --

    def _rows(self):
        rows = []
        for w in self.worlds:
            for f in w.fills:
                p = f.spec.pair
                ann = annualized(f.profit / f.capital, f.ms, f.spec.resolve_ms) if f.capital > 0 else None
                rows.append(dict(
                    cancel_model=w.cancel_model, latency_ms=w.latency, pair_id=f.spec.pid, family=p["family"],
                    maker_venue=f.spec.maker_venue, side=f.spec.side, placed_utc=utc(f.order_placed),
                    fill_utc=utc(f.ms), trade_timed=f.timed, kind=f.kind, stale=f.stale, price=round(f.price, 4),
                    size=round(f.size, 2), clipped=f.clipped, queue_wait_s=round(f.wait_ms / 1000, 1),
                    hedged=round(f.hedged, 2), hedge_status=f.hedge_status,
                    quoted_edge_c=round(f.quoted_edge * 100, 2),
                    realized_edge_c=round(f.profit / f.hedged * 100, 2) if f.hedged > EPS else None,
                    profit=round(f.profit, 2), capital=round(f.capital, 2),
                    annualized_pct=None if ann is None else round(ann * 100, 1),
                    **{f"markout_{s}s_c": (round(f.markouts[s] * 100, 2) if s in f.markouts else None)
                       for s in MARKOUTS_S},
                    uncertain=f.uncertain, est_resolution=p["est_resolution"], kalshi_question=p["kalshi_question"]))
        rows.sort(key=lambda r: (r["cancel_model"], r["latency_ms"], r["fill_utc"]))
        return rows


def run_live(data, from_date=None, to_date=None, log=print, **kw):
    dates = [d for d in data.dates("books") if (not from_date or d >= from_date) and (not to_date or d <= to_date)]
    m = LiveMaker(data, log=log, **kw)
    rows = m.run(dates)
    return rows, m


# ---- summary ---------------------------------------------------------------------------------------

def _stats(fills):
    st = collections.defaultdict(float)
    for f in fills:
        st["fills"] += 1
        st["size"] += f.size
        st["hedged"] += f.hedged
        st["stale"] += f.stale
        st["untimed"] += not f.timed
        st["uncertain"] += f.uncertain
        st["quoted"] += f.quoted_edge * f.hedged
        st["profit"] += f.profit
        for s in MARKOUTS_S:
            if s in f.markouts:
                st[f"mk{s}"] += f.markouts[s] * f.size
                st[f"mk{s}_n"] += f.size
    return st


def _line(label, quote_h, st, hours, peak=None, markouts=True):
    per_day = st["profit"] / hours * 24 if hours > 0 else 0.0
    q = f"{st['quoted'] / st['hedged'] * 100:8.2f}" if st["hedged"] > EPS else f"{'':8}"
    r = f"{st['profit'] / st['hedged'] * 100:9.2f}" if st["hedged"] > EPS else f"{'':9}"
    mk = "".join(f"{st[f'mk{s}'] / st[f'mk{s}_n'] * 100:8.2f}" if markouts and st[f"mk{s}_n"] > EPS else f"{'-':>8}"
                 for s in MARKOUTS_S)
    stale = f"{int(st['stale']):6d}" if markouts else f"{'-':>6}"
    return (f"  {label:28}{quote_h:8.1f}{int(st['fills']):6d}{st['size']:10.0f}{st['hedged']:9.0f}{stale}"
            f"{q}{r}{st['profit']:10.2f}{per_day:8.2f}{mk}" + (f"{peak:10.0f}" if peak is not None else ""))


def summary(rows, m, baseline=None):
    """Live runs side by side with Vihaan's baseline (maker_baseline.summary's stats), same days."""
    hours = m.covered_ms / 3600000
    lines = [f"Live maker model: {hours:.2f} h of live books, {m.n_trades} trades applied "
             f"({m.n_untimed} without a receive time, known at the end of their second)",
             "No rebates or liquidity rewards. Caps: ${:,.0f} per pair, ${:,.0f} in total; capital stays committed "
             "until resolution.".format(m.worlds[0].cap_pair, m.worlds[0].cap_total) if m.worlds else ""]
    if m.skipped_days:
        lines.append(f"Skipped (legacy polling data): {', '.join(m.skipped_days)}")
    head = (f"  {'':28}{'quote-h':>8}{'fills':>6}{'contracts':>10}{'hedged':>9}{'stale':>6}{'quoted c':>8}"
            f"{'realized c':>9}{'profit $':>10}{'$/day':>8}" + "".join(f"{f'mk {s}s c':>8}" for s in MARKOUTS_S))
    lines += ["", "== All models (realized c = profit per hedged contract; mk = markout, cents per contract; "
                  "stale = filled while a cancel was landing)", head + f"{'peak $':>10}"]
    if baseline is not None:
        b_rows, b_stats, b = baseline
        b_hours = b.covered_ms / 3600000
        for model in ("queue", "through"):
            st = collections.defaultdict(float)
            quote_h = 0.0
            for key, v in b_stats.items():
                if key[0] == model and key[1] == "venue":
                    quote_h += v["live_h"]
                    for k in ("fills", "size", "hedged", "quoted", "profit"):
                        st[k] += v[k]
            lines.append(_line(f"baseline {model} (Vihaan)", quote_h, st, b_hours, markouts=False))
    for w in m.worlds:
        lines.append(_line(f"live {w.cancel_model} {w.latency} ms", sum(w.quote_ms.values()) / 3600000,
                           _stats(w.fills), hours, peak=w.peak_total))
    lines += ["", "== Live runs by maker venue", head]
    for w in m.worlds:
        for venue in ("kalshi", "poly"):
            fills = [f for f in w.fills if f.spec.maker_venue == venue]
            lines.append(_line(f"{w.cancel_model} {w.latency} ms, {venue}", w.quote_ms[venue] / 3600000,
                               _stats(fills), hours))
    lines += ["", "== Live runs: fill quality"]
    for w in m.worlds:
        st = _stats(w.fills)
        status = collections.Counter(f.hedge_status for f in w.fills)
        lines.append(f"  {w.cancel_model} {w.latency} ms: orders {w.counts['orders']}, reprices {w.counts['reprices']}, "
                     f"cancels {w.counts['cancels']}; fills by untimed trades {int(st['untimed'])}, on suspect books "
                     f"{int(st['uncertain'])}; hedges {dict(status)}; unhedged contracts {st['size'] - st['hedged']:.0f}; "
                     f"clipped at the cap {sum(f.clipped for f in w.fills)}, blocked {w.counts['fills_blocked_by_cap']}")
    return lines
