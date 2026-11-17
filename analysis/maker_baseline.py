"""Vihaan's maker simulation (maker.py's QuoteSim, unchanged), run as a BASELINE on the curated data.

For every pair it replays four standing quotes (bid YES or NO, on Kalshi or Polymarket), each under two fill
models ("queue" and "through"), exactly as his analyze_maker.py did, so results can be compared with the new
maker model. It is a baseline, not a reproduction of his maker_fills.csv: by Binoy's decision (2026-11-17),
the workarounds of his 5 s polling recorder are not carried over:
  - no trade-coverage marks: the curated trades are complete, so quotes are not pulled at a "mark";
  - live days have no polling cycles, so "hedge at the next book recorded" means the first book update of any
    market after the fill (usually milliseconds later, far faster than his ~5 s). The new maker model replaces
    this with an explicit hedge latency.
His model's own rules are untouched: the 10 s minimum rest before a fill, joining the back of the queue,
cancelling and re-queueing after any fill, the "queue" and "through" fill models, and the annualized hurdle.

Trades are applied in receive-time order where known (recv_ms), else by the venue's own timestamp.
"""
import collections
import heapq
from datetime import datetime, timezone

from analysis import replay
from maker import QuoteSim  # noqa: E402  (Vihaan's unchanged maker simulation)

MODELS = ("queue", "through")
FIELDS = ["model", "pair_id", "family", "maker_venue", "side", "fill_utc", "kind", "price", "size", "hedged",
          "hedge_delay_s", "quoted_edge_c", "realized_edge_c", "profit", "capital", "annualized_pct",
          "est_resolution", "kalshi_question"]


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def resolve_ms(pair):
    return int(datetime.fromisoformat(pair["est_resolution"]).replace(tzinfo=timezone.utc).timestamp() * 1000)


def annualized(ret, ms, end_ms):
    days = (end_ms - ms) / 86400000
    return ret * 365 / days if days > 1 else None


def _sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def trade_rows(data, dates):
    """The days' trades in time order: (time_ms, venue, market, yes_price, size, taker). Time is our receive
    time where known, else the venue's timestamp."""
    paths = [(e["venue"], p) for d in dates for e, p in data.files("trades", d)]
    if not paths:
        return []
    parts = []
    for venue, p in paths:
        cols = {c[0] for c in data.con.execute(f"SELECT name FROM parquet_schema({_sql_str(p)})").fetchall()}
        recv = "coalesce(recv_ms, ts_ms)" if "recv_ms" in cols else "ts_ms"
        parts.append(f"SELECT {recv} AS t, {_sql_str(venue)} AS venue, market, yes_price, size, taker, trade_id "
                     f"FROM read_parquet({_sql_str(p)})")
    q = " UNION ALL ".join(parts) + " ORDER BY t, venue, trade_id"
    return [r[:6] for r in data.con.execute(q).fetchall()]


class Baseline:
    def __init__(self, data, size=100.0, min_edge_c=0.0, min_ann_pct=10.0, min_rest_s=10.0, max_gap=30, log=print):
        self.data, self.log = data, log
        self.size, self.min_edge, self.min_ann = size, min_edge_c / 100, min_ann_pct / 100
        self.min_rest_ms, self.max_gap = int(min_rest_s * 1000), max_gap
        self.pairs_from = None
        self.sims = {}                       # pair_id -> [(maker_venue, m_market, hedge_venue, h_market, model, sim)]
        self.by_trade_market = collections.defaultdict(list)
        self.by_book_market = collections.defaultdict(list)
        self.by_pair = {}
        self.state = {}
        self.valid = set()
        self.idle = set()
        self._unknown, self._recheck = set(), True
        self.pending = set()                 # pairs with a fill waiting to be hedged
        self.first_ms = self.last_ms = None
        self.covered_ms = 0
        self.n_gaps = self.n_trades = 0

    def _set_pairs(self, d):
        pairs, used = self.data.pairs(d)
        if used == self.pairs_from:
            return
        self.pairs_from = used
        for p in pairs:
            if p["pair_id"] in self.sims:
                continue
            self.by_pair[p["pair_id"]] = p
            venues = {"kalshi": (p["kalshi_ticker"], p["kalshi_fee_rate"], p.get("kalshi_maker_fee_rate", 0.0)),
                      "poly": (p["poly_yes_token"], p["poly_fee_rate"], p.get("poly_maker_fee_rate", 0.0))}
            self.by_book_market[("kalshi", p["kalshi_ticker"])].append(p["pair_id"])
            self.by_book_market[("poly", p["poly_yes_token"])].append(p["pair_id"])
            entries = []
            for maker_venue, hedge_venue in (("kalshi", "poly"), ("poly", "kalshi")):
                m_market, _, m_rate = venues[maker_venue]
                h_market, h_rate, _ = venues[hedge_venue]
                for side in ("yes", "no"):
                    for model in MODELS:
                        s = QuoteSim(side, m_rate, h_rate, hedge_venue == "kalshi", size=self.size,
                                     min_edge=self.min_edge, min_ann=self.min_ann, resolve_ms=resolve_ms(p),
                                     min_rest_ms=self.min_rest_ms, through_only=model == "through")
                        entries.append((maker_venue, m_market, hedge_venue, h_market, model, s))
                        self.by_trade_market[(maker_venue, m_market)].append((p["pair_id"], s))
            self.sims[p["pair_id"]] = entries
            self.idle.add(p["pair_id"])
        self.log(f"maker baseline pairs: {len(self.sims)} from {used}")

    def _cancel_all(self, ms):
        for entries in self.sims.values():
            for *_, s in entries:
                s.cancel(ms)
        self.idle = set(self.sims)

    def _refresh(self, ms, dirty, unknown=(), only=None):
        """Vihaan's per-pair step. `only` limits it to the pairs that can change (live days): pairs whose
        books changed, idle pairs, pairs with a fill to hedge, and unknown pairs. Others would be skipped
        by the step itself, so the result is the same; iterating every pair on every update is just slow."""
        pids = self.sims if only is None else [p for p in self.sims if p in only]
        for pid in pids:
            entries = self.sims[pid]
            if pid in unknown:
                for *_, s in entries:
                    s.cancel(ms)
                self.idle.add(pid)
                continue
            refresh = pid in dirty or pid in self.idle
            self.idle.discard(pid)
            for maker_venue, m_market, hedge_venue, h_market, _, s in entries:
                if not refresh and not s.pending:
                    continue
                mb, hb = self.state.get((maker_venue, m_market)), self.state.get((hedge_venue, h_market))
                if mb is None or hb is None:
                    continue
                s.on_books(ms, mb, hb)
        self.pending = {pid for pid in self.pending if any(e[5].pending for e in self.sims[pid])}

    def _trades_until(self, trades, i, upto_ms, apply=True):
        while i < len(trades) and trades[i][0] <= upto_ms:
            t, venue, market, price, size, taker = trades[i]
            if apply:
                self.n_trades += 1
                for pid, s in self.by_trade_market.get((venue, market), ()):
                    s.on_trade(t, price, size, taker)
                    if s.pending:
                        self.pending.add(pid)
            i += 1
        return i

    def _tick_gap(self, started):
        if self.last_ms is not None:
            gap = (started - self.last_ms) / 1000 > self.max_gap
            if gap:
                self.n_gaps += 1
                self._cancel_all(self.last_ms)
            else:
                self.covered_ms += started - self.last_ms
            return gap
        return False

    def run_legacy_day(self, d, trades, ti):
        for c in (c for c in replay.coverage(self.data, d) if c["event"] == "poll_cycle"):
            started, finished = c["recv_ms"], c["finished_ms"]
            self.first_ms = started if self.first_ms is None else self.first_ms
            if self._tick_gap(started):
                ti = self._trades_until(trades, ti, started, apply=False)
            self.last_ms = started
            ti = self._trades_until(trades, ti, started)
            dirty = set()
            for r in self._rows_until(finished):
                self.state[(r["venue"], r["market"])] = r
                dirty.update(self.by_book_market.get((r["venue"], r["market"]), ()))
            unknown = {pid for v, m in (c.get("missing") or []) for pid in self.by_book_market.get((v, m), ())}
            self._refresh(started, dirty, unknown)
        return ti

    def _rows_until(self, finished):
        out = []
        while self._pending is not None and self._pending["ts_ms"] <= finished:
            out.append(self._pending)
            self._pending = next(self._rows, None)
        return out

    def run_live_day(self, d, trades, ti):
        events = [(e["recv_ms"], 0, e) for e in replay.coverage(self.data, d)
                  if e["event"] in ("started", "stopped", "disconnected")]
        ticks = ((ms, 1, group) for ms, group in replay.grouped(replay.book_rows(self.data, d)))
        for ms, kind, item in heapq.merge(events, ticks, key=lambda x: (x[0], x[1])):
            if kind == 0:
                at = self.last_ms if item["event"] == "started" and self.last_ms is not None else ms
                self._cancel_all(at)
                self.valid.clear()
                self._recheck = True
                if item["event"] != "disconnected":
                    self.n_gaps += 1
                ti = self._trades_until(trades, ti, ms, apply=False)
                continue
            self.first_ms = ms if self.first_ms is None else self.first_ms
            if self.last_ms is not None:
                self.covered_ms += ms - self.last_ms
            self.last_ms = ms
            ti = self._trades_until(trades, ti, ms)
            dirty = set()
            for r in item:
                key = (r["venue"], r["market"])
                self.state[key] = r
                if key not in self.valid:
                    self._recheck = True          # a market became known again: recompute unknown pairs
                self.valid.add(key)
                dirty.update(self.by_book_market.get(key, ()))
            unknown = {pid for pid in self.sims
                       if ("kalshi", self.by_pair[pid]["kalshi_ticker"]) not in self.valid
                       or ("poly", self.by_pair[pid]["poly_yes_token"]) not in self.valid} if self._recheck else self._unknown
            self._unknown, self._recheck = unknown, False
            self._refresh(ms, dirty, unknown, only=dirty | self.idle | self.pending | unknown)
        return ti

    def run(self, dates):
        trades = trade_rows(self.data, dates)
        ti, prev_era = 0, None
        for d in dates:
            self._set_pairs(d)
            era = replay.era(self.data, d)
            if prev_era is not None and era != prev_era:
                self._cancel_all(self.last_ms)
                self.valid.clear()
            self.log(f"{d}: {era} data")
            if era == "legacy":
                self._rows = replay.book_rows(self.data, d)
                self._pending = next(self._rows, None)
                ti = self.run_legacy_day(d, trades, ti)
            else:
                ti = self.run_live_day(d, trades, ti)
            prev_era = era
        return self._results()

    def _results(self):
        rows = []
        stats = collections.defaultdict(lambda: collections.defaultdict(float))
        for pid, entries in self.sims.items():
            p = self.by_pair[pid]
            end = resolve_ms(p)
            for maker_venue, m_market, _, _, model, s in entries:
                if self.last_ms is not None:
                    s.cancel(self.last_ms)
                for key in ((model, "venue", maker_venue), (model, "family", p["family"])):
                    stats[key]["live_h"] += s.live_ms / 3600000
                for f in s.fills:
                    ann = annualized(f.profit / f.capital, f.ts_ms, end) if f.capital > 0 else None
                    rows.append(dict(
                        model=model, pair_id=pid, family=p["family"], maker_venue=maker_venue, side=s.side,
                        fill_utc=utc(f.ts_ms), kind=f.kind, price=round(f.price, 4), size=round(f.size, 2),
                        hedged=round(f.hedged, 2),
                        hedge_delay_s=round((f.hedge_ms - f.ts_ms) / 1000, 1) if f.hedge_ms else None,
                        quoted_edge_c=round(f.quoted_edge * 100, 2),
                        realized_edge_c=round(f.profit / f.hedged * 100, 2) if f.hedged else None,
                        profit=round(f.profit, 2), capital=round(f.capital, 2),
                        annualized_pct=None if ann is None else round(ann * 100, 1),
                        est_resolution=p["est_resolution"], kalshi_question=p["kalshi_question"]))
                    for key in ((model, "venue", maker_venue), (model, "family", p["family"])):
                        st = stats[key]
                        st["fills"] += 1
                        st["size"] += f.size
                        st["hedged"] += f.hedged
                        st["quoted"] += f.quoted_edge * f.hedged
                        st["profit"] += f.profit
                        st["capital"] += f.capital
        rows.sort(key=lambda r: (r["model"], r["fill_utc"]))
        return rows, stats


def run_baseline(data, from_date=None, to_date=None, log=print, **kw):
    dates = [d for d in data.dates("books") if (not from_date or d >= from_date) and (not to_date or d <= to_date)]
    b = Baseline(data, log=log, **kw)
    rows, stats = b.run(dates)
    return rows, stats, b


def summary(rows, stats, b):
    hours = b.covered_ms / 3600000
    lines = [f"Maker baseline (Vihaan's QuoteSim, unchanged): {hours:.2f} h of continuous books, {b.n_gaps} gaps, "
             f"{b.n_trades} trades applied", f"Fills: {len(rows)}"]
    for model in MODELS:
        lines.append(f"\n== Fill model: {model}")
        lines.append(f"  {'':22}{'quote-h':>9}{'fills':>7}{'contracts':>11}{'quoted c':>10}{'realized c':>12}"
                     f"{'profit $':>10}{'$/day':>9}")
        for kind in ("venue", "family"):
            keys = sorted((k for k in stats if k[0] == model and k[1] == kind), key=lambda k: -stats[k]["profit"])
            for key in keys:
                st = stats[key]
                label = ("maker on " if kind == "venue" else "") + key[2]
                if not st["hedged"]:
                    lines.append(f"  {label:22}{st['live_h']:9.1f}{0:7d}")
                    continue
                per_day = st["profit"] / hours * 24 if hours > 0 else 0
                lines.append(f"  {label:22}{st['live_h']:9.1f}{int(st['fills']):7d}{st['size']:11.0f}"
                             f"{st['quoted'] / st['hedged'] * 100:10.2f}{st['profit'] / st['hedged'] * 100:12.2f}"
                             f"{st['profit']:10.2f}{per_day:9.2f}")
    return lines
