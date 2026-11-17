"""Taker-on-both-legs episodes: Vihaan's analyze_stream.py, ported to the curated tables and extended.

An episode is an unbroken run in which one hedge (buy YES on one venue, NO on the other) could be filled at
a profit after fees, walking both books level by level with edge.walk() (unchanged). Episodes are cut when
coverage is lost, and marked so, because their true length is unknown.

Legacy days (Nov 9-12, 5 s polling) are evaluated once per polling cycle with exactly the original rules,
so the original columns reproduce Vihaan's episodes.csv. Live days are evaluated on every book change.

Columns, in order: Vihaan's original ones (LEGACY_FIELDS), then
    uncertain           the walk at the episode's peak used a book level marked wrong (suspect data)
    uncertain_any       some walk during the episode used a level marked wrong
    tbill_pct           3-month T-bill yield in effect on the start date (percent)
    excess_annualized_pct   peak_annualized_pct minus tbill_pct: return above the capital-cost hurdle
    profit_<L>ms / size_<L>ms   the hedge re-walked on the books as they stood L ms after the episode started;
                        "would it still have paid if a trader reacted L ms later?" (live days only)
On live days "cycles" counts book updates evaluated during the episode, not 5 s polls.
"""
import collections
import heapq
import statistics
from datetime import datetime, timezone

from analysis import replay  # also puts Vihaan's modules on the import path
from edge import baskets, walk  # noqa: E402  (Vihaan's unchanged depth-walk and fee math)

LEGACY_FIELDS = ["pair_id", "family", "rule_status", "basket", "start_utc", "duration_s", "cycles", "end_reason",
                 "peak_top_edge_c", "peak_profit", "peak_size", "peak_capital", "peak_return_pct",
                 "peak_annualized_pct", "est_resolution", "kalshi_question", "poly_question"]
LATENCIES_MS = (50, 250, 1000, 5000)
BASKETS = ("P_YES+K_NO", "P_NO+K_YES")
# Which Polymarket side a basket buys through: P_YES asks come from Polymarket NO bids, P_NO asks from YES bids.
BASKET_POLY_SIDE = {"P_YES+K_NO": "no", "P_NO+K_YES": "yes"}


def fields(latencies=LATENCIES_MS):
    lat = [f"{k}_{ms}ms" for ms in latencies for k in ("profit", "size")]
    return LEGACY_FIELDS + ["uncertain", "uncertain_any", "tbill_pct", "excess_annualized_pct"] + lat


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def annualized(ret, ms, est_resolution):
    end = datetime.fromisoformat(est_resolution).replace(tzinfo=timezone.utc)
    days = (end - datetime.fromtimestamp(ms / 1000, tz=timezone.utc)).total_seconds() / 86400
    return ret * 365 / days if days > 1 else None


# ---- suspect-data check -------------------------------------------------------------------------

def deepest_price(fill, asks):
    """The deepest ask price a walk looked at: the last level it bought from, plus the next level, whose
    price decided where it stopped. For an empty fill, just the top level."""
    if not asks:
        return None
    cum = 0.0
    for price, size in asks:
        if cum >= fill.size - 1e-9:
            return price
        cum += size
    return asks[-1][0]


def walk_uncertain(basket, fill, poly_asks, poly_row, kalshi_row):
    """True if the walk may have used a wrong level: any suspect Kalshi row (levels unknown), a suspect
    Polymarket row without known levels, or a marked Polymarket level at or before the deepest price walked.
    A marked level can also be one our book lacked (Polymarket had it): price ordering covers both cases."""
    if kalshi_row.get("suspect"):
        return True
    if not poly_row.get("suspect"):
        return False
    marks = poly_row.get("suspect_levels")
    if marks is None:
        return True
    deepest = deepest_price(fill, poly_asks)
    if deepest is None:
        return bool(marks)
    side = BASKET_POLY_SIDE[basket]
    return any(s == side and round(1 - p, 5) <= deepest + 1e-9 for s, p in marks)


# ---- episode tracking (Vihaan's Tracker, extended) -------------------------------------------------

class Tracker:
    def __init__(self, on_open=None):
        self.open = {}
        self.done = []
        self.on_open = on_open            # called with each new episode (to schedule latency checks)

    def update(self, pair, name, fill, ms, uncertain=False):
        key = (pair["pair_id"], name)
        ep = self.open.get(key)
        if fill.size <= 0 or fill.profit <= 0:
            self.close(key, ms, "edge_gone")
            return
        if ep is None:
            ep = self.open[key] = dict(pair=pair, basket=name, start=ms, last=ms, cycles=0, peak=None,
                                       peak_uncertain=False, any_uncertain=False, lat={})
            if self.on_open:
                self.on_open(ep)
        ep["last"] = ms
        ep["cycles"] += 1
        ep["any_uncertain"] = ep["any_uncertain"] or uncertain
        if ep["peak"] is None or fill.profit > ep["peak"].profit:
            ep["peak"] = fill
            ep["peak_ms"] = ms
            ep["peak_uncertain"] = uncertain

    def extend(self, pair_id, ms):
        """Legacy: a cycle in which the pair's books didn't change extends its open episodes unrecomputed."""
        for name in BASKETS:
            ep = self.open.get((pair_id, name))
            if ep:
                ep["last"] = ms
                ep["cycles"] += 1

    def close(self, key, ms, reason):
        ep = self.open.pop(key, None)
        if ep:
            ep["end"] = ms
            ep["reason"] = reason
            self.done.append(ep)

    def close_all(self, ms, reason):
        for key in list(self.open):
            self.close(key, ms, reason)

    def close_pair(self, pair_id, ms, reason):
        for key in [k for k in self.open if k[0] == pair_id]:
            self.close(key, ms, reason)


def row(ep, tbill_for, latencies=LATENCIES_MS):
    p, f = ep["pair"], ep["peak"]
    ret = f.profit / f.capital
    ann = annualized(ret, ep["peak_ms"], p["est_resolution"])
    out = dict(pair_id=p["pair_id"], family=p["family"], rule_status=p["rule_status"], basket=ep["basket"],
               start_utc=utc(ep["start"]), duration_s=round((ep["end"] - ep["start"]) / 1000, 1),
               cycles=ep["cycles"], end_reason=ep["reason"], peak_top_edge_c=round(f.top_edge * 100, 2),
               peak_profit=round(f.profit, 2), peak_size=round(f.size, 2), peak_capital=round(f.capital, 2),
               peak_return_pct=round(ret * 100, 3), peak_annualized_pct=None if ann is None else round(ann * 100, 1),
               est_resolution=p["est_resolution"], kalshi_question=p["kalshi_question"],
               poly_question=p["poly_question"])
    tbill = tbill_for(utc(ep["start"])[:10])
    out.update(uncertain=ep["peak_uncertain"], uncertain_any=ep["any_uncertain"], tbill_pct=tbill,
               excess_annualized_pct=None if ann is None or tbill is None else round(ann * 100 - tbill, 1))
    for ms in latencies:
        res = ep["lat"].get(ms)
        out[f"profit_{ms}ms"] = None if res is None else round(res[0], 2)
        out[f"size_{ms}ms"] = None if res is None else round(res[1], 2)
    return out


# ---- the run ------------------------------------------------------------------------------------------

class Run:
    """Episodes over a date range: legacy days with the per-cycle rules, live days per book change."""

    def __init__(self, data, max_gap=30, latencies=LATENCIES_MS, log=print):
        self.data, self.max_gap, self.latencies, self.log = data, max_gap, latencies, log
        self.probes = replay.Probes()
        self.tracker = Tracker(on_open=self._schedule_latency)
        self.state = {}                    # (venue, market) -> latest book row
        self.valid = set()                 # live: markets whose book is current (seen since the last gap)
        self.pairs, self.by_market, self.pairs_from = [], {}, None
        self.live = False
        self.stats = collections.Counter()
        self.latencies_seen, self.gaps = [], []
        self.first_ms = self.last_ms = None
        self.down_since = None             # live: when the recorder last stopped cleanly, until it starts again
        self._tbill = {}

    # -- helpers --

    def _set_pairs(self, d):
        pairs, used = self.data.pairs(d)
        if used != self.pairs_from:
            self.pairs, self.pairs_from = pairs, used
            self.by_market = collections.defaultdict(list)
            for p in pairs:
                self.by_market[("kalshi", p["kalshi_ticker"])].append(p)
                self.by_market[("poly", p["poly_yes_token"])].append(p)
            self.log(f"pairs for {d}: {len(pairs)} from {used}")

    def tbill_for(self, d):
        if d not in self._tbill:
            self._tbill[d] = self.data.tbill(d)[0]
        return self._tbill[d]

    def _books(self, pair):
        return self.state.get(("kalshi", pair["kalshi_ticker"])), self.state.get(("poly", pair["poly_yes_token"]))

    def _evaluate(self, pair, ms):
        kb, pb = self._books(pair)
        for name, poly_asks, kalshi_asks in baskets(pb, kb):
            fill = walk(poly_asks, kalshi_asks, pair["poly_fee_rate"], pair["kalshi_fee_rate"],
                        poly_min_size=pair["poly_min_order"])
            uncertain = self.live and walk_uncertain(name, fill, poly_asks, pb, kb)
            self.tracker.update(pair, name, fill, ms, uncertain)

    def _schedule_latency(self, ep):
        if not self.live:
            return                         # 5 s polling can't say what happened 50 ms later
        pair, basket = ep["pair"], ep["basket"]
        for ms in self.latencies:
            def check(ok, ep=ep, ms=ms):
                kb, pb = self._books(pair)
                legs_ok = ok and ("kalshi", pair["kalshi_ticker"]) in self.valid \
                    and ("poly", pair["poly_yes_token"]) in self.valid
                if not legs_ok:
                    return                 # data ended or coverage lost before then: unknown
                for name, poly_asks, kalshi_asks in baskets(pb, kb):
                    if name == basket:
                        f = walk(poly_asks, kalshi_asks, pair["poly_fee_rate"], pair["kalshi_fee_rate"],
                                 poly_min_size=pair["poly_min_order"])
                        ep["lat"][ms] = (f.profit, f.size)
            self.probes.schedule(ep["start"] + ms, check)

    # -- legacy (5 s polling): Vihaan's cycle loop --

    def run_legacy_day(self, d):
        cycles = [c for c in replay.coverage(self.data, d) if c["event"] == "poll_cycle"]
        rows = replay.book_rows(self.data, d)
        pending = next(rows, None)
        unknown = set()
        for c in cycles:
            started, finished = c["recv_ms"], c["finished_ms"]
            self.first_ms = started if self.first_ms is None else self.first_ms
            self.stats["cycles"] += 1
            self.latencies_seen.append(finished - started)
            missing = c.get("missing") or []
            self.stats["partial"] += bool(missing)
            updates = []
            while pending is not None and pending["ts_ms"] <= finished:
                updates.append(pending)
                pending = next(rows, None)
            self.stats["book_rows"] += len(updates)
            dirty = set()
            if self.last_ms is not None and (started - self.last_ms) / 1000 > self.max_gap:
                self.tracker.close_all(self.last_ms, "coverage_gap")
                self.gaps.append((self.last_ms, started))
                dirty.update(p["pair_id"] for p in self.pairs)
            self.last_ms = started
            for r in updates:
                self.state[(r["venue"], r["market"])] = r
                dirty.update(p["pair_id"] for p in self.by_market.get((r["venue"], r["market"]), []))
            now_unknown = set()
            for venue, market in missing:
                for p in self.by_market.get((venue, market), []):
                    now_unknown.add(p["pair_id"])
                    self.tracker.close_pair(p["pair_id"], started, "missing_data")
            dirty |= unknown - now_unknown
            unknown = now_unknown
            for p in self.pairs:
                pid = p["pair_id"]
                if pid in now_unknown:
                    continue
                kb, pb = self._books(p)
                if kb is None or pb is None:
                    continue
                if pid not in dirty:
                    self.tracker.extend(pid, started)
                    continue
                self._evaluate(p, started)

    # -- live (every book change) --

    def _gap(self, ms, reason="coverage_gap"):
        self.tracker.close_all(ms, reason)
        self.valid.clear()

    def _close_market(self, venue, market, ms, reason):
        for p in self.by_market.get((venue, market), []):
            self.tracker.close_pair(p["pair_id"], ms, reason)
        self.valid.discard((venue, market))

    def run_live_day(self, d):
        events = [(e["recv_ms"], 0, e) for e in replay.coverage(self.data, d)
                  if e["event"] in ("started", "stopped", "disconnected", "market_closed")]
        ticks = ((ms, 1, group) for ms, group in replay.grouped(replay.book_rows(self.data, d)))

        for ms, kind, item in heapq.merge(events, ticks, key=lambda x: (x[0], x[1])):
            self.probes.due(ms)
            if kind == 0:
                e = item
                if e["venue"] == "recorder" and e["event"] == "stopped":
                    self._gap(ms)
                    self.down_since = ms
                elif e["venue"] == "recorder" and e["event"] == "started":
                    if self.down_since is not None:                  # clean stop, then start: a known gap
                        self.gaps.append((self.down_since, ms))
                    elif self.last_ms is not None:                   # start without a stop: the recorder died
                        self._gap(self.last_ms)
                        self.gaps.append((self.last_ms, ms))
                    self.down_since = None
                    self.valid.clear()
                elif e["event"] == "disconnected":
                    self.tracker.close_all(ms, "coverage_gap")
                    self.valid = {k for k in self.valid if k[0] != e["venue"]}
                elif e["event"] == "market_closed":
                    market = e.get("market") or e.get("token")
                    venue = "kalshi" if e["venue"] == "kalshi" else "poly"
                    self._close_market(venue, market, ms, "market_closed")
                continue
            self.first_ms = ms if self.first_ms is None else self.first_ms
            self.last_ms = ms
            self.stats["ticks"] += 1
            self.stats["book_rows"] += len(item)
            dirty = {}
            for r in item:
                key = (r["venue"], r["market"])
                self.state[key] = r
                self.valid.add(key)
                for p in self.by_market.get(key, []):
                    dirty[p["pair_id"]] = p
            for pid, p in dirty.items():
                if ("kalshi", p["kalshi_ticker"]) in self.valid and ("poly", p["poly_yes_token"]) in self.valid:
                    self._evaluate(p, ms)

    # -- driver --

    def run(self, dates):
        prev_era = None
        for d in dates:
            self._set_pairs(d)
            day_era = replay.era(self.data, d)
            if prev_era is not None and day_era != prev_era and self.last_ms is not None:
                self.tracker.close_all(self.last_ms, "coverage_gap")     # the two eras never join
                self.valid.clear()
            self.live = day_era == "live"
            self.log(f"{d}: {day_era} data")
            (self.run_live_day if self.live else self.run_legacy_day)(d)
            prev_era = day_era
        self.probes.flush()
        if self.last_ms is not None:
            self.tracker.close_all(self.last_ms, "still_open")
        return sorted((row(e, self.tbill_for, self.latencies) for e in self.tracker.done),
                      key=lambda r: -r["peak_profit"])


def run_episodes(data, from_date=None, to_date=None, max_gap=30, latencies=LATENCIES_MS, log=print):
    """Episodes for every book date in [from_date, to_date] (the data object's range if not given)."""
    dates = [d for d in data.dates("books") if (not from_date or d >= from_date) and (not to_date or d <= to_date)]
    run = Run(data, max_gap, latencies, log)
    rows = run.run(dates)
    return rows, run


def summary(rows, run):
    """Printable summary, following the original analyzer's report."""
    lines = []
    if run.first_ms is None:
        return ["No data in range."]
    span = (run.last_ms - run.first_ms) / 1000
    lost = sum(b - a for a, b in run.gaps) / 1000
    if run.stats["cycles"]:
        lines.append(f"Legacy coverage: {run.stats['cycles']} cycles, {run.stats['partial']} with a missing book; "
                     f"cycle fetch time median {statistics.median(run.latencies_seen):.0f} ms")
    if run.stats["ticks"]:
        lines.append(f"Live updates evaluated: {run.stats['ticks']} distinct times")
    lines.append(f"Span {span / 3600:.2f} h, {len(run.gaps)} gaps totalling {lost:.0f} s; book rows {run.stats['book_rows']}")
    lines.append(f"\nEpisodes with positive after-fee profit: {len(rows)}")
    if not rows:
        return lines
    for label, cond in (("profit >= $1", lambda r: r["peak_profit"] >= 1),
                        ("profit >= $10", lambda r: r["peak_profit"] >= 10),
                        ("annualized >= 10%", lambda r: (r["peak_annualized_pct"] or 0) >= 10),
                        ("beats the T-bill rate", lambda r: (r["excess_annualized_pct"] or 0) > 0),
                        ("uncertain (used a level marked wrong)", lambda r: r["uncertain"]),
                        ("still profitable 1 s after start", lambda r: (r.get("profit_1000ms") or 0) > 0)):
        lines.append(f"  {label}: {sum(bool(cond(r)) for r in rows)}")
    lines.append("\nLargest episodes:")
    for r in rows[:15]:
        ann = r["peak_annualized_pct"]
        lines.append(f"  ${r['peak_profit']:>8.2f} on {r['peak_size']:>9.1f} pairs | top {r['peak_top_edge_c']:5.2f}c | "
                     f"{'n/a' if ann is None else str(ann) + '%':>8} ann | {r['duration_s']:>8.1f}s | "
                     f"{r['end_reason']:<13} | {r['basket']} | {r['kalshi_question'][:40]}")
    return lines
