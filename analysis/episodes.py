"""Taker-on-both-legs episodes: Vihaan's analyze_stream.py, ported to the curated tables and extended.

An episode is an unbroken run in which one hedge (buy YES on one venue, NO on the other) could be filled at
a profit after fees, walking both books level by level with edge.walk() (unchanged). Episodes are cut when
coverage is lost, and marked so, because their true length is unknown.

Legacy days (Nov 9-12, 5 s polling) are evaluated once per polling cycle with exactly the original rules,
so the original columns reproduce Vihaan's episodes.csv. Live days are evaluated on every book change.

Columns, in order: Vihaan's original ones (LEGACY_FIELDS, peak-based), then (docs/TAKER_REVIEW.md):
    rule_note, opportunity_id   the pair's rule notes; one id per opportunity, joining episodes of the same
                        pair/basket split only by a coverage gap shorter than JOIN_GAP_S (e.g. recorder restarts)
    start_*             the headline: what a trader acting when the episode began would get (profit, size,
                        top edge, capital in total and per venue, return, annualized to est_resolution)
    carry_pct           interest the venues pay on that capital (Kalshi APY; Polymarket holding rewards where
                        enabled), percent per year of total capital
    start_total_annualized_pct = start_annualized_pct + carry_pct;  tbill_pct;  start_excess_pct = total - T-bill
    worst_case_end, start_total_annualized_worst_pct   the same, annualized to the later of the venues' own end
                        dates (often cautious backstops): how bad it looks if capital is tied up that long
    est_resolution_passed   the pairs file's resolution date is already behind the episode (fix the pairs file)
    profit_1k ... size_50k  profit and size at the start within a capital budget of $1k / $10k / $50k
    peak_capital_kalshi, peak_capital_poly   capital per venue at the peak
    uncertain / uncertain_any   the peak walk / any walk used a book level marked wrong (suspect data)
    profit_<L>ms / size_<L>ms   the hedge re-walked on the books as they stood L ms after the start
                        ("would it still have paid if a trader reacted L ms later?"; live days only)
    actionable          read-close rules, profit_1k >= $1, start_excess_pct > 0 and still profitable 1 s after
                        the start; None on polling days, which have no sub-second data
On live days "cycles" counts book updates evaluated during the episode, not 5 s polls.
"""
import collections
import heapq
import statistics
from datetime import datetime, timezone

from analysis import economics, replay  # also puts Vihaan's modules on the import path
from edge import baskets, walk  # noqa: E402  (Vihaan's unchanged depth-walk and fee math)

LEGACY_FIELDS = ["pair_id", "family", "rule_status", "basket", "start_utc", "duration_s", "cycles", "end_reason",
                 "peak_top_edge_c", "peak_profit", "peak_size", "peak_capital", "peak_return_pct",
                 "peak_annualized_pct", "est_resolution", "kalshi_question", "poly_question"]
LATENCIES_MS = (50, 250, 1000, 5000)
BASKETS = ("P_YES+K_NO", "P_NO+K_YES")
# Which Polymarket side a basket buys through: P_YES asks come from Polymarket NO bids, P_NO asks from YES bids.
BASKET_POLY_SIDE = {"P_YES+K_NO": "no", "P_NO+K_YES": "yes"}


JOIN_GAP_S = 120


def fields(latencies=LATENCIES_MS, budgets=economics.BUDGETS):
    lat = [f"{k}_{ms}ms" for ms in latencies for k in ("profit", "size")]
    bud = [f"{k}_{b // 1000}k" for b in budgets for k in ("profit", "size")]
    return (LEGACY_FIELDS + ["rule_note", "opportunity_id",
                             "start_profit", "start_size", "start_top_edge_c", "start_capital",
                             "start_capital_kalshi", "start_capital_poly", "start_return_pct", "start_annualized_pct",
                             "carry_pct", "start_total_annualized_pct", "tbill_pct", "start_excess_pct",
                             "worst_case_end", "start_total_annualized_worst_pct", "est_resolution_passed"]
            + bud + ["peak_capital_kalshi", "peak_capital_poly", "uncertain", "uncertain_any"] + lat + ["actionable"])


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

    def update(self, pair, name, fill, ms, uncertain=False, asks=None):
        """asks = (poly_asks, kalshi_asks) the walk used; kept at the start and the peak for capital per venue
        and budget walks."""
        key = (pair["pair_id"], name)
        ep = self.open.get(key)
        if fill.size <= 0 or fill.profit <= 0:
            self.close(key, ms, "edge_gone")
            return
        if ep is None:
            ep = self.open[key] = dict(pair=pair, basket=name, start=ms, last=ms, cycles=0, peak=None,
                                       peak_uncertain=False, any_uncertain=False, lat={},
                                       start_fill=fill, start_asks=asks)
            if self.on_open:
                self.on_open(ep)
        ep["last"] = ms
        ep["cycles"] += 1
        ep["any_uncertain"] = ep["any_uncertain"] or uncertain
        if ep["peak"] is None or fill.profit > ep["peak"].profit:
            ep["peak"] = fill
            ep["peak_ms"] = ms
            ep["peak_uncertain"] = uncertain
            ep["peak_asks"] = asks

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


def row(ep, run):
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
    d = utc(ep["start"])[:10]
    kalshi_markets, poly_markets = run.markets_for(d)
    k_rec, p_rec = kalshi_markets.get(p["kalshi_ticker"]), poly_markets.get(str(p.get("poly_market_id")))
    sf, (s_poly, s_kalshi) = ep["start_fill"], ep["start_asks"] or ([], [])
    s_ret = sf.profit / sf.capital if sf.capital > 0 else None
    s_ann = economics.annualize(s_ret, ep["start"], p["est_resolution"]) if s_ret is not None else None
    cap_poly, cap_kalshi = economics.leg_capital(sf, s_poly, s_kalshi)
    carry = run.carry.blended_pct(d, cap_poly, cap_kalshi, p_rec)
    total = None if s_ann is None else s_ann * 100 + carry
    tbill = run.tbill_for(d)
    worst_end = economics.worst_case_end(ep["start"], k_rec, p_rec)
    worst_ann = economics.annualize(s_ret, ep["start"], worst_end) if s_ret is not None and worst_end else None
    out.update(rule_note=p.get("rule_note"), opportunity_id=ep.get("opportunity_id"),
               start_profit=round(sf.profit, 2), start_size=round(sf.size, 2),
               start_top_edge_c=round(sf.top_edge * 100, 2), start_capital=round(sf.capital, 2),
               start_capital_kalshi=round(cap_kalshi, 2), start_capital_poly=round(cap_poly, 2),
               start_return_pct=None if s_ret is None else round(s_ret * 100, 3),
               start_annualized_pct=None if s_ann is None else round(s_ann * 100, 1),
               carry_pct=round(carry, 2), start_total_annualized_pct=None if total is None else round(total, 1),
               tbill_pct=tbill,
               start_excess_pct=None if total is None or tbill is None else round(total - tbill, 1),
               worst_case_end=None if worst_end is None else worst_end.date().isoformat(),
               start_total_annualized_worst_pct=None if worst_ann is None else round(worst_ann * 100 + carry, 1),
               est_resolution_passed=p["est_resolution"] < d)
    pr, kr, minsz = p["poly_fee_rate"], p["kalshi_fee_rate"], p["poly_min_order"]
    for b in economics.BUDGETS:
        size, _, profit = economics.budget_walk(s_poly, s_kalshi, pr, kr, b, poly_min_size=minsz)
        out[f"profit_{b // 1000}k"], out[f"size_{b // 1000}k"] = round(profit, 2), round(size, 2)
    pk_poly, pk_kalshi = economics.leg_capital(f, *(ep.get("peak_asks") or ([], [])))
    out.update(peak_capital_kalshi=round(pk_kalshi, 2), peak_capital_poly=round(pk_poly, 2),
               uncertain=ep["peak_uncertain"], uncertain_any=ep["any_uncertain"])
    for ms in run.latencies:
        res = ep["lat"].get(ms)
        out[f"profit_{ms}ms"] = None if res is None else round(res[0], 2)
        out[f"size_{ms}ms"] = None if res is None else round(res[1], 2)
    out["actionable"] = actionable(out, ep.get("live", True))
    return out


def actionable(r, live):
    """Read-close rules, at least $1 at a $1k budget, total return above the T-bill, still profitable 1 s after
    the start. Unknown (None) on polling days, which can't say what happened 1 s later."""
    if not live:
        return None
    return bool(r["rule_status"] == "read-close" and (r["profit_1k"] or 0) >= 1
                and (r["start_excess_pct"] or 0) > 0 and (r.get("profit_1000ms") or 0) > 0)


def join_opportunities(episodes, join_gap_ms=JOIN_GAP_S * 1000):
    """Give each episode an opportunity_id: episodes of the same pair and basket where one ended in a coverage
    gap and the next began within join_gap_ms are one opportunity (a recorder restart, not a new gap)."""
    by_key = collections.defaultdict(list)
    for ep in episodes:
        by_key[(ep["pair"]["pair_id"], ep["basket"])].append(ep)
    for (pid, basket), eps in by_key.items():
        eps.sort(key=lambda e: e["start"])
        current = None
        for i, ep in enumerate(eps):
            prev = eps[i - 1] if i else None
            if not (prev and prev["reason"] == "coverage_gap" and ep["start"] - prev["end"] <= join_gap_ms):
                current = f"{pid}|{basket}|{utc(ep['start'])}"
            ep["opportunity_id"] = current


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
        self._tbill, self._markets = {}, {}
        self.carry = economics.Carry()

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

    def markets_for(self, d):
        if d not in self._markets:
            self._markets[d] = self.data.markets(d)
        return self._markets[d]

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
            before = self.tracker.open.get((pair["pair_id"], name))
            self.tracker.update(pair, name, fill, ms, uncertain, (poly_asks, kalshi_asks))
            ep = self.tracker.open.get((pair["pair_id"], name))
            if ep is not None and ep is not before:
                ep["live"] = self.live

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
        join_opportunities(self.tracker.done)
        return sorted((row(e, self) for e in self.tracker.done), key=lambda r: -r["peak_profit"])


def run_episodes(data, from_date=None, to_date=None, max_gap=30, latencies=LATENCIES_MS, log=print):
    """Episodes for every book date in [from_date, to_date] (the data object's range if not given)."""
    dates = [d for d in data.dates("books") if (not from_date or d >= from_date) and (not to_date or d <= to_date)]
    run = Run(data, max_gap, latencies, log)
    rows = run.run(dates)
    return rows, run


def summary(rows, run):
    """Printable summary. Leads with actionable opportunities (docs/TAKER_REVIEW.md B3/D1), then the breakdown by
    rule status (C1), then the largest opportunities by at-start profit (B1)."""
    lines = []
    if run.first_ms is None:
        return ["No data in range."]
    span = (run.last_ms - run.first_ms) / 1000
    lost = sum(b - a for a, b in run.gaps) / 1000
    if run.stats["cycles"]:
        lines.append(f"Polling-era coverage: {run.stats['cycles']} cycles, {run.stats['partial']} with a missing book; "
                     f"cycle fetch time median {statistics.median(run.latencies_seen):.0f} ms")
    if run.stats["ticks"]:
        lines.append(f"Live updates evaluated: {run.stats['ticks']} distinct times")
    lines.append(f"Span {span / 3600:.2f} h, {len(run.gaps)} gaps totalling {lost:.0f} s; book rows {run.stats['book_rows']}")
    if not rows:
        return lines + ["\nNo episodes with positive after-fee profit."]
    opps = collections.defaultdict(list)
    for r in rows:
        opps[r["opportunity_id"]].append(r)
    act = {o for o, rs in opps.items() if any(r["actionable"] for r in rs)}
    unknown = {o for o, rs in opps.items() if all(r["actionable"] is None for r in rs)}
    lines.append(f"\nEpisodes with positive after-fee profit: {len(rows)}, forming {len(opps)} opportunities "
                 f"(episodes split only by a recorder gap < {JOIN_GAP_S} s are one opportunity)")
    lines.append(f"  ACTIONABLE opportunities: {len(act)}   (read-close rules, >= $1 at a $1k budget, total return "
                 f"incl. carry above the T-bill, still profitable 1 s later)")
    if unknown:
        lines.append(f"  not assessable (polling-era days, no sub-second data): {len(unknown)}")
    lines.append("\nBy rule status (opportunities / actionable):")
    for status in sorted({r["rule_status"] for r in rows}):
        ids = {r["opportunity_id"] for r in rows if r["rule_status"] == status}
        lines.append(f"  {status:14} {len(ids):5} / {len(ids & act)}")
    for label, cond in (("beats the T-bill incl. carry (at start)", lambda r: (r["start_excess_pct"] or 0) > 0),
                        ("still profitable 1 s after start", lambda r: (r.get("profit_1000ms") or 0) > 0),
                        (">= $1 at a $1k budget", lambda r: (r["profit_1k"] or 0) >= 1),
                        ("uncertain (peak walk used a level marked wrong)", lambda r: r["uncertain"]),
                        ("pairs-file resolution date already passed", lambda r: r["est_resolution_passed"])):
        lines.append(f"  episodes {label}: {sum(bool(cond(r)) for r in rows)}")
    lines.append("\nLargest opportunities by profit at the start (what a trader acting then would get):")
    best = sorted(opps.values(), key=lambda rs: -max(r["start_profit"] for r in rs))[:15]
    for rs in best:
        r = max(rs, key=lambda x: x["start_profit"])
        tot, ex = r["start_total_annualized_pct"], r["start_excess_pct"]
        lines.append(f"  ${r['start_profit']:>7.2f} start (${r['profit_1k']:>5.2f} per $1k) | top {r['start_top_edge_c']:5.2f}c | "
                     f"{'n/a' if tot is None else str(tot) + '%':>7} total ann, {'n/a' if ex is None else f'{ex:+}':>6} vs T-bill | "
                     f"{'ACT' if r['actionable'] else '   '} | {r['rule_status']:13} | {r['basket']} | {r['kalshi_question'][:34]}")
    return lines
