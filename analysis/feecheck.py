"""Compare the pairs file's fee rates with what the venues publish (docs/TAKER_REVIEW.md, D2).

The analysis uses the per-pair rates from the pairs file (Binoy's decision). This check flags where those differ
from the venues' own published settings, so a stale or mistyped rate gets noticed:
  Polymarket: market record feesEnabled and feeSchedule.rate (taker) vs poly_fee_rate
              (maker rebates are left out of the analysis, Binoy 2026-11-17, so feeSchedule.rebateRate isn't checked)
  Kalshi:     series record fee_type / fee_multiplier; the standard quadratic taker fee is
              0.07 x multiplier x p x (1 - p), compared with kalshi_fee_rate

    python -m analysis.run fee-check --date 2026-11-18          # from the stored daily snapshot
    python -m analysis.run fee-check --live                     # fetch the venues' records now
"""
import gzip
import json

KALSHI_BASE_RATE = 0.07
TOL = 1e-9


def _published(data, d, live):
    if live:
        import requests
        from recorder import refdata
        pairs, _ = data.pairs(d)
        s = requests.Session()
        poly = {str(p["poly_market_id"]): refdata.fetch_market(s, refdata.GAMMA_MARKET, str(p["poly_market_id"]))
                for p in pairs}
        kal = {p["kalshi_ticker"]: refdata.fetch_market(s, refdata.KALSHI_MARKET, p["kalshi_ticker"]) for p in pairs}
        series = {}
        for st in {(r.get("event_ticker") or "").split("-")[0] for r in kal.values() if r.get("event_ticker")}:
            rec = refdata.fetch_market(s, refdata.KALSHI_SERIES, st)
            series[st] = rec.get("series", rec)
        return kal, poly, series
    kal, poly = data.markets(d)
    series = {}
    _, entry = data._nearest(data._ref("markets", "kalshi_series.jsonl.gz"), d)
    if entry is not None:
        with gzip.open(data.source.path(entry), "rt", encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                if r.get("ticker"):
                    series[r["ticker"]] = r
    return kal, poly, series


def check(data, d, live=False):
    """[{pair_id, venue, item, pairs_file, published, ok}] for every pair and checked fee item."""
    pairs, _ = data.pairs(d)
    kal, poly, series = _published(data, d, live)
    out = []

    def add(pid, venue, item, mine, theirs):
        ok = None if theirs is None else (mine is not None and abs(float(mine) - float(theirs)) <= TOL)
        out.append(dict(pair_id=pid, venue=venue, item=item, pairs_file=mine, published=theirs, ok=ok))

    for p in pairs:
        pm = poly.get(str(p.get("poly_market_id"))) or {}
        sched = pm.get("feeSchedule") or {}
        enabled = pm.get("feesEnabled")
        add(p["pair_id"], "poly", "taker fee rate", p.get("poly_fee_rate"),
            None if enabled is None else (sched.get("rate") if enabled else 0.0))
        km = kal.get(p["kalshi_ticker"]) or {}
        sr = series.get((km.get("event_ticker") or "").split("-")[0]) or {}
        expected = None
        if sr.get("fee_type") == "quadratic" and sr.get("fee_multiplier") is not None:
            expected = round(KALSHI_BASE_RATE * float(sr["fee_multiplier"]), 6)
        add(p["pair_id"], "kalshi", f"taker fee rate ({sr.get('fee_type', 'series not found')})",
            p.get("kalshi_fee_rate"), expected)
    return out


def summary(rows):
    bad = [r for r in rows if r["ok"] is False]
    unknown = [r for r in rows if r["ok"] is None]
    lines = [f"Fee check: {len(rows)} items, {sum(r['ok'] is True for r in rows)} match, {len(bad)} differ, "
             f"{len(unknown)} not published or not checkable"]
    by_item = {}
    for r in bad + unknown:
        key = (r["venue"], r["item"], r["pairs_file"], r["published"], r["ok"])
        by_item.setdefault(key, []).append(r["pair_id"])
    for (venue, item, mine, theirs, ok), pids in sorted(by_item.items(), key=lambda kv: -len(kv[1])):
        tag = "DIFFERS" if ok is False else "unchecked"
        lines.append(f"  {tag:9} {venue:6} {item:38} pairs file={mine!s:6} published={theirs!s:6} "
                     f"({len(pids)} pairs, e.g. {pids[0]})")
    return lines
