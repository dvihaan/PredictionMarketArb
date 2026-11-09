"""Turn recorded books into after-fee, depth-aware edge episodes (streaming version).

Gives the same results as analyze.py, but reads the database row by row instead of loading
every book into memory, so it works on weeks of data and on a small server.

An episode is an unbroken run of cycles in which one hedge was fillable at a profit.
Episodes are cut when coverage is lost, and marked so, because their true length is unknown.

    python analyze_stream.py
    python analyze_stream.py --max-gap 30 --csv data/episodes.csv
"""
import argparse
import collections
import csv
import json
import os
import sqlite3
import statistics
from datetime import datetime, timezone

from edge import baskets, walk

HERE = os.path.dirname(os.path.abspath(__file__))
FIELDS = ["pair_id", "family", "rule_status", "basket", "start_utc", "duration_s", "cycles", "end_reason",
          "peak_top_edge_c", "peak_profit", "peak_size", "peak_capital", "peak_return_pct", "peak_annualized_pct",
          "est_resolution", "kalshi_question", "poly_question"]


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def annualized(ret, ms, est_resolution):
    end = datetime.fromisoformat(est_resolution).replace(tzinfo=timezone.utc)
    days = (end - datetime.fromtimestamp(ms / 1000, tz=timezone.utc)).total_seconds() / 86400
    return ret * 365 / days if days > 1 else None


class Tracker:
    def __init__(self):
        self.open = {}
        self.done = []

    def update(self, pair, name, fill, ms):
        key = (pair["pair_id"], name)
        ep = self.open.get(key)
        if fill.size <= 0 or fill.profit <= 0:
            self.close(key, ms, "edge_gone")
            return
        if ep is None:
            ep = self.open[key] = dict(pair=pair, basket=name, start=ms, last=ms, cycles=0, peak=None)
        ep["last"] = ms
        ep["cycles"] += 1
        if ep["peak"] is None or fill.profit > ep["peak"].profit:
            ep["peak"] = fill
            ep["peak_ms"] = ms

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


def row(ep):
    p, f = ep["pair"], ep["peak"]
    ret = f.profit / f.capital
    ann = annualized(ret, ep["peak_ms"], p["est_resolution"])
    return dict(pair_id=p["pair_id"], family=p["family"], rule_status=p["rule_status"], basket=ep["basket"],
                start_utc=utc(ep["start"]), duration_s=round((ep["end"] - ep["start"]) / 1000, 1), cycles=ep["cycles"],
                end_reason=ep["reason"], peak_top_edge_c=round(f.top_edge * 100, 2), peak_profit=round(f.profit, 2),
                peak_size=round(f.size, 2), peak_capital=round(f.capital, 2), peak_return_pct=round(ret * 100, 3),
                peak_annualized_pct=None if ann is None else round(ann * 100, 1), est_resolution=p["est_resolution"],
                kalshi_question=p["kalshi_question"], poly_question=p["poly_question"])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watchlist", default=os.path.join(HERE, "watchlist.json"))
    ap.add_argument("--db", default=os.path.join(HERE, "data", "books.sqlite"))
    ap.add_argument("--max-gap", type=float, default=30, help="a longer gap between cycles counts as lost coverage (seconds)")
    ap.add_argument("--csv", default=os.path.join(HERE, "data", "episodes.csv"))
    args = ap.parse_args()

    pairs = json.load(open(args.watchlist, encoding="utf-8"))["pairs"]
    by_market = collections.defaultdict(list)
    for p in pairs:
        by_market[("kalshi", p["kalshi_ticker"])].append(p)
        by_market[("poly", p["poly_yes_token"])].append(p)

    # Stream both tables in cycle order inside one read transaction, so memory stays flat
    # however long the recording is, and a recorder writing at the same time can't skew the snapshot.
    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True, isolation_level=None)
    db.execute("BEGIN")
    first_ms, last_ms, n_cycles, max_id = db.execute(
        "SELECT MIN(started_ms), MAX(started_ms), COUNT(*), MAX(id) FROM cycles").fetchone()
    if not n_cycles:
        print("No cycles recorded yet.")
        return
    cycle_rows = db.cursor().execute(
        "SELECT id, started_ms, finished_ms, missing FROM cycles WHERE id <= ? ORDER BY id", (max_id,))
    book_rows = db.cursor().execute(
        "SELECT cycle_id, venue, market, yes_bids, no_bids FROM books WHERE cycle_id <= ? ORDER BY cycle_id, rowid",
        (max_id,))
    pending = next(book_rows, None)
    n_book_rows = 0

    state, tracker = {}, Tracker()
    unknown = set()
    prev_ms = None
    gaps = []
    latencies = []
    partial = 0
    for cid, started, finished, missing in cycle_rows:
        latencies.append(finished - started)
        partial += bool(missing)
        updates = []
        while pending is not None and pending[0] <= cid:
            if pending[0] == cid:
                updates.append(pending[1:])
            pending = next(book_rows, None)
        n_book_rows += len(updates)
        dirty = set()
        if prev_ms is not None and (started - prev_ms) / 1000 > args.max_gap:
            tracker.close_all(prev_ms, "coverage_gap")
            gaps.append((prev_ms, started))
            # Episodes were just closed, so re-evaluate every pair even if its book is unchanged.
            dirty.update(p["pair_id"] for p in pairs)
        prev_ms = started

        for venue, market, yes, no in updates:
            state[(venue, market)] = dict(yes_bids=json.loads(yes), no_bids=json.loads(no))
            dirty.update(p["pair_id"] for p in by_market.get((venue, market), []))

        now_unknown = set()
        if missing:
            for venue, market in json.loads(missing):
                for p in by_market.get((venue, market), []):
                    now_unknown.add(p["pair_id"])
                    tracker.close_pair(p["pair_id"], started, "missing_data")
        # A pair that just came back must be re-evaluated even if its book is unchanged.
        dirty |= unknown - now_unknown
        unknown = now_unknown

        for p in pairs:
            pid = p["pair_id"]
            if pid in now_unknown:
                continue
            kb, pb = state.get(("kalshi", p["kalshi_ticker"])), state.get(("poly", p["poly_yes_token"]))
            if kb is None or pb is None:
                continue
            if pid not in dirty:
                # Book unchanged: extend any open episode without recomputing it.
                for name in ("P_YES+K_NO", "P_NO+K_YES"):
                    ep = tracker.open.get((pid, name))
                    if ep:
                        ep["last"] = started
                        ep["cycles"] += 1
                continue
            for name, poly_asks, kalshi_asks in baskets(pb, kb):
                fill = walk(poly_asks, kalshi_asks, p["poly_fee_rate"], p["kalshi_fee_rate"],
                            poly_min_size=p["poly_min_order"])
                tracker.update(p, name, fill, started)

    db.execute("COMMIT")
    db.close()
    tracker.close_all(last_ms, "still_open")
    rows = sorted((row(e) for e in tracker.done), key=lambda r: -r["peak_profit"])
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, FIELDS)
        w.writeheader()
        w.writerows(rows)

    span = (last_ms - first_ms) / 1000
    lost = sum(b - a for a, b in gaps) / 1000
    print(f"Coverage: {n_cycles} cycles over {span / 3600:.2f} h, {len(gaps)} gaps totalling {lost:.0f} s, "
          f"{partial} cycles with a missing book")
    print(f"Cycle fetch time: median {statistics.median(latencies):.0f} ms, max {max(latencies)} ms")
    print(f"Book rows: {n_book_rows}. Pairs on watchlist: {len(pairs)}")
    print(f"\nEpisodes with positive after-fee profit: {len(rows)} (written to {args.csv})")
    if not rows:
        return
    for label, cond in (("profit >= $1", lambda r: r["peak_profit"] >= 1),
                        ("profit >= $10", lambda r: r["peak_profit"] >= 10),
                        ("annualized >= 10%", lambda r: (r["peak_annualized_pct"] or 0) >= 10),
                        ("profit >= $1, annualized >= 10%, lasted >= 15 s",
                         lambda r: r["peak_profit"] >= 1 and (r["peak_annualized_pct"] or 0) >= 10 and r["duration_s"] >= 15)):
        print(f"  {label}: {sum(cond(r) for r in rows)}")
    print("\nBy family (episodes, median duration s, median peak profit $):")
    fam = collections.defaultdict(list)
    for r in rows:
        fam[r["family"]].append(r)
    for name, v in sorted(fam.items(), key=lambda kv: -len(kv[1])):
        print(f"  {name:20} {len(v):5} {statistics.median(x['duration_s'] for x in v):10.1f} "
              f"{statistics.median(x['peak_profit'] for x in v):10.2f}")
    print("\nLargest episodes:")
    for r in rows[:15]:
        ann = r["peak_annualized_pct"]
        print(f"  ${r['peak_profit']:>8.2f} on {r['peak_size']:>9.1f} pairs | top {r['peak_top_edge_c']:5.2f}c | "
              f"{'n/a' if ann is None else str(ann) + '%':>8} ann | {r['duration_s']:>8.1f}s | {r['end_reason']:<12} | "
              f"{r['basket']} | {r['kalshi_question'][:45]}")


if __name__ == "__main__":
    main()
