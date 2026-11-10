"""Would resting a bid on one venue and hedging on the other have made money?

For every pair this replays four standing quotes (bid YES or NO, on Kalshi or Polymarket),
fills them from the recorded trades, and hedges each fill as a taker on the other venue at
the next recorded book. See maker.py for the fill rules. Two fill models are reported:

    queue    filled once trades at the quote's price have used up the queue ahead of it
    through  filled only when a trade prints past the quote's price (a lower bound)

    python analyze_maker.py
    python analyze_maker.py --size 200 --min-ann 15 --csv data/maker_fills.csv
"""
import argparse
import collections
import csv
import json
import os
import sqlite3
from datetime import datetime, timezone

from maker import QuoteSim

HERE = os.path.dirname(os.path.abspath(__file__))
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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watchlist", default=os.path.join(HERE, "watchlist.json"))
    ap.add_argument("--db", default=os.path.join(HERE, "data", "books.sqlite"))
    ap.add_argument("--max-gap", type=float, default=30, help="a longer gap between cycles counts as lost coverage (seconds)")
    ap.add_argument("--size", type=float, default=100, help="contracts per quote (default 100)")
    ap.add_argument("--min-edge", type=float, default=0.0, help="only quote above this hedged edge, in cents (default 0)")
    ap.add_argument("--min-ann", type=float, default=10.0,
                    help="only quote if the edge is at least this annualized return to resolution, in percent (default 10)")
    ap.add_argument("--min-rest", type=float, default=10.0,
                    help="seconds a quote must rest before a trade can fill it (default 10)")
    ap.add_argument("--csv", default=os.path.join(HERE, "data", "maker_fills.csv"))
    args = ap.parse_args()

    pairs = json.load(open(args.watchlist, encoding="utf-8"))["pairs"]
    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True, isolation_level=None)
    db.execute("BEGIN")
    if not db.execute("SELECT name FROM sqlite_master WHERE name = 'trades'").fetchone():
        print("This database has no trades. Run the current record.py against it first; it backfills them.")
        return
    marks = {(v, m): ms for v, m, ms in db.execute("SELECT venue, market, covered_ms FROM trade_marks")}

    # sims[pair_id] is a list of (maker_venue, maker_market, hedge_venue, hedge_market, model, QuoteSim).
    sims = collections.defaultdict(list)
    by_trade_market = collections.defaultdict(list)
    by_book_market = collections.defaultdict(list)
    for p in pairs:
        venues = {"kalshi": (p["kalshi_ticker"], p["kalshi_fee_rate"], p.get("kalshi_maker_fee_rate", 0.0)),
                  "poly": (p["poly_yes_token"], p["poly_fee_rate"], p.get("poly_maker_fee_rate", 0.0))}
        by_book_market[("kalshi", p["kalshi_ticker"])].append(p["pair_id"])
        by_book_market[("poly", p["poly_yes_token"])].append(p["pair_id"])
        for maker_venue, hedge_venue in (("kalshi", "poly"), ("poly", "kalshi")):
            m_market, _, m_rate = venues[maker_venue]
            h_market, h_rate, _ = venues[hedge_venue]
            for side in ("yes", "no"):
                for model in MODELS:
                    s = QuoteSim(side, m_rate, h_rate, hedge_venue == "kalshi", size=args.size,
                                 min_edge=args.min_edge / 100, min_ann=args.min_ann / 100, resolve_ms=resolve_ms(p),
                                 min_rest_ms=int(args.min_rest * 1000), through_only=model == "through")
                    entry = (maker_venue, m_market, hedge_venue, h_market, model, s)
                    sims[p["pair_id"]].append(entry)
                    by_trade_market[(maker_venue, m_market)].append(s)

    first_ms, last_ms, n_cycles, max_id = db.execute(
        "SELECT MIN(started_ms), MAX(started_ms), COUNT(*), MAX(id) FROM cycles").fetchone()
    if not n_cycles:
        print("No cycles recorded yet.")
        return
    cycle_rows = db.cursor().execute("SELECT id, started_ms, missing FROM cycles WHERE id <= ? ORDER BY id", (max_id,))
    book_rows = db.cursor().execute(
        "SELECT cycle_id, venue, market, yes_bids, no_bids FROM books WHERE cycle_id <= ? ORDER BY cycle_id, rowid",
        (max_id,))
    trade_rows = db.cursor().execute(
        "SELECT ts_ms, venue, market, yes_price, size, taker FROM trades WHERE ts_ms >= ? ORDER BY ts_ms", (first_ms,))
    next_book, next_trade = next(book_rows, None), next(trade_rows, None)

    state = {}
    idle = set(sims)            # pairs whose quotes are pulled and must be re-evaluated when data returns
    prev_ms = None
    covered_ms = n_gaps = n_trades = 0
    for cid, started, missing in cycle_rows:
        dirty = set()
        if prev_ms is not None:
            if (started - prev_ms) / 1000 > args.max_gap:
                # Nothing is known about the gap, so pull every quote and skip the trades inside it.
                n_gaps += 1
                for pid, entries in sims.items():
                    for *_, s in entries:
                        s.cancel(prev_ms)
                idle = set(sims)
                while next_trade is not None and next_trade[0] <= started:
                    next_trade = next(trade_rows, None)
            else:
                covered_ms += started - prev_ms
        prev_ms = started

        # Trades since the last cycle hit the quotes that were resting through it.
        while next_trade is not None and next_trade[0] <= started:
            ts, venue, market, yes_price, size, taker = next_trade
            n_trades += 1
            for s in by_trade_market.get((venue, market), ()):
                s.on_trade(ts, yes_price, size, taker)
            next_trade = next(trade_rows, None)

        while next_book is not None and next_book[0] <= cid:
            if next_book[0] == cid:
                _, venue, market, yes, no = next_book
                state[(venue, market)] = dict(yes_bids=json.loads(yes), no_bids=json.loads(no))
                dirty.update(by_book_market.get((venue, market), ()))
            next_book = next(book_rows, None)

        unknown = set()
        if missing:
            for venue, market in json.loads(missing):
                unknown.update(by_book_market.get((venue, market), ()))

        for pid, entries in sims.items():
            if pid in unknown:
                for *_, s in entries:
                    s.cancel(started)
                idle.add(pid)
                continue
            refresh = pid in dirty or pid in idle
            idle.discard(pid)
            for maker_venue, m_market, hedge_venue, h_market, _, s in entries:
                if not refresh and not s.pending:
                    continue                   # books unchanged: the quote keeps resting as it is
                mb, hb = state.get((maker_venue, m_market)), state.get((hedge_venue, h_market))
                if mb is None or hb is None:
                    continue
                mark = marks.get((maker_venue, m_market), 0)
                if started > mark:
                    s.cancel(mark)             # trades are not recorded this far yet, so fills are unknown
                    if s.pending:
                        s.on_books(started, mb, hb)
                        s.cancel()
                    continue
                s.on_books(started, mb, hb)
    db.execute("COMMIT")
    db.close()

    rows = []
    stats = collections.defaultdict(lambda: collections.defaultdict(float))
    by_pair = {p["pair_id"]: p for p in pairs}
    for pid, entries in sims.items():
        p = by_pair[pid]
        end = resolve_ms(p)
        for maker_venue, m_market, _, _, model, s in entries:
            s.cancel(min(last_ms, marks.get((maker_venue, m_market), 0)))
            for key in ((model, "venue", maker_venue), (model, "family", p["family"])):
                stats[key]["live_h"] += s.live_ms / 3600000
            for f in s.fills:
                ann = annualized(f.profit / f.capital, f.ts_ms, end) if f.capital > 0 else None
                rows.append(dict(
                    model=model, pair_id=pid, family=p["family"], maker_venue=maker_venue, side=s.side,
                    fill_utc=utc(f.ts_ms), kind=f.kind, price=round(f.price, 4), size=round(f.size, 2),
                    hedged=round(f.hedged, 2), hedge_delay_s=round((f.hedge_ms - f.ts_ms) / 1000, 1) if f.hedge_ms else None,
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
                    st["ann_weighted"] += (ann or 0) * f.capital
    rows.sort(key=lambda r: (r["model"], r["fill_utc"]))
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, FIELDS)
        w.writeheader()
        w.writerows(rows)

    hours = covered_ms / 3600000
    print(f"Coverage: {n_cycles} cycles, {hours:.2f} h of continuous books, {n_gaps} gaps, {n_trades} trades in range")
    print(f"Quote: {args.size:g} contracts at the best bid when the hedged edge is above {args.min_edge:g}c and "
          f"{args.min_ann:g}% annualized; must rest {args.min_rest:g} s before filling")
    print(f"Fills written to {args.csv}")
    if hours <= 0:
        return
    for model in MODELS:
        print(f"\n== Fill model: {model}")
        print(f"  {'':22}{'quote-h':>9}{'fills':>7}{'contracts':>11}{'quoted c':>10}{'realized c':>12}"
              f"{'profit $':>10}{'$/day':>9}{'ann %':>8}")
        for kind in ("venue", "family"):
            keys = sorted((k for k in stats if k[0] == model and k[1] == kind), key=lambda k: -stats[k]["profit"])
            for key in keys:
                st = stats[key]
                label = ("maker on " if kind == "venue" else "") + key[2]
                if not st["hedged"]:
                    print(f"  {label:22}{st['live_h']:9.1f}{0:7d}")
                    continue
                print(f"  {label:22}{st['live_h']:9.1f}{int(st['fills']):7d}{st['size']:11.0f}"
                      f"{st['quoted'] / st['hedged'] * 100:10.2f}{st['profit'] / st['hedged'] * 100:12.2f}"
                      f"{st['profit']:10.2f}{st['profit'] / hours * 24:9.2f}"
                      f"{st['ann_weighted'] / st['capital'] * 100:8.1f}")
    unhedged = sum(r["size"] - r["hedged"] for r in rows if r["model"] == "queue")
    if unhedged > 0.01:
        print(f"\n{unhedged:.0f} filled contracts (queue model) found no hedge in the next book and are left out of profit.")
    print("\nquote-h is the summed time quotes were resting. $/day scales profit to 24 h of coverage.")
    print("ann % is profit over capital, annualized to each pair's estimated resolution date.")


if __name__ == "__main__":
    main()
