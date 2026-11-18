"""Run analyses over the recorded data.

    python -m analysis.run episodes --from 2026-11-16 --to 2026-11-17          # source from PMA_URL/PMA_PASSWORD
    python -m analysis.run episodes --source data/raw-local --out data/analysis
    python -m analysis.run episodes --source s3://<bucket> --from 2026-11-09 --to 2026-11-12
    python -m analysis.run maker-baseline --from 2026-11-16           # Vihaan's maker model, unchanged, as a baseline
    python -m analysis.run maker-live --from 2026-11-16               # live maker model, beside the baseline
    python -m analysis.run fee-check --live                           # pairs-file fees vs the venues' published fees

Writes <out>/episodes.csv (and .parquet) and prints a summary. Sources: a web address (with PMA_PASSWORD),
s3://bucket, or a local store directory; remote files are cached locally (see analysis/data.py).
"""
import argparse
import csv
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from analysis import episodes, feecheck, maker_baseline, maker_live  # noqa: E402
from analysis.data import open_data  # noqa: E402


def write_rows(path_base, rows, fieldnames):
    os.makedirs(os.path.dirname(path_base) or ".", exist_ok=True)
    with open(path_base + ".csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames)
        w.writeheader()
        w.writerows(rows)
    import pyarrow as pa
    import pyarrow.parquet as pq
    if rows:
        pq.write_table(pa.Table.from_pylist(rows), path_base + ".parquet", compression="zstd")


def cmd_episodes(args):
    data = open_data(args.source, None, args.from_date, args.to_date, args.cache)
    rows, run = episodes.run_episodes(data, args.from_date, args.to_date, args.max_gap,
                                      log=lambda s: print(s, file=sys.stderr, flush=True))
    base = os.path.join(args.out, "episodes")
    write_rows(base, rows, episodes.fields())
    print("\n".join(episodes.summary(rows, run)))
    print(f"\nWritten to {base}.csv")


def cmd_maker_baseline(args):
    data = open_data(args.source, None, args.from_date, args.to_date, args.cache)
    rows, stats, b = maker_baseline.run_baseline(
        data, args.from_date, args.to_date, log=lambda s: print(s, file=sys.stderr, flush=True),
        size=args.size, min_edge_c=args.min_edge, min_ann_pct=args.min_ann, min_rest_s=args.min_rest)
    base = os.path.join(args.out, "maker_fills_baseline")
    write_rows(base, rows, maker_baseline.FIELDS)
    print("\n".join(maker_baseline.summary(rows, stats, b)))
    print(f"\nWritten to {base}.csv")


def cmd_maker_live(args):
    log = lambda s: print(s, file=sys.stderr, flush=True)  # noqa: E731
    data = open_data(args.source, None, args.from_date, args.to_date, args.cache)
    rows, m = maker_live.run_live(
        data, args.from_date, args.to_date, log=log, latencies=args.latency, cancel_models=args.cancel_model,
        size=args.size, min_edge_c=args.min_edge, min_ann_pct=args.min_ann, cap_pair=args.cap_pair,
        cap_total=args.cap_total, reconcile_ms=args.reconcile_ms)
    base = os.path.join(args.out, "maker_fills_live")
    write_rows(base, rows, maker_live.FIELDS)
    baseline = None
    if not args.no_baseline:
        live_days = [d for d in data.dates("books") if (not args.from_date or d >= args.from_date)
                     and (not args.to_date or d <= args.to_date) and d not in m.skipped_days]
        if live_days:
            baseline = maker_baseline.run_baseline(data, live_days[0], live_days[-1], log=log, size=args.size,
                                                   min_edge_c=args.min_edge, min_ann_pct=args.min_ann)
    print("\n".join(maker_live.summary(rows, m, baseline)))
    print(f"\nWritten to {base}.csv")


def cmd_fee_check(args):
    from datetime import datetime, timezone
    d = args.date or datetime.now(timezone.utc).date().isoformat()
    data = open_data(args.source, None, None, None, args.cache)
    rows = feecheck.check(data, d, live=args.live)
    base = os.path.join(args.out, "fee_check")
    write_rows(base, rows, ["pair_id", "venue", "item", "pairs_file", "published", "ok"])
    print("\n".join(feecheck.summary(rows)))
    print(f"\nWritten to {base}.csv")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)
    e = sub.add_parser("episodes", help="taker-on-both-legs episodes (port of analyze_stream.py)")
    m = sub.add_parser("maker-baseline", help="Vihaan's maker simulation, unchanged, as a baseline")
    lv = sub.add_parser("maker-live", help="live maker model (queue tracking, latency, markouts, caps)")
    f = sub.add_parser("fee-check", help="compare pairs-file fees with the venues' published fees")
    for p in (e, m, lv, f):
        p.add_argument("--source", help="web address, s3://bucket or local store (default: PMA_URL)")
        p.add_argument("--from", dest="from_date", help="first date, YYYY-MM-DD")
        p.add_argument("--to", dest="to_date", help="last date, YYYY-MM-DD")
        p.add_argument("--out", default=os.path.join(ROOT, "data", "analysis"), help="output folder")
        p.add_argument("--cache", help="cache folder for downloaded files")
    e.add_argument("--max-gap", type=float, default=30,
                   help="legacy data: a longer gap between polling cycles counts as lost coverage (seconds)")
    m.add_argument("--size", type=float, default=100, help="contracts per quote (default 100)")
    m.add_argument("--min-edge", type=float, default=0.0, help="quote only above this hedged edge, cents")
    m.add_argument("--min-ann", type=float, default=10.0, help="quote only above this annualized return, percent")
    m.add_argument("--min-rest", type=float, default=10.0, help="seconds a quote rests before a fill (his model)")
    lv.add_argument("--latency", type=int, nargs="+", default=list(maker_live.LATENCIES_MS),
                    help="round-trip latencies to run, ms (default 250 1000 5000)")
    lv.add_argument("--cancel-model", nargs="+", choices=maker_live.CANCEL_MODELS,
                    default=list(maker_live.CANCEL_MODELS), help="where unexplained size drops come from")
    lv.add_argument("--size", type=float, default=100, help="contracts per quote (default 100)")
    lv.add_argument("--min-edge", type=float, default=0.0, help="quote only above this hedged edge, cents")
    lv.add_argument("--min-ann", type=float, default=10.0, help="quote only above this annualized return, percent")
    lv.add_argument("--cap-pair", type=float, default=maker_live.CAP_PAIR, help="capital cap per pair, $")
    lv.add_argument("--cap-total", type=float, default=maker_live.CAP_TOTAL, help="capital cap in total, $")
    lv.add_argument("--reconcile-ms", type=int, default=maker_live.RECONCILE_MS,
                    help="how long a size drop waits for its trade before counting as a cancellation, ms")
    lv.add_argument("--no-baseline", action="store_true", help="skip running Vihaan's baseline alongside")
    f.add_argument("--date", help="day whose pairs and snapshot to check (default today, UTC)")
    f.add_argument("--live", action="store_true", help="fetch the venues' current records instead of the snapshot")
    args = ap.parse_args(argv)
    {"episodes": cmd_episodes, "maker-baseline": cmd_maker_baseline, "maker-live": cmd_maker_live,
     "fee-check": cmd_fee_check}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
