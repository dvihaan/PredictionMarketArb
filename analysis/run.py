"""Run analyses over the recorded data.

    python -m analysis.run episodes --from 2026-11-16 --to 2026-11-17          # source from PMA_URL/PMA_PASSWORD
    python -m analysis.run episodes --source data/raw-local --out data/analysis
    python -m analysis.run episodes --source s3://<bucket> --from 2026-11-09 --to 2026-11-12
    python -m analysis.run maker-baseline --from 2026-11-16           # Vihaan's maker model, unchanged, as a baseline

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

from analysis import episodes, maker_baseline  # noqa: E402
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


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)
    e = sub.add_parser("episodes", help="taker-on-both-legs episodes (port of analyze_stream.py)")
    m = sub.add_parser("maker-baseline", help="Vihaan's maker simulation, unchanged, as a baseline")
    for p in (e, m):
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
    args = ap.parse_args(argv)
    {"episodes": cmd_episodes, "maker-baseline": cmd_maker_baseline}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
