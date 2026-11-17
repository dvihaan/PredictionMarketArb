"""Query the recorded data from any computer with DuckDB, over the password-protected web address.
No AWS account needed: just the address and the password. Files are cached locally and only fetched
again when they change (see analysis/data.py).

    pip install duckdb
    export PMA_URL=https://<id>.cloudfront.net  PMA_PASSWORD=<password>

    python tools/query.py "SELECT venue, date, count(*) FROM books GROUP BY ALL ORDER BY 2, 1"
    python tools/query.py --from 2026-11-16 --to 2026-11-18 --csv out.csv "SELECT * FROM trades"
    python tools/query.py --local data/raw-local "SELECT count(*) FROM books"     # a local store instead
    python tools/query.py --source s3://<bucket> "SELECT ..."                      # the bucket, with AWS credentials

From Python (e.g. a notebook), the same views come back as a DuckDB connection:

    import sys; sys.path.insert(0, "tools")
    from query import connect
    con = connect(from_date="2026-11-16")
    df = con.sql("SELECT * FROM books WHERE venue = 'kalshi' LIMIT 1000").df()

Views: books, trades, coverage (each with venue/date columns from the file paths), plus result tables
when they exist. Only files in the date range are fetched.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from analysis.data import open_data  # noqa: E402


def connect(url=None, password=None, from_date=None, to_date=None, local_dir=None, source=None, cache_dir=None):
    """A DuckDB connection with one view per curated table over the chosen date range."""
    return open_data(local_dir or source or url, password, from_date, to_date, cache_dir).con


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sql")
    ap.add_argument("--from", dest="from_date", help="first date to include, YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", help="last date to include, YYYY-MM-DD")
    ap.add_argument("--csv", help="write the full result to this CSV file instead of printing it")
    ap.add_argument("--local", help="read a local store directory instead of the web address")
    ap.add_argument("--source", help="a web address, s3://bucket or local directory (default: PMA_URL)")
    ap.add_argument("--cache", help="cache folder for downloaded files (default ~/.cache/prediction-market-arb)")
    args = ap.parse_args(argv)
    data = open_data(args.local or args.source, None, args.from_date, args.to_date, args.cache)
    rel = data.con.sql(args.sql)
    if args.csv:
        rel.write_csv(args.csv)
        print(f"wrote {args.csv}")
    else:
        rel.show(max_rows=50)
    if data.source.downloaded:
        print(f"({data.source.downloaded} files downloaded; the rest came from the local cache)", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
