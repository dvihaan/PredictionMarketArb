"""Query the recorded data from any computer with DuckDB, over the password-protected web address.
No AWS account needed: just the address and the password.

    pip install duckdb
    export PMA_URL=https://<id>.cloudfront.net  PMA_PASSWORD=<password>

    python tools/query.py "SELECT venue, date, count(*) FROM books GROUP BY ALL ORDER BY 2, 1"
    python tools/query.py --from 2026-11-16 --to 2026-11-18 --csv out.csv "SELECT * FROM trades"
    python tools/query.py --local data/raw-local "SELECT count(*) FROM books"     # a local store instead

From Python (e.g. a notebook), the same views come back as a DuckDB connection:

    import sys; sys.path.insert(0, "tools")
    from query import connect
    con = connect(from_date="2026-11-16")
    df = con.sql("SELECT * FROM books WHERE venue = 'kalshi' LIMIT 1000").df()

Views: books, trades, coverage (each with venue/date columns from the file paths). Only files in the
date range are read, and DuckDB fetches just the columns and row groups a query needs.
"""
import argparse
import json
import os
import sys
import urllib.request

TABLES = ("books", "trades", "coverage")


def _manifest(url, password, local_dir):
    if local_dir:
        with open(os.path.join(local_dir, "curated", "manifest.json"), encoding="utf-8") as f:
            return json.load(f)
    req = urllib.request.Request(url.rstrip("/") + "/curated/manifest.json",
                                 headers={"Authorization": f"Bearer {password}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def connect(url=None, password=None, from_date=None, to_date=None, local_dir=None):
    """A DuckDB connection with books/trades/coverage views over the chosen date range."""
    import duckdb
    url = url or os.environ.get("PMA_URL")
    password = password or os.environ.get("PMA_PASSWORD")
    if not local_dir and not (url and password):
        raise SystemExit("Set PMA_URL and PMA_PASSWORD (or pass --local DIR).")
    con = duckdb.connect()
    if not local_dir:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("CREATE SECRET pma (TYPE http, EXTRA_HTTP_HEADERS MAP {'Authorization': "
                    + _sql_str(f"Bearer {password}") + "})")
    m = _manifest(url, password, local_dir)
    for table in TABLES:
        files = [f for f in m["tables"].get(table, [])
                 if (not from_date or f.get("date", "") >= from_date) and (not to_date or f.get("date", "") <= to_date)]
        if not files:
            continue
        base = local_dir if local_dir else url.rstrip("/")
        paths = ", ".join(_sql_str(f"{base}/{f['key']}") for f in files)
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet([{paths}], "
                    "hive_partitioning = true, union_by_name = true)")
    return con


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sql")
    ap.add_argument("--from", dest="from_date", help="first date to include, YYYY-MM-DD")
    ap.add_argument("--to", dest="to_date", help="last date to include, YYYY-MM-DD")
    ap.add_argument("--csv", help="write the full result to this CSV file instead of printing it")
    ap.add_argument("--local", help="read a local store directory instead of the web address")
    args = ap.parse_args(argv)
    con = connect(from_date=args.from_date, to_date=args.to_date, local_dir=args.local)
    rel = con.sql(args.sql)
    if args.csv:
        rel.write_csv(args.csv)
        print(f"wrote {args.csv}")
    else:
        rel.show(max_rows=50)


if __name__ == "__main__":
    sys.exit(main())
