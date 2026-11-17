"""Tests for analysis/data.py: views over local and remote (password-protected HTTP) sources, the
download cache, and the pairs / T-bill lookups.

    .venv/bin/python -m unittest discover -s tests
"""
import functools
import http.server
import json
import os
import sys
import tempfile
import threading
import time
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from analysis.data import open_data  # noqa: E402
from recorder.compact import write_manifest  # noqa: E402
from recorder.store import LocalStore  # noqa: E402

PASSWORD = "pw"


def write_parquet(store, key, rows):
    import io
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    store.write(key, buf.getvalue())


def build_store(root):
    store = LocalStore(root)
    write_parquet(store, "curated/books/venue=kalshi/date=2026-11-16/part-0.parquet", [{"ts_ms": 1, "market": "K"}])
    write_parquet(store, "curated/books/venue=poly/date=2026-11-16/part-0.parquet", [{"ts_ms": 2, "market": "Y"}])
    write_parquet(store, "curated/books/venue=kalshi/date=2026-11-17/part-0.parquet",
                  [{"ts_ms": 3, "market": "K"}, {"ts_ms": 4, "market": "K"}])
    write_parquet(store, "curated/trades/venue=kalshi/date=2026-11-16/part-0.parquet", [{"trade_id": "t"}])
    store.write("ref/pairs/date=2026-11-17/pairs.json", json.dumps({"pairs": [{"pair_id": "P17"}]}).encode())
    store.write("ref/pairs/date=2026-11-19/pairs.json", json.dumps({"pairs": [{"pair_id": "P19"}]}).encode())
    store.write("ref/rates/date=2026-11-17/tbill.json",
                json.dumps({"rate_date": "2026-11-16", "yield_pct": 4.26}).encode())
    write_manifest(store)
    return store


class AuthHandler(http.server.SimpleHTTPRequestHandler):
    requests = []

    def do_GET(self):
        if self.headers.get("Authorization") != f"Bearer {PASSWORD}":
            self.send_response(401)
            self.end_headers()
            return
        AuthHandler.requests.append(self.path)
        super().do_GET()

    def log_message(self, *args):
        pass


class LocalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        build_store(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_views_and_partition_columns(self):
        data = open_data(self.tmp.name)
        rows = data.con.sql("SELECT venue, CAST(date AS VARCHAR) d, count(*) FROM books GROUP BY ALL ORDER BY 2, 1").fetchall()
        self.assertEqual(rows, [("kalshi", "2026-11-16", 1), ("poly", "2026-11-16", 1), ("kalshi", "2026-11-17", 2)])
        self.assertEqual(data.tables, {"books": 3, "trades": 1})

    def test_date_range(self):
        data = open_data(self.tmp.name, from_date="2026-11-17")
        self.assertEqual(data.con.sql("SELECT count(*) FROM books").fetchall(), [(2,)])
        self.assertNotIn("trades", data.tables)            # no trades in range: no view

    def test_pairs_and_tbill_lookup(self):
        data = open_data(self.tmp.name)
        self.assertEqual(data.pairs("2026-11-18"), ([{"pair_id": "P17"}], "ref/pairs/date=2026-11-17"))
        self.assertEqual(data.pairs("2026-11-19")[1], "ref/pairs/date=2026-11-19")
        # Before the first snapshot (the legacy Nov 9-12 days): the earliest snapshot after.
        self.assertEqual(data.pairs("2026-11-10")[1], "ref/pairs/date=2026-11-17")
        self.assertEqual(data.tbill("2026-11-20"), (4.26, "2026-11-16"))

    def test_pairs_fall_back_to_watchlist(self):
        empty = tempfile.TemporaryDirectory()
        try:
            store = LocalStore(empty.name)
            write_parquet(store, "curated/books/venue=kalshi/date=2026-11-16/part-0.parquet", [{"ts_ms": 1}])
            data = open_data(empty.name)
            pairs, used = data.pairs("2026-11-10")
            self.assertEqual(used, "watchlist.json")
            self.assertGreater(len(pairs), 0)
            self.assertEqual(data.tbill("2026-11-10"), (None, None))
        finally:
            empty.cleanup()


class HttpsCacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store_dir = os.path.join(self.tmp.name, "store")
        self.cache = os.path.join(self.tmp.name, "cache")
        self.store = build_store(self.store_dir)
        handler = functools.partial(AuthHandler, directory=self.store_dir)
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        AuthHandler.requests = []

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def files_fetched(self):
        return [p for p in AuthHandler.requests if not p.endswith("manifest.json")]

    def test_first_run_downloads_then_cache_is_reused(self):
        d1 = open_data(self.url, PASSWORD, cache_dir=self.cache)
        self.assertEqual(d1.con.sql("SELECT count(*) FROM books").fetchall(), [(4,)])
        self.assertEqual(d1.source.downloaded, 4)
        AuthHandler.requests = []
        d2 = open_data(self.url, PASSWORD, cache_dir=self.cache)
        self.assertEqual(d2.con.sql("SELECT count(*) FROM books").fetchall(), [(4,)])
        self.assertEqual((d2.source.downloaded, self.files_fetched()), (0, []))

    def test_changed_file_is_fetched_again(self):
        open_data(self.url, PASSWORD, cache_dir=self.cache)
        time.sleep(0.01)
        write_parquet(self.store, "curated/books/venue=kalshi/date=2026-11-17/part-0.parquet",
                      [{"ts_ms": 3, "market": "K"}, {"ts_ms": 4, "market": "K"}, {"ts_ms": 5, "market": "K"}])
        write_manifest(self.store)
        AuthHandler.requests = []
        d = open_data(self.url, PASSWORD, cache_dir=self.cache)
        self.assertEqual(self.files_fetched(), ["/curated/books/venue=kalshi/date=2026-11-17/part-0.parquet"])
        self.assertEqual(d.con.sql("SELECT count(*) FROM books WHERE CAST(date AS VARCHAR) = '2026-11-17'").fetchall(), [(3,)])

    def test_only_files_in_range_are_downloaded(self):
        d = open_data(self.url, PASSWORD, from_date="2026-11-17", cache_dir=self.cache)
        self.assertEqual(self.files_fetched(), ["/curated/books/venue=kalshi/date=2026-11-17/part-0.parquet"])
        self.assertEqual(d.source.downloaded, 1)

    def test_wrong_password_fails(self):
        with self.assertRaises(Exception):
            open_data(self.url, "wrong", cache_dir=self.cache)


if __name__ == "__main__":
    unittest.main()
