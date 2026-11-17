"""Data access for analysis: one DuckDB connection over the curated tables, from any source.

    from analysis.data import open_data
    data = open_data("https://<id>.cloudfront.net", password="...", from_date="2026-11-16")
    data = open_data("s3://<bucket>")                      # AWS credentials from the usual places
    data = open_data("data/raw-local")                     # a local store directory (holds curated/)
    data.con.sql("SELECT venue, count(*) FROM books GROUP BY venue").show()
    pairs, used = data.pairs("2026-11-16")                 # the pairs in effect that day (and which file)
    rate = data.tbill("2026-11-16")                        # 3-month T-bill yield, percent

With no source argument, PMA_URL (and PMA_PASSWORD) from the environment are used.

Remote files (web address or S3) are downloaded once into a local cache that mirrors the bucket layout
(default ~/.cache/prediction-market-arb/<host>/), and fetched again only when the manifest's size or
modification time for that file changes (today's refreshed tables, or trade dates that got late trades).
DuckDB always reads local files, so venue/date columns come from the paths and queries run at disk speed.

Views: one per curated table in the date range (books, trades, coverage, and result tables when present),
each with venue/date columns from the file paths. Book files are not strictly time-ordered: sort by ts_ms.
"""
import json
import os
import re
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CACHE = os.path.join(os.path.expanduser("~"), ".cache", "prediction-market-arb")
MANIFEST_KEY = "curated/manifest.json"


# ---- sources -------------------------------------------------------------------------------------

class LocalSource:
    """A store directory on this machine; files are read in place."""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.downloaded = 0

    def manifest(self):
        path = os.path.join(self.root, *MANIFEST_KEY.split("/"))
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        # No manifest yet (e.g. a fresh local run): index the files directly, without writing anything.
        from recorder.store import LocalStore
        store, tables = LocalStore(self.root), {}
        for key, size, modified in store.list_meta("curated/"):
            if key.endswith(".parquet") and not key.startswith("curated/_state/"):
                parts = dict(p.split("=", 1) for p in key.split("/")[2:-1] if "=" in p)
                tables.setdefault(key.split("/")[1], []).append({"key": key, **parts, "bytes": size,
                                                                 "modified": modified})
        tables["ref"] = [{"key": k, "bytes": s, "modified": m} for k, s, m in store.list_meta("ref/")]
        return {"generated_ms": None, "tables": tables}

    def path(self, entry):
        return os.path.join(self.root, *entry["key"].split("/"))


class CachedSource:
    """Base for remote sources: keeps a local copy of each file, keyed on the manifest's size/modified."""

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.index_path = os.path.join(cache_dir, ".index.json")
        self.downloaded = 0
        try:
            with open(self.index_path, encoding="utf-8") as f:
                self.index = json.load(f)
        except (OSError, ValueError):
            self.index = {}

    def _get(self, key):
        raise NotImplementedError

    def manifest(self):
        return json.loads(self._get(MANIFEST_KEY))

    def path(self, entry):
        key = entry["key"]
        local = os.path.join(self.cache_dir, *key.split("/"))
        stamp = [entry.get("bytes"), entry.get("modified")]
        if os.path.exists(local) and self.index.get(key) == stamp and None not in stamp:
            return local
        data = self._get(key)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        tmp = local + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, local)
        self.index[key] = stamp
        self.downloaded += 1
        self._save_index()
        return local

    def _save_index(self):
        os.makedirs(self.cache_dir, exist_ok=True)
        tmp = self.index_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.index, f)
        os.replace(tmp, self.index_path)


class HttpsSource(CachedSource):
    """The password-protected web address (CloudFront). Sends the password as a Bearer token."""

    def __init__(self, url, password, cache_dir):
        super().__init__(cache_dir)
        self.url, self.password = url.rstrip("/"), password

    def _get(self, key):
        req = urllib.request.Request(f"{self.url}/{key}", headers={"Authorization": f"Bearer {self.password}"})
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.read()


class S3Source(CachedSource):
    """The bucket itself, with AWS credentials (environment, profile or instance role)."""

    def __init__(self, url, cache_dir, region=None):
        super().__init__(cache_dir)
        import boto3
        self.bucket, _, prefix = url[5:].partition("/")
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def _get(self, key):
        full = f"{self.prefix}/{key}" if self.prefix else key
        return self.s3.get_object(Bucket=self.bucket, Key=full)["Body"].read()


def make_source(source=None, password=None, cache_dir=None, region=None):
    source = source or os.environ.get("PMA_URL")
    if not source:
        raise SystemExit("No data source: pass a web address, s3://bucket or a local directory (or set PMA_URL).")
    if source.startswith(("https://", "http://")):
        password = password or os.environ.get("PMA_PASSWORD")
        if not password:
            raise SystemExit("The web address needs a password: pass password=... or set PMA_PASSWORD.")
        host = re.sub(r"[^A-Za-z0-9.-]", "_", source.split("://", 1)[1].split("/")[0])
        return HttpsSource(source, password, os.path.join(cache_dir or DEFAULT_CACHE, host))
    if source.startswith("s3://"):
        name = re.sub(r"[^A-Za-z0-9.-]", "_", source[5:].strip("/"))
        return S3Source(source, os.path.join(cache_dir or DEFAULT_CACHE, name), region or "us-east-2")
    return LocalSource(source)


# ---- the connection ------------------------------------------------------------------------------

def _sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def _in_range(entry, from_date, to_date):
    d = entry.get("date")
    if d is None:
        return True
    return (not from_date or d >= from_date) and (not to_date or d <= to_date)


class Data:
    def __init__(self, source, from_date=None, to_date=None):
        import duckdb
        self.source = source
        self.from_date, self.to_date = from_date, to_date
        self.manifest = source.manifest()
        self.con = duckdb.connect()
        self.tables = {}
        for table, entries in sorted(self.manifest["tables"].items()):
            if table == "ref":
                continue
            files = [source.path(e) for e in entries if _in_range(e, from_date, to_date)]
            if not files:
                continue
            paths = ", ".join(_sql_str(p) for p in files)
            self.con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet([{paths}], "
                             "hive_partitioning = true, union_by_name = true)")
            self.tables[table] = len(files)

    # ---- reference data ----

    def _ref(self, kind, name):
        """{date: manifest entry} for ref/<kind>/date=D/<name>."""
        out = {}
        for e in self.manifest["tables"].get("ref", []):
            m = re.fullmatch(rf"ref/{kind}/date=(\d{{4}}-\d{{2}}-\d{{2}})/{re.escape(name)}", e["key"])
            if m:
                out[m.group(1)] = e
        return out

    @staticmethod
    def _nearest(by_date, d):
        """The entry for date d, else the latest before it, else the earliest after it."""
        if not by_date:
            return None, None
        before = [x for x in by_date if x <= d]
        chosen = max(before) if before else min(by_date)
        return chosen, by_date[chosen]

    def pairs(self, d):
        """(pairs, where they came from) for day d. Before the first daily snapshot (the legacy Nov 9-12 data),
        the nearest snapshot is used, else the repository's watchlist.json."""
        chosen, entry = self._nearest(self._ref("pairs", "pairs.json"), d)
        if entry is not None:
            with open(self.source.path(entry), encoding="utf-8") as f:
                return json.load(f)["pairs"], f"ref/pairs/date={chosen}"
        with open(os.path.join(ROOT, "watchlist.json"), encoding="utf-8") as f:
            return json.load(f)["pairs"], "watchlist.json"

    def tbill(self, d):
        """(yield percent, rate date) in effect on day d: the latest published on or before d, else the earliest
        stored. (None, None) if no rate has been stored yet."""
        chosen, entry = self._nearest(self._ref("rates", "tbill.json"), d)
        if entry is None:
            return None, None
        with open(self.source.path(entry), encoding="utf-8") as f:
            r = json.load(f)
        return r["yield_pct"], r["rate_date"]


def open_data(source=None, password=None, from_date=None, to_date=None, cache_dir=None, region=None):
    """A Data object: .con (DuckDB with one view per curated table in the date range), .pairs(d), .tbill(d)."""
    return Data(make_source(source, password, cache_dir, region), from_date, to_date)
