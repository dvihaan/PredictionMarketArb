"""Daily reference snapshots: the pairs being watched, each watched market's metadata, and the T-bill rate.

    ref/pairs/date=D/pairs.json            the pairs file as it was when the nightly job ran on day D
    ref/pairs/date=D/pairs.jsonl           the same pairs, one JSON object per line (for the Glue/Athena `pairs` table)
    ref/markets/date=D/kalshi.jsonl.gz     Kalshi's full market record per ticker (title, rules, status, result, ...)
    ref/markets/date=D/poly.jsonl.gz       Polymarket's full market record per market (question, description, closed, ...)
    ref/markets/date=D/kalshi_series.jsonl.gz   Kalshi's series record per watched series (fee_type, fee_multiplier, ...)
    ref/rates/date=D/tbill.json            latest 3-month (13-week) Treasury bill yield published by the US Treasury,
                                           coupon-equivalent percent: the capital-cost hurdle for annualized returns

The venues only serve current metadata, so D is the date it was fetched, not the day of data being
processed. Records are stored exactly as the APIs return them, plus "fetched_ms", so rule wording,
status changes and settlement results can be compared over time. A market that can't be fetched is
recorded as {"id": ..., "error": ...} rather than skipped.
"""
import gzip
import json
import time

import requests

from recorder import pairs as pairs_mod

KALSHI_MARKET = "https://external-api.kalshi.com/trade-api/v2/markets/{}"
KALSHI_SERIES = "https://external-api.kalshi.com/trade-api/v2/series/{}"
GAMMA_MARKET = "https://gamma-api.polymarket.com/markets/{}"
TBILL_CSV = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/"
             "{year}/all?type=daily_treasury_bill_rates&field_tdr_date_value={year}&page&_format=csv")
TBILL_COLUMN = "13 WEEKS COUPON EQUIVALENT"


def parse_tbill_csv(text):
    """Latest (date ISO, yield percent) from the Treasury's daily bill-rate CSV, or None if absent."""
    import csv
    best = None
    for row in csv.DictReader(text.splitlines()):
        value = (row.get(TBILL_COLUMN) or "").strip()
        if not value or value.upper() == "N/A":
            continue
        month, day, year = row["Date"].split("/")
        d = f"{year}-{int(month):02d}-{int(day):02d}"
        if best is None or d > best[0]:
            best = (d, float(value))
    return best


def fetch_tbill(session, today):
    """Latest 3-month bill yield published on or before today: tries this year's file, then last year's
    (early January may have no rows yet). Returns a dict, or None if the Treasury site can't be reached."""
    year = int(today[:4])
    for y in (year, year - 1):
        url = TBILL_CSV.format(year=y)
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
        except Exception:
            continue
        found = parse_tbill_csv(r.text)
        if found:
            return {"rate_date": found[0], "yield_pct": found[1], "series": TBILL_COLUMN, "source": url}
    return None


def _jsonl_gz(rows):
    return gzip.compress("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode())


RETRY_STATUS = (429, 500, 502, 503, 504)


def fetch_market(session, url, ident, attempts=4, backoff_s=1.0, sleep=time.sleep):
    """One market's metadata. Rate limits (429) and server errors are retried with a growing pause:
    the recorder's trade polling shares this machine's address, so occasional 429s are expected."""
    for attempt in range(attempts):
        try:
            r = session.get(url.format(ident), timeout=15)
            if getattr(r, "status_code", 200) in RETRY_STATUS and attempt < attempts - 1:
                sleep(backoff_s * 2 ** attempt)
                continue
            r.raise_for_status()
            body = r.json()
            return body.get("market", body) if isinstance(body, dict) else {"id": ident, "body": body}
        except Exception as e:
            if attempt == attempts - 1:
                return {"id": ident, "error": f"{type(e).__name__}: {e}"[:300]}
            sleep(backoff_s * 2 ** attempt)


def snapshot(store, d, pairs_source, region=None, pause_s=0.1):
    """Write the pairs file and market metadata for every watched market under date d."""
    raw_pairs = pairs_mod.load(pairs_source, region)
    store.write(f"ref/pairs/date={d}/pairs.json", json.dumps({"pairs": raw_pairs}, indent=1).encode())
    store.write(f"ref/pairs/date={d}/pairs.jsonl",
                "".join(json.dumps(p, separators=(",", ":")) + "\n" for p in raw_pairs).encode())
    session = requests.Session()
    session.headers["User-Agent"] = "market-book-recorder/0.2"
    tbill = fetch_tbill(session, d)
    if tbill:
        store.write(f"ref/rates/date={d}/tbill.json",
                    json.dumps({**tbill, "fetched_ms": int(time.time() * 1000)}, indent=1).encode())
    kalshi, poly = [], []
    for ticker in sorted({p["kalshi_ticker"] for p in raw_pairs}):
        kalshi.append({**fetch_market(session, KALSHI_MARKET, ticker), "fetched_ms": int(time.time() * 1000)})
        time.sleep(pause_s)
    for market_id in sorted({str(p["poly_market_id"]) for p in raw_pairs if p.get("poly_market_id")}):
        poly.append({**fetch_market(session, GAMMA_MARKET, market_id), "fetched_ms": int(time.time() * 1000)})
        time.sleep(pause_s)
    store.write(f"ref/markets/date={d}/kalshi.jsonl.gz", _jsonl_gz(kalshi))
    # Fee settings live on the series (e.g. KXAKSENATE for KXAKSENATE-26NOV03): one record per watched series.
    series = []
    for st in sorted({(r.get("event_ticker") or "").split("-")[0] for r in kalshi if r.get("event_ticker")}):
        rec = fetch_market(session, KALSHI_SERIES, st)
        series.append({**(rec.get("series", rec) if isinstance(rec, dict) else rec), "fetched_ms": int(time.time() * 1000)})
        time.sleep(pause_s)
    store.write(f"ref/markets/date={d}/kalshi_series.jsonl.gz", _jsonl_gz(series))
    store.write(f"ref/markets/date={d}/poly.jsonl.gz", _jsonl_gz(poly))
    errors = sum("error" in r for r in kalshi + poly)
    return len(raw_pairs), len(kalshi), len(poly), errors, tbill
