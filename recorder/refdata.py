"""Daily reference snapshots: the pairs being watched, and each watched market's metadata.

    ref/pairs/date=D/pairs.json            the pairs file as it was when the nightly job ran on day D
    ref/markets/date=D/kalshi.jsonl.gz     Kalshi's full market record per ticker (title, rules, status, result, ...)
    ref/markets/date=D/poly.jsonl.gz       Polymarket's full market record per market (question, description, closed, ...)

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
GAMMA_MARKET = "https://gamma-api.polymarket.com/markets/{}"


def _jsonl_gz(rows):
    return gzip.compress("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode())


def fetch_market(session, url, ident):
    try:
        r = session.get(url.format(ident), timeout=15)
        r.raise_for_status()
        body = r.json()
        return body.get("market", body) if isinstance(body, dict) else {"id": ident, "body": body}
    except Exception as e:
        return {"id": ident, "error": f"{type(e).__name__}: {e}"[:300]}


def snapshot(store, d, pairs_source, region=None, pause_s=0.1):
    """Write the pairs file and market metadata for every watched market under date d."""
    raw_pairs = pairs_mod.load(pairs_source, region)
    store.write(f"ref/pairs/date={d}/pairs.json", json.dumps({"pairs": raw_pairs}, indent=1).encode())
    session = requests.Session()
    session.headers["User-Agent"] = "market-book-recorder/0.2"
    kalshi, poly = [], []
    for ticker in sorted({p["kalshi_ticker"] for p in raw_pairs}):
        kalshi.append({**fetch_market(session, KALSHI_MARKET, ticker), "fetched_ms": int(time.time() * 1000)})
        time.sleep(pause_s)
    for market_id in sorted({str(p["poly_market_id"]) for p in raw_pairs if p.get("poly_market_id")}):
        poly.append({**fetch_market(session, GAMMA_MARKET, market_id), "fetched_ms": int(time.time() * 1000)})
        time.sleep(pause_s)
    store.write(f"ref/markets/date={d}/kalshi.jsonl.gz", _jsonl_gz(kalshi))
    store.write(f"ref/markets/date={d}/poly.jsonl.gz", _jsonl_gz(poly))
    errors = sum("error" in r for r in kalshi + poly)
    return len(raw_pairs), len(kalshi), len(poly), errors
