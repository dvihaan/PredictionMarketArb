"""REST trade fetchers for both venues, from Vihaan's original recorder (deprecated/record.py).

Both venues publish full trade history, so these can backfill any period:
  kalshi_trades(session, ticker, since_ms, stop)        -> rows for one Kalshi market
  poly_trades(session, by_condition, since_ms, stop)    -> rows for up to 20 Polymarket markets
Each row is (venue, trade_id, market, ts_ms, yes_price, size, taker, tx_hash), in YES terms.
tx_hash is the Polymarket settlement transaction (None for Kalshi). Compaction uses it to find the
WebSocket print of the same trade, which gives our receive time (the one change from Vihaan's version).
"""
import calendar
import hashlib
import time
from datetime import datetime

KALSHI_TRADES = "https://api.elections.kalshi.com/trade-api/v2/markets/trades"
POLY_TRADES = "https://data-api.polymarket.com/v2/trades"
POLY_TRADE_BATCH = 20      # the API accepts at most 20 condition ids per request
TRADE_PAGE = 1000
TRADE_MAX_PAGES = 200
TRADE_LAG_MS = 60_000      # trades can be published late, so each poll re-reads the last minute
TIMEOUT = 15


def request_json(session, method, url, **kw):
    """One retry after a rate limit or server error; a slow cycle should stay visible."""
    for attempt in range(2):
        r = session.request(method, url, timeout=TIMEOUT, **kw)
        if r.status_code in (429, 500, 502, 503, 504) and attempt == 0:
            time.sleep(1.0)
            continue
        r.raise_for_status()
        return r.json()


def chunks(items, n):
    return [items[i:i + n] for i in range(0, len(items), n)]


def iso_ms(text):
    """UTC timestamp like 2026-11-10T17:03:16.710012Z to epoch ms, with any number of decimals."""
    whole, _, frac = text.rstrip("Z").partition(".")
    seconds = calendar.timegm(datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").timetuple())
    return seconds * 1000 + int((frac + "000")[:3])


def kalshi_trades(session, ticker, since_ms, stop):
    """Every trade on one market since since_ms. taker is the side the taker took in YES terms."""
    rows, cursor = [], None
    for _ in range(TRADE_MAX_PAGES):
        params = {"ticker": ticker, "limit": TRADE_PAGE, "min_ts": since_ms // 1000}
        if cursor:
            params["cursor"] = cursor
        data = request_json(session, "GET", KALSHI_TRADES, params=params)
        for t in data.get("trades") or []:
            ts = iso_ms(t["created_time"])
            book_side = t.get("taker_book_side") or {"yes": "bid", "no": "ask"}[t["taker_side"]]
            rows.append(("kalshi", t["trade_id"], ticker, ts, float(t["yes_price_dollars"]), float(t["count_fp"]),
                         "buy_yes" if book_side == "bid" else "sell_yes", None))
        cursor = data.get("cursor")
        if not cursor or stop.is_set():
            return rows
    raise RuntimeError(f"more than {TRADE_MAX_PAGES} pages of trades for {ticker}")


def poly_trades(session, by_condition, since_ms, stop):
    """Every trade on a batch of markets since since_ms. by_condition maps condition id to YES token.

    The API returns newest first with no time filter, so page back until since_ms is passed.
    A trade in the NO token is the mirror trade in YES: buying NO at p is selling YES at 1 - p.
    """
    rows, cursor = [], None
    for _ in range(TRADE_MAX_PAGES):
        params = {"condition": ",".join(by_condition), "limit": TRADE_PAGE}
        if cursor:
            params["cursor"] = cursor
        data = request_json(session, "GET", POLY_TRADES, params=params)
        page = data.get("data") or []
        for t in page:
            ts = int(t["timestamp"]) * 1000
            if ts < since_ms:
                continue
            yes_token = by_condition[t["condition_id"]]
            is_yes = t["token_id"] == yes_token
            price = float(t["price"])
            buys_yes = (t["side"] == "BUY") == is_yes
            # No trade id is published, so build one. Wallet and size keep fills in one transaction apart.
            raw = "|".join(str(t[k]) for k in ("transaction_hash", "token_id", "side", "price", "size", "proxy_wallet"))
            rows.append(("poly", hashlib.sha1(raw.encode()).hexdigest()[:24], yes_token, ts,
                         round(price if is_yes else 1 - price, 6), float(t["size"]),
                         "buy_yes" if buys_yes else "sell_yes", t.get("transaction_hash")))
        pg = data.get("pagination") or {}
        cursor = pg.get("next_cursor")
        reached = page and int(page[-1]["timestamp"]) * 1000 < since_ms
        if reached or not page or not pg.get("has_more") or not cursor or stop.is_set():
            return rows
    raise RuntimeError(f"more than {TRADE_MAX_PAGES} pages of Polymarket trades")
