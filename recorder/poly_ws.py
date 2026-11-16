"""Polymarket CLOB WebSocket feed (public, no key) plus a periodic REST check of the books.

Every message is written to the poly_ws raw stream exactly as received. Books are kept in memory so the
REST check can compare them: the feed has no sequence numbers, so a dropped message could otherwise
leave a book wrong without notice. A token that still differs from REST on a second look is
re-subscribed, which makes Polymarket send a fresh full book. REST books are also written to the
poly_rest stream, so compaction has periodic ground-truth snapshots.

Only YES tokens are subscribed: the NO book is the exact mirror of the YES book.
"""
import asyncio
import json
import time

import requests
import websockets

from recorder.books import PolyBook, _price, _size

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
REST_BOOKS = "https://clob.polymarket.com/books"
REST_BATCH = 50
PING_S = 10
SILENCE_S = 30          # no message at all, not even PONG, for this long means the connection is dead
RECHECK_S = 3


def now_ms():
    return int(time.time() * 1000)


def rest_books(session, tokens):
    """{token: book dict} from the REST endpoint, in batches. Raises on HTTP errors."""
    out = {}
    for i in range(0, len(tokens), REST_BATCH):
        r = session.post(REST_BOOKS, json=[{"token_id": t} for t in tokens[i:i + REST_BATCH]], timeout=15)
        r.raise_for_status()
        for b in r.json():
            out[b["asset_id"]] = b
    return out


def same_book(local, rest):
    """True if a local PolyBook has exactly the levels of a REST book."""
    def levels(side):
        return sorted((_price(x["price"]), _size(x["size"])) for x in rest.get(side) or [] if float(x["size"]) > 0)
    return sorted(local.bid.items()) == levels("bids") and sorted(local.ask.items()) == levels("asks")


class PolyFeed:
    def __init__(self, sink, coverage, url=WS_URL, log=print):
        self.sink, self.coverage, self.url, self.log = sink, coverage, url, log
        self.tokens = set()
        self.subscribed = set()
        self.books = {}
        self.ws = None
        self.messages = 0
        self.mismatches = 0
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "market-book-recorder/0.2"

    async def set_tokens(self, tokens):
        self.tokens = set(tokens)
        if self.ws is not None:
            await self._sync_subscriptions()

    async def run(self, stop):
        backoff = 1.0
        while not stop.is_set():
            started = time.monotonic()
            try:
                await self._session(stop)
            except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as e:
                self.coverage("poly", "disconnected", error=f"{type(e).__name__}: {e}"[:300])
                self.log(f"poly: disconnected: {type(e).__name__}: {e}")
            finally:
                self.ws, self.subscribed = None, set()
            if stop.is_set():
                break
            if time.monotonic() - started > 60:
                backoff = 1.0
            await asyncio.wait([asyncio.ensure_future(stop.wait())], timeout=backoff)
            backoff = min(backoff * 2, 60.0)

    async def _session(self, stop):
        async with websockets.connect(self.url, max_size=None, ping_interval=None, open_timeout=15) as ws:
            self.ws = ws
            self.books.clear()
            self.coverage("poly", "connected")
            self.log("poly: connected")
            if self.tokens:
                await ws.send(json.dumps({"assets_ids": sorted(self.tokens), "type": "market"}))
                self.subscribed = set(self.tokens)
            pinger = asyncio.ensure_future(self._ping(ws))
            stopper = asyncio.ensure_future(stop.wait())
            try:
                while True:
                    recv = asyncio.ensure_future(ws.recv())
                    done, _ = await asyncio.wait({recv, stopper}, timeout=SILENCE_S,
                                                 return_when=asyncio.FIRST_COMPLETED)
                    if stopper in done:
                        recv.cancel()
                        return
                    if not done:
                        recv.cancel()
                        raise asyncio.TimeoutError(f"no message for {SILENCE_S} s")
                    self._handle(recv.result())
            finally:
                pinger.cancel()
                stopper.cancel()

    async def _ping(self, ws):
        while True:
            await asyncio.sleep(PING_S)
            await ws.send("PING")

    async def _sync_subscriptions(self):
        add, drop = sorted(self.tokens - self.subscribed), sorted(self.subscribed - self.tokens)
        if add:
            await self.ws.send(json.dumps({"assets_ids": add, "operation": "subscribe"}))
        if drop:
            await self.ws.send(json.dumps({"assets_ids": drop, "operation": "unsubscribe"}))
            for t in drop:
                self.books.pop(t, None)
        self.subscribed = set(self.tokens)
        if add or drop:
            self.coverage("poly", "markets_changed", added=add, dropped=drop)

    async def resubscribe(self, tokens):
        """Unsubscribe and subscribe again: Polymarket then sends a fresh full book."""
        if self.ws is None or not tokens:
            return
        await self.ws.send(json.dumps({"assets_ids": tokens, "operation": "unsubscribe"}))
        await self.ws.send(json.dumps({"assets_ids": tokens, "operation": "subscribe"}))

    def _handle(self, raw):
        recv = now_ms()
        self.messages += 1
        if raw == "PONG":
            return                                  # heartbeat replies are not recorded
        self.sink.write("poly_ws", {"recv_ms": recv, "raw": raw})
        m = json.loads(raw)
        for ev in m if isinstance(m, list) else [m]:
            kind = ev.get("event_type")
            if kind == "book" and ev.get("asset_id") in self.subscribed:
                self.books[ev["asset_id"]] = PolyBook.from_book(ev)
            elif kind == "price_change":
                ts = int(ev["timestamp"]) if ev.get("timestamp") else None
                for c in ev.get("price_changes") or []:
                    b = self.books.get(c["asset_id"])
                    if b is not None:
                        b.apply_change(c, ts)

    # ---- REST check ------------------------------------------------------------------------------

    async def check_loop(self, stop, interval_s):
        while not stop.is_set():
            await asyncio.wait([asyncio.ensure_future(stop.wait())], timeout=interval_s)
            if stop.is_set() or self.ws is None or not self.books:
                continue
            try:
                await self.check_once()
            except Exception as e:
                self.coverage("poly", "check_failed", error=f"{type(e).__name__}: {e}"[:300])
                self.log(f"poly: REST check failed: {type(e).__name__}: {e}")

    async def check_once(self):
        tokens = sorted(self.books)
        rest = await self._fetch(tokens)
        bad = [t for t in tokens if t in rest and t in self.books and not same_book(self.books[t], rest[t])]
        if not bad:
            return []
        # Books move between the two reads, so look again before calling it a real mismatch.
        await asyncio.sleep(RECHECK_S)
        rest = await self._fetch(bad)
        bad = [t for t in bad if t in rest and t in self.books and not same_book(self.books[t], rest[t])]
        if bad:
            self.mismatches += len(bad)
            self.coverage("poly", "book_mismatch", tokens=bad)
            self.log(f"poly: {len(bad)} books differ from REST; re-subscribing them")
            await self.resubscribe(bad)
        return bad

    async def _fetch(self, tokens):
        rest = await asyncio.to_thread(rest_books, self.http, tokens)
        self.sink.write("poly_rest", {"recv_ms": now_ms(), "books": list(rest.values())})
        missing = sorted(set(tokens) - set(rest))
        if missing:
            self.coverage("poly", "rest_missing", tokens=missing)
        return rest
