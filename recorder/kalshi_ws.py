"""Kalshi WebSocket feed: order book deltas and trades for a set of markets.

Every message is written to the kalshi_ws raw stream exactly as received. The feed also keeps
books in memory, to detect skipped messages and to report status:
  - a gap in seq numbers triggers a get_snapshot for every subscribed market (Kalshi's documented resync);
  - an empty snapshot means the market was determined, so it is unsubscribed;
  - a dropped connection reconnects with exponential backoff and resubscribes, which sends fresh snapshots.
Connection events go to the coverage stream so analysis can tell missing data from no change.
"""
import asyncio
import json
import time

import websockets

from recorder.books import KalshiBook, SeqTracker

WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"
BOOK, TRADE = "orderbook_delta", "trade"


def now_ms():
    return int(time.time() * 1000)


class KalshiFeed:
    def __init__(self, key, sink, coverage, url=WS_URL, log=print):
        self.key, self.sink, self.coverage, self.url, self.log = key, sink, coverage, url, log
        self.tickers = set()              # what we want subscribed
        self.subscribed = set()           # what the live connection has
        self.closed = set()               # determined markets; not resubscribed while in the pairs list
        self.books = {}
        self.seq = SeqTracker()
        self.sids = {}                    # channel -> sid on the current connection
        self.ws = None
        self.next_id = 0
        self.messages = 0
        self.resyncs = 0

    # ---- control -------------------------------------------------------------------------------

    async def set_tickers(self, tickers):
        """Change the markets to record. Applied to the live connection if there is one."""
        self.tickers = set(tickers)
        if self.ws is not None and self.sids:
            await self._sync_subscriptions()

    async def run(self, stop):
        backoff = 1.0
        while not stop.is_set():
            started = time.monotonic()
            try:
                await self._session(stop)
            except (OSError, websockets.WebSocketException, asyncio.TimeoutError) as e:
                self.coverage("kalshi", "disconnected", error=f"{type(e).__name__}: {e}"[:300])
                self.log(f"kalshi: disconnected: {type(e).__name__}: {e}")
            finally:
                self.ws, self.sids, self.subscribed = None, {}, set()
            if stop.is_set():
                break
            if time.monotonic() - started > 60:
                backoff = 1.0                     # it had been up a while: retry quickly
            await asyncio.wait([asyncio.ensure_future(stop.wait())], timeout=backoff)
            backoff = min(backoff * 2, 60.0)

    # ---- connection ------------------------------------------------------------------------------

    async def _session(self, stop):
        async with websockets.connect(self.url, additional_headers=self.key.headers("GET", WS_PATH),
                                      max_size=None, ping_interval=20, ping_timeout=20,
                                      open_timeout=15) as ws:
            self.ws = ws
            self.seq.reset()
            self.coverage("kalshi", "connected")
            self.log("kalshi: connected")
            await self._subscribe_all()
            stopper = asyncio.ensure_future(stop.wait())
            try:
                while True:
                    recv = asyncio.ensure_future(ws.recv())
                    done, _ = await asyncio.wait({recv, stopper}, return_when=asyncio.FIRST_COMPLETED)
                    if stopper in done:
                        recv.cancel()
                        return
                    await self._handle(recv.result())
            finally:
                stopper.cancel()

    async def _send(self, cmd, params):
        self.next_id += 1
        await self.ws.send(json.dumps({"id": self.next_id, "cmd": cmd, "params": params}))

    async def _subscribe_all(self):
        want = sorted(self.tickers - self.closed)
        if not want:
            return
        # use_yes_price is pinned to false: NO prices stay in NO terms, as record.py stored them.
        # Kalshi says its default will change, so this keeps the recorded format stable.
        await self._send("subscribe", {"channels": [BOOK], "market_tickers": want, "use_yes_price": False})
        await self._send("subscribe", {"channels": [TRADE], "market_tickers": want})
        self.subscribed = set(want)

    async def _sync_subscriptions(self):
        want = self.tickers - self.closed
        add, drop = sorted(want - self.subscribed), sorted(self.subscribed - want)
        if not self.subscribed and add:
            await self._subscribe_all()
            return
        for sid in self.sids.values():
            if add:
                await self._send("update_subscription", {"sids": [sid], "market_tickers": add, "action": "add_markets"})
            if drop:
                await self._send("update_subscription", {"sids": [sid], "market_tickers": drop,
                                                         "action": "delete_markets"})
        for t in drop:
            self.books.pop(t, None)
        self.subscribed = set(want)
        if add or drop:
            self.coverage("kalshi", "markets_changed", added=add, dropped=drop)

    async def _resync(self, reason):
        """Ask for fresh snapshots of every subscribed market (Kalshi's documented gap recovery)."""
        self.resyncs += 1
        self.coverage("kalshi", "resync", reason=reason)
        sid = self.sids.get(BOOK)
        if sid is not None and self.subscribed:
            await self._send("update_subscription", {"sids": [sid], "market_tickers": sorted(self.subscribed),
                                                     "action": "get_snapshot"})

    # ---- messages ------------------------------------------------------------------------------

    async def _handle(self, raw):
        recv = now_ms()
        self.sink.write("kalshi_ws", {"recv_ms": recv, "raw": raw})
        self.messages += 1
        m = json.loads(raw)
        kind = m.get("type")
        # Every message carrying a sid counts toward its seq, including "ok" replies to our commands.
        if m.get("sid") is not None and m.get("seq") is not None and self.seq.gap(m["sid"], m["seq"]):
            if m["sid"] == self.sids.get(BOOK):
                await self._resync(f"seq gap at {m['seq']}")
            else:
                # Trade channel: nothing to rebuild; the REST trade poll backfills any missed trades.
                self.coverage("kalshi", "trade_seq_gap", seq=m["seq"])
        if kind == "subscribed":
            self.sids[m["msg"]["channel"]] = m["msg"]["sid"]
            if self.sids.keys() >= {BOOK, TRADE} and self.subscribed != self.tickers - self.closed:
                await self._sync_subscriptions()   # pairs changed while we were subscribing
            return
        if kind == "error":
            self.coverage("kalshi", "error", detail=m.get("msg"))
            self.log(f"kalshi: error {m.get('msg')}")
            return
        if kind not in ("orderbook_snapshot", "orderbook_delta"):
            return                                 # trade, ok, unsubscribed: kept in the raw stream only
        msg = m["msg"]
        ticker = msg["market_ticker"]
        if kind == "orderbook_snapshot":
            book = self.books[ticker] = KalshiBook.from_snapshot(msg)
            if not book.yes and not book.no:
                # Kalshi sends an empty snapshot when a market is determined, but an open market with no
                # resting orders looks the same. Check the market's status before dropping it.
                asyncio.ensure_future(self._check_closed(ticker))
        elif ticker in self.books:
            self.books[ticker].apply_delta(msg)

    async def _check_closed(self, ticker):
        try:
            status = await asyncio.to_thread(market_status, ticker)
        except Exception as e:  # a failed check just leaves the market subscribed
            self.log(f"kalshi: status check for {ticker} failed: {type(e).__name__}: {e}")
            return
        if status in CLOSED_STATUSES and ticker not in self.closed:
            self.closed.add(ticker)
            self.books.pop(ticker, None)
            self.coverage("kalshi", "market_closed", market=ticker, status=status)
            self.log(f"kalshi: {ticker} is {status}; unsubscribed")
            if self.ws is not None:
                await self._sync_subscriptions()


REST_URL = "https://external-api.kalshi.com/trade-api/v2"
CLOSED_STATUSES = {"closed", "determined", "settled", "finalized", "disputed", "amended"}


def market_status(ticker):
    import requests
    r = requests.get(f"{REST_URL}/markets/{ticker}", timeout=15)
    r.raise_for_status()
    return r.json()["market"]["status"]
