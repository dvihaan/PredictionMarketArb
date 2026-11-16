"""Stage 1.1 probe: check Kalshi auth and both venues' WebSocket feeds, and save sample messages.

Read-only. Never prints the private key.

    .venv/bin/python tools/probe_ws.py                    # key from SSM, 60 s capture
    .venv/bin/python tools/probe_ws.py --key-file ~/.kalshi/kalshi_private.pem --key-id <uuid>
    .venv/bin/python tools/probe_ws.py --seconds 120 --pairs KXFRENCHPRES-27-DLIS KXTIME-26-ZOH

Writes tests/fixtures/{kalshi,poly}_ws_sample.jsonl: one {"recv_ms", "raw"} per message.
"""
import argparse
import asyncio
import base64
import collections
import json
import os
import sys
import time

import requests
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FIXTURES = os.path.join(ROOT, "tests", "fixtures")
KALSHI_REST = "https://external-api.kalshi.com"
KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SSM_KEY_ID = "/prediction-market-arb/kalshi/key-id"
SSM_PRIVATE_KEY = "/prediction-market-arb/kalshi/private-key"


def now_ms():
    return int(time.time() * 1000)


def load_key(args):
    if args.key_file:
        key_id = args.key_id
        pem = open(os.path.expanduser(args.key_file), "rb").read()
    else:
        import boto3
        ssm = boto3.client("ssm", region_name=args.region)
        key_id = ssm.get_parameter(Name=SSM_KEY_ID, WithDecryption=True)["Parameter"]["Value"].strip()
        pem = ssm.get_parameter(Name=SSM_PRIVATE_KEY, WithDecryption=True)["Parameter"]["Value"].strip().encode()
    if not key_id:
        sys.exit("No Kalshi key id given.")
    return key_id, serialization.load_pem_private_key(pem, password=None)


def sign(key, text):
    """Kalshi signature: Ed25519 over the text, or RSA-PSS SHA-256 with digest-length salt. Base64."""
    data = text.encode()
    if isinstance(key, ed25519.Ed25519PrivateKey):
        sig = key.sign(data)
    elif isinstance(key, rsa.RSAPrivateKey):
        sig = key.sign(data, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                       hashes.SHA256())
    else:
        raise TypeError(f"unsupported key type {type(key).__name__}")
    return base64.b64encode(sig).decode()


def auth_headers(key_id, key, method, path):
    ts = str(now_ms())
    return {"KALSHI-ACCESS-KEY": key_id, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": sign(key, ts + method + path.split("?")[0])}


def rest_auth_check(key_id, key):
    path = "/trade-api/v2/portfolio/orders"
    r = requests.get(KALSHI_REST + path, params={"limit": 1}, headers=auth_headers(key_id, key, "GET", path), timeout=15)
    body = r.text[:300] if r.status_code != 200 else "(ok)"
    print(f"[kalshi rest] signed GET {path}: HTTP {r.status_code} {body}")
    return r.status_code == 200


async def capture_kalshi(key_id, key, tickers, seconds, out):
    headers = auth_headers(key_id, key, "GET", KALSHI_WS_PATH)
    try:
        ws = await websockets.connect(KALSHI_WS, additional_headers=headers, max_size=None)
    except Exception as e:
        print(f"[kalshi ws] handshake failed: {type(e).__name__}: {e}")
        return
    print(f"[kalshi ws] connected to {KALSHI_WS}")
    async with ws:
        await ws.send(json.dumps({"id": 1, "cmd": "subscribe",
                                  "params": {"channels": ["orderbook_delta"], "market_tickers": tickers}}))
        await ws.send(json.dumps({"id": 2, "cmd": "subscribe",
                                  "params": {"channels": ["trade"], "market_tickers": tickers}}))
        end = time.monotonic() + seconds
        while (left := end - time.monotonic()) > 0:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=left)
            except asyncio.TimeoutError:
                break
            out.append({"recv_ms": now_ms(), "raw": raw})


async def capture_poly(tokens, seconds, out):
    async with websockets.connect(POLY_WS, max_size=None) as ws:
        print(f"[poly ws] connected to {POLY_WS}")
        await ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))

        async def ping():
            while True:
                await asyncio.sleep(10)
                await ws.send("PING")

        pinger = asyncio.create_task(ping())
        end = time.monotonic() + seconds
        try:
            while (left := end - time.monotonic()) > 0:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=left)
                except asyncio.TimeoutError:
                    break
                out.append({"recv_ms": now_ms(), "raw": raw})
        finally:
            pinger.cancel()


def save(name, rows):
    os.makedirs(FIXTURES, exist_ok=True)
    path = os.path.join(FIXTURES, name)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"  saved {len(rows)} messages to {os.path.relpath(path, ROOT)}")


def kind_counts(rows, key):
    c = collections.Counter()
    for r in rows:
        try:
            m = json.loads(r["raw"])
        except ValueError:
            c[f"text:{r['raw'][:10]}"] += 1
            continue
        for x in (m if isinstance(m, list) else [m]):
            c[x.get(key, "?")] += 1
    return dict(c)


def compare_kalshi(rows, tickers):
    """First WS snapshot per ticker vs a fresh REST book: top 3 levels each side."""
    r = requests.get(KALSHI_REST + "/trade-api/v2/markets/orderbooks",
                     params=[("tickers", t) for t in tickers], timeout=15).json()
    rest = {b["ticker"]: b.get("orderbook_fp") or {} for b in r.get("orderbooks", [])}
    for row in rows:
        m = json.loads(row["raw"])
        if m.get("type") != "orderbook_snapshot":
            continue
        msg = m["msg"]
        t = msg["market_ticker"]
        if t not in rest:
            continue
        for side in ("yes", "no"):
            ws_top = sorted(msg.get(f"{side}_dollars_fp") or [], key=lambda x: -float(x[0]))[:3]
            rs_top = sorted(rest[t].get(f"{side}_dollars") or [], key=lambda x: -float(x[0]))[:3]
            print(f"  {t} {side}: ws={ws_top} rest={rs_top}")
        rest.pop(t)


def poly_book_coverage(rows, tokens_by_pair):
    """Do we get a separate book per token, and is the NO book the mirror of the YES book?"""
    books = {}
    for row in rows:
        m = json.loads(row["raw"]) if row["raw"].startswith(("{", "[")) else None
        for x in (m if isinstance(m, list) else [m] if m else []):
            if x.get("event_type") == "book" and x["asset_id"] not in books:
                books[x["asset_id"]] = x
    for pid, (yes, no) in tokens_by_pair.items():
        yb, nb = books.get(yes), books.get(no)
        print(f"  {pid}: YES book {'yes' if yb else 'MISSING'}, NO book {'yes' if nb else 'MISSING'}")
        if yb and nb:
            y_best_ask = min((float(a["price"]) for a in yb["asks"]), default=None)
            n_best_bid = max((float(b["price"]) for b in nb["bids"]), default=None)
            print(f"    YES best ask {y_best_ask} vs 1 - NO best bid "
                  f"{None if n_best_bid is None else round(1 - n_best_bid, 4)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--pairs", nargs="*", default=["KXFRENCHPRES-27-DLIS", "KXFRENCHPRES-27-EPHI", "KXTIME-26-ZOH"])
    ap.add_argument("--key-file")
    ap.add_argument("--key-id")
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args()

    pairs = {p["pair_id"]: p for p in json.load(open(os.path.join(ROOT, "watchlist.json"), encoding="utf-8"))["pairs"]}
    chosen = [pairs[p] for p in args.pairs]
    tickers = [p["kalshi_ticker"] for p in chosen]

    # The watchlist stores only the YES token; look up the NO token from Polymarket's market metadata.
    tokens_by_pair = {}
    for p in chosen:
        m = requests.get(f"https://gamma-api.polymarket.com/markets/{p['poly_market_id']}", timeout=15).json()
        ids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
        no = next(t for t in ids if t != p["poly_yes_token"])
        tokens_by_pair[p["pair_id"]] = (p["poly_yes_token"], no)
    tokens = [t for pair in tokens_by_pair.values() for t in pair]

    key_id, key = load_key(args)
    print(f"Kalshi key {key_id[:8]}…, type {type(key).__name__}")
    rest_auth_check(key_id, key)

    k_rows, p_rows = [], []

    async def run():
        await asyncio.gather(capture_kalshi(key_id, key, tickers, args.seconds, k_rows),
                             capture_poly(tokens, args.seconds, p_rows))

    print(f"Capturing {args.seconds:g} s from both venues for {len(chosen)} pairs…")
    asyncio.run(run())

    print("\n[kalshi ws] message types:", kind_counts(k_rows, "type"))
    if k_rows:
        save("kalshi_ws_sample.jsonl", k_rows)
        compare_kalshi(k_rows, tickers)
    print("\n[poly ws] message types:", kind_counts(p_rows, "event_type"))
    if p_rows:
        save("poly_ws_sample.jsonl", p_rows)
        poly_book_coverage(p_rows, tokens_by_pair)


if __name__ == "__main__":
    main()
