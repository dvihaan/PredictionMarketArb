"""Record Kalshi and Polymarket order books and trades for the pairs in a pairs file.

Read-only: it places no orders. Kalshi's feed needs a read-only API key; Polymarket's needs none.

    .venv/bin/python -m recorder.main --out data/raw-local                          # key from SSM
    .venv/bin/python -m recorder.main --kalshi-key-file ~/.kalshi/kalshi_private.pem --kalshi-key-id <uuid>
    .venv/bin/python -m recorder.main --s3-bucket <bucket> --pairs s3://<bucket>/ref/pairs/current.json

Stops cleanly on Ctrl+C or SIGTERM: open raw files are closed and, with --s3-bucket, uploaded.
"""
import argparse
import asyncio
import os
import signal
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)          # lets "python recorder/main.py" find the recorder package

from recorder import pairs as pairs_mod  # noqa: E402
from recorder.auth import KalshiKey  # noqa: E402
from recorder.kalshi_ws import KalshiFeed  # noqa: E402
from recorder.poly_ws import PolyFeed  # noqa: E402
from recorder.sink import RawSink, S3Uploader  # noqa: E402
from recorder.trades import TradePoller  # noqa: E402


def now_ms():
    return int(time.time() * 1000)


def log(text):
    print(time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + text, flush=True)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pairs", default=os.path.join(ROOT, "watchlist.json"),
                    help="pairs file: a local path or s3://bucket/key (default: watchlist.json)")
    ap.add_argument("--out", default=os.path.join(ROOT, "data", "raw-local"),
                    help="local directory for raw files and state")
    ap.add_argument("--s3-bucket", help="upload finished raw files to this bucket; unset = keep them locally")
    ap.add_argument("--s3-prefix", default="", help="key prefix inside the bucket")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--kalshi-key-file", help="private key PEM; default: read key id and key from SSM")
    ap.add_argument("--kalshi-key-id", help="Kalshi key id, required with --kalshi-key-file")
    ap.add_argument("--no-kalshi", action="store_true", help="record Polymarket only")
    ap.add_argument("--rotate", type=float, default=300, help="seconds per raw file (default 300)")
    ap.add_argument("--poly-check", type=float, default=30,
                    help="seconds between Polymarket REST book checks (default 30, 0 = off)")
    ap.add_argument("--pairs-reload", type=float, default=300, help="seconds between pairs file reloads (default 300)")
    ap.add_argument("--trade-interval", type=float, default=60, help="seconds between trade polls (default 60, 0 = off)")
    ap.add_argument("--backfill-hours", type=float, default=24,
                    help="how far back to fetch trades when no trade state exists (default 24)")
    ap.add_argument("--upload-interval", type=float, default=60, help="seconds between upload passes (default 60)")
    ap.add_argument("--status-interval", type=float, default=60, help="seconds between status lines (default 60)")
    ap.add_argument("--duration", type=float, default=0, help="stop after this many seconds (default: run until stopped)")
    args = ap.parse_args(argv)
    if args.kalshi_key_file and not args.kalshi_key_id:
        ap.error("--kalshi-key-id is required with --kalshi-key-file")
    return args


async def every(stop, seconds, fn):
    """Run async fn every `seconds` until stop is set. Errors are logged, not fatal."""
    while not stop.is_set():
        await asyncio.wait([asyncio.ensure_future(stop.wait())], timeout=seconds)
        if stop.is_set():
            return
        try:
            await fn()
        except Exception as e:
            log(f"{getattr(fn, '__name__', 'task')} failed: {type(e).__name__}: {e}")


async def run(args):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    sink = RawSink(args.out, window_s=args.rotate)
    recovered = sink.recover()
    if recovered:
        log(f"closed out {recovered} raw files left by a previous crash")

    def coverage(venue, event, **detail):
        sink.write("coverage", {"recv_ms": now_ms(), "venue": venue, "event": event, **detail})

    coverage("recorder", "started", pid=os.getpid())
    pairs = pairs_mod.load(args.pairs, args.region)
    tickers, tokens, conditions = pairs_mod.markets(pairs)
    log(f"recording {len(pairs)} pairs: {len(tickers)} Kalshi markets, {len(tokens)} Polymarket YES tokens")

    poly = PolyFeed(sink, coverage, log=log)
    await poly.set_tokens(tokens)
    tasks = [asyncio.ensure_future(poly.run(stop))]
    if args.poly_check > 0:
        tasks.append(asyncio.ensure_future(poly.check_loop(stop, args.poly_check)))

    kalshi = None
    if not args.no_kalshi:
        key = (KalshiKey.from_file(args.kalshi_key_id, args.kalshi_key_file) if args.kalshi_key_file
               else KalshiKey.from_ssm(args.region))
        log(f"kalshi key {key!r}")
        kalshi = KalshiFeed(key, sink, coverage, log=log)
        await kalshi.set_tickers(tickers)
        tasks.append(asyncio.ensure_future(kalshi.run(stop)))

    state = {"conditions": conditions, "tickers": tickers}

    async def reload_pairs():
        new = pairs_mod.load(args.pairs, args.region)
        t, k, c = pairs_mod.markets(new)
        if (t, k) != (state["tickers"], sorted(poly.tokens)):
            log(f"pairs changed: {len(new)} pairs")
            coverage("recorder", "pairs_reloaded", pairs=len(new))
        state.update(tickers=t, conditions=c)
        await poly.set_tokens(k)
        if kalshi:
            await kalshi.set_tickers(t)

    trades = TradePoller(os.path.join(args.out, "state", "trade_marks.json"), args.backfill_hours, log=log)
    trade_count = [0]

    async def poll_trades():
        rows, errors = await asyncio.to_thread(trades.poll, state["tickers"], state["conditions"])
        recv = now_ms()
        for r in rows:
            sink.write("trades", {"recv_ms": recv, **r})
        trade_count[0] += len(rows)
        if errors:
            coverage("trades", "poll_errors", errors=errors[:5], count=len(errors))
            log(f"trade poll: {len(errors)} errors, first: {errors[0]}")

    async def rotate():
        sink.rotate_due()

    uploader = S3Uploader(args.out, args.s3_bucket, args.s3_prefix, args.region) if args.s3_bucket else None

    async def upload():
        ok, failed = await asyncio.to_thread(uploader.upload_pending)
        if failed:
            log(f"upload: {ok} files uploaded, {failed} failed (will retry)")

    async def status():
        k = (f"kalshi msgs={kalshi.messages} books={len(kalshi.books)}/{len(kalshi.tickers - kalshi.closed)} "
             f"resyncs={kalshi.resyncs} closed={len(kalshi.closed)} ") if kalshi else ""
        log(f"status: {k}poly msgs={poly.messages} books={len(poly.books)}/{len(poly.tokens - poly.closed)} "
            f"mismatches={poly.mismatches} closed={len(poly.closed)} trades={trade_count[0]}")

    tasks += [asyncio.ensure_future(every(stop, 5, rotate)),
              asyncio.ensure_future(every(stop, args.pairs_reload, reload_pairs)),
              asyncio.ensure_future(every(stop, args.status_interval, status))]
    if args.trade_interval > 0:
        tasks.append(asyncio.ensure_future(poll_trades()))           # backfill straight away
        tasks.append(asyncio.ensure_future(every(stop, args.trade_interval, poll_trades)))
    if uploader:
        tasks.append(asyncio.ensure_future(every(stop, args.upload_interval, upload)))
    if args.duration:
        loop.call_later(args.duration, stop.set)

    await stop.wait()
    log("stopping")
    trades.stop.set()
    await asyncio.wait(tasks, timeout=20)
    for t in tasks:
        t.cancel()
    await status()
    coverage("recorder", "stopped")
    sink.close_all()
    if uploader:
        ok, failed = await asyncio.to_thread(uploader.upload_pending)
        log(f"final upload: {ok} files uploaded, {failed} failed")
    log("stopped")


def main(argv=None):
    asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    main()
