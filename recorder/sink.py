"""Raw capture files: gzip JSON lines, rotated on a fixed time window, optionally uploaded to S3.

Layout, relative to the output directory and to the S3 prefix alike:

    raw/stream=<stream>/date=YYYY-MM-DD/hour=HH/<host>-<start_ms>-<run_ms>.jsonl.gz

start_ms is the 5-minute window; run_ms is when this recorder process started. The run part keeps a
restart inside the same window from writing a file with the same name, which would overwrite the
previous run's uploaded file in S3 (this happened on 2026-11-16/17 before run_ms existed; older files
are named <host>-<start_ms>.jsonl.gz). Readers order files by (start_ms, run_ms).

Streams: kalshi_ws, poly_ws (every WebSocket message as received), poly_rest (periodic REST
books used for gap checks), trades (normalized REST trades), coverage (connects, disconnects,
resyncs, mismatches). Every line is a JSON object with at least "recv_ms".

A file being written ends in ".part". It is flushed every few seconds with a gzip sync flush, so
after a crash it is readable up to the last flush. Closed files lose the ".part" suffix and are
then safe to upload. On start, leftover ".part" files from a crash are closed out the same way.
"""
import gzip
import io
import json
import os
import socket
import time
import zlib
from datetime import datetime, timezone

PART = ".part"


def window_start(ms, window_s):
    return ms - ms % (int(window_s) * 1000)


def raw_path(stream, start_ms, host, run_ms=None):
    t = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    name = f"{host}-{start_ms}-{run_ms}.jsonl.gz" if run_ms is not None else f"{host}-{start_ms}.jsonl.gz"
    return os.path.join("raw", f"stream={stream}", f"date={t:%Y-%m-%d}", f"hour={t:%H}", name)


class RawSink:
    def __init__(self, out_dir, window_s=300, flush_s=5.0, host=None, clock=None, run_ms=None):
        self.out_dir = out_dir
        self.window_s = window_s
        self.flush_s = flush_s
        self.host = host or socket.gethostname().split(".")[0]
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.run_ms = run_ms if run_ms is not None else self.clock()    # this process's start: part of every name
        self.open = {}                 # stream -> (window_start_ms, relative path, gzip file, last flush time)
        self.lines = {}                # stream -> lines written since start, for heartbeats

    def write(self, stream, record):
        """Append one record; record must contain recv_ms. Rotates when the time window changes."""
        now = self.clock()
        start = window_start(now, self.window_s)
        cur = self.open.get(stream)
        if cur is None or cur[0] != start:
            if cur is not None:
                self._close(stream)
            rel = raw_path(stream, start, self.host, self.run_ms)
            full = os.path.join(self.out_dir, rel + PART)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            # Append mode is a safety net only: with run_ms in the name, each run writes its own files.
            cur = self.open[stream] = (start, rel, gzip.open(full, "ab"), time.monotonic())
        f = cur[2]
        f.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
        self.lines[stream] = self.lines.get(stream, 0) + 1
        if time.monotonic() - cur[3] >= self.flush_s:
            f.flush(zlib.Z_SYNC_FLUSH)
            self.open[stream] = cur[:3] + (time.monotonic(),)

    def rotate_due(self):
        """Close files whose time window has ended, even if their stream has gone quiet."""
        start = window_start(self.clock(), self.window_s)
        for stream in [s for s, cur in self.open.items() if cur[0] != start]:
            self._close(stream)

    def close_all(self):
        for stream in list(self.open):
            self._close(stream)

    def _close(self, stream):
        _, rel, f, _ = self.open.pop(stream)
        f.close()
        full = os.path.join(self.out_dir, rel)
        os.replace(full + PART, full)

    def recover(self):
        """Close out ".part" files left by a crash. Returns how many were recovered."""
        n = 0
        for root, _, files in os.walk(os.path.join(self.out_dir, "raw")):
            for name in files:
                if name.endswith(PART):
                    full = os.path.join(root, name)
                    os.replace(full, full[: -len(PART)])
                    n += 1
        return n


def closed_files(out_dir):
    """Relative paths of finished raw files, oldest first."""
    found = []
    for root, _, files in os.walk(os.path.join(out_dir, "raw")):
        for name in files:
            if name.endswith(".jsonl.gz"):
                found.append(os.path.relpath(os.path.join(root, name), out_dir))
    return sorted(found)


def read_lines(path):
    """Read a raw file (a path, or its bytes), tolerating a truncated tail left by a crash."""
    out = []
    with gzip.open(io.BytesIO(path) if isinstance(path, bytes) else path, "rb") as f:
        try:
            for line in f:
                if line.endswith(b"\n"):
                    out.append(json.loads(line))
        except (EOFError, zlib.error, gzip.BadGzipFile):
            pass
    return out


class S3Uploader:
    """Uploads finished raw files and deletes the local copy after a successful upload."""

    def __init__(self, out_dir, bucket, prefix="", region=None):
        import boto3
        self.out_dir, self.bucket, self.prefix = out_dir, bucket, prefix.strip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def upload_pending(self):
        """Blocking; run it in a worker thread. Returns (uploaded, failed)."""
        ok = failed = 0
        for rel in closed_files(self.out_dir):
            key = "/".join(x for x in (self.prefix, rel.replace(os.sep, "/")) if x)
            try:
                self.s3.upload_file(os.path.join(self.out_dir, rel), self.bucket, key)
                os.remove(os.path.join(self.out_dir, rel))
                ok += 1
            except Exception:
                failed += 1
        return ok, failed
