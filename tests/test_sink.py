"""Tests for recorder/sink.py.

    .venv/bin/python -m unittest discover -s tests
"""
import gzip
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.sink import PART, RawSink, closed_files, raw_path, read_lines  # noqa: E402

T0 = 1_794_841_200_000          # 2026-11-16 15:00:00 UTC, on a 5-minute boundary


class Clock:
    def __init__(self, ms):
        self.ms = ms

    def __call__(self):
        return self.ms


class SinkTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.clock = Clock(T0)
        self.sink = RawSink(self.dir, window_s=300, host="h", clock=self.clock)

    def tearDown(self):
        self.tmp.cleanup()

    def test_path_layout(self):
        self.assertEqual(raw_path("kalshi_ws", T0, "h", T0 + 7),
                         os.path.join("raw", "stream=kalshi_ws", "date=2026-11-16", "hour=15",
                                      f"h-{T0}-{T0 + 7}.jsonl.gz"))
        # Files written before run ids existed keep the old name.
        self.assertTrue(raw_path("kalshi_ws", T0, "h").endswith(f"h-{T0}.jsonl.gz"))

    def test_open_file_is_part_until_closed(self):
        self.sink.write("poly_ws", {"recv_ms": T0, "raw": "x"})
        self.assertEqual(closed_files(self.dir), [])
        self.sink.close_all()
        files = closed_files(self.dir)
        self.assertEqual(len(files), 1)
        self.assertEqual(read_lines(os.path.join(self.dir, files[0])), [{"recv_ms": T0, "raw": "x"}])

    def test_rotates_on_window_change(self):
        self.sink.write("poly_ws", {"recv_ms": T0, "n": 1})
        self.clock.ms = T0 + 299_999
        self.sink.write("poly_ws", {"recv_ms": self.clock.ms, "n": 2})
        self.clock.ms = T0 + 300_000
        self.sink.write("poly_ws", {"recv_ms": self.clock.ms, "n": 3})
        self.assertEqual(len(closed_files(self.dir)), 1)     # first window closed by the rotation
        self.sink.close_all()
        files = closed_files(self.dir)
        self.assertEqual([len(read_lines(os.path.join(self.dir, f))) for f in files], [2, 1])

    def test_rotate_due_closes_quiet_streams(self):
        self.sink.write("coverage", {"recv_ms": T0})
        self.clock.ms = T0 + 300_000
        self.sink.rotate_due()
        self.assertEqual(len(closed_files(self.dir)), 1)
        self.assertEqual(self.sink.open, {})

    def test_streams_are_separate(self):
        self.sink.write("kalshi_ws", {"recv_ms": T0})
        self.sink.write("poly_ws", {"recv_ms": T0})
        self.sink.close_all()
        self.assertEqual(len(closed_files(self.dir)), 2)

    def test_recover_after_crash_keeps_flushed_lines(self):
        sink = RawSink(self.dir, window_s=300, flush_s=0, host="h", clock=self.clock)
        for i in range(3):
            sink.write("kalshi_ws", {"recv_ms": T0, "i": i})
        # Simulate a crash: the gzip stream is never closed, only sync-flushed.
        _, rel, f, _ = sink.open["kalshi_ws"]
        f.fileobj.flush()
        self.assertEqual(RawSink(self.dir, host="h").recover(), 1)
        rows = read_lines(os.path.join(self.dir, rel))
        self.assertEqual([r["i"] for r in rows], [0, 1, 2])
        f.fileobj.close()

    def test_restart_in_same_window_never_reuses_an_uploaded_name(self):
        """Regression (2026-11-17): a restart inside a 5-minute window wrote a file with the same name as the
        previous run's, and its upload overwrote the previous run's file in S3, losing up to 5 minutes."""
        uploaded = {}

        def upload_and_delete():                 # what S3Uploader does: upload, then delete the local copy
            for rel in closed_files(self.dir):
                with open(os.path.join(self.dir, rel), "rb") as f:
                    uploaded[rel] = f.read()
                os.remove(os.path.join(self.dir, rel))

        first = RawSink(self.dir, window_s=300, host="h", clock=self.clock, run_ms=T0)
        first.write("poly_ws", {"recv_ms": T0, "n": 1})
        first.close_all()
        upload_and_delete()
        self.clock.ms = T0 + 60_000              # restarted a minute later, same window
        again = RawSink(self.dir, window_s=300, host="h", clock=self.clock)
        again.write("poly_ws", {"recv_ms": self.clock.ms, "n": 2})
        again.close_all()
        upload_and_delete()
        self.assertEqual(len(uploaded), 2)       # two distinct names: nothing overwritten
        rows = sorted((r["n"] for data in uploaded.values() for r in read_lines(data)))
        self.assertEqual(rows, [1, 2])

    def test_read_tolerates_truncated_file(self):
        path = os.path.join(self.dir, "t.jsonl.gz")
        with gzip.open(path, "wb") as f:
            f.write(b'{"recv_ms":1}\n{"recv_ms":2}\n')
        with open(path, "rb") as f:
            data = f.read()
        with open(path, "wb") as f:
            f.write(data[:-6])                               # chop the gzip trailer
        self.assertEqual(read_lines(path), [{"recv_ms": 1}, {"recv_ms": 2}])


if __name__ == "__main__":
    unittest.main()
