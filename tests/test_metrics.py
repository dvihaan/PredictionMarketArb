"""Tests for recorder/metrics.py with a fake CloudWatch client.

    .venv/bin/python -m unittest discover -s tests
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from recorder.metrics import Metrics  # noqa: E402


class FakeCloudWatch:
    def __init__(self):
        self.calls = []

    def put_metric_data(self, **kw):
        self.calls.append(kw)


class MetricsTest(unittest.TestCase):
    def test_reports_heartbeat_and_per_interval_counts(self):
        cw = FakeCloudWatch()
        m = Metrics("PMA/Recorder", client=cw)
        self.assertEqual(m.report({"PolyMessages": 100, "KalshiMessages": 10}),
                         {"Heartbeat": 1, "PolyMessages": 100, "KalshiMessages": 10})
        self.assertEqual(m.report({"PolyMessages": 130, "KalshiMessages": 10}),
                         {"Heartbeat": 1, "PolyMessages": 30, "KalshiMessages": 0})
        call = cw.calls[-1]
        self.assertEqual(call["Namespace"], "PMA/Recorder")
        self.assertEqual({d["MetricName"]: d["Value"] for d in call["MetricData"]},
                         {"Heartbeat": 1.0, "KalshiMessages": 0.0, "PolyMessages": 30.0})


if __name__ == "__main__":
    unittest.main()
