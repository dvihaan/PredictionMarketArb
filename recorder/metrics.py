"""Recorder heartbeat metrics for CloudWatch alarms.

Each status interval the recorder reports, as counts for that interval:
    Heartbeat        always 1: a missing heartbeat means the recorder is down
    KalshiMessages   Kalshi WebSocket messages received (0 for a while means the feed is silent)
    PolyMessages     Polymarket WebSocket messages received
    PolyMismatches   Polymarket books corrected after a REST check
    UploadFailures   raw files that failed to upload to S3
The nightly job reports NightlyOK = 1 after a successful run.
"""


class Metrics:
    def __init__(self, namespace, region=None, client=None):
        self.namespace = namespace
        if client is None:
            import boto3
            client = boto3.client("cloudwatch", region_name=region)
        self.client = client
        self.last = {}

    def deltas(self, totals):
        """Turn running totals into per-interval counts. The first call reports the totals themselves."""
        out = {name: value - self.last.get(name, 0) for name, value in totals.items()}
        self.last = dict(totals)
        return out

    def put(self, values):
        """Blocking; run in a worker thread. values maps metric name to number."""
        self.client.put_metric_data(Namespace=self.namespace, MetricData=[
            {"MetricName": name, "Value": float(value), "Unit": "Count"} for name, value in sorted(values.items())])

    def report(self, totals):
        """Heartbeat plus per-interval counts for the given running totals."""
        values = {"Heartbeat": 1, **self.deltas(totals)}
        self.put(values)
        return values
