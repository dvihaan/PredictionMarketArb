"""Load the list of pairs to record, from a local file or from S3.

The file is watchlist.json's format: {"pairs": [{"pair_id", "kalshi_ticker", "poly_yes_token",
"poly_condition_id", ...}, ...]}. The recorder reloads it periodically, so pairs can be added or
dropped without a restart.
"""
import json

REQUIRED = ("pair_id", "kalshi_ticker", "poly_yes_token")


def parse(text):
    data = json.loads(text)
    pairs = data["pairs"] if isinstance(data, dict) else data
    for p in pairs:
        missing = [k for k in REQUIRED if not p.get(k)]
        if missing:
            raise ValueError(f"pair {p.get('pair_id', '?')} is missing {', '.join(missing)}")
    return pairs


def load(source, region=None):
    """source is a file path or s3://bucket/key."""
    if source.startswith("s3://"):
        import boto3
        bucket, _, key = source[5:].partition("/")
        body = boto3.client("s3", region_name=region).get_object(Bucket=bucket, Key=key)["Body"].read()
        return parse(body.decode("utf-8"))
    with open(source, encoding="utf-8") as f:
        return parse(f.read())


def markets(pairs):
    """(Kalshi tickers, Polymarket YES tokens, {condition id: YES token}) for a list of pairs."""
    tickers = sorted({p["kalshi_ticker"] for p in pairs})
    tokens = sorted({p["poly_yes_token"] for p in pairs})
    conditions = {p["poly_condition_id"]: p["poly_yes_token"] for p in pairs if p.get("poly_condition_id")}
    return tickers, tokens, conditions
