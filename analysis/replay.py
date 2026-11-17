"""Replay the curated tables in time order: book rows, coverage events, and scheduled look-ahead checks.

Two eras of data, two clocks:
  legacy (Nov 9-12, source = rest_poll): Vihaan's recorder polled every 5 s. A "cycle" is one poll; its book
      rows are those received between the cycle's start and finish (both stored in the poll_cycle coverage
      event). Analysis evaluates once per cycle, exactly as the original analyzers did.
  live (Nov 16 on): every book change is a row. Rows received in the same millisecond are applied together,
      so one WebSocket message never produces made-up in-between states. Rows with the same ts_ms keep the
      file's own order (several Kalshi deltas to one book can share a millisecond).

Book rows come out as plain Python: (yes_bids, no_bids) as lists of (price, size) tuples, best first, ready
for edge.py; suspect_levels as a list of (side, price) tuples, or None.
"""
import heapq
import json

BATCH_ROWS = 20_000


def era(data, d):
    """'legacy' if day d's books are the migrated polling data, else 'live'."""
    keys = [e["key"] for e, _ in data.files("books", d)]
    return "legacy" if keys and all(k.endswith("part-legacy.parquet") for k in keys) else "live"


def _sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def _levels(lst):
    return [(x["price"], x["size"]) for x in lst] if lst else []


def max_lateness(con, path):
    """How far (ms) any row of a file is earlier than a row stored before it. Book files are in time order
    except Polymarket suspect rows, which are written just before their repair (see recorder/compact.py), so
    this is about one suspect window at most. Reads only two small columns."""
    q = (f"SELECT coalesce(max(prior_max - ts_ms), 0) FROM ("
         f"  SELECT ts_ms, max(ts_ms) OVER (ORDER BY file_row_number ROWS UNBOUNDED PRECEDING) AS prior_max"
         f"  FROM read_parquet({_sql_str(path)}, file_row_number = true))")
    return max(int(con.execute(q).fetchall()[0][0]), 0)


def _file_rows(con, venue, path):
    """One book file's rows in time order (ties in file order), with bounded memory: rows are streamed in file
    order through a reorder buffer that holds only max_lateness ms of rows, instead of sorting the whole day
    (a full sort of a day of nested books needs ~1 GB, too much for the 0.5 GB server)."""
    con = con.cursor()          # one open streaming result per connection: each file stream gets its own cursor
    cols = {c[0] for c in con.execute(f"SELECT name FROM parquet_schema({_sql_str(path)})").fetchall()}
    levels_col = "suspect_levels" if "suspect_levels" in cols else "NULL::STRUCT(side VARCHAR, price DOUBLE)[]"
    late = max_lateness(con, path)
    rel = con.sql(f"SELECT ts_ms, market, source, suspect, {levels_col} AS suspect_levels, yes_bids, no_bids, "
                  f"file_row_number AS rn FROM read_parquet({_sql_str(path)}, file_row_number = true)")
    # DuckDB 2.x: to_arrow_reader(); 1.x: fetch_record_batch().
    reader = rel.to_arrow_reader(BATCH_ROWS) if hasattr(rel, "to_arrow_reader") else rel.fetch_record_batch(BATCH_ROWS)
    heap, newest = [], None
    for batch in reader:
        for r in batch.to_pylist():
            heapq.heappush(heap, (r["ts_ms"], r["rn"], r))
            newest = r["ts_ms"] if newest is None else max(newest, r["ts_ms"])
            # Nothing still to come can be earlier than newest - late, so everything up to then is final.
            while heap and heap[0][0] < newest - late:
                yield venue, heapq.heappop(heap)[2]
    while heap:
        yield venue, heapq.heappop(heap)[2]


def book_rows(data, d):
    """Day d's book rows in time order (ties: venue, then file order), streamed with flat memory.

    Yields dicts: ts_ms, venue, market, source, suspect, suspect_levels, yes_bids, no_bids."""
    paths = sorted((e["venue"], p) for e, p in data.files("books", d))
    streams = [_file_rows(data.con, venue, p) for venue, p in paths]
    for venue, r in heapq.merge(*streams, key=lambda x: (x[1]["ts_ms"], x[0], x[1]["rn"])):
        levels = r["suspect_levels"]
        yield {"ts_ms": r["ts_ms"], "venue": venue, "market": r["market"], "source": r["source"],
               "suspect": bool(r["suspect"]),
               "suspect_levels": [(x["side"], x["price"]) for x in levels] if levels is not None else None,
               "yes_bids": _levels(r["yes_bids"]), "no_bids": _levels(r["no_bids"])}


def grouped(rows):
    """Group consecutive rows with the same ts_ms: yields (ts_ms, [rows])."""
    cur, group = None, []
    for r in rows:
        if r["ts_ms"] != cur and group:
            yield cur, group
            group = []
        cur = r["ts_ms"]
        group.append(r)
    if group:
        yield cur, group


def coverage(data, d):
    """Day d's coverage events, in time order: dicts with recv_ms, venue, event and the decoded detail."""
    out = []
    for _, p in data.files("coverage", d):
        for recv, venue, event, detail in data.con.execute(
                f"SELECT recv_ms, venue, event, detail FROM read_parquet({_sql_str(p)})").fetchall():
            out.append({"recv_ms": recv, "venue": venue, "event": event,
                        **(json.loads(detail) if detail else {})})
    out.sort(key=lambda e: e["recv_ms"])
    return out


class Probes:
    """Look-ahead checks scheduled for a future time (e.g. "the books 250 ms after this episode started").

    Before applying the next events at time T, call due(T) to run every check scheduled before T: the
    books at that point are exactly as they stood at the check's time, and each check runs as fn(True).
    At the end of the data, flush() runs the remaining checks as fn(False): nothing is known about a time
    after the data ends, so the check must record "unknown" rather than read the last books."""

    def __init__(self):
        self.heap, self.n = [], 0

    def schedule(self, at_ms, fn):
        heapq.heappush(self.heap, (at_ms, self.n, fn))
        self.n += 1

    def due(self, before_ms):
        while self.heap and self.heap[0][0] < before_ms:
            _, _, fn = heapq.heappop(self.heap)
            fn(True)

    def flush(self):
        while self.heap:
            _, _, fn = heapq.heappop(self.heap)
            fn(False)
