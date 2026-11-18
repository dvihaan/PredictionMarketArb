# Stage 3 — Analysis design

> **Status:** draft for Binoy's approval, 2026-11-17. Decisions come from the 2026-11-16/17 design
> rounds (recorded in [TASKS.md](../TASKS.md)). Builds on [ARCHITECTURE_PLAN.md](ARCHITECTURE_PLAN.md).

## 1. Goal

Port Vihaan's analysis to the new data, and extend it to answer the second half of his hypothesis.
The first half, *"do cross-venue gaps exist after fees?"*, is what his analyzers already measure.
The second half is *"could anyone actually trade them?"* The new millisecond-level data is what makes
that answerable, and the published research says it is the deciding question: most gaps are
short-lived or stale quotes.

## 2. Decisions

| Topic | Decision |
|---|---|
| Where it runs | Vihaan's laptop for experiments, **and** nightly on EC2 for yesterday, with results published as tables |
| Suspect data | **Per-level markers**: an episode or fill is "uncertain" only if its depth walk used a level that was wrong |
| Cross-venue clock | **Our receive time** (one clock for both venues: what a trader at the recorder could have seen) |
| Capital hurdle | **Daily 3-month T-bill yield**, fetched from the US Treasury, stored with the reference data |
| Laptop data access | Password-protected web address, plus a **local cache** that re-downloads only changed files |
| SQL additions | `pairs` and results tables, starter research queries, and extra research tables (below) |
| Correctness | **Reproduce the legacy results** on the migrated Nov 9–12 data |
| Maker simulation | **Improve now**, as a new model next to the unchanged `maker.py` (kept as the "legacy" mode) |
| Maker improvements | Queue tracking; hedge latency; markouts; position caps (rebates and liquidity rewards dropped, 2026-11-17) |
| Research additions | Latency sensitivity; gap time series; lead-lag and stale quotes; daily report page |
| Fees | Per-pair rates from that day's pairs file (no separate schedule) |

## 3. Architecture

```
S3 curated/ + ref/  ──(DuckDB: local dir | s3:// | https + cache)──▶  analysis/data.py
                                                                        │
                                              analysis/replay.py  (time-ordered events, books, coverage, suspect levels)
                         ┌──────────────┬───────────────┬───────────┴──────┬────────────────┐
                 episodes.py      maker (legacy)     maker_live.py      research.py        report.py
                 (edge.py)        (maker.py)         (new model)        (gaps, lead-lag,   (daily HTML)
                                                                         stale quotes)
                         └──────────────┴───────────────┴──────────────────┴────────────────┘
                     python -m analysis.run <command> --from D --to D [--source ...] [--out ...]
                     nightly on EC2: after compaction → curated/results/* tables + reports/<date>.html
```

`analysis/edge.py` and `analysis/maker.py` stay unchanged, and their tests stay green.

### 3.1 Upstream changes (nightly processing and reference data)

- **Per-level suspect markers.** The books table gains `suspect_levels: list<struct<side, price>>`.
  - At a Polymarket repair, the job compares the rebuilt book with the REST book. The side/price levels that differed become the markers, applied to that token's rows in the suspect window.
  - A Kalshi resync leaves the wrong levels unknown, so `suspect = true` with `suspect_levels = NULL` means "whole book uncertain".
  - The existing `suspect` flag is kept for compatibility.
- **Manifest metadata.** Each file entry gains `bytes` and `modified`, so the laptop cache knows when to re-download.
  This matters because late trades can update past trade partitions.
- **Trades with receive time.** Curated trades gain a nullable `recv_ms`.
  - Kalshi WebSocket trades are recorded live and get it.
  - Polymarket `last_trade_price` WebSocket events (already in the raw files) are extracted as trade prints with `recv_ms`, matched to REST trades by transaction hash (decided 2026-11-17). REST rows recorded before the recorder kept the hash fall back to a price match.
  - REST-only trades fall back to server time. Their timestamps are second-resolution, so the maker model allows for that.
- **Pairs as a table.** `ref/pairs/date=D/pairs.jsonl`, one pair per line, for a Glue `pairs` table.
- **T-bill rate.** `ref/rates/date=D/tbill.json` with the daily 3-month bill yield from the US Treasury's public data. The exact endpoint will be confirmed in implementation. A missing day falls back to the latest known rate, and the report says so.

### 3.2 Data access (`analysis/data.py`)

- One function opens a DuckDB connection for a date range from any source: a local store, `s3://`, or `https://` + password.
- The HTTPS source keeps a cache folder (default `~/.cache/prediction-market-arb/`). A file is re-downloaded only when its manifest `bytes`/`modified` change.
- It replaces `tools/query.py`'s internals; `tools/query.py` becomes a thin wrapper.

### 3.3 Replay engine (`analysis/replay.py`)

- Merges Kalshi and Polymarket book rows, trades and coverage events into one stream ordered by receive time.
- Keeps the current book per market and maps markets to pairs, using that day's pairs file.
- Knows when coverage is lost:
  - live era: recorder stopped/started, venue disconnected/connected, market closed
  - legacy era: gaps between `poll_cycle` events longer than `--max-gap` (30 s, as before)
- Answers "what did book X look like at time t" for look-ahead features (latency, markouts) by buffering a short window ahead, so memory stays flat.

### 3.4 Taker episodes (`analysis/episodes.py`, ported from `analyze_stream.py`)

- On every book change of either leg, run `edge.walk()` for both hedge directions. Episode start, extend and end rules are as in Vihaan's `Tracker`.
- New per-episode columns:
  - `uncertain`: the peak walk touched a suspect level
  - `excess_return` over the T-bill rate, for the time to resolution
  - **latency sensitivity**: walk both books as they stood 50 ms / 250 ms / 1 s / 5 s after the episode start, and record profit and size at each delay. "Survives 1 s" means it was still profitable after a 1-second reaction.

### 3.5 Maker simulation

- **Legacy mode:** Vihaan's `QuoteSim` (`maker.py`), unchanged, driven by the replay. Used for the reproduction check.
- **Live mode (`analysis/maker_live.py`, new):** same quoting policy (join the best bid when the hedged edge clears the hurdle), with:
  - **Queue tracking.** On placement, `queue_ahead` = the displayed size at our price.
    - A size decrease at our price that's explained by trades at that price consumes `queue_ahead` first.
    - An unexplained decrease (a cancellation) follows `--cancel-model pessimistic|pro-rata`: either all behind us, or split in proportion.
    - Trades at our price beyond `queue_ahead` fill us; trades through our price fill fully.
  - **Hedge latency** `--hedge-latency-ms`: the hedge walks the other venue's book as it stood fill time + latency.
  - **Markouts:** the maker venue's mid 1 s / 10 s / 60 s after each fill, relative to our price (adverse selection).
  - **Position caps** per pair and in total; quoting pauses at the cap.
  - **No rebates, no liquidity rewards** (Binoy, 2026-11-17): both are real Polymarket programs, but they are left out to keep the model to fees and fills; the results say so.

### 3.6 Research tables (`analysis/research.py`)

- **gaps:** per pair, per second in which either book changed, the best-price gap after fees for both hedge directions, plus the size at that price. Unchanged seconds are implied (carry the last value forward), which keeps the table small.
- **leadlag:** per pair per day:
  - which venue's best price moved first, and how often the other followed within 1 s / 10 s
  - lead time distribution
- **stale:** the share of positive-gap time in which one venue's book hadn't changed for more than N seconds while the other moved. That's the "false gap" signature.

### 3.7 Nightly analysis and publishing

- After compaction, the nightly job runs episodes, both maker modes, gaps, lead-lag and stale analysis for yesterday. It writes:
  - `curated/results/{episodes,maker_fills,gaps,leadlag,stale}/date=D/part-0.parquet`
  - a `reports/D.html` page plus `reports/index.html`, behind the same password
- New Glue tables: `pairs`, `episodes`, `maker_fills`, `gaps`, `leadlag`, `stale`.
- New starter queries on the query page:
  - best-price gap per pair per minute
  - minutes with a positive gap
  - top episodes by profit
  - episodes that survive a 1 s delay
  - leader venue per pair
- The 3-hourly refresh doesn't run analysis; only complete days are analyzed.

### 3.8 Daily report page

- A static HTML page generated nightly:
  - yesterday's episode count and profit
  - top pairs
  - survival by latency
  - maker results (legacy versus live)
  - data quality: coverage gaps, suspect share, repairs per hour
- Charts are inline SVG generated in Python, with no extra libraries on the server.

### 3.9 Command line

`python -m analysis.run episodes|maker|gaps|leadlag|report --from D --to D [--source ...] [--out DIR]`
writes CSV and Parquet locally. Once the port reproduces the legacy results, the old
`analysis/analyze_stream.py` and `analyze_maker.py` move to `deprecated/`, following the earlier pattern.

## 4. Correctness checks

1. **Legacy reproduction (the acceptance gate).** On the migrated Nov 9–12 data:
   - `episodes` must match Vihaan's original `episodes.csv`: same pair/basket set and the same peak profit/size/top edge, with start and end times within one 5 s cycle
   - legacy-mode maker fills must match `maker_fills.csv`
2. **Unit tests per module:** synthetic books for replay ordering, coverage cuts, suspect-level uncertainty, queue tracking (trades vs cancellations under both models), hedge latency look-ahead, markouts, caps, gap series and lead-lag.
3. **Live-era sanity:** on a recorded day, every positive gap in the `gaps` table falls inside an episode, and the reverse.
4. **Nightly budget on the t4g.nano:** compaction plus analysis under ~15 minutes, with no out-of-memory kills. Measured on the first full day.

## 5. Risks and unknowns

- **Polymarket trade timing (validated 2026-11-17):** REST trades have one-second timestamps. `last_trade_price` prints on our YES-only subscription cover YES-token trades (98.6% matched by transaction hash on Nov 17) but never NO-token trades (0 of 352). Matched by hash, ~74% of Polymarket trades get a receive time. The rest are timed at the end of their second, which limits Polymarket queue-tracking precision; the results say so. Subscribing to NO tokens would close the gap (left for later).
- **Nightly CPU and memory:** analysis adds to compaction on a 0.5 GB instance. If the budget is exceeded, the options are writing outputs to disk instead of memory, running analysis at a different hour, or a larger instance (a decision for Binoy).
- **Repair rate rising** (~31 → ~77 an hour): if it keeps rising, more data becomes "uncertain". The per-level markers limit the damage, and the report tracks the rate.

## 6. Build order (sub-stages, each verified before the next)

1. **3.A** Upstream changes: suspect levels, manifest metadata, trade `recv_ms` and Polymarket WebSocket trades, `pairs.jsonl`, T-bill
2. **3.B** Data access + cache
3. **3.C** Replay engine + episodes port + **legacy episode reproduction**
4. **3.D** Legacy maker mode + **legacy maker reproduction**
5. **3.E** Live maker model (queue tracking, hedge latency, markouts, caps)
6. **3.F** Research: latency sensitivity, gaps, lead-lag, stale quotes
7. **3.G** Nightly analysis + results tables + Glue + query-page examples
8. **3.H** Daily report page
9. **3.I** Move the old analyzer scripts to `deprecated/`; update the docs
