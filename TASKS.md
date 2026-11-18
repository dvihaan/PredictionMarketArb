# TASKS — Architecture redesign tracker

Branch: `arch-redesign` · Foundational plan: [docs/ARCHITECTURE_PLAN.md](docs/ARCHITECTURE_PLAN.md)

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[?]` blocked on a decision

**Working rules**
- Ask Binoy before any major design or implementation decision; record the answer in the Decisions log.
- Keep this file current as work happens; expand a stage's steps when we learn something new.
- Leave the contents of `analysis/edge.py` and `analysis/maker.py` unchanged. Their tests must stay green throughout.
- Don't push to `origin` until asked.
- Run all tests: `.venv/bin/python -m unittest discover -s tests`

**Repository layout** (since 2026-11-16)
| Folder | Contents |
|---|---|
| `recorder/` | Everything that runs on EC2: the live recorder (`python -m recorder.main`) and the nightly processing job (`python -m recorder.compact`), which share the book-rebuild code |
| `analysis/` | Vihaan's edge/maker math and analyzers (`edge.py`, `maker.py`, `analyze_stream.py`, `analyze_maker.py`) |
| `tools/` | One-off utilities, not part of the running system (`probe_ws.py`, `migrate_sqlite.py`) |
| `tests/` | All tests, incl. Vihaan's `test_edge.py`/`test_maker.py`; captured messages in `tests/fixtures/` |
| `infra/` | CloudFormation and deploy scripts (Stage 5) |
| `docs/` | Architecture plan, Kalshi key guide |
| `deprecated/` | Original versions, reference only; see `deprecated/README.md` |
| `data/` | Local data (SQLite recording, CSV outputs) |
| root | `README.md`, `TASKS.md`, `requirements.txt`, `watchlist.json` (the pairs file) |

---

## Stage 0 — Setup
- [x] Create branch `arch-redesign` from `main`
- [x] Save the foundational plan as `docs/ARCHITECTURE_PLAN.md`
- [x] Create this tracker
- [x] Commit setup on the branch

## Stage 1 — Recorder rewrite (WebSocket)
- [x] 1.1 Confirm the WebSocket message formats against live endpoints and save sample messages as test fixtures (`tools/probe_ws.py`, run 2026-11-16 from Binoy's Mac)
  - [x] Kalshi auth: RSA-PSS signing works. A signed REST GET returned HTTP 200 and the WS handshake was accepted at `wss://external-api-ws.kalshi.com/trade-api/ws/v2`. The "verify location" notice does not block market data (from the Mac; re-check from AWS in Stage 6).
  - [x] Kalshi `orderbook_snapshot`/`orderbook_delta`: `*_dollars_fp` string pairs and signed `delta_fp`; `seq` contiguous per `sid`; `clock` and `ts_ms` present. WS snapshots matched REST top-3 levels on every book side but one (the book moved between the two calls).
  - [x] Kalshi clock: `sending_ts_ms` is about 20 ms ahead of the Mac's clock → store server timestamps next to `recv_ms`; on EC2, rely on Amazon Time Sync.
  - [x] Polymarket: the initial `book` dump arrives as a JSON **list** in one frame; later events are single objects. `book` carries `bids`/`asks` sorted worst-first, plus `hash`, `tick_size` and `last_trade_price`. Every `price_change` carries **two entries, YES and its mirrored NO**. Text `PING`/`PONG` heartbeat works. No sequence numbers, so gaps can't be detected from the feed.
  - [x] Polymarket NO book = exact mirror of YES (best YES ask = 1 − best NO bid on all 3 pairs).
  - [ ] Kalshi `trade` message not observed yet (no trades in 60 s); capture one during the Stage 1 smoke tests
  - Fixtures: `tests/fixtures/kalshi_ws_sample.jsonl`, `tests/fixtures/poly_ws_sample.jsonl`
  - Neither venue's per-connection limit matters at 57 pairs (Kalshi documents 500,000 markets per subscription; Polymarket documents no limit).
- [~] 1.0 Kalshi API key from Binoy's account
  - [x] Read-only key created on kalshi.com with Binoy's own RSA public key (`dbinoy-api-key`, Key ID `b496ed3a-…21da`, 2026-11-15). Private key stays at `~/.kalshi/kalshi_private.pem`. The website offers a read-only option, so the scope-reduction script isn't needed.
  - [~] Check that a signed read request and the WebSocket handshake work despite the "Verify your location" notice: **works from the Mac** (1.1); re-check from AWS us-east-1 in Stage 6
  - [x] Stored in SSM Parameter Store by Binoy (2026-11-15): `/prediction-market-arb/kalshi/key-id` and `/prediction-market-arb/kalshi/private-key`. Both are Standard tier SecureStrings in us-east-1, encrypted with `alias/aws/ssm`. Checked: the key ID is a UUID; the private key is a complete PKCS#8 PEM with a trailing newline, so the recorder must strip whitespace when loading it.
  - [x] Both parameters tagged `project`, `environment=dev`, `owner=binoy` (Binoy added the missing tags; verified 2026-11-16)
  - [ ] Stage 5 IAM role: allow `ssm:GetParameter` on `/prediction-market-arb/kalshi/*` **in us-east-2** (decrypting with the AWS-managed key needs no extra KMS grant)
- [x] 1.2 `recorder/books.py`: `KalshiBook` (snapshot + signed deltas), `SeqTracker` (per-sid gap detection, reset on reconnect), `PolyBook` (book + absolute-size price changes, YES/NO view in record.py's format). Top 25 levels; same price/size rounding as `record.levels()`. `tests/test_books.py`: 11 tests, incl. fixture replays (the Polymarket YES/NO mirror holds after 91 changes).
  - Local dev env: `.venv` on Python 3.12.13; run tests with `.venv/bin/python -m unittest discover -s tests`
  - [x] Kalshi delta replay checked live (2026-11-16): after 90 s and ~130 deltas, **all 56 rebuilt books matched REST exactly**
- [x] 1.3 Feeds. `recorder/kalshi_ws.py`:
  - signed handshake; subscribes `orderbook_delta` (with `use_yes_price: false` pinned) and `trade`
  - add/delete markets on a live connection; seq-gap → `get_snapshot` resync
  - empty snapshot → checks REST market status before dropping a closed market
  - reconnects with backoff
  
  `recorder/poly_ws.py`:
  - YES tokens only; `PING` every 10 s; reconnects after 30 s of silence
  - subscribe/unsubscribe on a live connection
  - REST book check every `--poly-check` s (default 30, was 120): re-checks after 3 s, then re-subscribes tokens that still differ
  - REST books saved to the `poly_rest` stream as checkpoints
  - `recorder/auth.py`: key from SSM or a file; RSA-PSS/Ed25519 signing
- [x] 1.4 `recorder/sink.py`:
  - raw layout `raw/stream=<stream>/date=/hour=/<host>-<start_ms>.jsonl.gz`; streams `kalshi_ws`, `poly_ws`, `poly_rest`, `trades`, `coverage`
  - files written as `.part` with a gzip sync-flush every 5 s, so a crash loses at most ~5 s
  - leftover `.part` files are recovered on start; finished files upload to S3 and are then deleted locally
  - Deviation from the plan: partitioned by `stream=` instead of `venue=`, because each venue has several streams
- [x] 1.5 `recorder/pairs.py`: load from a file or `s3://`; reload every `--pairs-reload` s (default 300). Closed Kalshi markets are dropped automatically (live: `KXGOVAK-26-JKRE` found `finalized`).
- [x] 1.6 `recorder/trades.py`: REST trade backfill using `record.py`'s fetchers. Watermarks are kept in `state/trade_marks.json`, with a `--backfill-hours` fallback; marks don't advance on an interrupted fetch.
- [x] 1.7 Coverage events: started/stopped, connected/disconnected, resync, trade_seq_gap, market_closed, markets_changed, book_mismatch, rest_missing, check_failed, poll_errors
- [x] 1.8 `recorder/main.py` (all intervals configurable; `--no-kalshi`, `--duration`, S3 upload optional) and `requirements.txt` (pinned)
  - Live run (150 s, 57 pairs) wrote all 5 streams, backfilled 216 trades and stopped cleanly
  - Bug found and fixed: Kalshi `ok` replies carry seq numbers, and skipping them caused a false gap → regression test added
  - `tests/test_feeds.py` + `tests/test_sink.py`: 27 tests in total, all passing; Vihaan's 25 still pass
- [x] 1.9 Old files kept but flagged (Binoy's choice), each with a "DEPRECATED - do not use" header naming its replacement:
  - `deprecated/record.py`, `deprecated/analyze.py`, `deprecated/digest.py`, `deprecated/backup.py`
  - `deprecated/deploy/setup.sh`, `deprecated/deploy/recorder.service`
  - `deprecated/README.md` lists what replaced each one
  - Moved with `git mv`, so history follows. A first pass used a `_deprecated` filename suffix at the root; it was replaced the same day by the folder.
  - The trade fetchers were copied unchanged into `recorder/rest_trades.py`, so no new code imports a deprecated file
  - README has a banner pointing to the redesign and the moved files; its body is rewritten in Stage 7
  - ⚠ Vihaan's scheduled task `predictionmarketarb-daily-update` runs `python digest.py`; it will fail on this branch (the digest is replaced by CloudWatch alarms)
- [x] 1.11 Repository reorganized by role (Binoy, 2026-11-16):
  - `edge.py`, `maker.py`, `analyze_stream.py`, `analyze_maker.py` → `analysis/`, contents unchanged except the analyzers' default paths, which now point to the project root
  - `test_edge.py`, `test_maker.py` → `tests/`, with a two-line path setup added
  - Untracked Vihaan's committed `__pycache__/*.pyc` (already in `.gitignore`)
  - Verified: all 53 tests pass with one command; both analyzers give **byte-identical** CSV output before and after the move; the recorder still starts
- [x] 1.10 Polymarket closed-market handling, matching Kalshi. A subscribed token with no book is looked up each check: if REST has a book, it re-subscribes (`book_missing`); if Polymarket's market data says closed, it logs `market_closed` and unsubscribes.
  - Quirk: Polymarket's markets endpoint omits closed markets unless `closed=true` is passed, and then returns only closed ones
  - Live: the Alaska governor token was detected closed and unsubscribed; books 56/56
- **Finding: Polymarket's live feed can be wrong with no fault on our side.** In the first live run, 2 of 57 books diverged within about 2 minutes: one level off by exactly 60 or 20 contracts, from a `price_change` stamped 1 ms after Polymarket's own book timestamp. Both REST and a fresh WS snapshot omitted that change for 25+ s, and no reversing message ever arrived. The REST check caught and fixed both. Implication: between checks, a Polymarket book can be wrong for up to `--poly-check` seconds.
  - [ ] Compaction (Stage 2) should flag the window between a detected mismatch and its correction as suspect
  - [x] Check interval lowered to 30 s (Binoy, 2026-11-16)
  - [ ] Watch the mismatch rate over the first days on AWS. Early runs show roughly 1 mismatch per 30 s check across 56 books, each a single level. Investigate whether it's always the same pattern (a `price_change` that Polymarket's own book never includes).

## Stage 2 — Compaction & migration
- [x] 2.0 Measured data volume from the live run (57 pairs): about **29 msgs/s, ~350 MB/day of compressed raw files (~10 GB/month)**. The Polymarket feed is ~230 MB/day and the REST checks ~84 MB/day at 30 s. The plan estimated under 1 GB/month (made before the switch to WebSockets). S3 Standard ≈ $0.24/month per month of raw data kept. Re-measure on AWS: this sample is 150 s at one time of day.
- [x] 2.1 `recorder/compact.py` (nightly job: `python -m recorder.compact --store <dir|s3://bucket> [--date D] [--days N] [--pairs <file>]`)
  - Replays one UTC day's raw messages through `recorder/books.py`, the same code the recorder runs live
  - Outputs, each overwritten on re-run, so the job is idempotent:
    - `curated/books/venue=/date=/part-0.parquet`: one row per book change, top 25 levels. Columns: `ts_ms` (our receive time), `server_ms`, `market`, `yes_bids`, `no_bids` (list of {price, size}), `source` (snapshot/delta/rest_repair), `suspect`
    - `curated/trades/venue=/date=<trade date>/part-0.parquet`: merged into the trade's own date and de-duplicated on `trade_id`; REST rows win over WS rows
    - `curated/coverage/date=/part-0.parquet`: recorder events, with details as JSON
  - Book state carries across days via `curated/_state/books/date=D.json`. A missing previous day is processed first, up to 7 days back.
  - Suspect rows. Kalshi: from a `resync` until the market's fresh snapshot. Polymarket: from the last REST check that matched until the confirmed mismatch. At the mismatch, a `rest_repair` row applies Polymarket's REST book.
  - Verified on live data: both repairs exactly matched the fresh snapshot Polymarket sent ~110 ms later
  - Size: Polymarket's curated books are ~90–115 MB/day (top-25 levels repeated on every change). Sorting by market would only save 1.3×, so rows stay in time order for single-pass replay.
  - `recorder/store.py`: the same interface over a local directory or S3. `sink.read_lines` also accepts file bytes.
  - `tests/test_compact.py`: 8 tests (day carry-over, catch-up, Kalshi resync flags, Polymarket flag-and-repair, NO entries ignored, trade de-duplication across days, identical re-runs, coverage JSON)
- [x] 2.2 `tools/migrate_sqlite.py`: Vihaan's SQLite data → curated tables as `part-legacy.parquet`, so it can't collide with the nightly job's files. Book rows get `source = rest_poll`; polling cycles become `poll_cycle` coverage events.
  - Verified: books 28,660 + 84,839, trades 7,588 + 3,912 and 7,232 cycles all reconcile 1:1; a spot-checked row matches exactly; 86 MB SQLite → 5.5 MB Parquet
  - [ ] Run it against S3 once the bucket exists (Stage 6)
- [x] 2.3 Reference data (`recorder/refdata.py`, run by the nightly job with `--pairs`):
  - `ref/pairs/date=D/pairs.json`, plus each venue's full market record per watched market in `ref/markets/date=D/{kalshi,poly}.jsonl.gz` (title, rules text, status, settlement result)
  - D is the date fetched, since the venues only serve current metadata. Intraday pair changes are visible in `pairs_reloaded` coverage events.
  - Verified live: 57 + 57 records, 0 errors; Kalshi already shows `KXGOVAK-26-JKRE` finalized with result `yes`
  - `valid_from`/`valid_to` versioning of pairs is deferred to Stage 4, where the pair-discovery agent maintains the list
- [ ] 2.4 Nightly schedule on EC2: a systemd timer, defined in Stage 5's instance setup
- Total tests: 61 passing (36 new + Vihaan's 25)

## Stage 3 — Analysis (design: [docs/STAGE3_DESIGN.md](docs/STAGE3_DESIGN.md), awaiting Binoy's approval)
- [x] 3.0 Design rounds with Binoy (2026-11-17): all decisions are in the design doc and the decisions log
- [~] 3.A Upstream changes: code done and tested; **not deployed yet**
  - Pre-checks on real data (2026-11-17):
    - Polymarket `last_trade_price` prints exist, arrive ~47 ms after the trade, YES tokens only
    - Treasury bill-rate CSV works: 13-week coupon equivalent 4.26% on 2026-11-16
  - [x] `suspect_levels list<struct<side, price>>` on books
    - Polymarket: at a repair, the levels where our book differed from REST, in yes_bids/no_bids terms. Suspect rows are held per token until the repair, so **files are no longer strictly time-ordered** (documented: sort by ts_ms).
    - Kalshi resync, or a Polymarket window without a REST book: NULL (whole book uncertain)
  - [x] Manifest entries carry `bytes` + `modified` (store `list_meta` for local/S3)
  - [x] Trades `recv_ms`
    - Kalshi WS trades keep it, including when the REST row wins the de-duplication
    - Polymarket REST trades get it from a matching print (same YES token and price, within 2 s, each print used once)
  - [x] `ref/pairs/date=D/pairs.jsonl`; `ref/rates/date=D/tbill.json` (falls back to last year's file in early January; logged if not fetched)
  - [x] Tests: 8 new (80 total)
  - [x] Validated on a local copy of Nov 16 raw data: row counts identical to production; 49 s; ~350 MB peak on the Mac
    - **Suspect markers:** all 5,626 suspect Polymarket rows have known levels (avg 1.4). Of ~8,000 marked levels: **33 at the best price**, 827 at 2–5, 3,556 at 6–25, 3,611 beyond the top 25 or removed.
    - **Trade timing:** Kalshi 655/686 (95%). Polymarket **31/175 (18%)**: prints are YES-token only and REST prices are averaged per taker order. → Polymarket maker timing mostly uses 1-second REST timestamps. Raise in 3.E: subscribe to NO tokens too, for their prints?
  - [x] Deployed before tonight's run (Binoy's choice, 2026-11-17 04:05 UTC): pushed (auto-synced) and `infra/update.sh` → server on `2ae4590`
    - Reprocessed Nov 16 on the server (85 s): same row counts, new columns
    - Refreshed Nov 17 (74 s; 10/43 Polymarket trades timed by a print)
    - Reference snapshot now has `pairs.jsonl` and `tbill.json` (4.26%, 2026-11-16); 0 OOM kills
    - One Kalshi metadata fetch hit **HTTP 429** (rate limit, shared with the recorder's trade polling) → `fetch_market` now retries 429/5xx with backoff (1, 2, 4 s); tests added. Ships with the next update.
  - Glue columns for `suspect_levels` and trades `recv_ms` come with the 3.G template deploy
- [x] 3.B `analysis/data.py`: `open_data(source, password, from_date, to_date)` → DuckDB views for every curated table in range, plus `.pairs(d)` and `.tbill(d)`
  - Sources: local dir (read in place), `https://` + password (Bearer), `s3://` (AWS credentials)
  - Remote files are cached under `~/.cache/prediction-market-arb/<host>/` and re-fetched only when the manifest's `bytes`/`modified` change
  - `pairs(d)`: that day's snapshot → latest before → earliest after → `watchlist.json` (needed for the legacy Nov 9–12 days, which predate the snapshots); returns which one was used
  - `tbill(d)`: same lookup; (None, None) if none stored yet
  - `analysis/__init__.py` appends the package folder to `sys.path`, so Vihaan's unchanged `maker.py` (`from edge import …`) can be imported
  - `tools/query.py` is now a thin wrapper (`--source`, `--cache`) and reports cache downloads
  - Tests: `tests/test_data.py`, 8 tests (local views and dates, pairs/T-bill fallbacks, a real HTTP server with password: first run downloads, second downloads nothing, a changed file is re-fetched alone, range limits downloads, wrong password fails)
  - Live: first run 32 files / 31 MB / 8.3 s; second run 0 downloads / 0.5 s. Live data shows `suspect_levels` (Nov 16: 5,626 rows; Nov 17: 6,143; 0 whole-book); legacy files without the column read fine (`union_by_name`).
- [x] 3.C `analysis/replay.py` + `analysis/episodes.py` + `analysis/run.py` (`python -m analysis.run episodes`)
  - Legacy days: one evaluation per polling cycle, exactly as the original. Live days: one evaluation per distinct receive time; rows sharing a ms are applied together, in file order.
  - New columns: `uncertain` (peak walk used a marked level) and `uncertain_any`, `tbill_pct`, `excess_annualized_pct`, and `profit/size_{50,250,1000,5000}ms` (books as they stood L ms after the start; unknown if data ended or coverage was lost first)
  - Live coverage: recorder stopped→started is a gap; started without a stop means a crash (gap from the last data); a venue disconnect ends episodes; market_closed ends that pair's episodes
  - **GATE PASSED: Vihaan's `episodes.csv` reproduced exactly.** 157/157 episodes identical in all 17 original columns, same order (times exact, not just within one cycle). It's a permanent test (`tests/test_episodes.py::LegacyReproductionTest`).
  - Live Nov 16–17 (8.5 h): 352 episodes; 21 beat the T-bill; 240 still profitable 1 s after start; 0 uncertain at the peak, 8 uncertain at some point (consistent with only 33 of ~8,000 marked levels being at the best price)
  - Bug found and fixed: gap accounting lost clean stop→start gaps (now 2 gaps / 1,180 s, as expected)
  - 9 tests: 8 live rules on synthetic data + the legacy reproduction
  - **Memory:** ~0.9–1.0 GB peak on the Mac for 8.5 h of live data
    - Tried and rejected: DuckDB memory caps (it can't sort nested books within 256 MB)
    - Replaced the full sort with a streaming reorder that holds only `max_lateness` ms of rows: identical results, kept, but **peak memory unchanged (~1 GB)** → the sort wasn't the cause
    - Binoy's call (2026-11-17): stop optimizing; move to **t4g.micro (1 GB)**; measure the real peak on the server in 3.G and revisit only if it doesn't fit
- [x] 3.C.1 Template ready (`InstanceType` default `t4g.micro` on `RecorderInstance`; launch template pinned to `t4g.nano` literally). **Change-set preview (2026-11-17):** LaunchTemplate *not* changed; `RecorderInstance` Modify (InstanceType; 'Conditional' = stop/start for EBS-backed); `InstanceRecoveryAlarm` dimensions refreshed. Preview deleted. Originally: put `InstanceType` on the instance resource (stop/start, disk kept) rather than in the launch template (replacement, ~18 min reinstall). Preview with a change set first. Deploy the raw-file overwrite fix (`408f712`) **before** the resize, since the resize restarts the recorder.
  - First `deploy.sh` (2026-11-17 ~15:58 UTC): stack UPDATE_COMPLETE, but **the instance stayed t4g.nano**. `cloudformation deploy` keeps the *stored* value of parameters not passed (InstanceType was stored as t4g.nano), and the new `InstanceType` property still caused a stop/start (~16 s recording gap, no benefit).
  - Fix: `deploy.sh` now passes `InstanceType` (env `INSTANCE_TYPE`, default t4g.micro) and documents the stored-parameter behaviour. Second change-set preview: RecorderInstance Modify (stop/start), launch template untouched.
  - [x] Binoy re-ran `deploy.sh` (2026-11-17 ~16:03 UTC). **Instance is t4g.micro** (confirmed from instance metadata), same instance and disk.
    - Memory 910 MB total, 642 MB available while recording; swap unused
    - Recorder stopped 16:03:48 and was active 16:04:15 → ~27 s gap
    - Code `286be0e`; timers intact (refresh 18:15, nightly 00:30)
    - Cost ≈ +$3/mo, total ≈ $13.50/mo
- [x] 3.D **Maker baseline** (`analysis/maker_baseline.py`, `python -m analysis.run maker-baseline`). Binoy's decision: run Vihaan's `QuoteSim` *unchanged* as a baseline next to the new model; don't carry the polling recorder's workarounds forward and don't chase an exact match.
  - Not carried over: trade-coverage marks (curated trades are complete)
  - Live days have no polling cycles, so "hedge at the next book recorded" = the first book update of any market after the fill. That's usually ms later, far faster than his ~5 s. This is a known baseline artifact; the new model uses an explicit hedge latency.
  - Kept unchanged: his 10 s minimum rest, back-of-queue joining, re-queue after any fill, queue/through fill models, annualized hurdle
  - **Polling days: matches his `maker_fills.csv` row for row (39/39)** even without the marks, which never affected a fill. Permanent test.
  - Live Nov 16–17 (19.6 h of books): 57 fills. Kalshi-side queue model: 39 fills, 915 contracts, quoted 2.97¢ / realized 2.87¢, $26.29. Polymarket side: 2 fills. Through model: 16 fills, $16.50.
  - Runtime: 345 s → 243 s for two live days after scoping the per-update loop to pairs that can change (byte-identical output). Not optimized further (Binoy: no more tuning). Measure the real nightly runtime in 3.G.
  - Review of his maker assumptions (all shown to Binoy):
    - coverage marks and the 10 s rest are polling workarounds
    - "next book" hedging timing
    - re-queue after partial fills
    - never improves the price
    - no caps or rebates
    - Kalshi hedge fee rounded up per fill
  - Fee check: Polymarket prints say `fee_rate_bps: 0` on every trade, but Polymarket's market records show `feesEnabled: true`, `feeSchedule.rate` 0.04 (politics) / 0.05 (culture), takerOnly, `rebateRate: 0.25`. **Vihaan's fee rates are right.** The 25% maker rebate share feeds 3.E. (Kalshi market records carry no fee fields; check the series endpoint later.)
- [x] 3.D.1 **Critical review of the taker analysis**: [docs/TAKER_REVIEW.md](docs/TAKER_REVIEW.md). Binoy adopted A1, A2 (amended), A3, B1, B2, B3, C1, D1, D2; B4 not adopted; C2 stays a documented limitation.
  - Checks behind the amendments:
    - all 57 Polymarket markets have `holdingRewardsEnabled: false` today
    - Kalshi APY 3.50% (help page, verified 2026-11-17; launched 4.05% in Oct 2025); no API
    - venue dates are unreliable as resolution dates (Kalshi expected expiration is a cautious backstop, e.g. Alaska 2027-11-03; Polymarket endDate is sometimes already past, e.g. Ljubljana 2026-11-16)
  - Polymarket market records also carry **liquidity rewards** (`clobRewards`, `rewardsMaxSpread`, `rewardsMinSize`), relevant to the maker model → raise in 3.E
- [x] 3.C.2 **Taker revisions** (from the review), all implemented and tested:
  - `analysis/economics.py`:
    - capital per venue
    - `budget_walk` (edge.walk's rules + a capital cap; with no cap it equals `edge.walk` on 500 random books)
    - carry (Kalshi APY on the Kalshi leg; Polymarket holding rewards where enabled)
    - worst-case end date
  - `analysis/carry_rates.json`: dated rates (Kalshi 4.05% from 2025-10, 3.50% verified 2026-11-17; Polymarket holding rewards 4.00%/3.25%, with sources and caveats)
  - `episodes.py` new columns: `rule_note`, `opportunity_id`, `start_*` (headline), `carry_pct`, `start_total_annualized_pct`, `start_excess_pct`, `worst_case_end` and its worst-case total, `est_resolution_passed`, `profit/size_{1k,10k,50k}`, `peak_capital_kalshi/poly`, `actionable`. Vihaan's original columns are unchanged (the legacy reproduction test still passes).
  - Summary leads with actionable opportunities and the rule-status breakdown, then the largest opportunities by at-start profit
  - D2 `analysis/feecheck.py` + `python -m analysis.run fee-check [--live]`
    - the daily snapshot now also stores Kalshi series fee settings (`kalshi_series.jsonl.gz`; ships with the next update)
    - **Live check (2026-11-17): all 114 taker-fee rates match** (57 Polymarket, 57 Kalshi with multiplier 1 → 0.07)
    - flagged: `poly_maker_rebate_share` missing from the pairs file (published 0.25) → 3.E
  - Tests: economics 9, episodes +4 (start/capital/carry/excess, budgets, opportunity joining, actionable), fee check 1; **115 total**
  - **First results with the revisions** (Nov 16–17, 19.6 h live):
    - 538 episodes → 379 opportunities → **13 actionable**
    - **Marine Le Pen P_YES+K_NO:** $172.86 at start, $28.36 per $1k, 2.76¢; total 5.7% incl. carry = **+1.4 pts over the T-bill** (arbitrage alone ~2.2%: carry flips the verdict, as the review suspected)
    - Glucksmann +0.5; Time POY Mamdani +10.3
    - **142 opportunities excluded only because their pairs are `template-read`** (e.g. Oscars, +3.2 pts) → reading those rules is high-value (Stage 4 / Vihaan)
  - [x] ~~Add `poly_maker_rebate_share`~~ **Dropped** (Binoy, 2026-11-17): rebates and liquidity rewards are out of the maker model (see 3.E)
- [x] 3.E.0 **Validations before 3.E** (Binoy: validate before deciding):
  - **Rebates are real.** Official Polymarket docs (market details): `feeSchedule.rebateRate` = "fraction of taker fees rebated back to the resting maker", paid daily from a per-market pool. Liquidity rewards are a separate daily per-market program (quadratic score, sampled per minute). Neither matters for the taker hypothesis.
  - **Polymarket fee change effective 2026-11-20 23:59 UTC** (official docs): Culture and non-election Politics 0.04/0.05 → 0.01; maker rebates become tiered 0–15%. Unclear whether this covers the international CLOB we record ("Polymarket DeFi on polymarket.com"). The daily snapshot's `feeSchedule.rate` will show it per market and the fee check will flag it → **pairs-file fee update will be needed (decision then)**.
  - **NO-token prints, validated:**
    - busy external market, 4 min: 226/226 YES-token trades printed on the YES-only connection
    - our markets, all of Nov 17: YES-token trades 1,005/1,019 (98.6%) printed (tx hash); NO-token trades **352/352 never** printed to the YES-only feed
    - our 57 pairs, 15 min with YES-only and NO-only connections: 14 YES trades printed only on YES; 1 NO trade printed only on NO
  - **Root cause of the "18% timed":** prints were matched by *price*, but REST prices are averaged per taker order. **Matching by transaction hash** times ~74% of all Polymarket trades (98.6% of YES-token trades).
  - Data API v1 has been retired (HTTP 410); v2 (used by the recorder) is fine
- [ ] 3.E.1 Transaction-hash matching:
  - keep `tx_hash` with Polymarket REST trades (raw `trades` stream, curated `trades` column)
  - match prints by tx hash, with the price match only as a fallback for older raw files
  - fee check: drop the rebate item
  - recorder part ships with the next update
- [ ] 3.E `analysis/maker_live.py` (scope confirmed by Binoy, 2026-11-17):
  - joins the best bid only
  - queue tracking (`--cancel-model pessimistic|pro-rata`)
  - explicit hedge latency
  - markouts at 1/10/60 s
  - position caps per pair and in total
  - **no rebates and no liquidity rewards** (stated in the results)
  - results side by side with Vihaan's baseline
- [ ] 3.F `analysis/research.py`: `gaps` (per pair per changed second), `leadlag`, `stale`
- [ ] 3.G Nightly analysis on EC2 after compaction
  - writes `curated/results/*` and Glue tables `pairs`, `episodes`, `maker_fills`, `gaps`, `leadlag`, `stale`
  - starter research queries on the query page
  - budget: compaction + analysis under ~15 min on the nano, no OOM
- [ ] 3.H Daily report page `reports/D.html` + `index.html` (inline SVG charts) behind the password
- [ ] 3.I Move the old `analysis/analyze_stream.py` and `analyze_maker.py` to `deprecated/` once reproduced; update the docs
- Earlier notes folded into the design: per-level suspect flag; `pairs` table; research queries (DuckDB has `ASOF JOIN`, Athena doesn't)

## Stage 4 — Pair discovery & maintenance (expanded 2026-11-16 at Binoy's request)
Goal: a **separate agent** that periodically compares Kalshi and Polymarket and uses agentic exploration to find newly active cross-venue pairs and detect pairs that should be dropped, so the watched list stays current. The recorder already reloads the pairs list, so it follows the agent's updates.
- [ ] 4.0 Design questions to ask Binoy before building:
  - Autonomy: may the agent add pairs on its own, or only propose them for human approval? (Vihaan's rule-equivalence check is what makes a pair a real hedge.) Can it drop pairs automatically?
  - Where and how often it runs: locally, or on AWS on a schedule (e.g., EventBridge → Lambda/ECS task, or a timer on the existing EC2)?
  - Model and cost: which Claude model; monthly budget for API calls; where the Anthropic API key is stored (SSM)
  - Output: PR or commit to `pairs.json`, an S3 `ref/pairs/` version, an email summary, or a combination
  - Scope: categories (sports excluded for now), minimum liquidity or volume
- [ ] 4.1 Deterministic candidate generation (`match.py`): pull open markets on both venues (excluding sports, intraday crypto, weather) and fuzzy-match titles and dates
- [ ] 4.2 Agent step: compare both venues' rules text for candidates and judge equivalence, with reasoning and a confidence level
- [ ] 4.3 Drop detection: closed or settled markets, pairs whose rules changed, pairs that went illiquid
- [ ] 4.4 Publish a new `pairs.json` version (`valid_from`/`valid_to`) through the approval flow chosen in 4.0

## Stage 5 — Infrastructure (CloudFormation)
**Account findings (read-only inspection, 2026-11-16).** The AWS account is shared with company workloads: ~30 stacks (`Nemsis*`, `rule-engine-*`, `rymetrics-*`, …), 5 Athena workgroups and an existing budget. The active cost-allocation tag is `Project` (capital P). IAM Identity Center exists. The GitHub repo `dvihaan/PredictionMarketArb` is **public**, so nothing secret may be committed: alert emails, the CloudFront password and the account ID go in as deploy-time parameters or SSM values.

**Venue server locations** (third-party latency monitor; neither venue publishes them): Polymarket → AWS eu-west-2 (London), Kalshi → AWS us-east-2 (Ohio). The recorder stores each venue's server timestamps, so cross-venue alignment doesn't depend on where it runs.

- [x] 5.0 Binoy copied the Kalshi SSM parameters to **us-east-2** (2026-11-16) and deleted the us-east-1 originals. Verified: both are SecureString/Standard, tagged `Project`/`environment`/`owner`, the private key is a complete PEM, and a signed read-only Kalshi request using the us-east-2 key returned HTTP 200.
- [x] 5.1 `infra/stack.yaml` (CloudFormation, us-east-2): written; `cfn-lint` clean; accepted by `aws cloudformation validate-template`
  - **Network:** a dedicated VPC `10.42.0.0/24` with one public subnet, an internet gateway and a free S3 gateway endpoint; no NAT gateway
  - **Storage:** S3 bucket `prediction-market-arb-<account>-<region>` (private, Block Public Access, TLS-only, SSE-S3, `DeletionPolicy: Retain`, Standard forever); `athena-results/` expires after 7 days
  - **Compute:** EC2 t4g.nano from a launch template (AL2023 arm64 via SSM AMI parameter), 8 GB encrypted gp3, IMDSv2, security group with no inbound rules, SSM Session Manager
  - **IAM:** instance role scoped to the bucket's `raw/`, `curated/` and `ref/`, the Kalshi SSM parameters, and `PutMetricData` in one namespace
  - **Deployment:** UserData writes `/etc/pma.env`, clones the repo (branch parameter) and runs `infra/server/pma-update`, which runs `infra/server/setup.sh`:
    - installs Python 3.12 and git; adds 1 GB swap
    - creates the `pma` user (owns only `/opt/pma/data`) and the venv with pinned requirements
    - seeds `ref/pairs/current.json` from `watchlist.json` (first time only)
    - installs systemd units `pma-recorder.service` and `pma-compact.service`, plus `pma-compact.timer` at 00:30 UTC
  - **Logs:** **changed from the plan.** No CloudWatch Logs agent: it would use ~50–80 MB of the nano's 0.5 GB. Logs stay in `journalctl`, reachable through Session Manager. Alarms come from the recorder's own metrics. (Claude default; Binoy can ask for the agent.)
  - Bug caught in review: git as root refuses a checkout owned by another user ("dubious ownership"). Root now owns the code and venv; `pma` owns only its data.
- [x] 5.2 Monitoring:
  - `recorder/metrics.py` sends Heartbeat, KalshiMessages, PolyMessages, PolyMismatches and UploadFailures every minute; the nightly job sends NightlyOK
  - Alarms → SNS (1–2 emails from deploy-time parameters): recorder-down (no heartbeat for 10 min), kalshi-feed-silent and poly-feed-silent (15 min), upload-failures, nightly-job-missing (**expect one false alarm on day 1**), instance-recovery (with EC2 recover action), athena-daily-scan
- [x] 5.3 Glue database `prediction_market_arb` with tables `books`, `trades` and `coverage` (Parquet, partition projection on venue/date from 2026-11-01). Athena workgroup `prediction-market-arb`: 10 GiB per-query cutoff, engine v3, results to `athena-results/`, CloudWatch metrics on.
- [x] 5.4 Data access without AWS credentials: CloudFront (PriceClass_100, caching off because files are overwritten nightly) in front of the private bucket. A CloudFront Function checks the password, and only its SHA-256 is embedded. It accepts:
  - a `pma_auth` cookie set by `login.html`
  - `Authorization: Bearer <password>` (tools)
  - `Basic <any-user>:<password>` (browsers, curl)

  Unauthenticated HTML redirects to the login page; anything else gets a 401. The password is created by `infra/deploy.sh` in SSM `/prediction-market-arb/web/password`.
- [x] 5.5 Query front ends:
  - `infra/site/index.html`: one SQL page with two engines. **In this browser** runs DuckDB-WASM 1.29.0 over the files for the chosen dates, via `curated/manifest.json`. **On AWS** runs Athena through `/api/*`. Shows results and offers a CSV download.
  - Query Lambda (inline in the template): GET `/api/query|status|results|cancel`. Only one SELECT/WITH/SHOW/DESCRIBE/EXPLAIN statement is allowed, with at most 2 queries running. Reachable only through CloudFront (function URL with AWS_IAM + OAC). Read-only Glue/S3 permissions; writes only to `athena-results/`.
  - `tools/query.py`: the same views for DuckDB/pandas on a laptop (`PMA_URL` + `PMA_PASSWORD`). Tested locally and over HTTP: legacy and new data together, venue/date read from the file paths, date filter applied.
  - `tests/test_infra.py`: the password function runs in Node (9 cases, incl. a password containing a colon); the query Lambda runs with a fake Athena client (read-only checks, concurrency cap, status/results)
- [x] 5.6 AWS Budget **$20/month**, filter `TagKeyValue: user:Project$prediction-market-arb`, alerts at 80% actual and 100% forecast via SNS. Unverified: whether `AWS::Budgets::Budget` deploys from us-east-2 (the docs don't say); the first deploy will tell.
- [x] 5.7 Tags: stack tags from `deploy.sh` (`Project`, `environment`, `owner`) propagate to supported resources; explicit tags on the instance, volume, ENI (via the launch template) and the distribution
- [x] 5.8 `cfn-lint` clean; `requirements-dev.txt` holds local-only tools (duckdb, cfn-lint, PyYAML)
- Total tests: 68 passing
- [x] 5.9 Binoy approved the push (2026-11-16): `arch-redesign` pushed to GitHub at `138a500` after a scan for secrets and personal data (clean). Binoy runs `infra/deploy.sh` himself.

## Stage 6 — Deploy & verify
- [x] 6.1 `infra/deploy.sh` (password → `cloudformation deploy` → upload pages) and `infra/update.sh` (SSM → `pma-update`). The planned `infra/teardown.sh` is not written yet.
- [x] 6.2 Local smoke tests done in Stage 1: live runs on the Mac; Kalshi rebuilt books matched REST; Polymarket mismatches caught and repaired
- [~] 6.3 First deploy (Binoy ran `deploy.sh`, 2026-11-16 19:11 UTC): stack `prediction-market-arb` **CREATE_COMPLETE** in us-east-2 with no failures. The budget deployed fine from us-east-2.
  - Outputs: web address (CloudFront; printed by `deploy.sh`, kept out of the public repo), bucket `prediction-market-arb-<account>-us-east-2`, first instance `i-0f34f7009abfee82f` (replaced), workgroup `prediction-market-arb`, database `prediction_market_arb`
  - **Bug: server self-install failed.** On the t4g.nano (412 MB usable), `dnf install git` in UserData was OOM-killed ("Killed"). Swap was only created later, in setup.sh, so the boot script stopped before the clone and nothing was installed.
    - Fix: UserData creates 1 GB of swap before any `dnf`; setup.sh's swap step skips if it exists
    - Changing UserData creates a new launch template version, so the next deploy **replaces the instance** (the failed one holds no data)
  - [x] Fix pushed (`8bf70ca`); Binoy re-ran `deploy.sh` → UPDATE_COMPLETE; instance replaced by `i-0a830747039912ad2`
    - First-boot setup finished 19:39 UTC, after **~18 min** (mostly pip installs on the nano; only on first boot or a full reinstall)
    - Swap now active before `dnf`, so no OOM
  - [x] **Recorder live on AWS since 2026-11-16 19:39 UTC.** Checked after 36 min:
    - Kalshi accepts the key from AWS (connected, 56/56 books, 0 resyncs)
    - Polymarket 56/56 books, 21 mismatches corrected (≈ one per two 30 s checks, as on the Mac)
    - Alaska governor market closed and dropped on both venues
    - 5,060 trades backfilled
    - Raw files land in S3 every 5 min (~850 KB per Polymarket file)
    - Memory: recorder RSS ~115 MB, 165 MB available, swap 24 MB used
  - [x] Alarms all OK except `nightly-job-missing` (the expected day-1 false alarm until the first 00:31 UTC run). Metrics flowing: e.g. 927 Kalshi and 8,521 Polymarket messages per 5 min, 0 upload failures.
    - SNS: one subscription confirmed, one **PendingConfirmation**
  - [x] Web checked with curl:
    - `/` redirects to login; `/login.html` is public
    - no password or wrong password → 401; right password via Bearer or Basic (any user) → 200
    - `/api` without password → 401; DROP rejected
    - `SHOW TABLES` via `/api` → books/coverage/trades; result CSV downloadable through CloudFront
  - [x] Legacy SQLite loaded into S3 with `tools/migrate_sqlite.py` (reconciled 1:1 as before)
  - [x] One-off partial processing of 2026-11-16 on the instance: 12 s, peak RSS ~256 MB, 7,033 Kalshi + 62,733 Polymarket book rows, manifest written. NightlyOK was not sent; tonight's run overwrites this with the full day.
  - [x] Athena via `/api`: books by venue/date/source over legacy + live data, scanning 3.3 KB. Unquoted `date` works.
  - [x] `tools/query.py` against the real address over HTTPS (no AWS credentials): live rows seconds old
  - [~] **Browser query page (DuckDB-WASM):** Binoy ran it (2026-11-16). Row counts matched Athena, but two display bugs showed up:
    - DATE shown as epoch ms
    - HUGEINT `sum()` shown as `"""\""0\"""""` in the CSV
    - Also, the default 7-day range hid Nov 9
    - Fixed in `db2b60d`: per-column Arrow-type formatting (date, timestamp, decimal, BigInt, lists) and an all-dates default, with a Node regression test
    - Binoy uploaded the new `index.html`. Live copy verified byte-identical through CloudFront; the same query via DuckDB over the live address returns the Athena result exactly.
    - [x] Binoy uploaded the quote fix (`f54d2f8`) too
  - [x] First nightly run (2026-11-17 00:31 UTC) succeeded:
    - processed all of Nov 16 (19:39–24:00): Kalshi 52,521 and Polymarket 447,426 book rows, 206 coverage events, ~5.9k trades
    - 88 s wall, 66 CPU-s
    - wrote the end-of-day checkpoint `date=2026-11-16.json`, the Nov 17 reference snapshot (57 + 57 markets, 0 errors) and the manifest
    - NightlyOK cleared the alarm
    - memory: 0 OOM kills since boot, swap 36 MB used of 1.4 GB, 178 MB available
  - [x] 03:15 UTC refresh (Nov 17) checked: the first day built on a saved checkpoint. Kalshi 49,291 rows, **all deltas** (no new snapshots, as expected) built on the Nov 16 checkpoint; first rows at 00:00:00.03 UTC.
    - Polymarket 316,864 rows; 63 s wall / 46 CPU-s
    - Repairs ~77/hour (270 in 3.5 h, 49 tokens), up from ~31/hour on the first evening → track this rate
  - [ ] Check the first **full-day** nightly run (2026-11-18 00:31 UTC, ~5–6× Nov 16's 4.3 h): duration, swap, OOM. If memory gets tight, have `ParquetOut` write to a temp file instead of an in-memory buffer.
- [~] 6.6 **3-hourly refresh of today's tables** (Binoy, 2026-11-16). Until now, clean tables were rebuilt only nightly, so they lagged up to ~24.5 h.
  - `recorder.compact --today` rebuilds today's books, trades and coverage plus the manifest
  - Processing today never saves end-of-day books; only a past day does, enforced in `process_day` with a regression test. A mid-day snapshot saved as "end of day" would corrupt the next day if a nightly run failed.
  - `pma-refresh.timer` runs at 03,06,09,12,15,18,21:15 UTC (00:xx is left to the nightly job at 00:31) and sends no NightlyOK. Both jobs take `flock /opt/pma/data/compact.lock`, so they never overlap.
  - Cost: each refresh reprocesses today from midnight, ~5–7 min for a full day (estimate), within the nano's CPU allowance at this frequency
  - [x] Pushed (`001cd9e`, auto-synced); Binoy ran `infra/update.sh`. Server on `001cd9e`; recorder restarted 22:01:18 UTC (few-second gap). Timers: nightly 00:30 UTC, refresh 03:15 UTC; `systemd-analyze` confirms the calendar expression.
  - [x] Binoy deleted the mid-day checkpoint `curated/_state/books/date=2026-11-16.json` left by the earlier one-off run (verified: checkpoint folder empty; today's tables intact). Tonight's nightly run writes the correct one.
  - [x] Ran `pma-refresh` once by hand (22:05 UTC): success.
    - 2026-11-16 so far: Kalshi 31,929 and Polymarket 285,853 book rows; 53 s wall and 39 CPU-s for ~2.4 h of data
    - "day in progress: end-of-day books not saved", as designed
    - Extrapolation: ~6–7 min for a full day; ~30 CPU-min/day for 7 refreshes plus the nightly run, against ~144 CPU-min/day of t4g.nano baseline
    - Memory peak isn't reported by systemd for this unit; check during the nightly run
- [x] 6.4 Alarm test by stopping the recorder (Binoy chose the real outage test, 2026-11-16)
  - Recorder stopped 22:11:39 UTC (clean SIGINT: files closed and uploaded) and restarted early at 22:30:03 → **order-book gap 18 min 24 s**, recorded in coverage. Trades backfill from the watermarks.
  - Heartbeats stopped exactly at 22:11 UTC, as intended
  - **Finding: `recorder-down` did NOT fire within 18 min**; no email
    - Cause (documented by AWS): while ≥ EvaluationPeriods real datapoints remain in the alarm's look-back "evaluation range", CloudWatch evaluates those and *ignores* TreatMissingData=breaching. The size of that range isn't published, so heartbeat detection is late by an unknown amount.
    - `nightly-job-missing` has the same flaw once it has had a datapoint
  - [x] Experiment (Binoy OK'd, 2026-11-16): three temporary alarms (no actions) on a dummy metric in `PredictionMarketArb/AlarmTest`; 12 min of heartbeats, then stop at 22:46:08 UTC. Fired after:
    - **A, `FILL(m1,0)` with 1-min periods, 10 of 10: 10.1 min**
    - B, 1-min periods only: 15.6 min
    - C, current 5-min × 2: 20.6 min (consistent with no alarm by the 18.4-min restart)
    - Test alarms deleted (0 remain)
    - A first attempt failed: zsh didn't word-split a `$TAGS` variable; the experiment was redone in Python
  - [x] Fix in `infra/stack.yaml`:
    - `recorder-down` uses design A
    - `nightly-job-missing` uses FILL over **hourly** buckets, 26 of 26: it fires only if no success in the last 26 h (calendar-day buckets would fire nightly between midnight and the 00:30 run)
    - Regression tests in `tests/test_infra.py` (AlarmDesignTest)
  - [x] Binoy ran `deploy.sh` → UPDATE_COMPLETE 02:49 UTC (2026-11-17). Both alarms verified in AWS: metric math `FILL(m1, 0)`, 60 s × 10 and 3600 s × 26.
  - [x] `infra/ops.sh test-alarm`: recorder-down went ALARM at 02:52:07 and OK at 02:52:49 UTC. **Binoy confirmed the email arrived.** Detection timing is covered by the dummy-metric experiment (identical design); a second real outage is optional, at a quiet time.
  - [ ] Re-verify after the fix (method to be agreed; the dummy-metric test may be enough)
- [x] 6.8 **Recorder bug: raw-file overwrite on restarts within a 5-minute window** (found in 3.C, 2026-11-17)
  - Names were `<host>-<window>.jsonl.gz`; a restart in the same window reused the name, and its upload overwrote the previous run's S3 file (no versioning)
  - Lost ~80 s around the 2026-11-16 22:01 restart and ~60 s around the 2026-11-17 04:05 restart, across all streams
  - Fix `408f712`: names include the run's start ms (`<host>-<window>-<run>.jsonl.gz`); compaction orders by (window, run); regression tests
  - [x] Deployed 2026-11-17: Binoy pushed and ran `infra/update.sh` (server on `286be0e`)
    - **Verified in production:** the 15:55 UTC window holds separate files for the old-code run (stopped 15:57:24) and the new-code run (stopped 15:58:34); a third run started 15:58:50 with its own name → no overwrite across two restarts in one window
- [x] 6.7 `infra/ops.sh` operations helper (Binoy asked how to check and toggle state from the CLI):
  - commands: `status`, `logs [recorder|nightly|refresh] [n]`, `stop|start|restart`, `refresh`, `alarms`, `test-alarm [name]` (set-alarm-state, no outage), `shell` (Session Manager)
  - Read-only commands checked from both bash and zsh. SSM parameters are JSON-encoded via python3, to avoid shell word-splitting differences (the cause of a failed first attempt at the alarm experiment).
- [ ] 6.5 Activate cost-allocation tags in Billing (manual); check Cost Explorer after 24 h. `Project` is already active; `environment` and `owner` are optional.

## Stage 7 — Docs
- [ ] 7.1 Rewrite README: architecture, Kalshi key setup, deploy, querying (Athena + DuckDB), cost, teardown
- [ ] 7.2 Handoff notes for Vihaan (what changed and why)
- [x] 7.3 `docs/KALSHI_API_KEY.md`: key-pair generation (macOS/Linux + Windows), read-only key creation, SSM storage, rotation. `*.pem` added to `.gitignore`.
  - [ ] Fill in the recorder's key flags/env vars once Stage 1 defines them; confirm the Windows steps on a Windows machine

---

## Decisions log
| Date | Decision | Approved by |
|---|---|---|
| 2026-11-15 | Polymarket venue: international CLOB (research only) | Binoy |
| 2026-11-15 | Capture via WebSocket on both venues; Kalshi key from Binoy's account for now, to be replaced with Vihaan's | Binoy |
| 2026-11-15 | Scope: curated pairs only, plus a local matching helper; no sports | Binoy |
| 2026-11-15 | Compute: EC2 t4g.nano on-demand, us-east-1 | Binoy |
| 2026-11-15 | IaC: CloudFormation; tags `project=prediction-market-arb`, `environment=dev`, `owner=binoy` | Binoy |
| 2026-11-15 | Storage: S3 Standard, kept forever | Binoy |
| 2026-11-15 | Analysis: Athena + Glue; port Vihaan's analyzers | Binoy |
| 2026-11-15 | Replace the AgentMail digest with CloudWatch alarms → SNS email | Binoy |
| 2026-11-15 | Kalshi key: Binoy's own RSA key pair, created read-only on the website (private key never leaves his machine) | Binoy |
| 2026-11-16 | Recorder runs as one Python process with independent per-venue connection tasks that reconnect on their own | Binoy |
| 2026-11-16 | Polymarket gap check: compare against REST books every **120 s** (configurable); on mismatch, log it and re-subscribe for a fresh snapshot | Binoy |
| 2026-11-16 | Server runtime: Python 3.12 from the Amazon Linux 2023 repo; develop and test locally on 3.12 | Binoy |
| 2026-11-16 | ~~Polymarket: subscribe to both YES and NO tokens~~ **Revised the same day:** YES token only. The NO book is an exact mirror (full depth matched on 3 pairs, mirror held after 91 live changes, shared exchange). NO-token trades still come from the REST trade history; the 120 s REST check catches any divergence. | Binoy |
| 2026-11-16 | The number of watched pairs comes from the pairs file and isn't hard-coded; it changes on reload | Binoy |
| 2026-11-16 | Polymarket REST check interval lowered from 120 s to **30 s** after live runs showed feed divergences | Binoy |
| 2026-11-16 | Old files are kept with a do-not-use header rather than deleted; first renamed with a `_deprecated` suffix, then (same day) moved into a `deprecated/` folder under their original names | Binoy |
| 2026-11-16 | Repository organized by role: `recorder/`, `analysis/`, `tools/`, `tests/`, `infra/`, `docs/`, `deprecated/`; compiled caches untracked | Binoy |
| 2026-11-16 | Nightly processing runs on the recorder's EC2 instance (systemd timer), not Lambda or locally | Binoy |
| 2026-11-16 | Clean book table: one row per book change, top 25 levels (Vihaan's format) | Binoy |
| 2026-11-16 | Polymarket mismatch windows: **flag and repair** (`suspect` column + a `rest_repair` row with the REST book) | Binoy |
| 2026-11-16 | Parquet written with pyarrow (zstd); nightly job lives in `recorder/`, next to the code it shares; SQLite migration in `tools/` | Claude default; Binoy can change |
| 2026-11-16 | Deploy to AWS (Stages 5–6) before porting the analyzers (Stage 3), so data starts accumulating | Binoy |
| 2026-11-16 | Use the existing shared AWS account (not a separate account) | Binoy |
| 2026-11-16 | Region **us-east-2 (Ohio)**, next to Kalshi's servers, instead of us-east-1 from the original plan; EU regions considered and rejected (Polymarket restrictions, untested Kalshi access, 10–15% cost) | Binoy |
| 2026-11-16 | Dedicated VPC for the project (not the default VPC) | Binoy |
| 2026-11-16 | Security priority: easy access for Vihaan from anywhere rather than tight lockdown | Binoy |
| 2026-11-16 | Data access without IAM credentials: CloudFront + username/password in front of a private bucket | Binoy |
| 2026-11-16 | Querying: browser DuckDB page + laptop DuckDB helper + Lambda/Athena query API | Binoy |
| 2026-11-16 | Athena guardrails: 10 GB per-query cutoff, read-only role, 2 concurrent queries, daily-scan alarm at 50 GB | Binoy |
| 2026-11-16 | Alerts go to two email addresses (Binoy's and Vihaan's), passed at deploy time and kept out of the public repo | Binoy |
| 2026-11-16 | Budget $20/month (the plan said $15) | Binoy |
| 2026-11-16 | Tag key `Project` (capital P, the account's active cost-allocation tag) instead of lowercase `project` | Binoy |
| 2026-11-16 | Code reaches the server by `git clone` from the public GitHub repo (not an S3 bundle) | Binoy |
| 2026-11-16 | Binoy copies the Kalshi key parameters to us-east-2 himself | Binoy |
| 2026-11-16 | Add a **3-hourly refresh** of today's clean tables, on top of the nightly run | Binoy |
| 2026-11-16 | Alarm test by real outage (accepted an ~18 min order-book gap); then test alarm fixes on a dummy metric, with no outage | Binoy |
| 2026-11-17 | Stage 3: analyzers run on the laptop **and** nightly on EC2, with results published as tables | Binoy |
| 2026-11-17 | Suspect data: per-level markers; results are "uncertain" only if the depth walk used a wrong level | Binoy |
| 2026-11-17 | Cross-venue alignment by our receive time | Binoy |
| 2026-11-17 | Capital hurdle: daily 3-month T-bill yield fetched from the US Treasury | Binoy |
| 2026-11-17 | Laptop data: password-protected web address + local cache | Binoy |
| 2026-11-17 | SQL: pairs + results tables + research queries, plus Claude's research suggestions (all accepted) | Binoy |
| 2026-11-17 | Acceptance: reproduce Vihaan's legacy episodes and maker fills | Binoy |
| 2026-11-17 | Maker sim improved now (new model next to the unchanged maker.py): queue tracking, hedge latency, markouts, caps + rebates | Binoy |
| 2026-11-17 | Research additions: latency sensitivity, gap time series, lead-lag and stale quotes, daily report page | Binoy |
| 2026-11-17 | Fees: per-pair rates from the daily pairs file only | Binoy |
| 2026-11-17 | Deploy 3.A immediately (before the first full-day nightly run) | Binoy |
| 2026-11-17 | Memory: stop optimizing; upgrade to **t4g.micro** (1 GB, +~$3/mo); measure the real nightly peak in 3.G | Binoy |
| 2026-11-17 | **Working principle:** Vihaan's code is exploratory, not a spec. Don't bend the design to reproduce it; surface questionable parts with details and ask. | Binoy |
| 2026-11-17 | Maker: run Vihaan's model unchanged as a **baseline** next to the new model (no exact-reproduction work) | Binoy |
| 2026-11-17 | Critically review the taker analysis and bring a list of proposed changes before 3.G | Binoy |
| 2026-11-17 | Taker review adopted: A1 carry, A2 dates (amended: hand date + worst case), A3 capital per venue, B1 at-start headline, B2 capital budgets, B3 actionable (without stale), C1 rule status, D1 group restart splits, D2 fee cross-check. Not adopted: B4 stale-leg flag. | Binoy |
| 2026-11-17 | A1 Kalshi APY kept in a dated rate file in the repo (no API); Polymarket holding-reward eligibility from the daily snapshot | Binoy |
| 2026-11-17 | **Working principle (again):** validate claims with data before proposing decisions built on them | Binoy |
| 2026-11-17 | Polymarket trade timing: transaction-hash matching only (~74% timed); no NO-token subscription for now | Binoy |
| 2026-11-17 | Drop rebates and liquidity rewards from the maker model | Binoy |
| 2026-11-17 | 3.E scope: join the best bid only; queue tracking, hedge latency, markouts, position caps | Binoy |
| 2026-11-17 | 3.E latency: one round-trip latency for placing, cancelling and hedging (a cancel takes L to land, so stale quotes can be hit); replaces Vihaan's 10 s minimum rest | Binoy |
| 2026-11-17 | 3.E partial fills: the unfilled remainder keeps its queue place (Vihaan requeued) | Binoy |
| 2026-11-17 | 3.E caps: capital committed (both legs, fees, unhedged legs), $1k per pair, $10k total | Binoy |
| 2026-11-17 | 3.E results at 250 ms, 1 s, 5 s latency x pessimistic / pro-rata cancels (6 runs), beside the baseline | Binoy |
| 2026-11-16 | Expand Stage 4 into a separate, periodic pair-discovery agent (design questions in 4.0) | Binoy |

## Open questions
1. **Recorded data is in git.** `data/books.sqlite` (86 MB) and its `-wal`/`-shm` files are committed. GitHub warns on files over 50 MB and blocks files over 100 MB. Proposal: stop tracking `data/` on this branch (`git rm --cached` + `.gitignore`) after migrating to S3. Purging it from git history is a destructive rewrite; Binoy and Vihaan decide.
2. **For Vihaan:** add Polymarket US later? Sports later? Does he agree with the methodology additions (capital-cost hurdle, rule basis risk)?
3. **Unknown until tested:** Kalshi's per-subscription market limit and Polymarket's per-connection asset limit. Neither is documented; both matter only if the scope grows.
