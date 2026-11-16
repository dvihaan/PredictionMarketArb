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

## Stage 3 — Analyzers
- [ ] 3.1 `store.py`: DuckDB reader over a local dir or `s3://`
- [ ] 3.2 Port `analyze_stream.py` to per-event replay; take gaps from `coverage`
- [ ] 3.3 Port `analyze_maker.py`
- [ ] 3.4 Option to compare returns against a capital-cost hurdle (Treasury-bill rate)
- [ ] 3.5 Check that the ported analyzers on migrated data match the original output

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

- [ ] 5.0 Binoy copies the Kalshi SSM parameters from us-east-1 to **us-east-2** and tags them `Project`/`environment`/`owner` (commands given 2026-11-16). Whether to keep or delete the us-east-1 copies is his call.
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
- [ ] 5.9 Before deploying: get Binoy's OK to **push** `arch-redesign` to GitHub, since the server clones from it. The remote branch is currently at `9e1d525`, behind local.

## Stage 6 — Deploy & verify
- [ ] 6.1 `infra/deploy.sh` (package code → S3 → `cloudformation deploy` → SSM restart), `infra/teardown.sh`
- [ ] 6.2 Local smoke test: recorder runs 10 min; rebuilt books match REST snapshots
- [ ] 6.3 Deploy to dev; data lands in S3 within 5 min; Athena counts work
- [ ] 6.4 Stop the service; confirm the alarm email arrives
- [ ] 6.5 Activate cost-allocation tags in Billing (manual); check Cost Explorer after 24 h

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
| 2026-11-16 | Expand Stage 4 into a separate, periodic pair-discovery agent (design questions in 4.0) | Binoy |

## Open questions
1. **Recorded data is in git.** `data/books.sqlite` (86 MB) and its `-wal`/`-shm` files are committed. GitHub warns on files over 50 MB and blocks files over 100 MB. Proposal: stop tracking `data/` on this branch (`git rm --cached` + `.gitignore`) after migrating to S3. Purging it from git history is a destructive rewrite; Binoy and Vihaan decide.
2. **For Vihaan:** add Polymarket US later? Sports later? Does he agree with the methodology additions (capital-cost hurdle, rule basis risk)?
3. **Unknown until tested:** Kalshi's per-subscription market limit and Polymarket's per-connection asset limit. Neither is documented; both matter only if the scope grows.
