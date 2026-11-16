# TASKS — Architecture redesign tracker

Branch: `arch-redesign` · Foundational plan: [docs/ARCHITECTURE_PLAN.md](docs/ARCHITECTURE_PLAN.md)

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[?]` blocked on a decision

**Working rules**
- Ask Binoy before any major design or implementation decision; record the answer in the Decisions log.
- Keep this file current as work happens; expand a stage's steps when we learn something new.
- Leave `edge.py` and `maker.py` unchanged. Their tests must stay green throughout.
- Don't push to `origin` until asked.

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
  - [ ] Stage 5 IAM role: allow `ssm:GetParameter` on `/prediction-market-arb/kalshi/*` (decrypting with the AWS-managed key needs no extra KMS grant)
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
- [x] 1.9 Old files kept but flagged (Binoy's choice). Each gets a `_deprecated` suffix (via `git mv`, so history follows) and a "DEPRECATED - do not use" header naming its replacement:
  - `record_deprecated.py`, `analyze_deprecated.py`, `digest_deprecated.py`, `backup_deprecated.py`
  - `deploy/setup_deprecated.sh`, `deploy/recorder_deprecated.service`, which still work and point at `record_deprecated.py`
  - The trade fetchers were copied unchanged into `recorder/rest_trades.py`, so no new code imports a deprecated file
  - README has a banner pointing to the redesign; its body is rewritten in Stage 7
  - ⚠ Vihaan's scheduled task `predictionmarketarb-daily-update` runs `python digest.py`; it will fail on this branch (the digest is replaced by CloudWatch alarms)
- [x] 1.10 Polymarket closed-market handling, matching Kalshi. A subscribed token with no book is looked up each check: if REST has a book, it re-subscribes (`book_missing`); if Polymarket's market data says closed, it logs `market_closed` and unsubscribes.
  - Quirk: Polymarket's markets endpoint omits closed markets unless `closed=true` is passed, and then returns only closed ones
  - Live: the Alaska governor token was detected closed and unsubscribed; books 56/56
- **Finding: Polymarket's live feed can be wrong with no fault on our side.** In the first live run, 2 of 57 books diverged within about 2 minutes: one level off by exactly 60 or 20 contracts, from a `price_change` stamped 1 ms after Polymarket's own book timestamp. Both REST and a fresh WS snapshot omitted that change for 25+ s, and no reversing message ever arrived. The REST check caught and fixed both. Implication: between checks, a Polymarket book can be wrong for up to `--poly-check` seconds.
  - [ ] Compaction (Stage 2) should flag the window between a detected mismatch and its correction as suspect
  - [x] Check interval lowered to 30 s (Binoy, 2026-11-16)
  - [ ] Watch the mismatch rate over the first days on AWS. Early runs show roughly 1 mismatch per 30 s check across 56 books, each a single level. Investigate whether it's always the same pattern (a `price_change` that Polymarket's own book never includes).

## Stage 2 — Compaction & migration
- [ ] 2.1 `compact.py`: raw → curated Parquet (`books`, `trades`, `coverage`), idempotent per date
- [ ] 2.2 `migrate_sqlite.py`: existing `data/books.sqlite` → curated Parquet; check row counts
- [ ] 2.3 Define the `ref/pairs` (versioned, `valid_from`/`valid_to`) and `ref/markets` (metadata, rules, settlement) schemas

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
- [ ] 5.1 `infra/stack.yaml`:
  - S3 bucket (Block Public Access, TLS-only, SSE-S3)
  - IAM role
  - Security group with no inbound rules
  - Launch template + EC2 t4g.nano (AL2023 arm64)
  - 8 GB gp3 volume
- [ ] 5.2 CloudWatch heartbeat metrics, status-check auto-recover, alarms → SNS email
- [ ] 5.3 Glue database + tables (partition projection) and an Athena workgroup (query results expire after 7 days)
- [ ] 5.4 AWS Budget at $15, filtered on the `project` tag
- [ ] 5.5 Tag every resource `project=prediction-market-arb`, `environment=dev`, `owner=binoy` (stack tags plus explicit tags; EBS via `TagSpecifications`)
- [ ] 5.6 Run `cfn-lint` clean

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
| 2026-11-16 | Old files are kept, renamed with a `_deprecated` suffix and a do-not-use header, rather than deleted | Binoy |
| 2026-11-16 | Expand Stage 4 into a separate, periodic pair-discovery agent (design questions in 4.0) | Binoy |

## Open questions
1. **Recorded data is in git.** `data/books.sqlite` (86 MB) and its `-wal`/`-shm` files are committed. GitHub warns on files over 50 MB and blocks files over 100 MB. Proposal: stop tracking `data/` on this branch (`git rm --cached` + `.gitignore`) after migrating to S3. Purging it from git history is a destructive rewrite; Binoy and Vihaan decide.
2. **For Vihaan:** add Polymarket US later? Sports later? Does he agree with the methodology additions (capital-cost hurdle, rule basis risk)?
3. **Unknown until tested:** Kalshi's per-subscription market limit and Polymarket's per-connection asset limit. Neither is documented; both matter only if the scope grows.
