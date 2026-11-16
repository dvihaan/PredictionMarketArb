# PredictionMarketArb — Analysis & Implementation Plan

> **Status:** Foundational plan, approved 2026-11-15. Branch: `arch-redesign`.
> Progress, decisions and open questions are tracked in [`../TASKS.md`](../TASKS.md).
> This document records the baseline analysis and agreed design; changes to the design are logged
> in the TASKS.md decisions log and reflected here.

## Context
Vihaan's project (`PredictionMarketArb/`) records public Kalshi + Polymarket order books and trades for
57 hand-matched, rule-checked market pairs, then replays them to test whether **cross-venue arbitrage**
exists after fees (taker-taker locked pairs, and maker-on-one-venue/taker-hedge-on-the-other).
It runs on a Windows laptop writing to SQLite. Goal: validate the premise, fix inefficiencies, move
recording to AWS at minimum cost, and store data cheaply in a form usable locally and by Athena.

## 1. Analysis summary (done, read-only)
**Code** (high quality; tests pass 9+16):
- `record.py` — 5 s REST polling of both venues' book endpoints (top 25 levels, write-on-change), 60 s trade poller with watermarks/backfill, per-cycle success/failure log.
- `edge.py` (depth-walk + fees), `maker.py` (queue/through maker sim) — pure, unit-tested. **Keep unchanged.**
- `analyze_stream.py`, `analyze_maker.py` — streaming replays; `analyze.py` is a redundant in-memory duplicate.
- `digest.py` (AgentMail email digest), `backup.py`, `deploy/` (EC2 t4g.nano + systemd).

**Data findings**: 61.7 h span but **51.5 h of gaps** (laptop sleep / Windows Update) → ~10 h usable.
~150–200 MB/day of JSON-in-SQLite for 114 books. Results so far: biggest edges are ~1–2 % locked to
May 2027 (~2 % annualized, below T-bills); maker sim ~$8 over 10 h. Too little data to conclude.

**Premise check**:
- Historical order books: **correct, none offered** by either venue → must record (vendors exist but focus on crypto/sports; his niche markets unverified).
- Historical trades: **incorrect — fully available** (Kalshi `/markets/trades` + `/historical/trades`; Polymarket data-api + on-chain). No live capture needed; backfill is enough.
- Better capture: Polymarket CLOB WebSocket market channel is public; Kalshi WebSocket needs an API key (scope `read` suffices). Captures sub-5 s changes REST polling misses.
- Kalshi API has no IPv6 → public IPv4 required (~$3.65/mo).

**Hypothesis**: persistent cross-venue mispricing on equivalent contracts. Literature (Ng et al. SSRN 5331995; arXiv 2601.01706; PredictMarketCap 2026) says gaps exist but are seconds-to-minutes long, median ~0.78¢ vs ~3¢ break-even after 2026 Polymarket fees; non-convergence driven by rule differences. His approach is sound; methodology additions recommended: capital-cost hurdle (T-bill), rule basis risk, simultaneity of quotes. Polymarket international is not US-tradable (accepted: research only).

## 2. Decisions (confirmed with user)
| Topic | Decision |
|---|---|
| Polymarket venue | International CLOB (as now) |
| Capture | WebSocket on both venues (Kalshi key from user's account now, rotate to Vihaan's later) |
| Scope | Curated pairs only (+ hot-reload, auto-drop on close) **+ local pair-matching helper**; exclude sports |
| Compute | EC2 t4g.nano on-demand, us-east-1, Amazon Linux 2023 arm64 |
| IaC | CloudFormation; tags `project=prediction-market-arb`, `environment=dev`, `owner=binoy` on all resources |
| Storage | S3 Standard, keep forever |
| Analysis | Athena + Glue catalog; port Vihaan's analyzers to the new layout |
| Digest | Replace with CloudWatch alarms → SNS email; drop AgentMail |

## 3. Target architecture
```
EC2 t4g.nano (no inbound ports; SSM Session Manager)      S3 bucket (Standard, versioning off, SSE-S3)
 recorder (systemd)                                         raw/venue=*/date=*/hour=*/*.jsonl.gz      (every WS msg + recv_ms)
  ├ Kalshi WS: orderbook_delta + trade (signed, key in SSM) curated/books/venue=*/date=*/*.parquet    (top-25 book on each change)
  ├ Poly WS: book / price_change / last_trade_price         curated/trades/venue=*/date=*/*.parquet
  ├ REST resync + 60 s trade reconciliation (existing code) curated/coverage/date=*/*.parquet        (conn up/down, resyncs, gaps)
  ├ pairs reload from s3://…/ref/pairs/current.json (5 min) ref/pairs/ (versioned), ref/markets/ (daily metadata+rules+settlement)
  └ rotate raw file every 5 min → upload; nightly compaction → curated Parquet (DuckDB)
CloudWatch: heartbeat metrics per venue, EC2 status-check auto-recover, alarms → SNS email
Glue DB + tables with partition projection (no crawler) → Athena workgroup (results prefix expires 7 d)
```
Design rationale: always-on stream → EC2 (Batch/Lambda don't fit daemons; Fargate costs more + needs image pipeline). Raw capture keeps everything reprocessable; curated Parquet keeps the `books` semantics Vihaan's analyzers already use. 5-min upload bounds data loss on instance failure. Partition projection avoids crawler cost.

**Estimated monthly cost**: t4g.nano $3.07 + IPv4 $3.65 + 8 GB gp3 $0.64 + CloudWatch ~$1–2 + S3 <$0.10 (≈1 GB/mo) + Athena cents + SSM Parameter Store $0 → **~$9/mo**. AWS Budget (free) at $15 filtered on `project` tag.

> **Layout update (2026-11-16):** the repository is now organized by role. `edge.py`, `maker.py` and the
> analyzers are in `analysis/`, all tests are in `tests/`, and the replaced originals are in `deprecated/`.
> File names in the steps below are as originally planned; see the layout table in TASKS.md for current locations.

## 4. Implementation steps
0. **Setup (first, before any code change)**:
   - In `PredictionMarketArb/` (clean, on `main`, remote `origin` = github.com/dvihaan/PredictionMarketArb): `git switch -c arch-redesign`. No push until user asks.
   - Persist this plan as `docs/ARCHITECTURE_PLAN.md` (foundational plan: analysis, decisions, architecture, cost, rationale).
   - Create `TASKS.md` tracker: stages (Setup, Recorder, Compaction/Migration, Analyzers, Matching helper, Infra, Deploy/Verify, Docs), each task with status `[ ]/[~]/[x]`, sub-steps, a **Decisions log** (date, decision, who approved), and an **Open questions** section. Commit both on the branch.
   - Working rule: ask the user before any major design/implementation decision; keep `TASKS.md` updated and expand task steps as discoveries happen.
   - Open question to log (not act on): `data/books.sqlite` (86 MB) + `-wal`/`-shm` are committed to git; GitHub blocks files >100 MB. Proposed: stop tracking `data/` on the branch (`git rm --cached`, add to `.gitignore`) after migration to S3; whether to purge from history (destructive rewrite) is the user's/Vihaan's call.
1. **Restructure repo** (`PredictionMarketArb/`): package `recorder/` (`kalshi_ws.py`, `poly_ws.py`, `books.py` delta→book reconstruction with seq-gap resync, `sink.py` rotating gzip JSONL + S3 upload, `pairs.py` hot reload, `main.py`); reuse `levels()`, `iso_ms()`, `kalshi_trades()`, `poly_trades()` from `record.py`. Delete `analyze.py`, `digest.py`, `backup.py`, Windows `keep_awake`. Pin deps in `requirements.txt` (`websockets`, `cryptography`, `boto3`, `duckdb`).
2. **Compaction** `compact.py`: raw JSONL → curated Parquet (books/trades/coverage), idempotent per date; systemd timer nightly + on demand.
3. **One-time migration** `migrate_sqlite.py`: existing `data/books.sqlite` → curated Parquet so the 10 h already recorded aren't lost.
4. **Port analyzers**: new `store.py` reader (DuckDB over local dir or `s3://`) yielding the same (ts, venue, market, yes_bids, no_bids) stream; `analyze_stream.py`/`analyze_maker.py` iterate per book-change event instead of per 5 s cycle, gaps from `coverage`. Add T-bill hurdle option to annualized output.
5. **Matching helper** `match.py` (local): pull open non-sports, non-crypto-intraday, non-weather markets from both venues, fuzzy-match titles/dates, emit `candidates.csv` with both rules texts side-by-side for Vihaan's review; approved rows appended to `pairs.json` (watchlist successor, with `valid_from/valid_to`).
6. **CloudFormation** `infra/stack.yaml`: S3 data bucket (block public access, TLS-only policy), IAM role (scoped S3 prefix, SSM param read, CloudWatch), SG with no ingress, EC2 + UserData (pull code bundle from S3, install, systemd), EBS gp3 8 GB, CloudWatch alarms + auto-recover, SNS topic + email subscription, Glue DB/tables (projection), Athena workgroup, Budget. Tags as stack tags + explicit `Tags` on every taggable resource incl. EBS via LaunchTemplate `TagSpecifications`.
7. **Deploy scripts** `infra/deploy.sh`: package code → S3 `code/`, `aws cloudformation deploy`, SSM Run Command to restart on code update; `infra/teardown.sh` (keeps bucket).
8. **README rewrite**: architecture, Kalshi key setup, deploy, querying (Athena + DuckDB examples), cost, teardown.

## 5. Kalshi API key (user's account, for now)
1. Sign up at kalshi.com (18+, identity verification). Docs don't mention a deposit requirement for API keys.
2. kalshi.com/account/profile → **API Keys → Create New API Key** (Ed25519 default). Save Key ID + private key — shown once.
3. UI keys default to full `read`+`write`. I'll provide a script that uses that key to call `POST /api_keys/generate` with `scopes: ["read"]`, then you delete the full-access key.
4. Store: `aws ssm put-parameter --type SecureString` for key id + private key (`/prediction-market-arb/kalshi/*`). Rotation = overwrite params + restart service.

## 6. Verification
- Unit tests: existing `test_edge.py`, `test_maker.py` unchanged + new tests for delta reconstruction (seq gap → resync), Poly price_change application, sink rotation, compaction idempotency, migration row counts.
- Local smoke: run recorder 10 min against live venues writing to a local dir; compare reconstructed books vs REST snapshots (must match top levels); run compaction + ported analyzers; confirm results on migrated SQLite data match the current analyzer output.
- AWS: `cfn-lint`, deploy to dev, confirm files land in S3 within 5 min, Athena `SELECT count(*)` per venue/day, stop the service to verify the alarm email fires, check Cost Explorer by tag after 24 h (after activating cost-allocation tags in Billing).

## 7. Open items for Vihaan (not blocking)
Add Polymarket US later? Sports later? Confirm methodology additions (capital hurdle, rule basis risk).

## Appendix A — Measurements taken during analysis (2026-11-15)
- Recorded SQLite (`data/books.sqlite`): 7,232 cycles, 113,499 book rows (Kalshi 28,660 / Poly 84,839), 11,500 trades; 11 gaps > 30 s totalling 51.5 h; median cycle fetch 353 ms, max 60 s (timeouts).
- Open-market counts from public APIs: Kalshi ~180k non-parlay markets (25.6k traded in 24 h; ~400k extra auto-generated `KXMVE*` parlay markets); Polymarket ~169k active markets (13k traded in 24 h).
- DNS: `api.elections.kalshi.com` has no AAAA record (IPv4 required); Polymarket endpoints are dual-stack.

## Appendix B — Sources
- Kalshi docs: [historical data](https://docs.kalshi.com/getting_started/historical_data), [API keys & scopes](https://docs.kalshi.com/getting_started/api_keys), [WebSocket connection](https://docs.kalshi.com/websockets/websocket-connection), [historical trades](https://docs.kalshi.com/api-reference/historical/get-historical-trades)
- Polymarket docs: [WebSocket overview](https://docs.polymarket.com/market-data/websocket/overview.md), [market channel](https://docs.polymarket.com/market-data/websocket/market-channel.md)
- Research: [Ng, Peng, Tao, Zhou — Price Discovery and Trading in Modern Prediction Markets (SSRN)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5331995); [Semantic Non-Fungibility and Violations of the Law of One Price (arXiv 2601.01706)](https://arxiv.org/html/2601.01706v1); [PredictMarketCap arbitrage study](https://predictmarketcap.com/analysis/prediction-market-arbitrage-study); [QuantPedia summary](https://quantpedia.com/systematic-edges-in-prediction-markets/)
- Historical-data vendors (unverified coverage): [Predexon guide](https://predexon.com/blog/kalshi-historical-orderbook-data-guide), [DepthFeed](https://depthfeed.com/kalshi-historical-data), [TickFoundry sample](https://github.com/tickfoundry/polymarket-historical-data), [PMData](https://pmdata.dev/polymarket-order-book-data)
- Fees: [Polymarket fees guide](https://resolvedmarkets.com/guides/polymarket-fees), [Polymarket US fee schedule](https://docs.polymarket.us/fees)
