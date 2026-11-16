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
- [ ] 1.1 Confirm the WebSocket message formats against live endpoints and save sample messages as test fixtures
  - [ ] Polymarket market channel: `book`, `price_change`, `last_trade_price`, `tick_size_change`; per-connection asset limit
  - [ ] Kalshi: handshake signing (Ed25519), `orderbook_snapshot`/`orderbook_delta` seq semantics, `trade` channel; per-subscription market limit (needs key, Stage 1.0)
- [~] 1.0 Kalshi API key from Binoy's account
  - [x] Read-only key created on kalshi.com with Binoy's own RSA public key (`dbinoy-api-key`, Key ID `b496ed3a-…21da`, 2026-11-15). Private key stays at `~/.kalshi/kalshi_private.pem`. The website offers a read-only option, so the scope-reduction script isn't needed.
  - [ ] Check that a signed read request and the WebSocket handshake work despite the "Verify your location" notice, from the Mac and later from AWS us-east-1
  - [x] Stored in SSM Parameter Store by Binoy (2026-11-15): `/prediction-market-arb/kalshi/key-id` and `/prediction-market-arb/kalshi/private-key`. Both are Standard tier SecureStrings in us-east-1, encrypted with `alias/aws/ssm`. Checked: the key ID is a UUID; the private key is a complete PKCS#8 PEM with a trailing newline, so the recorder must strip whitespace when loading it.
  - [?] Both parameters are tagged only `project=prediction-market-arb`; `environment=dev` and `owner=binoy` are missing
  - [ ] Stage 5 IAM role: allow `ssm:GetParameter` on `/prediction-market-arb/kalshi/*` (decrypting with the AWS-managed key needs no extra KMS grant)
- [ ] 1.2 `recorder/books.py`: rebuild books from snapshots and deltas, top 25 levels; resync on a seq gap. Unit tests.
- [ ] 1.3 `recorder/poly_ws.py`, `recorder/kalshi_ws.py`: connect, subscribe, reconnect with backoff, heartbeat
- [ ] 1.4 `recorder/sink.py`: gzip JSONL raw files rotated every 5 min; upload to S3 (or a local dir in dev)
- [ ] 1.5 `recorder/pairs.py`: reload pairs (local file or S3) every 5 min; drop markets once they close
- [ ] 1.6 Trades: reuse `kalshi_trades()`/`poly_trades()` from `record.py` for reconciliation and backfill
- [ ] 1.7 Coverage events (connection up/down, resync, gap) so "no data" stays distinct from "no change"
- [ ] 1.8 `recorder/main.py` + `requirements.txt` (pinned)
- [ ] 1.9 Remove `analyze.py`, `digest.py`, `backup.py`, Windows `keep_awake`, and the old `record.py` once replaced

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

## Stage 4 — Pair-matching helper (local)
- [ ] 4.1 `match.py`: pull open markets (excluding sports, intraday crypto, weather) and fuzzy-match titles and dates
- [ ] 4.2 `candidates.csv` with both venues' rules text side by side for Vihaan's review
- [ ] 4.3 Workflow for adding approved pairs to `pairs.json` (watchlist successor)

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

## Open questions
1. **Recorded data is in git.** `data/books.sqlite` (86 MB) and its `-wal`/`-shm` files are committed. GitHub warns on files over 50 MB and blocks files over 100 MB. Proposal: stop tracking `data/` on this branch (`git rm --cached` + `.gitignore`) after migrating to S3. Purging it from git history is a destructive rewrite; Binoy and Vihaan decide.
2. **For Vihaan:** add Polymarket US later? Sports later? Does he agree with the methodology additions (capital-cost hurdle, rule basis risk)?
3. **Unknown until tested:** Kalshi's per-subscription market limit and Polymarket's per-connection asset limit. Neither is documented; both matter only if the scope grows.
