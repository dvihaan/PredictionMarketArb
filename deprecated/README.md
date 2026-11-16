# Deprecated — reference only

These are the original versions of files replaced during the `arch-redesign` work. They are kept so
the earlier approach can still be read and compared. **Do not run or import them.** They are no
longer maintained, and several won't run from this folder because they expect to sit in the
project root.

| File | What it did | Replaced by |
|---|---|---|
| `record.py` | Polled both venues' REST order-book endpoints every 5 s into SQLite; polled trades every 60 s | `recorder/` (WebSocket recorder: `python -m recorder.main`). Its trade fetchers were copied unchanged into `recorder/rest_trades.py`. |
| `analyze.py` | In-memory version of the taker analysis | `analysis/analyze_stream.py` (same results, streams the data) |
| `digest.py` | Daily plain-text status digest, emailed via AgentMail by a scheduled task | AWS CloudWatch alarms → SNS email (Stage 5) |
| `backup.py` | Safe copy of the live SQLite database | Raw files uploaded to S3 by the new recorder; SQLite is no longer written |
| `deploy/setup.sh`, `deploy/recorder.service` | One-time EC2 setup and systemd unit for `record.py` | CloudFormation + deploy scripts in `infra/` (Stage 5–6) |

Why each was replaced: see [docs/ARCHITECTURE_PLAN.md](../docs/ARCHITECTURE_PLAN.md) and the decisions
log in [TASKS.md](../TASKS.md).
