# Prediction market book recorder

Records public Kalshi and Polymarket order books and trades for the rule-checked pairs in `watchlist.json`, then measures two things:

- **Taker on both legs** (`analyze_stream.py`): how often a hedged pair was profitable after fees, at what size, and for how long.
- **Maker on one leg** (`analyze_maker.py`): what a resting bid on one venue, hedged as a taker on the other, would have been filled for and earned.

It is read-only: it uses no API keys and places no orders.

## Use

Requires Python 3 and `requests`.

```bash
python test_edge.py
```

```bash
python test_maker.py
```

```bash
python record.py
```

```bash
python analyze_stream.py
```

```bash
python analyze_maker.py
```

`record.py` runs until Ctrl+C and can be restarted at any time; it appends to `data/books.sqlite`. Either analyzer can run while it records, and writes every episode to `data/episodes.csv`.

- `analyze_stream.py` reads the database row by row, so memory stays flat. Use it for anything longer than a day or two.
- `analyze.py` is the original version. It gives identical results but loads every book into memory, so it needs several GB for a multi-week recording.

Trades are polled once a minute in the background and carry their own timestamps, so a restart backfills what was missed, back to the first recorded book. `--trade-interval 0` records books only.

To copy the database while the recorder is running, use `python backup.py`. Copying `books.sqlite` directly mid-write can produce a broken file.

## Running it on EC2 for 2–4 weeks

Rough cost for 4 weeks in us-east-1: about $8.50 (t4g.nano $2.82, public IPv4 $3.36, 30 GB disk about $2.20). A new AWS account's credits may cover it.

### 1. Launch the instance (AWS console, EC2, Launch instance)

- **Region:** US East (N. Virginia), us-east-1.
- **AMI:** Amazon Linux 2023, **64-bit (Arm)**.
- **Instance type:** `t4g.nano`, or `t4g.micro` if it's free-tier eligible on your account.
- **Key pair:** create one and save the `.pem` file.
- **Network:** auto-assign a public IP. Allow SSH from **My IP** only; nothing else needs to be open.
- **Storage:** 30 GB gp3. That leaves room for about 7 GB of data plus a backup copy.

Also set an AWS Budgets alert (for example $15) so a mistake can't run up a bill unnoticed.

### 2. Upload and start (PowerShell on your PC, from this folder)

Replace `KEY` with your `.pem` path and `IP` with the instance's public IP.

```powershell
ssh -i KEY ec2-user@IP "mkdir -p ~/recorder"
```

```powershell
scp -i KEY -r record.py edge.py maker.py analyze.py analyze_stream.py analyze_maker.py backup.py test_edge.py test_maker.py watchlist.json README.md deploy ec2-user@IP:~/recorder/
```

```powershell
ssh -i KEY ec2-user@IP "bash ~/recorder/deploy/setup.sh"
```

`setup.sh` installs Python, adds 1 GB of swap, runs the tests, and does a 20-second live check from the server. Only if both venues answer does it start the recorder as a service, which restarts after crashes and reboots.

### 3. Check on it

```powershell
ssh -i KEY ec2-user@IP "journalctl -u recorder -n 5 --no-pager; df -h /; free -m"
```

Healthy output shows a heartbeat line about once a minute with Kalshi and Polymarket at 57/57, and disk use growing a few hundred MB a day. After the first day, check CPU in the EC2 console: it should stay under the t4g.nano's 5% baseline.

### 4. Get the data and analyze it

```powershell
ssh -i KEY ec2-user@IP "cd ~/recorder && python3 backup.py"
```

```powershell
scp -i KEY ec2-user@IP:~/recorder/data/books-copy.sqlite data/books.sqlite
```

```powershell
python analyze_stream.py
```

Then run `python analyze_maker.py` for the maker results. `analyze_stream.py` can also run on the server: `ssh -i KEY ec2-user@IP "cd ~/recorder && python3 analyze_stream.py"`.

### 5. Shut it down

```powershell
ssh -i KEY ec2-user@IP "sudo systemctl stop recorder"
```

Download the data first, then **terminate** the instance in the EC2 console. Stopping it isn't enough, because the disk keeps billing. Terminating also deletes the disk and frees the public IP.

## Reading the results

- An episode's `end_reason` of `coverage_gap` or `missing_data` means its true length is unknown. `still_open` means it was still live when the data ended.
- Fills assume taker orders on both legs, with fees of `rate * p * (1 - p)` per contract and Kalshi fees rounded up to the cent per order. Check the current fee schedules before relying on them.
- Edges are measured from books fetched moments apart, so they may not have been fillable together. Gaps shorter than the polling interval (5 s by default) are invisible.
- A book row is only written when that book changes. Failed fetches are logged per cycle, so unchanged data and missing data stay distinct.

### Maker results (`analyze_maker.py`)

Each pair gets four standing quotes: a bid for YES or NO, resting on Kalshi or on Polymarket. A quote joins the best bid whenever the hedged edge clears the hurdle (`--min-ann`, default 10% annualized to resolution, and `--min-edge` in cents). Every fill is written to `data/maker_fills.csv`.

- **Two fill models.** `queue` fills the quote once trades at its price have used up the size that was ahead of it. `through` fills only when a trade prints past its price. Treat `through` as the floor and `queue` as the optimistic case.
- **Quoted vs realized edge.** Quoted is the edge seen when the quote was placed. Realized uses the hedge price in the first book recorded after the fill, so the difference is what adverse selection and the hedge delay cost.
- **Not modelled:** the simulated quote does not change the market, positions are not capped, capital sitting idle on two venues earns nothing, and Polymarket's liquidity rewards and maker rebates are left out. Polymarket reports one averaged price per taker order, so some fills at the quote's price are missed.
- The simulation stops where trade coverage stops, so run it after the recorder has been up for a minute or two.

## Watchlist

`rule_status` records how each pair was checked:

- `read-close`: rules read on this pair or a same-event sibling, and judged close to equivalent.
- `template-read`: rules read on one pair in the family, with the same wording assumed for the rest.

`poly_condition_id` is needed to fetch Polymarket trades. The maker fee rates were 0 on both venues when checked on 2026-11-10 (Kalshi `fee_type` "quadratic", Polymarket `takerOnly` fees); re-check them along with the taker rates.

Neither status clears a pair for trading. Read both venues' full rules first, because this list was built by comparing rule text on a few pairs, not all pairs.
