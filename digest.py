"""Build a plain-text digest of what this project has done since the last digest.

    python digest.py               # print the digest, save it to .digest/latest.txt
    python digest.py --mark-sent   # after the digest was delivered, record it as sent

Nothing is sent from here. The digest is plain text so that the sending step
(see DIGEST.md) can choose the recipient and deliver it. `--mark-sent` only
advances the saved state, so a failed send is retried with the same new items.

Read-only: it opens the database read-only and changes no project data.
"""
import argparse
import csv
import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(HERE, ".digest")
STATE_PATH = os.path.join(STATE_DIR, "state.json")
PENDING_PATH = os.path.join(STATE_DIR, "pending.json")
LATEST_PATH = os.path.join(STATE_DIR, "latest.txt")
DB_PATH = os.path.join(HERE, "data", "books.sqlite")
EPISODES_CSV = os.path.join(HERE, "data", "episodes.csv")
FILLS_CSV = os.path.join(HERE, "data", "maker_fills.csv")
STALE_S = 300        # a recorder cycle older than this means the recorder is not running
MAX_LIST = 10        # new items listed in full; the rest are counted


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def ago(seconds):
    seconds = int(seconds)
    if seconds < 90:
        return f"{seconds} s ago"
    if seconds < 5400:
        return f"{seconds // 60} min ago"
    if seconds < 86400 * 2:
        return f"{seconds // 3600} h ago"
    return f"{seconds // 86400} days ago"


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def git(*args):
    r = subprocess.run(["git", "-C", HERE, *args], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def code_section(state):
    lines = []
    head = git("rev-parse", "--short", "HEAD")
    lines.append(f"- Current commit: {head or 'unknown'}")
    last = state.get("last_commit")
    if last and git("cat-file", "-t", last):
        log = git("log", f"{last}..HEAD", "--format=%h %s (%ad)", "--date=short")
        label = "Commits since last update"
    else:
        log = git("log", "-5", "--format=%h %s (%ad)", "--date=short")
        label = "Latest commits (no earlier update to compare with)"
    commits = log.splitlines() if log else []
    lines.append(f"- {label}: {len(commits)}")
    lines += [f"    {c}" for c in commits[:MAX_LIST]]
    # Ignore scratch state: the digest's own folder, bytecode, and SQLite's live -wal/-shm files.
    status = [s for s in git("status", "--porcelain").splitlines()
              if not s.split(None, 1)[1].startswith((".digest/", "__pycache__/", "data/books.sqlite-"))]
    if status:
        lines.append(f"- Uncommitted changes: {len(status)} file(s)")
        lines += [f"    {s}" for s in status[:MAX_LIST]]
    else:
        lines.append("- Uncommitted changes: none")
    return lines


def recorder_section(state, now_ms):
    lines = []
    last_run_ms = state.get("last_run_ms", 0)
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        cyc = con.execute(
            "select id, finished_ms, kalshi_ok, poly_ok, errors from cycles order by id desc limit 1"
        ).fetchone()
        first_ms = con.execute("select min(started_ms) from cycles").fetchone()[0]
        totals = {
            "cycles": con.execute("select count(*) from cycles").fetchone()[0],
            "books": con.execute("select count(*) from books").fetchone()[0],
            "trades": con.execute("select count(*) from trades").fetchone()[0],
        }
        since = {
            "cycles": con.execute("select count(*) from cycles where started_ms > ?", (last_run_ms,)).fetchone()[0],
            "books": con.execute("select count(*) from books where recv_ms > ?", (last_run_ms,)).fetchone()[0],
            "trades": con.execute("select count(*) from trades where ts_ms > ?", (last_run_ms,)).fetchone()[0],
        }
        errors_since = con.execute(
            "select count(*) from cycles where started_ms > ? and errors is not null and errors != ''",
            (last_run_ms,),
        ).fetchone()[0]
    finally:
        con.close()

    if cyc is None:
        return ["- No recorder cycles in the database yet."]

    _, finished_ms, k_ok, p_ok, _ = cyc
    age_s = (now_ms - finished_ms) / 1000
    if age_s <= STALE_S:
        status = f"running (last cycle {ago(age_s)})"
    else:
        status = f"NOT RUNNING (last cycle {ago(age_s)}, {fmt(utc(finished_ms))})"
    lines.append(f"- Status: {status}")
    lines.append(f"- Markets fetched in last cycle: Kalshi {k_ok}, Polymarket {p_ok}")
    lines.append(f"- Recording since: {fmt(utc(first_ms))}")
    lines.append(
        f"- Since last update: {since['cycles']} cycles, {since['books']} book rows, "
        f"{since['trades']} trades, {errors_since} cycles with errors"
    )
    lines.append(
        f"- Totals: {totals['cycles']} cycles, {totals['books']} book rows, {totals['trades']} trades"
    )
    return lines


def episode_key(r):
    return f"{r.get('pair_id')}|{r.get('start_utc')}"


def fill_key(r):
    return f"{r.get('model')}|{r.get('pair_id')}|{r.get('fill_utc')}|{r.get('side')}|{r.get('kind')}"


def results_section(state):
    lines = []
    episodes = read_csv(EPISODES_CSV)
    seen_eps = set(state.get("seen_episodes", []))
    new_eps = [r for r in episodes if episode_key(r) not in seen_eps]
    lines.append(f"- Taker episodes: {len(episodes)} total, {len(new_eps)} new since last update")
    for r in new_eps[:MAX_LIST]:
        lines.append(
            f"    {r['pair_id']} ({r['family']}) start {r['start_utc']} UTC, "
            f"{float(r['duration_s']):.0f} s, end: {r['end_reason']}, "
            f"peak return {r['peak_return_pct']}%, annualized {r['peak_annualized_pct']}%"
        )

    fills = read_csv(FILLS_CSV)
    seen_fills = set(state.get("seen_fills", []))
    new_fills = [r for r in fills if fill_key(r) not in seen_fills]
    new_profit = sum(float(r["profit"]) for r in new_fills if r.get("profit"))
    lines.append(
        f"- Maker fills: {len(fills)} total, {len(new_fills)} new since last update "
        f"(profit on new fills: ${new_profit:.2f})"
    )
    for r in new_fills[:MAX_LIST]:
        lines.append(
            f"    [{r['model']}] {r['pair_id']} {r['maker_venue']} {r['side']} @ {r['price']} "
            f"x{float(r['size']):.0f}, {r['fill_utc']} UTC, hedged {r['hedged']}, "
            f"profit ${float(r['profit']):.2f}"
        )

    pending = {
        "seen_episodes": sorted(set(state.get("seen_episodes", [])) | {episode_key(r) for r in episodes}),
        "seen_fills": sorted(set(state.get("seen_fills", [])) | {fill_key(r) for r in fills}),
    }
    return lines, pending


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mark-sent", action="store_true",
                    help="promote the last generated digest to saved state (call after a successful send)")
    args = ap.parse_args()
    os.makedirs(STATE_DIR, exist_ok=True)

    if args.mark_sent:
        pending = load_json(PENDING_PATH, None)
        if pending is None:
            raise SystemExit("No pending digest to mark. Run digest.py first.")
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(pending, f, indent=2)
        os.remove(PENDING_PATH)
        print("Marked the last digest as sent.")
        return

    state = load_json(STATE_PATH, {})
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    last_run_ms = state.get("last_run_ms")
    window = f"since {fmt(utc(last_run_ms))}" if last_run_ms else "first digest (totals shown)"

    results_lines, seen = results_section(state)
    body = [
        "PredictionMarketArb update",
        f"Generated {fmt(now)} ({window})",
        "",
        "RECORDER",
        *recorder_section(state, now_ms),
        "",
        "CODE",
        *code_section(state),
        "",
        "RESULTS",
        *results_lines,
    ]
    text = "\n".join(body) + "\n"

    with open(LATEST_PATH, "w", encoding="utf-8") as f:
        f.write(text)
    pending = {
        "last_run_ms": now_ms,
        "last_commit": git("rev-parse", "HEAD"),
        **seen,
    }
    with open(PENDING_PATH, "w", encoding="utf-8") as f:
        json.dump(pending, f, indent=2)

    print(text)
    print(f"Saved to {LATEST_PATH}")
    print("After a successful send, run: python digest.py --mark-sent")


if __name__ == "__main__":
    main()
