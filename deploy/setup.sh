#!/usr/bin/env bash
# One-time setup on a fresh Amazon Linux 2023 instance. Run from the uploaded folder:
#   bash deploy/setup.sh
# Installs Python, adds swap, runs the tests and a short live check, then starts the
# recorder as a service that restarts after crashes and reboots.
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"

echo "== Installing Python"
sudo dnf install -y -q python3 python3-requests

echo "== Adding 1 GB of swap as a safety margin"
if [ ! -f /swapfile ]; then
  sudo dd if=/dev/zero of=/swapfile bs=1M count=1024 status=none
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  echo '/swapfile none swap defaults 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

echo "== Running edge math tests"
python3 test_edge.py

echo "== 20-second live check from this server"
SMOKE=/tmp/recorder-smoke.sqlite
rm -f "$SMOKE"*
python3 record.py --duration 20 --db "$SMOKE"
python3 - "$SMOKE" <<'EOF'
import json, sqlite3, sys
pairs = json.load(open("watchlist.json", encoding="utf-8"))["pairs"]
want_k = len({p["kalshi_ticker"] for p in pairs})
want_p = len({p["poly_yes_token"] for p in pairs})
k, p, err = sqlite3.connect(sys.argv[1]).execute(
    "SELECT kalshi_ok, poly_ok, errors FROM cycles ORDER BY id DESC LIMIT 1").fetchone()
print(f"Last cycle: Kalshi {k}/{want_k}, Polymarket {p}/{want_p}")
if k == 0 or p == 0:
    sys.exit(f"Live check failed, so the recorder was not started. Error: {err}")
if k < want_k or p < want_p:
    print("Warning: some markets returned no book. They may have closed; the recorder logs them as missing.")
EOF
rm -f "$SMOKE"*

echo "== Installing and starting the service"
sed -e "s|__APP_DIR__|$APP_DIR|g" -e "s|__USER__|$(id -un)|g" deploy/recorder.service \
  | sudo tee /etc/systemd/system/recorder.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable --now recorder
sleep 8
systemctl --no-pager --lines=5 status recorder
echo "== Done. Recording to $APP_DIR/data/books.sqlite"
