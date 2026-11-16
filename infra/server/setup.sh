#!/usr/bin/env bash
# Server setup and update for the recorder EC2 instance (Amazon Linux 2023, arm64). Run as root.
#
# First boot: the instance's UserData writes /etc/pma.env and runs this script from a fresh clone.
# Updates:    /usr/local/bin/pma-update (installed below) pulls the branch and re-runs this script.
# Idempotent: safe to run again at any time.
#
# /etc/pma.env provides: AWS_REGION, PMA_BUCKET, PMA_REPO, PMA_BRANCH, PMA_METRICS_NAMESPACE
set -euo pipefail
source /etc/pma.env

APP=/opt/pma/app            # git checkout
VENV=/opt/pma/venv
DATA=/opt/pma/data          # raw files waiting to upload, trade watermarks

echo "== packages"
dnf install -y -q python3.12 python3.12-pip git

echo "== swap (1 GB safety margin for the nightly job on a 0.5 GB instance)"
if [ ! -f /swapfile ]; then
  dd if=/dev/zero of=/swapfile bs=1M count=1024 status=none
  chmod 600 /swapfile
  mkswap /swapfile >/dev/null
  swapon /swapfile
  echo '/swapfile none swap defaults 0 0' >> /etc/fstab
fi

echo "== service user and folders"
# root owns the code and the Python environment (updates run git as root); the service user only
# needs to write its data folder.
id pma >/dev/null 2>&1 || useradd --system --home-dir /opt/pma/data --shell /sbin/nologin pma
mkdir -p "$DATA"
chown -R pma:pma "$DATA"

echo "== python environment"
[ -x "$VENV/bin/python" ] || python3.12 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r "$APP/requirements.txt"

echo "== pairs file in S3 (seeded from the repo's watchlist.json on first run only)"
if ! aws s3api head-object --region "$AWS_REGION" --bucket "$PMA_BUCKET" --key ref/pairs/current.json >/dev/null 2>&1; then
  aws s3 cp --region "$AWS_REGION" "$APP/watchlist.json" "s3://$PMA_BUCKET/ref/pairs/current.json"
fi

echo "== update command"
install -m 0755 "$APP/infra/server/pma-update" /usr/local/bin/pma-update

echo "== systemd units"
install -m 0644 "$APP"/infra/server/pma-recorder.service /etc/systemd/system/
install -m 0644 "$APP"/infra/server/pma-compact.service /etc/systemd/system/
install -m 0644 "$APP"/infra/server/pma-compact.timer /etc/systemd/system/
install -m 0644 "$APP"/infra/server/pma-refresh.service /etc/systemd/system/
install -m 0644 "$APP"/infra/server/pma-refresh.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable pma-recorder.service pma-compact.timer pma-refresh.timer
systemctl restart pma-recorder.service
systemctl start pma-compact.timer pma-refresh.timer

sleep 5
systemctl --no-pager --lines=5 status pma-recorder.service || true
echo "== setup done ($(git -C "$APP" rev-parse --short HEAD) on $PMA_BRANCH)"
