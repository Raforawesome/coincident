#!/usr/bin/env bash
# Install or update the archiver on the server. From the repo root:
#   ssh USER@HOST mkdir -p /tmp/coincident
#   scp packages/ingest/src/coincident_ingest/archive.py infra/install-archiver.sh USER@HOST:/tmp/coincident/
#   scp -r infra/systemd USER@HOST:/tmp/coincident/
#   ssh USER@HOST sudo bash /tmp/coincident/install-archiver.sh
# Safe to re-run: it only replaces the script and unit files, never the archive.
set -euo pipefail
SRC="$(cd "$(dirname "$0")" && pwd)"

id coincident >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin coincident
install -d -o root -g root -m 755 /opt/coincident
install -d -o coincident -g coincident -m 750 /srv/coincident /srv/coincident/raw
install -o root -g root -m 644 "$SRC/archive.py" /opt/coincident/archive.py
install -o root -g root -m 644 "$SRC/systemd/coincident-archive.service" /etc/systemd/system/
install -o root -g root -m 644 "$SRC/systemd/coincident-archive.timer" /etc/systemd/system/
restorecon -R /opt/coincident /srv/coincident /etc/systemd/system/coincident-archive.* 2>/dev/null || true

systemctl daemon-reload
systemctl enable --now coincident-archive.timer
echo "installed. first run: sudo systemctl start --no-block coincident-archive.service"
