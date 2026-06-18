#!/usr/bin/env bash
set -euo pipefail

# Install system deps (Debian/Ubuntu)
if [ "$(uname -s)" = "Linux" ]; then
  sudo apt-get update
  sudo apt-get install -y build-essential cmake libhiredis-dev libssl-dev pkg-config python3 python3-venv python3-pip git
fi

# Python env for scoring-worker
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r scoring-worker/requirements.txt

echo "Dev environment prepared."
