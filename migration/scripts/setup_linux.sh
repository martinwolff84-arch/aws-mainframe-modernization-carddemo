#!/usr/bin/env bash
# Explicit installation for a disposable Debian/Ubuntu Devin environment.
set -euo pipefail
cd "$(dirname "$0")/.."
if ! command -v cobc >/dev/null || ! command -v javac >/dev/null; then
  if [[ "$(id -u)" == 0 ]]; then
    apt-get update
    apt-get install -y gnucobol openjdk-17-jdk-headless python3-venv build-essential
  else
    sudo apt-get update
    sudo apt-get install -y gnucobol openjdk-17-jdk-headless python3-venv build-essential
  fi
fi
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
printf 'Setup complete. Activate .venv, confirm Java 17 and run the README checks.\n'
