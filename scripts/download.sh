#!/usr/bin/env bash
# Download one day (24 hourly files) of GH Archive data.
# Usage: scripts/download.sh 2024-01-01 ./gh
set -euo pipefail

day="${1:-2024-01-01}"
out="${2:-./gh}"
mkdir -p "$out"

for h in $(seq 0 23); do
  f="${day}-${h}.json.gz"
  if [[ -s "$out/$f" ]]; then
    echo "have $f"
    continue
  fi
  echo "fetching $f"
  curl -fsSL --retry 3 -o "$out/$f" "https://data.gharchive.org/$f"
done
du -sh "$out"
