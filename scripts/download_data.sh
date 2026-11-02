#!/usr/bin/env bash
# Download Amazon Reviews 2023 (McAuley Lab) raw review + metadata files for the given categories.
# Usage: scripts/download_data.sh All_Beauty Video_Games
set -euo pipefail
OUT="${RAW_DIR:-data/raw}"
UCSD="https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw"
HF="https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw"
mkdir -p "$OUT"
[ $# -gt 0 ] || { echo "usage: $0 <Category> [Category...]"; exit 1; }

fetch() {  # $1 = remote dir (review_categories|meta_categories), $2 = file stem
  local dir=$1 stem=$2
  if [ -s "$OUT/$stem.jsonl.gz" ] || [ -s "$OUT/$stem.jsonl" ]; then echo "exists: $stem"; return; fi
  echo "downloading $stem ..."
  if curl -fL --retry 3 -o "$OUT/$stem.jsonl.gz.part" "$UCSD/$dir/$stem.jsonl.gz"; then
    mv "$OUT/$stem.jsonl.gz.part" "$OUT/$stem.jsonl.gz"
  else
    echo "UCSD mirror failed, trying Hugging Face (uncompressed)"
    rm -f "$OUT/$stem.jsonl.gz.part"
    curl -fL --retry 3 -o "$OUT/$stem.jsonl.part" "$HF/$dir/$stem.jsonl"
    mv "$OUT/$stem.jsonl.part" "$OUT/$stem.jsonl"
  fi
}

for c in "$@"; do
  fetch review_categories "$c"
  fetch meta_categories "meta_$c"
done
ls -lh "$OUT"
mkdir -p "${RESULTS_DIR:-docs/results}"
# record what was downloaded (sizes + sha256) as evidence
( cd "$OUT" && shasum -a 256 * 2>/dev/null || sha256sum * ) > "${RESULTS_DIR:-docs/results}/raw_files_sha256.txt"
