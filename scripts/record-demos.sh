#!/usr/bin/env bash
# Record VHS demo tapes and convert GIF output to WebP.
#
# Usage:
#   ./scripts/record-demos.sh              # record all tapes
#   ./scripts/record-demos.sh demo.tape    # record one tape
#
# Requires: vhs, gif2webp (from webp package), ffmpeg, chromium
# VHS infra setup: see CLAUDE.md "Recording Demo GIFs"

set -euo pipefail

TAPE_DIR="docs/assets"
PATH="/tmp/vhs-bin:$HOME/go/bin:/home/linuxbrew/.linuxbrew/bin:$PATH"

if [ $# -gt 0 ]; then
    tapes=("$@")
else
    tapes=("$TAPE_DIR"/*.tape)
fi

for tape in "${tapes[@]}"; do
    echo "==> Recording $tape"
    vhs "$tape"

    # Extract the gif output path from the tape file
    gif=$(grep -m1 '^Output ' "$tape" | awk '{print $2}')
    webp="${gif%.gif}.webp"

    echo "==> Converting $gif -> $webp"
    gif2webp -q 80 "$gif" -o "$webp"
    rm "$gif"

    echo "==> Done: $webp ($(du -h "$webp" | cut -f1))"
    echo
done
