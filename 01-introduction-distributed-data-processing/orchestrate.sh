#!/usr/bin/env bash
# Locally-distributed processing
# ================================
# Discovers all CSV files in a directory, then launches one independent
# process_single_threaded.py process per file — all running in parallel.
#
# Each process gets its own temporary directory containing a symlink to
# its assigned file, which is how we give it exactly one file to process
# without modifying the script itself.
#
# Usage: ./orchestrate.sh <input-dir>

set -euo pipefail

INPUT_DIR="${1:?Usage: $0 <input-dir>}"

pids=()
tmp_dirs=()

# Ensure temp dirs are always removed, even on error or Ctrl-C.
cleanup() {
    for tmp_dir in "${tmp_dirs[@]:-}"; do
        rm -rf "$tmp_dir"
    done
}
trap cleanup EXIT

shopt -s nullglob
csv_files=("$INPUT_DIR"/*.csv)

if [[ ${#csv_files[@]} -eq 0 ]]; then
    echo "ERROR: no CSV files found in '$INPUT_DIR'" >&2
    exit 1
fi

echo "Found ${#csv_files[@]} file(s) — launching one process per file"
start=$(date +%s)

for csv_file in "${csv_files[@]}"; do
    tmp_dir=$(mktemp -d)
    tmp_dirs+=("$tmp_dir")

    uv run python process_orchestrated.py --input-file "$csv_file" &
    echo "Launched PID $! → $(basename "$csv_file")"
    pids+=($!)
done

# Block until every child process exits.
for pid in "${pids[@]}"; do
    wait "$pid"
done

end=$(date +%s)
echo "All processes completed in $((end - start)) s"
