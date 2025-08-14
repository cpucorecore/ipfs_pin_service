#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_GEN="$SCRIPT_DIR/gen_random_file.py"

usage() {
  cat <<EOF
Usage: $(basename "$0") -m MIN_MB -M MAX_MB -c COUNT [-d OUT_DIR]

Options:
  -m, --min     Minimum file size in MB (inclusive)
  -M, --max     Maximum file size in MB (inclusive)
  -c, --count   Number of files to generate
  -d, --dir     Output directory (default: current directory)

Notes:
  - Generated filenames follow the pattern r{SIZE}M (e.g., r10M).
  - If a filename already exists, a numeric suffix -N will be appended.
  - A list of generated files will be written to a file named 'files' in the output directory.
EOF
}

MIN_MB=""
MAX_MB=""
COUNT=""
OUT_DIR="$(pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--min)
      MIN_MB="$2"; shift 2 ;;
    -M|--max)
      MAX_MB="$2"; shift 2 ;;
    -c|--count)
      COUNT="$2"; shift 2 ;;
    -d|--dir)
      OUT_DIR="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$MIN_MB" || -z "$MAX_MB" || -z "$COUNT" ]]; then
  echo "Missing required arguments." >&2
  usage
  exit 1
fi

if ! [[ "$MIN_MB" =~ ^[0-9]+$ && "$MAX_MB" =~ ^[0-9]+$ && "$COUNT" =~ ^[0-9]+$ ]]; then
  echo "-m, -M, -c must be positive integers" >&2
  exit 1
fi

if (( MIN_MB > MAX_MB )); then
  echo "MIN_MB must be <= MAX_MB" >&2
  exit 1
fi

if [[ ! -f "$PY_GEN" ]]; then
  echo "Generator not found: $PY_GEN" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
FILES_LIST="$OUT_DIR/files"
: > "$FILES_LIST"  # truncate

generate_random_size() {
  local min=$1 max=$2
  local range=$(( max - min + 1 ))
  # $RANDOM is 0..32767; modulo bias is acceptable for this use
  echo $(( (RANDOM % range) + min ))
}

for (( i=1; i<=COUNT; i++ )); do
  size_mb=$(generate_random_size "$MIN_MB" "$MAX_MB")
  base_name="r${size_mb}M"
  out_path="$OUT_DIR/$base_name"
  # Avoid overwriting if same size repeats
  if [[ -e "$out_path" ]]; then
    n=1
    while [[ -e "${out_path}-$n" ]]; do n=$((n+1)); done
    out_path="${out_path}-$n"
  fi

  echo "Generating $out_path (${size_mb} MB)"
  python3 "$PY_GEN" -o "$out_path" -s "$size_mb" --source random
  echo "$out_path" >> "$FILES_LIST"
done

echo "Generated $(wc -l < "$FILES_LIST" | tr -d ' ') files. List written to: $FILES_LIST"


