#!/usr/bin/env bash
set -euo pipefail

# find-open-port.sh
#
# Finds the first open TCP port on localhost within a range.
#
# Usage:
#   chmod +x find-open-port.sh                            # make the script executable
#   ./find-open-port.sh                                   # uses defaults (4000..5000)
#   ./find-open-port.sh --port-start 8080 --port-end 9000 # custom range (8080..9000)

PORT_START=4000
PORT_END=5000

# Read flags.
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port-start)
      PORT_START="${2:?Missing value for --port-start}"
      shift 2
      ;;
    --port-end)
      PORT_END="${2:?Missing value for --port-end}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--port-start N] [--port-end N]"
      echo "Defaults: --port-start 4000 --port-end 5000"
      exit 0
      ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Use --help for usage." >&2
      exit 2
      ;;
  esac
done

# Basic validation.
if ! [[ "$PORT_START" =~ ^[0-9]+$ && "$PORT_END" =~ ^[0-9]+$ ]]; then
  echo "PORT_START and PORT_END must be whole numbers." >&2
  exit 2
fi

if (( PORT_START > PORT_END )); then
  echo "PORT_START must be less than or equal to PORT_END." >&2
  exit 2
fi

# Loop through the port numbers, lowest to highest.
port="$PORT_START"
while [ "$port" -le "$PORT_END" ]; do
  # lsof exits with:
  # - 0 if it found a listening process on that port
  # - non-zero if it found nothing (which means the port is open)
  if ! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "$port"
    exit 0
  fi

  port=$((port + 1))
done

echo "No open port found between $PORT_START and $PORT_END." >&2
exit 1