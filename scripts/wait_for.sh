#!/usr/bin/env bash
# Usage: wait_for.sh <url> [timeout_s]
# Polls a URL until it answers 2xx or the timeout elapses.
set -u
url="$1"
timeout="${2:-240}"
end=$((SECONDS + timeout))
until curl -fsS -o /dev/null "$url"; do
  if [ "$SECONDS" -ge "$end" ]; then
    echo "timeout waiting for $url" >&2
    exit 1
  fi
  sleep 3
done
