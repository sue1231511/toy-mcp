#!/data/data/com.termux/files/usr/bin/sh
# Termux launcher for the toy bridge. Loads bridge.env if present, takes a wake lock when available.
cd "$(dirname "$0")" || exit 1

if [ -f ./bridge.env ]; then
  set -a
  . ./bridge.env
  set +a
fi

if command -v termux-wake-lock >/dev/null 2>&1; then
  termux-wake-lock
else
  echo "termux-wake-lock not available: the phone may suspend this process" >&2
fi

exec python bridge.py
