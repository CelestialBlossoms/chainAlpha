#!/bin/sh
set -eu

mkdir -p "$HOME/.config/gmgn"

python - <<'PY' > "$HOME/.config/gmgn/.env"
from config import GMGN_API_KEY

if not GMGN_API_KEY:
    raise SystemExit("GMGN_API_KEY is empty in config.py")

print(f"GMGN_API_KEY={GMGN_API_KEY}")
PY

chmod 600 "$HOME/.config/gmgn/.env"

exec "$@"
