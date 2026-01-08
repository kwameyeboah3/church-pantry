#!/usr/bin/env bash
set -e

# Local uses ./data (Render can set PANTRY_DATA_DIR=/data)
DATA_DIR="${PANTRY_DATA_DIR:-./data}"
mkdir -p "$DATA_DIR"

export PANTRY_DB_PATH="$DATA_DIR/church_pantry.db"

# Initialize DB if missing
if [ ! -f "$PANTRY_DB_PATH" ]; then
  echo "Initializing database at $PANTRY_DB_PATH"
  python3 pantry_app.py --init-db || true
fi

exec gunicorn --bind 0.0.0.0:${PORT:-5000} pantry_app:APP
