#!/bin/bash
# start.sh — Starts the DodgeAI O2C Graph System

cd "$(dirname "$0")"

if [ ! -f "o2c.db" ]; then
  echo "Database not found. Running ingestion..."
  python3 backend/ingest.py
fi

echo "Starting server at http://localhost:8000"
python3 -m uvicorn backend.main:app --port 8000 --host 0.0.0.0 --reload
