#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

echo "╔══════════════════════════════════════════╗"
echo "║  MAMI — Multi-Asset Risk Platform        ║"
echo "╚══════════════════════════════════════════╝"

# Create venv if needed
if [ ! -d "venv" ]; then
  echo "→ Creating virtual environment…"
  python3 -m venv venv
fi

source venv/bin/activate
echo "→ Installing dependencies…"
pip install -q -r requirements.txt

mkdir -p data

echo "→ Starting MAMI on http://localhost:8000"
echo "   Dashboard: http://localhost:8000"
echo "   API docs:  http://localhost:8000/docs"
echo ""
python run.py
