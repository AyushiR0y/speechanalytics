#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# Bajaj Life Insurance — Speech Analytics Platform
# Startup Script
# ═══════════════════════════════════════════════════════════════════════════

set -e

echo ""
echo "  ╔═══════════════════════════════════════════════╗"
echo "  ║   Bajaj Life Insurance                         ║"
echo "  ║   Speech Analytics Platform v1.0               ║"
echo "  ╚═══════════════════════════════════════════════╝"
echo ""

# Check for OpenAI API key
if [ -z "$OPENAI_API_KEY" ]; then
  echo "  ⚠  WARNING: OPENAI_API_KEY not set"
  echo "     The system will run in DEMO MODE with mock analysis"
  echo "     Set your key: export OPENAI_API_KEY=sk-..."
  echo ""
else
  echo "  ✓  OpenAI API Key found (GPT-4o analysis enabled)"
fi

echo "  ✓  Starting FastAPI server on http://localhost:8000"
echo ""

# Navigate to backend and start
cd "$(dirname "$0")/backend"
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
