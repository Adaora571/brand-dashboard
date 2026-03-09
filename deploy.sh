#!/bin/bash
# ============================================================
# HTV Brand Dashboard — Quick Deploy Helper
# ============================================================
# Usage:
#   1. Install Render CLI: brew install render
#   2. Set GOOGLE_SA_KEY_JSON in Render dashboard
#   3. Run: bash deploy.sh
# ============================================================

echo "🚀 HTV Brand Dashboard — Deployment"
echo "======================================"
echo ""

# Check if render CLI is available
if command -v render &> /dev/null; then
  echo "✅ Render CLI found"
  echo "   Deploying via render.yaml..."
  render deploy
else
  echo "ℹ️  Render CLI not found."
  echo "   You can deploy manually:"
  echo ""
  echo "   1. Push this folder to a GitHub repo"
  echo "   2. Go to https://dashboard.render.com"
  echo "   3. Click 'New' → 'Web Service'"
  echo "   4. Connect your repo"
  echo "   5. Set environment variable: GOOGLE_SA_KEY_JSON"
  echo "      (paste the full service account JSON as one line)"
  echo "   6. Set any TOKEN_* env vars for secure access"
  echo "   7. Deploy!"
  echo ""
  echo "   Or run locally:"
  echo "   pip install -r requirements.txt"
  echo "   uvicorn app:app --reload --port 8000"
  echo ""
  echo "   Then visit: http://localhost:8000/brand/four20?token=f20_demo_token_2025"
fi
