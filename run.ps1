# ─────────────────────────────────────────────────────────────────────────────
# run.ps1  — Local development launcher
#
# Usage:
#   .\run.ps1            → starts the FastAPI backend (default)
#   .\run.ps1 dashboard  → starts the Streamlit metrics dashboard
# ─────────────────────────────────────────────────────────────────────────────

# sentence-transformers / faiss-cpu live in a short-path venv to avoid
# Windows MAX_PATH (260-char) errors from torch's deeply nested dist-info.
$env:PYTHONPATH = "C:\venv\sq\Lib\site-packages"

$PYTHON = ".\venv\Scripts\python.exe"

if ($args[0] -eq "dashboard") {
    Write-Host "Starting Streamlit metrics dashboard on http://localhost:8501 ..." -ForegroundColor Cyan
    & $PYTHON -m streamlit run metrics_dashboard.py
} else {
    Write-Host "Starting FastAPI backend on http://localhost:8000 ..." -ForegroundColor Cyan
    & $PYTHON -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
}
