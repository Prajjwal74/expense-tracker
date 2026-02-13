#!/bin/bash
# Expense Tracker - Start script
# Run this to launch the app: ./start.sh
# Or double-click from Finder after: chmod +x start.sh

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Start Ollama if not running
if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "Starting Ollama..."
    brew services start ollama 2>/dev/null || ollama serve &
    sleep 3
fi

# Activate venv and run Streamlit
source "$DIR/venv/bin/activate"
exec streamlit run app.py --server.headless true --server.port 8501
