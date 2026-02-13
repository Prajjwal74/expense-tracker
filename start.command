#!/bin/bash
# ============================================
#   Expense Tracker - Double-click to launch
# ============================================
PROJECT="/Users/prajjwalagarwal/Desktop/Cursor/expense-tracker"

echo "Starting Expense Tracker..."

# Start Ollama if not running
if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "Starting Ollama..."
    /opt/homebrew/bin/brew services start ollama 2>/dev/null
    sleep 3
fi

# Kill any existing streamlit on 8501
lsof -ti:8501 | xargs kill -9 2>/dev/null 2>&1
sleep 1

# Start Streamlit
cd "$PROJECT"
source "$PROJECT/venv/bin/activate"
echo "Starting Streamlit on http://localhost:8501 ..."
streamlit run app.py --server.headless true --server.port 8501 &
STPID=$!

# Wait for it to be ready, then open browser
for i in $(seq 1 20); do
    if curl -s -o /dev/null http://localhost:8501; then
        echo "Ready! Opening browser..."
        open http://localhost:8501
        echo ""
        echo "Expense Tracker is running at http://localhost:8501"
        echo "Close this terminal window to stop the app."
        wait $STPID
        exit 0
    fi
    sleep 1
done

echo "Waiting for Streamlit..."
wait $STPID
