#!/bin/bash
# Daily fetch + backup script (called by launchd)
# Runs fetch_daily.py then backup.sh
DIR="/Users/prajjwalagarwal/Desktop/Cursor/expense-tracker"
cd "$DIR"

echo "=== $(date) ==="

# Ensure Ollama is running
if ! curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    /opt/homebrew/bin/brew services start ollama 2>/dev/null
    sleep 5
fi

# Fetch new transactions from email
"$DIR/venv/bin/python" "$DIR/fetch_daily.py" 2>&1

# Backup database to GitHub
/bin/bash "$DIR/backup.sh" 2>&1

echo "=== Done ==="
