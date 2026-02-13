#!/bin/bash
# ============================================
#   Expense Tracker - Restore on a new laptop
#   Run this after cloning the repo:
#
#   git clone https://github.com/Prajjwal74/expense-tracker.git
#   cd expense-tracker
#   ./restore.sh
# ============================================
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "=== Expense Tracker - New Laptop Setup ==="
echo ""

# 1. Create venv and install dependencies
if [ ! -d "venv" ]; then
    echo "Step 1: Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    echo "Dependencies installed."
else
    echo "Step 1: Virtual environment already exists."
    source venv/bin/activate
fi

# 2. Install Ollama if not present
if ! command -v ollama &> /dev/null; then
    echo "Step 2: Installing Ollama..."
    brew install ollama
    brew services start ollama
    sleep 3
    ollama pull llama3.2
    echo "Ollama installed and model pulled."
else
    echo "Step 2: Ollama already installed."
    brew services start ollama 2>/dev/null
fi

# 3. Restore database from backup
mkdir -p data
if [ -f "backups/expense_tracker_latest.db" ]; then
    if [ -f "data/expense_tracker.db" ]; then
        echo "Step 3: Database already exists. Skipping restore."
        echo "  (To force restore, delete data/expense_tracker.db first)"
    else
        cp backups/expense_tracker_latest.db data/expense_tracker.db
        echo "Step 3: Database restored from backup."
    fi
else
    echo "Step 3: No backup found. A fresh database will be created on first run."
fi

# 4. Create .env if needed
if [ ! -f ".env" ]; then
    cp .env.example .env 2>/dev/null
    echo "Step 4: Created .env file (edit if needed)."
else
    echo "Step 4: .env already exists."
fi

echo ""
echo "=== Setup complete! ==="
echo "Run: source venv/bin/activate && streamlit run app.py"
echo "Or double-click the 'Expense Tracker' app on your Desktop."
