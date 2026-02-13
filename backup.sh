#!/bin/bash
# ============================================
#   Expense Tracker - Backup database to GitHub
#   Run: ./backup.sh
# ============================================
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

BACKUP_DIR="$DIR/backups"
mkdir -p "$BACKUP_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/expense_tracker_${TIMESTAMP}.db"

# Copy current database
cp data/expense_tracker.db "$BACKUP_FILE"

# Also keep a "latest" copy (this one gets pushed to GitHub)
cp data/expense_tracker.db "$BACKUP_DIR/expense_tracker_latest.db"

echo "Backed up to: $BACKUP_FILE"
echo "Latest copy:  $BACKUP_DIR/expense_tracker_latest.db"

# Push to GitHub
git add backups/expense_tracker_latest.db
git -c user.name="Prajjwal Agarwal" -c user.email="prajjwal@local" \
    commit -m "Backup database: $TIMESTAMP ($(wc -l < /dev/null))" \
    -m "Transactions: $(sqlite3 data/expense_tracker.db 'SELECT COUNT(*) FROM transactions')" \
    2>/dev/null

git push origin main 2>&1

echo ""
echo "Database backed up and pushed to GitHub."
echo "To restore on a new laptop, run: ./restore.sh"
