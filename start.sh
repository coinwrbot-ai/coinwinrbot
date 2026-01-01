#!/bin/bash

echo "🚀 Starting CoinWinRBot on Render..."
echo "📅 Date: $(date)"
echo "🐍 Python: $(python --version)"

# Run database migrations if needed
python -c "from database_factory import db; print('✅ Database ready')"

# Start bot
python bot.py