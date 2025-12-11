#!/bin/bash
set -e

echo "Telegram Dispatch Bot - Setup"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 not installed"
    exit 1
fi

# Create venv
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Activate and install
source venv/bin/activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

# Create .env
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "Created .env - edit with your TELEGRAM_TOKEN and CLAIM_CODE"
    fi
fi

echo "Setup complete. Run: python main.py"
