#!/bin/bash

echo "🚗 Setting up Car Connectivity Dev Environment..."

# 1. Set Project & Environment Variables
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$CURRENT_PROJECT" ]; then
    echo "⚠️  No GCP project set. Please set it first using:"
    echo "gcloud config set project <your-project-id>"
    # Use return instead of exit so it doesn't close the terminal if sourced
    return 1 2>/dev/null || exit 1
fi

export GCP_PROJECT=$CURRENT_PROJECT
echo "✅ GCP Project set to: $GCP_PROJECT"

# 2. Create and activate Python virtual environment
echo "🐍 Setting up Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate
echo "✅ Virtual environment activated."

# 3. Install necessary packages
echo "📦 Installing dependencies..."
python3 -m pip install --upgrade pip -q

# Loop through all project directories and install their requirements
for dir in ingest-car-data frontend elevation-backfill; do
    if [ -f "$dir/requirements.txt" ]; then
        echo "Installing $dir requirements..."
        pip install -r "$dir/requirements.txt"
    fi
done

# Install functions framework for local Cloud Function testing
pip install functions-framework

echo ""
echo "🎉 Setup complete! Your virtual environment is active and ready to go."