#!/bin/bash
set -e  # Exit on error

echo "📦 Syncing uv dependencies..."
uv sync --all-groups

echo "🔧 Activating virtual environment..."
source .venv/bin/activate

echo "📦 Setting up Node.js environment..."
# uv run nodeenv --node=22.1.0 -p

echo "📦 Installing AWS CDK..."
npm install aws-cdk@latest

echo "✅ Setup complete!"
