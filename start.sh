#!/bin/bash

# DAG Dependency Analyzer Startup Script

echo "=== DAG Dependency Analyzer ==="
echo

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Check if requirements are installed
echo "🔍 Checking dependencies..."

if python3 -c "import flask, git, networkx" 2>/dev/null; then
    echo "✅ Core dependencies found"
else
    echo "⚠️  Installing dependencies..."
    python3 -m pip install -r requirements.txt
    
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install dependencies"
        echo "Please run: python3 -m pip install -r requirements.txt"
        exit 1
    fi
fi

# Start the Flask application
echo
echo "🚀 Starting DAG Dependency Analyzer..."
echo "📱 Access the web interface at: http://localhost:5000"
echo
echo "Press Ctrl+C to stop the server"
echo

python3 app.py