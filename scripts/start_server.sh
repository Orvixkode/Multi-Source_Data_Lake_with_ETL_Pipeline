#!/bin/bash

# Simple FastAPI server startup
echo "Starting Multi-Source Data Lake API Server..."

# Navigate to project directory
cd "/Users/iktider_sarker/Programming/Python Projects/Multi-Source_Data_Lake_with_ETL_Pipeline"

# Create logs directory if it doesn't exist
mkdir -p logs

# Start server in background
nohup "/Users/iktider_sarker/Programming/Python Projects/.venv/bin/python" -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload > logs/server.log 2>&1 &

# Get the process ID
SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"
echo "Server PID saved to logs/server.pid"
echo $SERVER_PID > logs/server.pid

# Wait a moment for server to start
sleep 3

# Test the server
echo "Testing server..."
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ Server is running successfully!"
    echo "🌐 Access your API at: http://localhost:8000"
    echo "📚 API Documentation: http://localhost:8000/docs"
else
    echo "❌ Server may not be responding yet. Check logs/server.log"
fi

echo "To stop the server later, run: kill $SERVER_PID"