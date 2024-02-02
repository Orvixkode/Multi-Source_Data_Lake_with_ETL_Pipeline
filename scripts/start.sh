#!/bin/bash

# Multi-Source Data Lake Startup Script
# This script initializes and starts the complete data lake system

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Multi-Source Data Lake System Startup${NC}"
echo "=================================================="

# Function to print status
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if Python virtual environment exists
if [ ! -d ".venv" ] && [ ! -d "venv" ]; then
    print_warning "Python virtual environment not found. Creating one..."
    python3 -m venv .venv
    print_status "Created Python virtual environment"
fi

# Activate virtual environment
if [ -d ".venv" ]; then
    source .venv/bin/activate
    print_status "Activated Python virtual environment (.venv)"
elif [ -d "venv" ]; then
    source venv/bin/activate
    print_status "Activated Python virtual environment (venv)"
fi

# Install/upgrade dependencies
echo -e "\n${BLUE}📦 Installing Dependencies${NC}"
pip install --upgrade pip
pip install -r requirements.txt
print_status "Dependencies installed"

# Create necessary directories
echo -e "\n${BLUE}📁 Setting up Directory Structure${NC}"
mkdir -p data/{raw,staging,processed,archive}
mkdir -p logs
mkdir -p airflow/logs
mkdir -p airflow/plugins
print_status "Directory structure created"

# Set environment variables if .env doesn't exist
if [ ! -f ".env" ]; then
    print_warning ".env file not found. Creating from template..."
    cp .env.example .env
    print_status "Created .env file from template"
fi

# Generate sample data for testing
echo -e "\n${BLUE}🗄️ Generating Sample Data${NC}"
python3 << 'EOF'
import json
import pandas as pd
from datetime import datetime, timedelta
import random
import os

# Create sample PostgreSQL data
users_data = []
for i in range(100):
    users_data.append({
        'user_id': i + 1,
        'name': f'User {i + 1}',
        'email': f'user{i + 1}@example.com',
        'created_at': (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat(),
        'status': random.choice(['active', 'inactive', 'pending'])
    })

transactions_data = []
for i in range(500):
    transactions_data.append({
        'transaction_id': f'TXN{i + 1:06d}',
        'user_id': random.randint(1, 100),
        'amount': round(random.uniform(10, 1000), 2),
        'type': random.choice(['purchase', 'refund', 'transfer']),
        'timestamp': (datetime.now() - timedelta(hours=random.randint(1, 168))).isoformat()
    })

# Save sample data
os.makedirs('data/raw', exist_ok=True)

with open('data/raw/sample_users.json', 'w') as f:
    json.dump(users_data, f, indent=2)

with open('data/raw/sample_transactions.json', 'w') as f:
    json.dump(transactions_data, f, indent=2)

# Create sample CSV data
df_products = pd.DataFrame({
    'product_id': range(1, 51),
    'name': [f'Product {i}' for i in range(1, 51)],
    'category': [random.choice(['Electronics', 'Clothing', 'Books', 'Home']) for _ in range(50)],
    'price': [round(random.uniform(5, 500), 2) for _ in range(50)],
    'stock': [random.randint(0, 100) for _ in range(50)]
})

df_products.to_csv('data/raw/sample_products.csv', index=False)

print("Sample data generated successfully!")
EOF

print_status "Sample data generated"

# Start the FastAPI server
echo -e "\n${BLUE}🌐 Starting FastAPI Server${NC}"
echo "Starting server on http://localhost:8000"

# Check if server is already running
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null ; then
    print_warning "Server already running on port 8000"
else
    # Start server in background
    nohup python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload > logs/api.log 2>&1 &
    sleep 3
    
    if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null ; then
        print_status "FastAPI server started successfully"
    else
        print_error "Failed to start FastAPI server. Check logs/api.log"
    fi
fi

# Display system information
echo -e "\n${BLUE}📊 System Information${NC}"
echo "=================================================="
echo -e "API Server:          ${GREEN}http://localhost:8000${NC}"
echo -e "API Documentation:   ${GREEN}http://localhost:8000/docs${NC}"
echo -e "Health Check:        ${GREEN}http://localhost:8000/health${NC}"
echo -e "Configuration:       ${GREEN}http://localhost:8000/api/v1/config${NC}"
echo ""
echo -e "Data Directories:"
echo -e "  Raw Data:          ${YELLOW}./data/raw/${NC}"
echo -e "  Staging:           ${YELLOW}./data/staging/${NC}"
echo -e "  Processed:         ${YELLOW}./data/processed/${NC}"
echo -e "  Logs:              ${YELLOW}./logs/${NC}"
echo ""

# Test the API
echo -e "${BLUE}🧪 Testing API Endpoints${NC}"
sleep 2

# Test basic endpoints
if curl -s http://localhost:8000/health > /dev/null; then
    print_status "API health check passed"
else
    print_error "API health check failed"
fi

if curl -s http://localhost:8000/api/v1/config > /dev/null; then
    print_status "API configuration endpoint working"
else
    print_error "API configuration endpoint failed"
fi

# Display next steps
echo -e "\n${BLUE}🎯 Next Steps${NC}"
echo "=================================================="
echo "1. Visit http://localhost:8000/docs for interactive API documentation"
echo "2. Check system status: curl http://localhost:8000/api/v1/status"
echo "3. Run sample ETL job via API or Airflow"
echo "4. Monitor logs in ./logs/ directory"
echo "5. Install Docker to run the full infrastructure (PostgreSQL, MongoDB, etc.)"
echo ""
echo -e "${GREEN}✅ Multi-Source Data Lake System is ready!${NC}"
echo ""

# Keep the script running to show logs
echo -e "${BLUE}📋 Live API Logs (Ctrl+C to exit)${NC}"
echo "=================================================="
tail -f logs/api.log 2>/dev/null || echo "API log file not found yet. Server may still be starting..."