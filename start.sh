#!/bin/bash

# Distributed Storage System Startup Script

echo "Starting Distributed File Storage System..."

check_service() {
    local service_name=$1
    local port=$2
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service_name to be ready on port $port..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z localhost $port 2>/dev/null; then
            echo "$service_name is ready!"
            return 0
        fi
        
        echo "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $service_name failed to start within expected time"
    return 1
}

echo "Stopping existing containers..."
docker-compose down

echo "Starting PostgreSQL database..."
docker-compose up -d postgres

check_service "PostgreSQL" 5432

echo "Starting FastAPI controller..."
docker-compose up -d controller

check_service "Controller" 8000

echo "Starting storage nodes..."
docker-compose up -d storage-node-1 storage-node-2 storage-node-3

echo "Waiting for storage nodes to register..."
sleep 10

# Check system status
echo ""
echo "=== System Status ==="
echo "Checking controller health..."
curl -s http://localhost:8000/health | python -m json.tool

echo ""
echo "Checking registered nodes..."
curl -s http://localhost:8000/nodes | python -m json.tool

echo ""
echo "=== Distributed Storage System Started Successfully! ==="
echo ""
echo "Available endpoints:"
echo "- Controller API: http://localhost:8000"
echo "- API Documentation: http://localhost:8000/docs"
echo "- Health Check: http://localhost:8000/health"
echo ""
echo "Example usage:"
echo "  python client.py upload /path/to/file.txt"
echo "  python client.py list"
echo "  python client.py nodes"
echo ""
echo "To stop the system: docker-compose down"
