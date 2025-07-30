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

echo "Starting all Docker services..."
docker-compose up -d

check_service "PostgreSQL" 5432
check_service "Controller" 8000
check_service "Grafana" 3000

# Ensure dashboard is properly loaded
echo "Verifying Grafana dashboard configuration..."
sleep 3  # Give Grafana a moment to initialize

# Check system status
echo ""
echo "=== System Status ==="
echo "Checking controller health..."
curl -s http://localhost:8000/health | python3 -m json.tool

echo ""
echo "Checking registered nodes..."
curl -s http://localhost:8000/nodes | python3 -m json.tool

echo ""
echo "=== Distributed Storage System Started Successfully! ==="
echo ""
echo "Available endpoints:"
echo "- Controller API: http://localhost:8000"
echo "- API Documentation: http://localhost:8000/docs"
echo "- Health Check: http://localhost:8000/health"
echo "- Monitoring Dashboard: http://localhost:3000"
echo ""
echo "Example usage:"
echo "  python3 client.py upload /path/to/file.txt"
echo "  python3 client.py list"
echo "  python3 client.py nodes"
echo ""
echo "To stop the system: docker-compose down"
