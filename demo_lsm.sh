#!/bin/bash

# LSM-Tree Demo Script
# This script demonstrates the LSM-tree functionality

echo "=== LSM-Tree Database Demo ==="
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

print_header "Building LSM-Tree Database"
print_status "Building Docker image..."
docker build -t lsmdb . || {
    print_error "Failed to build Docker image"
    exit 1
}

print_header "Starting Database Container"
print_status "Starting LSM-Tree database..."
docker run -d --name lsmdb-demo -p 8080:8080 -p 8081:8081 lsmdb || {
    print_error "Failed to start container"
    exit 1
}

# Wait for container to start
print_status "Waiting for database to start..."
sleep 5

print_header "Testing Database Health"
print_status "Checking health endpoint..."
if curl -s http://localhost:8081/health | grep -q "OK"; then
    print_status "Database is healthy!"
else
    print_error "Database health check failed"
    docker logs lsmdb-demo
    exit 1
fi

print_header "Running LSM-Tree Tests"
print_status "Executing comprehensive tests..."

# Run tests inside container
docker exec lsmdb-demo go test ./pkg/store/... -v || {
    print_error "Tests failed"
    docker logs lsmdb-demo
    exit 1
}

print_header "Checking Data Directory Structure"
print_status "Examining LSM-tree files..."
docker exec lsmdb-demo ls -la /data/ || {
    print_warning "Could not list data directory"
}

print_header "Monitoring Database Logs"
print_status "Recent database activity:"
docker logs lsmdb-demo --tail 20

print_header "Performance Test"
print_status "Running performance test..."

# Create a simple performance test
cat > /tmp/performance_test.go << 'EOF'
package main

import (
    "fmt"
    "time"
    "lsmdb/pkg/store"
)

type timeProvider struct{}

func (tp *timeProvider) Now() time.Time {
    return time.Now()
}

func main() {
    store := store.New("/tmp/perf_test", &timeProvider{})
    
    start := time.Now()
    
    // Write test
    for i := 0; i < 1000; i++ {
        key := fmt.Sprintf("perf_key_%d", i)
        value := fmt.Sprintf("perf_value_%d", i)
        store.PutString(key, value)
    }
    
    writeTime := time.Since(start)
    fmt.Printf("Write 1000 keys: %v\n", writeTime)
    
    start = time.Now()
    
    // Read test
    for i := 0; i < 1000; i++ {
        key := fmt.Sprintf("perf_key_%d", i)
        store.GetString(key)
    }
    
    readTime := time.Since(start)
    fmt.Printf("Read 1000 keys: %v\n", readTime)
    
    fmt.Printf("Average write time: %v\n", writeTime/1000)
    fmt.Printf("Average read time: %v\n", readTime/1000)
}
EOF

# Copy and run performance test
docker cp /tmp/performance_test.go lsmdb-demo:/tmp/
docker exec lsmdb-demo go run /tmp/performance_test.go

print_header "LSM-Tree Architecture Verification"
print_status "Checking LSM-tree components..."

# Check if WAL file exists
if docker exec lsmdb-demo test -f /data/wal.log; then
    print_status "✓ WAL (Write-Ahead Log) exists"
else
    print_warning "⚠ WAL file not found"
fi

# Check if manifest exists
if docker exec lsmdb-demo test -f /data/MANIFEST; then
    print_status "✓ Manifest file exists"
else
    print_warning "⚠ Manifest file not found"
fi

# Check for SSTable files
sstable_count=$(docker exec lsmdb-demo find /data -name "*.sst" | wc -l)
if [ $sstable_count -gt 0 ]; then
    print_status "✓ Found $sstable_count SSTable files"
else
    print_warning "⚠ No SSTable files found (may be in memtable)"
fi

print_header "Database Statistics"
print_status "Container resource usage:"
docker stats lsmdb-demo --no-stream

print_header "Cleanup"
print_status "Stopping and removing demo container..."
docker stop lsmdb-demo
docker rm lsmdb-demo

print_header "Demo Complete"
print_status "LSM-Tree database demonstration completed successfully!"
print_status "Key features demonstrated:"
echo "  - In-memory memtable with WAL durability"
echo "  - SSTable creation and management"
echo "  - Multi-level storage organization"
echo "  - Automatic compaction (when triggered)"
echo "  - gRPC network API"
echo "  - Health monitoring"
echo "  - Performance testing"
echo "  - Container deployment"

print_status "The LSM-tree implementation includes:"
echo "  - Write-Ahead Logging for durability"
echo "  - Bloom filters for fast key lookup"
echo "  - Block cache for performance"
echo "  - Level-based compaction"
echo "  - Manifest for metadata management"
echo "  - Thread-safe operations"
echo "  - Network API with health checks"
