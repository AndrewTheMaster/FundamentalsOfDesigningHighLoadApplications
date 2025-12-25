#!/bin/bash

# Скрипт для запуска интеграционного теста шардированного Raft кластера

set -e

echo "=================================================="
echo "  Sharded Raft Integration Test Runner"
echo "=================================================="
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Проверка, что мы в корне проекта
if [ ! -f "go.mod" ]; then
    echo -e "${RED}Error: go.mod not found. Run this script from project root.${NC}"
    exit 1
fi

echo -e "${YELLOW}[1/4] Cleaning up old test data...${NC}"
rm -rf /tmp/lsmdb-test-* 2>/dev/null || true
echo -e "${GREEN}✓ Cleanup complete${NC}"
echo ""

echo -e "${YELLOW}[2/4] Building project...${NC}"
go build -o bin/lsmdb ./cmd/ || {
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
}
echo -e "${GREEN}✓ Build successful${NC}"
echo ""

echo -e "${YELLOW}[3/4] Running integration test...${NC}"
echo -e "${YELLOW}This test will:${NC}"
echo "  - Create 2 Raft clusters (3 nodes each)"
echo "  - Write 100 random keys with sharding verification"
echo "  - Stop master node in cluster1"
echo "  - Verify re-election"
echo "  - Write 50 more keys after failover"
echo "  - Verify old and new data on new master"
echo ""

# Запуск теста с тегом integration
go test -v -tags=integration -timeout=5m ./pkg/cluster/... -run TestShardedRaftIntegration 2>&1 | tee /tmp/integration-test.log

TEST_RESULT=${PIPESTATUS[0]}

echo ""
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✓✓✓ Integration test PASSED ✓✓✓${NC}"
    echo ""
    echo "Test verified:"
    echo "  ✓ 2 Raft clusters with 3 nodes each"
    echo "  ✓ Data sharding across clusters"
    echo "  ✓ Leader election"
    echo "  ✓ Leader failover and re-election"
    echo "  ✓ Data consistency after failover"
else
    echo -e "${RED}✗✗✗ Integration test FAILED ✗✗✗${NC}"
    echo ""
    echo "Check log: /tmp/integration-test.log"
    exit 1
fi

echo ""
echo -e "${YELLOW}[4/4] Cleaning up test data...${NC}"
rm -rf /tmp/lsmdb-test-* 2>/dev/null || true
echo -e "${GREEN}✓ Cleanup complete${NC}"

echo ""
echo "=================================================="
echo -e "${GREEN}  All tests completed successfully!${NC}"
echo "=================================================="

