# Интеграционный тест шардированного Raft кластера

## Описание

Комплексный интеграционный тест, который проверяет полную функциональность шардированной архитектуры с репликацией через Raft.

## Что тестируется

### 1. **Создание кластеров**
- 2 Raft кластера
- По 3 ноды в каждом кластере
- Автоматическая конфигурация peers
- Изолированные директории данных

### 2. **Шардирование данных**
- Запись 100 случайных ключей
- Автоматическое распределение между кластерами через consistent hashing
- Проверка, что данные попали в правильные кластеры
- Верификация распределения (оба кластера должны получить данные)

### 3. **Репликация через Raft**
- Выбор лидера в каждом кластере
- Запись данных через лидера
- Автоматическая репликация на followers
- Проверка доступности данных с любой ноды

### 4. **Failover и re-election**
- Остановка master ноды в cluster1
- Автоматический перевыбор нового лидера
- Обновление топологии кластера
- Проверка работоспособности нового лидера

### 5. **Consistency после failover**
- Запись 50 новых ключей после смены лидера
- Проверка наличия **старых** данных на новом лидере (были зареплицированы до падения)
- Проверка наличия **новых** данных на новом лидере (записаны после failover)
- Верификация целостности данных

## Структура теста

```
TestShardedRaftIntegration
├── STEP 1: Создание 2 кластеров
├── STEP 2: Запуск всех 6 нод
├── STEP 3: Ожидание выбора лидеров
├── STEP 4: Настройка ShardedRaftDB
├── STEP 5: Запись 100 случайных ключей
├── STEP 6: Проверка доступности через разные ноды
├── STEP 7: Остановка master ноды cluster1
├── STEP 8: Ожидание перевыбора лидера
├── STEP 9: Обновление топологии
├── STEP 10: Запись 50 новых ключей
├── STEP 11: Проверка старых и новых данных
└── STEP 12: Финальная верификация
```

## Запуск теста

### Способ 1: Через скрипт (рекомендуется)
```bash
./test_integration.sh
```

Скрипт автоматически:
- Очистит старые данные
- Скомпилирует проект
- Запустит тест с подробным выводом
- Очистит временные файлы

### Способ 2: Напрямую через go test
```bash
# Очистка старых данных
rm -rf /tmp/lsmdb-test-*

# Запуск теста
go test -v -tags=integration -timeout=5m ./pkg/cluster/... -run TestShardedRaftIntegration

# Очистка после теста
rm -rf /tmp/lsmdb-test-*
```

### Способ 3: С дополнительными опциями
```bash
# С race detector
go test -v -race -tags=integration -timeout=10m ./pkg/cluster/... -run TestShardedRaftIntegration

# С подробным логированием
LSMDB_LOG_LEVEL=DEBUG go test -v -tags=integration ./pkg/cluster/... -run TestShardedRaftIntegration

# Запуск несколько раз для проверки стабильности
for i in {1..5}; do
  echo "=== Run $i ==="
  go test -v -tags=integration ./pkg/cluster/... -run TestShardedRaftIntegration || break
done
```

## Ожидаемый результат

```
=== Starting Sharded Raft Integration Test ===

[STEP 1] Creating two Raft clusters...
✓ Cluster1 created: 3 nodes
✓ Cluster2 created: 3 nodes

[STEP 2] Starting clusters...
✓ Started node cluster1:1 on port 18080
✓ Started node cluster1:2 on port 18081
✓ Started node cluster1:3 on port 18082
✓ Started node cluster2:1 on port 19080
✓ Started node cluster2:2 on port 19081
✓ Started node cluster2:3 on port 19082

[STEP 3] Waiting for leader election...
✓ Leader elected in cluster cluster1: node 1
✓ Leader elected in cluster cluster2: node 1

[STEP 4] Setting up sharded databases...
✓ ShardedRaftDB configured for all nodes

[STEP 5] Writing random data and verifying sharding...
✓ Written 100 keys: 52 to cluster1, 48 to cluster2

[STEP 6] Verifying data availability from different nodes...
✓ Verified 100/100 keys successfully

[STEP 7] Stopping leader node in cluster1...
✗ Stopped node cluster1:1

[STEP 8] Waiting for new leader election in cluster1...
✓ New leader elected in cluster1: node 2 (was node 1)

[STEP 9] Updating cluster topology with new leader...
✓ Topology updated

[STEP 10] Writing new data after leader change...
✓ Written 50 new keys after failover

[STEP 11] Verifying old and new data on new leader...
✓ Verified 52 old keys on new leader
✓ Verified 26 new keys on new leader

[STEP 12] Final verification...
Summary:
  - Initial data: 100 keys (52 in cluster1, 48 in cluster2)
  - Old data verified on new leader: 52 keys
  - New data after failover: 26 keys verified
  - Leader failover: successful (old=1, new=2)

=== Integration Test Completed Successfully ===
PASS
```

## Топология теста

```
Cluster 1 (cluster1)           Cluster 2 (cluster2)
┌──────────────────┐          ┌──────────────────┐
│  Node 1 (Leader) │          │  Node 1 (Leader) │
│  Port: 18080     │          │  Port: 19080     │
│  ID: 1           │          │  ID: 1           │
└────────┬─────────┘          └────────┬─────────┘
         │                             │
    ┌────┴────┐                   ┌────┴────┐
    ▼         ▼                   ▼         ▼
┌────────┐ ┌────────┐        ┌────────┐ ┌────────┐
│ Node 2 │ │ Node 3 │        │ Node 2 │ │ Node 3 │
│ 18081  │ │ 18082  │        │ 19081  │ │ 19082  │
│ ID: 2  │ │ ID: 3  │        │ ID: 2  │ │ ID: 3  │
└────────┘ └────────┘        └────────┘ └────────┘

    Raft Replication            Raft Replication
```

После failover в Cluster 1:
```
Cluster 1 (cluster1)
┌──────────────────┐
│  Node 1 (DEAD)   │  ← Остановлена
│  Port: 18080     │
│  ID: 1           │
└──────────────────┘

┌──────────────────┐
│  Node 2 (Leader) │  ← Новый лидер
│  Port: 18081     │
│  ID: 2           │
└────────┬─────────┘
         │
         ▼
    ┌────────┐
    │ Node 3 │
    │ 18082  │
    │ ID: 3  │
    └────────┘
```

## Что проверяется

### ✅ Функциональность
- [x] Создание и запуск нескольких Raft кластеров
- [x] Выбор лидера в каждом кластере
- [x] Consistent hashing для шардирования
- [x] Запись через ShardedRaftDB
- [x] Чтение с любой ноды
- [x] Межкластерное взаимодействие через HTTP
- [x] Leader failover
- [x] Автоматический re-election
- [x] Consistency после failover

### ✅ Свойства системы
- [x] **Availability**: Данные доступны после падения лидера
- [x] **Consistency**: Старые данные сохранены на новом лидере
- [x] **Partition tolerance**: Кластеры работают независимо
- [x] **Durability**: Данные переживают failover

## Временные характеристики

- **Создание кластеров**: ~100ms
- **Запуск нод**: ~200ms
- **Выбор лидера**: ~1-3 секунды
- **Запись 100 ключей**: ~2-5 секунд
- **Проверка данных**: ~1-2 секунды
- **Re-election после failover**: ~5-10 секунд
- **Общее время теста**: ~30-60 секунд

## Troubleshooting

### Проблема: Timeout при выборе лидера
**Решение:**
```bash
# Увеличьте timeout в тесте или проверьте логи
go test -v -tags=integration -timeout=10m ./pkg/cluster/...
```

### Проблема: Порты заняты
**Решение:**
```bash
# Проверьте, не запущены ли старые процессы
lsof -i :18080-18082,19080-19082
kill -9 <PID>
```

### Проблема: Race conditions
**Решение:**
```bash
# Запустите с race detector
go test -v -race -tags=integration ./pkg/cluster/...
```

### Проблема: Тест зависает
**Решение:**
- Проверьте логи в `/tmp/integration-test.log`
- Убедитесь, что cleanup выполнен: `rm -rf /tmp/lsmdb-test-*`
- Проверьте доступность портов

## Расширение теста

### Добавление новых сценариев:

```go
// Тест с 3 кластерами
func TestShardedRaftIntegration_ThreeClusters(t *testing.T) { ... }

// Тест с множественными failover
func TestShardedRaftIntegration_MultipleFailovers(t *testing.T) { ... }

// Тест с сетевыми партициями
func TestShardedRaftIntegration_NetworkPartition(t *testing.T) { ... }
```

## Зависимости

- Go 1.21+
- etcd/raft v3
- Минимум 500MB RAM
- Свободные порты: 18080-18082, 19080-19082
- Linux/MacOS (Windows с WSL2)

## CI/CD Integration

```yaml
# .github/workflows/integration.yml
name: Integration Tests
on: [push, pull_request]
jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: '1.21'
      - name: Run Integration Tests
        run: ./test_integration.sh
```

## Метрики успеха

- ✅ Все 12 шагов должны пройти успешно
- ✅ Минимум 90% ключей должны быть верифицированы
- ✅ Re-election должна произойти за < 15 секунд
- ✅ Оба типа данных (старые/новые) доступны после failover
- ✅ Нет race conditions (при запуске с -race)

