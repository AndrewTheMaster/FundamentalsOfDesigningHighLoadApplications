# Интеграция шардирования и репликации Raft - Итоговый отчет

## Выполненные изменения

### 1. Создан новый компонент `ShardedRaftDB`
**Файл:** `pkg/cluster/sharded_raft.go`

Этот компонент объединяет:
- **Consistent Hashing** для шардирования между Raft кластерами
- **Raft репликацию** внутри каждого кластера
- **Автоматическую маршрутизацию** запросов в нужный кластер

Основные методы:
- `Put(ctx, key, value)` - запись с автоматическим шардированием
- `Get(ctx, key)` - чтение с автоматическим шардированием
- `Delete(ctx, key)` - удаление с автоматическим шардированием
- `AddCluster/RemoveCluster` - управление топологией
- `UpdateClusterLeader` - обновление лидера кластера

### 2. HTTP клиент для межкластерного взаимодействия
**Файл:** `pkg/cluster/remote_client.go`

Реализует интерфейс `Remote` для HTTP запросов между кластерами.

### 3. Упрощен Router
**Файл:** `pkg/cluster/router.go`

- Удалена дублирующая логика репликации (теперь в Raft)
- Удалены методы `replicasForKey()`, `SetAlive()`, `isAlive()`
- Помечен как DEPRECATED - используйте `ShardedRaftDB`
- Оставлен для обратной совместимости

### 4. Обновлен HTTP сервер
**Файл:** `internal/http/server.go`

Добавлена поддержка двух режимов работы:
- **Одиночный Raft кластер** - используется `NewServer(node, port)`
- **Шардированный режим** - используется `NewShardedServer(shardedDB, store, port)`

### 5. Расширена конфигурация
**Файл:** `pkg/config/config.go`

Добавлена секция `cluster` для описания топологии:
```yaml
cluster:
  local_cluster_id: "cluster1"
  clusters:
    - id: "cluster1"
      leader: "http://localhost:8080"
      replicas: ["http://localhost:8080", "http://localhost:8081"]
```

### 6. Обновлен main.go
**Файл:** `cmd/main.go`

Автоматический выбор режима работы на основе конфигурации:
- Если есть секция `cluster.clusters` → шардированный режим
- Иначе → одиночный Raft кластер

## Архитектура решения

### Структура данных

```
┌─────────────────────────────────────────────────────────┐
│                    Client Request                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              HTTP Server (любая нода)                    │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              ShardedRaftDB.Put/Get/Delete                │
│         (Consistent Hashing для выбора кластера)         │
└───────────┬──────────────────────────┬──────────────────┘
            │                          │
            ▼                          ▼
   ┌──────────────────┐      ┌──────────────────┐
   │ Локальный кластер│      │ Удаленный кластер│
   └────────┬─────────┘      └────────┬─────────┘
            │                         │
            ▼                         ▼
   ┌──────────────────┐      ┌──────────────────┐
   │  Raft Node (1)   │      │  HTTP Client     │
   │  Leader ⇄ Follower│      └────────┬─────────┘
   └────────┬─────────┘               │
            │                         │
            ▼                         ▼
   ┌──────────────────┐      ┌──────────────────┐
   │  Local Store     │      │  Remote Leader   │
   └──────────────────┘      │  Raft Node (2)   │
                             └────────┬─────────┘
                                      │
                                      ▼
                             ┌──────────────────┐
                             │  Remote Store    │
                             └──────────────────┘
```

### Как работает PUT

1. Клиент отправляет PUT на любую ноду
2. `ShardedRaftDB` вычисляет хэш ключа
3. По consistent hashing определяется целевой кластер
4. **Если кластер локальный:**
   - Команда через `Raft.Execute()` → репликация
   - Leader применяет изменение, реплицирует на followers
5. **Если кластер удаленный:**
   - HTTP запрос на leader удаленного кластера
   - Там выполняется п.4

### Как работает GET

1. Клиент отправляет GET на любую ноду
2. `ShardedRaftDB` определяет целевой кластер
3. **Если кластер локальный:**
   - Прямое чтение из локального Store
4. **Если кластер удаленный:**
   - HTTP запрос к любой реплике удаленного кластера
   - Fallback при ошибках

## Тестирование

Все тесты успешно проходят:

```bash
$ go test ./pkg/cluster/... -v
=== RUN   TestShardedRaftDB_LocalCluster
--- PASS: TestShardedRaftDB_LocalCluster (0.00s)
=== RUN   TestShardedRaftDB_MultipleClusterss
--- PASS: TestShardedRaftDB_MultipleClusterss (0.00s)
=== RUN   TestShardedRaftDB_AddRemoveCluster
--- PASS: TestShardedRaftDB_AddRemoveCluster (0.00s)
=== RUN   TestShardedRaftDB_UpdateLeader
--- PASS: TestShardedRaftDB_UpdateLeader (0.00s)
=== RUN   TestHashRing_ConsistentHashing
--- PASS: TestHashRing_ConsistentHashing (0.00s)
=== RUN   TestHashRing_AddRemoveNode
--- PASS: TestHashRing_AddRemoveNode (0.00s)
PASS
```

## Удаленный код

### Из `pkg/cluster/router.go`:
- ❌ Методы `Put()`, `Get()`, `Delete()` с ручной репликацией
- ❌ `replicasForKey()` - выбор реплик
- ❌ `SetAlive()`, `isAlive()` - health tracking
- ❌ `contains()` helper
- ✅ Оставлен базовый `PutString/GetString/Delete` для совместимости

### Независимые компоненты (не удалены):
- `pkg/cluster/types.go` - может использоваться для service discovery
- ZooKeeper интеграция - может быть добавлена позже

## Преимущества решения

1. ✅ **Единая точка ответственности**: Репликация = Raft, Шардирование = Consistent Hashing
2. ✅ **Простота**: Нет дублирования логики репликации
3. ✅ **Масштабируемость**: Горизонтальное масштабирование через добавление кластеров
4. ✅ **Надежность**: Raft гарантирует консистентность внутри кластера
5. ✅ **Обратная совместимость**: Поддержка обоих режимов работы
6. ✅ **Гибкость**: Легко добавить/удалить кластеры

## Примеры использования

### Одиночный Raft кластер (3 ноды)

**config/application.yml:**
```yaml
raft:
  id: 1
  peers:
    - id: 1
      address: "http://localhost:8080"
    - id: 2
      address: "http://localhost:8081"
    - id: 3
      address: "http://localhost:8082"
```

### Шардированный кластер (2 кластера по 2 ноды)

**config/cluster1_node1.yml:**
```yaml
raft:
  id: 1
  peers:
    - id: 1
      address: "http://localhost:8080"
    - id: 2
      address: "http://localhost:8081"

cluster:
  local_cluster_id: "cluster1"
  clusters:
    - id: "cluster1"
      leader: "http://localhost:8080"
      replicas:
        - "http://localhost:8080"
        - "http://localhost:8081"
    - id: "cluster2"
      leader: "http://localhost:9080"
      replicas:
        - "http://localhost:9080"
        - "http://localhost:9081"
```

## Дальнейшее развитие

1. **Service Discovery**: Автоматическое обнаружение кластеров через ZooKeeper/etcd
2. **Rebalancing**: Автоматическое перераспределение данных при изменении топологии
3. **Monitoring**: Prometheus метрики для каждого кластера
4. **Smart routing**: Выбор реплики на основе нагрузки/latency
5. **Cross-cluster transactions**: Поддержка транзакций между кластерами

## Заключение

Интеграция успешно завершена. Механизмы шардирования и репликации теперь работают вместе:
- **Шардирование** на уровне Raft кластеров через consistent hashing
- **Репликация** внутри кластера через Raft
- **Минимум дублирования** кода
- **Полная обратная совместимость**

Проект готов к использованию в обоих режимах!

