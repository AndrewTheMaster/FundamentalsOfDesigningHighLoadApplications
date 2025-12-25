# Интеграция шардирования и репликации Raft

## Обзор

Данный документ описывает интеграцию механизмов шардирования (consistent hashing) и репликации (Raft) в LSMDB.

## Архитектура

### Ключевые концепции

1. **Raft Кластер** - группа нод с репликацией (1 leader + N followers)
2. **Шардирование** - распределение данных между Raft кластерами через consistent hashing
3. **ShardedRaftDB** - компонент, объединяющий оба механизма

### Компоненты

#### 1. `pkg/cluster/sharded_raft.go`
Основной компонент интеграции. Отвечает за:
- Маршрутизацию запросов между Raft кластерами
- Управление топологией кластеров
- Взаимодействие с локальным Raft узлом
- Проксирование запросов в удаленные кластеры

#### 2. `pkg/cluster/remote_client.go`
HTTP клиент для взаимодействия с удаленными нодами в других кластерах.

#### 3. `pkg/cluster/router.go`
Упрощенный роутер (deprecated) - оставлен для обратной совместимости.

## Режимы работы

### Режим 1: Одиночный Raft кластер (без шардирования)

**Конфигурация:**
```yaml
raft:
  id: 1
  peers:
    - id: 1
      address: "http://localhost:8080"
    - id: 2
      address: "http://localhost:8081"
```

**Поведение:**
- Все данные хранятся в одном Raft кластере
- Leader принимает записи, реплицирует на followers
- Чтение возможно с любой ноды
- Нет шардирования

### Режим 2: Шардированный Raft кластер

**Конфигурация:**
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

**Поведение:**
- Данные распределяются между кластерами по consistent hashing
- Каждый кластер - независимая Raft группа
- Запись идет на leader соответствующего кластера
- Чтение с любой реплики соответствующего кластера
- Автоматическая маршрутизация между кластерами

## Потоки данных

### PUT операция в шардированном режиме

```
Client
  ↓
HTTP Server (любая нода)
  ↓
ShardedRaftDB.Put()
  ↓
Определение кластера по ключу (consistent hashing)
  ↓
Локальный кластер?
  ├─ Да → Raft Node.Execute() → Репликация через Raft → Store
  └─ Нет → HTTP Client → Удаленный кластер leader → их Raft → их Store
```

### GET операция в шардированном режиме

```
Client
  ↓
HTTP Server (любая нода)
  ↓
ShardedRaftDB.Get()
  ↓
Определение кластера по ключу (consistent hashing)
  ↓
Локальный кластер?
  ├─ Да → Чтение из локального Store
  └─ Нет → HTTP Client → Любая реплика удаленного кластера → их Store
```

## Преимущества решения

1. **Горизонтальное масштабирование**: Добавление новых Raft кластеров увеличивает емкость
2. **Высокая доступность**: Репликация внутри каждого кластера через Raft
3. **Согласованность**: Raft гарантирует линеаризуемость записей внутри кластера
4. **Простота**: Consistent hashing для распределения нагрузки
5. **Обратная совместимость**: Работает в обоих режимах (с шардированием и без)

## Удаленные компоненты

### Что было убрано:

1. **Дублирующая логика репликации** из `Router`:
   - Старые методы `Put()`, `Get()`, `Delete()` с ручной репликацией
   - Методы `replicasForKey()`, `SetAlive()`, `isAlive()`
   - Теперь репликация полностью на Raft

2. **Независимая логика ZooKeeper**:
   - Не удалена, но не используется в новой архитектуре
   - Может быть добавлена позже для service discovery

### Что осталось:

1. **HashRing** - используется для шардирования между кластерами
2. **Remote интерфейс** - для взаимодействия между кластерами
3. **Router** - помечен как deprecated, оставлен для совместимости

## Пример использования

### Запуск одиночного кластера

```bash
# Нода 1 (leader)
./bin/lsmdb --config=config/node1.yml

# Нода 2 (follower)
./bin/lsmdb --config=config/node2.yml

# Нода 3 (follower)
./bin/lsmdb --config=config/node3.yml
```

### Запуск шардированного кластера

```bash
# Cluster 1, Node 1
./bin/lsmdb --config=config/cluster1_node1.yml

# Cluster 1, Node 2
./bin/lsmdb --config=config/cluster1_node2.yml

# Cluster 2, Node 1
./bin/lsmdb --config=config/cluster2_node1.yml

# Cluster 2, Node 2
./bin/lsmdb --config=config/cluster2_node2.yml
```

### API запросы

```bash
# PUT - автоматически попадет в нужный кластер
curl -X PUT "http://localhost:8080/api/string" \
  -d "key=user:123&value=Alice"

# GET - автоматически маршрутизируется
curl "http://localhost:8080/api/string?key=user:123"

# DELETE
curl -X DELETE "http://localhost:8080/api?key=user:123"
```

## Дальнейшее развитие

1. **Service Discovery**: Интеграция с ZooKeeper/etcd для автоматического обнаружения кластеров
2. **Перебалансировка**: Автоматическое перераспределение данных при добавлении/удалении кластеров
3. **Мониторинг**: Метрики для каждого кластера и шардов
4. **Умная маршрутизация**: Учет нагрузки и latency при выборе реплики для чтения
5. **Кэширование**: Кэш топологии и метаданных кластеров

## Тестирование

Запуск тестов:
```bash
go test ./pkg/cluster/... -v
go test ./internal/http/... -v
```

## Отладка

Логи помогут понять, как работает маршрутизация:

```
[INFO] Starting in sharded mode local_cluster=cluster1 total_clusters=2
[INFO] cluster added to topology cluster_id=cluster1 leader=http://localhost:8080 replicas=2
[INFO] cluster added to topology cluster_id=cluster2 leader=http://localhost:9080 replicas=2
```

При каждой операции можно увидеть, в какой кластер идет запрос.

