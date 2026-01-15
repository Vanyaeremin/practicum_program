# Команды для просмотра логов

## Общие команды Docker

### Просмотр логов всех сервисов
```bash
docker-compose logs
```

### Просмотр логов конкретного сервиса
```bash
docker-compose logs [имя_сервиса]
```

### Просмотр логов в реальном времени (follow)
```bash
docker-compose logs -f [имя_сервиса]
```

### Просмотр последних N строк логов
```bash
docker-compose logs --tail=50 [имя_сервиса]
```

### Просмотр логов с временными метками
```bash
docker-compose logs -t [имя_сервиса]
```

## Команды для конкретных сервисов

### API Checkout
```bash
# Все логи
docker-compose logs api_checkout

# Логи в реальном времени
docker-compose logs -f api_checkout

# Последние 100 строк
docker-compose logs --tail=100 api_checkout
```

### Inference Service
```bash
# Все логи
docker-compose logs inference

# Логи в реальном времени
docker-compose logs -f inference

# Последние 100 строк
docker-compose logs --tail=100 inference
```

### Outbox Worker
```bash
# Все логи
docker-compose logs outbox

# Логи в реальном времени
docker-compose logs -f outbox

# Последние 100 строк
docker-compose logs --tail=100 outbox
```

### Outbox S3 Worker
```bash
# Все логи
docker-compose logs outbox_s3

# Логи в реальном времени
docker-compose logs -f outbox_s3

# Последние 100 строк
docker-compose logs --tail=100 outbox_s3
```

### Runner Service
```bash
# Все логи
docker-compose logs runner

# Логи в реальном времени
docker-compose logs -f runner

# Последние 100 строк
docker-compose logs --tail=100 runner
```

## Полезные комбинации команд

### Просмотр логов нескольких сервисов одновременно
```bash
docker-compose logs -f api_checkout inference
```

### Просмотр логов всех сервисов в реальном времени
```bash
docker-compose logs -f
```

### Просмотр логов с фильтрацией по ключевым словам
```bash
docker-compose logs [имя_сервиса] | grep "ключевое_слово"
```

### Сохранение логов в файл
```bash
docker-compose logs [имя_сервиса] > logs_[имя_сервиса].txt
```

## Диагностика проблем

### Проверка статуса всех контейнеров
```bash
docker-compose ps
```

### Просмотр логов только для остановленных контейнеров
```bash
docker-compose logs --no-color
```

### Просмотр логов с информацией о выходе контейнера
```bash
docker-compose logs --no-log-prefix [имя_сервиса]
```

## Горячие клавиши при просмотре логов в реальном времени

- `Ctrl + C` - остановить просмотр логов в реальном времени
- `Ctrl + S` - приостановить вывод
- `Ctrl + Q` - возобновить вывод