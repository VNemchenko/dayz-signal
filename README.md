# DayZ Signal

Узкий HTTP-шлюз для глобальных сообщений администратора в DayZ через BattlEye RCon. Telegram остаётся ответственностью n8n; этот сервис выполняет только команду `say -1 <message>`.

## Гарантии доставки

- Русский текст проходит NFKC-нормализацию и детерминированно транслитерируется в printable ASCII.
- Результат длиннее 160 символов отклоняется с `422`: сервис не обрезает и не разбивает сообщения молча.
- Очередь FIFO ограничена 50 командами, новая команда имеет TTL `1..300` секунд, rate limit по умолчанию — 30 команд в минуту с burst 5.
- Перед UDP-записью состояние `sending` синхронно сохраняется в JSONL-журнал. После рестарта такое состояние становится `delivery_unknown` и автоматически не повторяется.
- `acknowledged` означает ответ BattlEye на команду, но не подтверждает отображение сообщения каждым игровым клиентом. Server-message packet с code `2` корректно ACK-ается и не считается ответом на команду.
- Журнал ограничен количеством записей и размером, атомарно компактизируется. Текст терминальных записей после retention удаляется, SHA-256 payload остаётся для обнаружения повторов.

## Запуск

Требуется Node.js 24 или Docker. Скопируйте `.env.example` в `.env` и замените placeholder-значения. `API_KEY` должен содержать минимум 32 символа, `RCON_PASSWORD` — минимум 12 printable ASCII символов. Пустые, короткие и `change_me*`/`replace_with*` значения приводят к fail-closed остановке.

```bash
docker compose up --build -d
docker compose ps
```

По умолчанию HTTP-порты публикуются только на `127.0.0.1`. Для удалённого n8n используйте закрытую сеть/VPN или аутентифицированный reverse proxy.

Вместо inline-секретов можно создать локальные файлы в `.secrets/` и запустить дополнительный Compose-файл:

```bash
docker compose -f docker-compose.yml -f docker-compose.secrets.yml up --build -d
```

Каталог `.secrets/` исключён из Git и Docker build context. Файлы должны содержать реальные значения без placeholder-текста; сами файлы репозиторий не создаёт.

## API

Для рабочих запросов нужен `Authorization: Bearer <API_KEY>` либо `X-API-Key: <API_KEY>`.

### `POST /v1/broadcasts`

Заголовок `Idempotency-Key` обязателен и должен точно совпадать с `command_id`. `server_id` должен совпадать с настроенным `SERVER_ID`, канал v1 — только `global`.

```http
POST /v1/broadcasts
Idempotency-Key: cmd_delivery-018f4d2a
Content-Type: application/json
Authorization: Bearer <API_KEY>
```

```json
{
  "schema": "dayz.command.v1",
  "command_id": "cmd_delivery-018f4d2a",
  "server_id": "livonia-1",
  "created_at": "2026-08-17T12:00:00Z",
  "expires_at": "2026-08-17T12:05:00Z",
  "channel": "global",
  "message": "Сервер будет перезапущен через 10 минут",
  "metadata": {
    "event_id": "evt_0123456789abcdef",
    "policy_version": "v1"
  }
}
```

Успешно принятая новая или идемпотентно повторённая команда всегда получает `202` и `Location: /v1/broadcasts/<command_id>`; её фактическое состояние читается через GET. Вычисляется hash полного нормализованного envelope, поэтому повтор того же `command_id` с любым изменённым полем получает `409 idempotency_conflict`. `created_at` и `expires_at` должны быть RFC3339 UTC (`Z`), а их разница — `1..300` секунд. Просроченная новая команда фиксируется как `expired` и не отправляется; слишком далёкий будущий `created_at` отклоняется.

### `GET /v1/broadcasts/:command_id`

Для найденной команды всегда возвращает HTTP `200` и одно из состояний: `queued`, `sending`, `acknowledged`, `delivery_unknown`, `expired`, `failed`. Неизвестный идентификатор получает `404`.

### `POST /broadcast`

Совместимый маршрут со старым телом:

```json
{ "message": "Restart in 10 minutes" }
```

Он также принимает прежние необязательные `request_id` и `ttl_seconds`, ждёт RCon-ответ в пределах `HTTP_WAIT_MS` и сохраняет поля ответа `ok`, `service`, `command`, `response`.

### Health endpoints

- `GET /livez` — процесс принимает HTTP;
- `GET /readyz` — `200` только при подключённом RCon, исправном журнале и доступной очереди;
- `GET /health` — безопасное состояние RCon, очереди и журнала без паролей, адреса журнала и текста сообщений.

## Основные настройки

- `SERVER_ID=livonia-1`;
- `JOURNAL_PATH=./data/broadcasts.jsonl` (в Compose `/data/broadcasts.jsonl`);
- `QUEUE_CAPACITY=50`;
- `BROADCAST_TTL_SECONDS=30`, `BROADCAST_MAX_TTL_SECONDS=300`, `COMMAND_MAX_FUTURE_SKEW_SECONDS=30`;
- `RATE_LIMIT_PER_MINUTE=30`, `RATE_LIMIT_BURST=5`;
- `IDEMPOTENCY_RETENTION_MS=86400000`, `IDEMPOTENCY_MAX_RECORDS=10000`;
- `JOURNAL_MAX_BYTES=16777216`;
- `STARTUP_TIMEOUT_MS=15000`, `SHUTDOWN_GRACE_MS=10000`;
- `RCON_COMMAND_TIMEOUT_MS=7000`;
- `RCON_RECONNECT_BASE_MS=1000`, `RCON_RECONNECT_MAX_MS=30000`.

`API_KEY_FILE` и `RCON_PASSWORD_FILE` поддерживаются вместо inline-переменных. Одновременно задавать значение и соответствующий `_FILE` нельзя.

## Проверка

```bash
npm ci
npm test
npm run check
npm audit --omit=dev
docker compose --env-file .env.example config --quiet
docker build -t dayz-signal:test .
```

Автотесты используют только локальный UDP RCon emulator и не обращаются к игровым серверам, webhook или Telegram.
