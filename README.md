# Асинхронный сервис процессинга платежей

Микросервис принимает запросы на оплату, ставит их в очередь, обрабатывает через эмуляцию
платёжного шлюза и уведомляет клиента о результате по webhook.

**Стек:** FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ (FastStream) · Alembic · Docker.

## Архитектура

```
 Client ──POST /api/v1/payments──► API (FastAPI)
                                    │  одна транзакция:
                                    │  INSERT payments + INSERT outbox
                                    ▼
                               PostgreSQL ◄───────────────┐
                                    │                      │ UPDATE status
                     Outbox relay   │ (фоновая задача API) │
                SELECT ... FOR UPDATE SKIP LOCKED          │
                                    ▼                      │
                    RabbitMQ: exchange payments ──► payments.new ──► Consumer
                                                      ▲   │              │
                                      TTL-очереди     │   │ ошибка       │ POST webhook
                                   payments.new.retry.N◄──┘              ▼
                                                      │ 3-я ошибка     Client
                                                      ▼
                                              payments.dlq (через payments.dlx)
```

### Поток обработки

1. `POST /api/v1/payments` в **одной транзакции** сохраняет платёж (`pending`) и событие в таблицу `outbox`.
   Клиент получает `202 Accepted`.
2. **Outbox relay** (фоновая задача в процессе API) вычитывает неопубликованные события и публикует их
   в `payments.new` с подтверждением брокера (publisher confirms), после чего ставит `published_at`.
3. **Consumer** получает сообщение, эмулирует шлюз (2-5 с, 90% успех / 10% отказ), обновляет статус
   в БД и отправляет webhook.
4. При ошибке (например, webhook-приёмник недоступен) сообщение уходит на повтор с экспоненциальной
   задержкой; после 3-й неудачной попытки оказывается в **Dead Letter Queue**.

## Запуск

```bash
docker compose up --build
```

Поднимутся `postgres`, `rabbitmq`, `api` (миграции применяются автоматически при старте), `consumer`.

| Сервис               | Адрес                                   |
|----------------------|-----------------------------------------|
| API                  | http://localhost:8000                   |
| Swagger UI           | http://localhost:8000/docs              |
| RabbitMQ Management  | http://localhost:15672 (guest / guest)  |

API-ключ по умолчанию: `secret-api-key` (переменная `API_KEY`).

Для проверки webhook'ов можно поднять встроенный приёмник (профиль `demo`):

```bash
docker compose --profile demo up --build
docker compose logs -f webhook-mock consumer
```

## Примеры

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-123" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Оплата заказа №123",
        "metadata": {"order_id": 123, "customer": "ivan@example.com"},
        "webhook_url": "http://webhook-mock:9000/hook"
      }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "7b0c1b6e-5b0a-4f3e-9a49-3f0b2f6a1c11",
  "status": "pending",
  "created_at": "2026-10-14T10:00:00.123456Z"
}
```

Повтор запроса с тем же `Idempotency-Key` и тем же телом вернёт **тот же** `payment_id` и не создаст дубль.
Тот же ключ с другим телом -> `409 Conflict`.

### Получение платежа

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: secret-api-key"
```

Через 2-5 секунд статус станет `succeeded` или `failed`, появится `processed_at`.

### Формат webhook

```json
{
  "event": "payment.succeeded",
  "payment_id": "7b0c1b6e-5b0a-4f3e-9a49-3f0b2f6a1c11",
  "status": "succeeded",
  "amount": "1500.50",
  "currency": "RUB",
  "description": "Оплата заказа №123",
  "metadata": {"order_id": 123, "customer": "ivan@example.com"},
  "created_at": "2026-10-14T10:00:00.123456+00:00",
  "processed_at": "2026-10-14T10:00:03.456789+00:00"
}
```

Успехом считается любой ответ 2xx.

### Проверка retry и DLQ

Укажите `webhook_url` на эндпоинт, который всегда отвечает 500:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" -H "Idempotency-Key: dlq-test-1" \
  -H "Content-Type: application/json" \
  -d '{"amount":"10","currency":"USD","description":"dlq test","webhook_url":"http://webhook-mock:9000/fail"}'
```

В логах consumer будут попытки 1/3, 2/3 (через 5 с), 3/3 (ещё через 10 с), после чего сообщение
появится в очереди `payments.dlq` (видно в RabbitMQ Management -> Queues, заголовок `x-last-error`
содержит причину). Статус самого платежа при этом уже финальный (`succeeded`/`failed`) - не доставлен
только webhook.

## Ключевые решения

**Outbox pattern.** Платёж и событие пишутся в одной транзакции, поэтому нельзя получить «платёж без
события» или наоборот. Relay публикует с publisher confirms и помечает событие только после подтверждения;
`FOR UPDATE SKIP LOCKED` позволяет безопасно запускать несколько реплик API. Гарантия доставки -
*at-least-once*; возможный дубль сообщения безвреден, т.к. consumer идемпотентен.

**Идемпотентность.**
- API: `Idempotency-Key` + `UNIQUE`-ограничение в БД. Гонка двух одновременных запросов разрешается
  через перехват `IntegrityError` и возврат уже созданного платежа.
- Consumer: платёж отправляется в «шлюз» только если он ещё `pending`; статус меняется через
  compare-and-set (`UPDATE ... WHERE status = 'pending'`). Поле `webhook_delivered_at` не даёт
  отправить webhook повторно, если он уже доставлен (например, при повторной доставке сообщения).

**Retry и DLQ.** Топология (`app/messaging.py`):
- `payments` (direct) -> `payments.new`; у очереди настроен DLX `payments.dlx` -> `payments.dlq`
  (страховка на случай reject).
- `payments.retry` (direct) -> `payments.new.retry.1`, `payments.new.retry.2` с `x-message-ttl`
  = `base * 2^(n-1)` (по умолчанию 5 с и 10 с). По истечении TTL сообщение через dead-letter
  возвращается в `payments.new`. TTL задан на очередь, а не на сообщение, чтобы не было блокировки
  головы очереди.
- Номер попытки хранится в заголовке `x-attempt`. На попытке `MAX_ATTEMPTS` (3) сообщение
  публикуется в `payments.dlx` -> `payments.dlq`.

Что считается ошибкой: недоступность БД, не-2xx/таймаут при отправке webhook и т.п.
Отказ платёжного шлюза (10%) - это **штатный результат** (`failed`), а не ошибка обработки: статус
фиксируется и клиенту уходит webhook `payment.failed`.

## Конфигурация

| Переменная                     | По умолчанию | Описание                                          |
|--------------------------------|--------------|---------------------------------------------------|
| `DATABASE_URL`                 | см. `.env.example` | DSN PostgreSQL (`postgresql+asyncpg://...`) |
| `RABBITMQ_URL`                 | см. `.env.example` | DSN RabbitMQ                                |
| `API_KEY`                      | `secret-api-key` | Значение заголовка `X-API-Key`                |
| `MAX_ATTEMPTS`                 | `3`          | Всего попыток обработки                           |
| `RETRY_BASE_DELAY_SECONDS`     | `5`          | Базовая задержка экспоненциального retry          |
| `WEBHOOK_TIMEOUT_SECONDS`      | `10`         | Таймаут HTTP-запроса webhook'а                    |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1`          | Период опроса outbox                              |
| `OUTBOX_BATCH_SIZE`            | `100`        | Размер пачки relay                                |

> RabbitMQ не позволяет менять аргументы существующей очереди. Если измените `MAX_ATTEMPTS` или
> `RETRY_BASE_DELAY_SECONDS` после первого запуска, удалите retry-очереди (или том:
> `docker compose down -v`).

## Локальная разработка без Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# нужны запущенные PostgreSQL и RabbitMQ
alembic upgrade head
uvicorn app.main:app --reload          # API + outbox relay
faststream run app.consumer:app        # consumer (в другом терминале)
```

## Структура проекта

```
app/
  main.py         FastAPI, lifespan (топология, outbox relay)
  api/            эндпоинты и проверка X-API-Key
  models.py       Payment, OutboxEvent
  schemas.py      Pydantic v2 схемы
  messaging.py    топология RabbitMQ, публикация, retry/DLQ
  outbox.py       relay: outbox -> RabbitMQ
  consumer.py     FastStream-обработчик payments.new
  processing.py   идемпотентная обработка платежа
  gateway.py      эмуляция платёжного шлюза
  webhook.py      доставка webhook'ов
alembic/          миграции (таблицы payments и outbox)
tools/            тестовый приёмник webhook'ов
```

