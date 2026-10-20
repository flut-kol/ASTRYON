# ASTRYON

Розподілена платформа даних. Джерело істини - `DEVELOPMENT_PLAN.md`; автентифікація - `docs/architecture/authentication.md`.

## Швидкий старт (dev)

```bash
cp .env.example .env     # опційно, є дефолти
make dev-up              # Kafka, PostgreSQL, ClickHouse, MinIO (чекає на healthy)
make topics              # створює topics з infra/kafka/topics.yaml
make migrate             # викликає db/apply.sh, якщо існує
make test                # go vet + go test для всіх Go-модулів
make dev-down            # зупинка (scripts/dev/down.sh --volumes видаляє дані)
```

## Порти (лише 127.0.0.1)

| Сервіс | Порт | Примітка |
|---|---|---|
| Kafka (KRaft, 1 вузол) | 19092 | у мережі compose: `kafka:9092` |
| PostgreSQL 16 | 15432 | |
| ClickHouse HTTP / native | 18124 / 19010 | |
| MinIO S3 / консоль | 19200 / 19201 | S3 emulator |

Порти змінюються через `.env`. Креди - лише dev-плейсхолдери.

## Структура

- `infra/docker` - docker-compose для dev; `infra/kafka/topics.yaml` - topics-as-code (§5.2; у dev менше партицій і RF=1, у prod RF=3)
- `db/` - міграції PostgreSQL/ClickHouse, seeds
- `libs/`, `services/` - Go-модулі (`go.work`)
- `scripts/dev` - up/down/create-topics
- `tests/`, `docs/`
