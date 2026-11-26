# ImageProcessor

ImageProcessor — сервис на Go для асинхронной обработки изображений JPEG и PNG. Поддерживает изменение размеров, создание миниатюр и наложение водяных знаков с сохранением оригинального изображения.

## Возможности

- Изменение размеров с сохранением пропорций, без обрезки и увеличения оригинала.
- Квадратные миниатюры с обрезкой по центру и возможностью увеличения.
- Полупрозрачная надпись `ImageProcessor` с тенью в правом нижнем углу.
- Несколько разных операций для одного изображения.
- Сохранение формата изображения и прозрачности PNG.
- Автоматическая коррекция ориентации изображения по метаданным EXIF.
- Сохранение результатов JPEG с качеством кодирования 90 по шкале от 1 до 100.
- Метаданные, статусы обработки, список с курсорной пагинацией.
- Просмотр и скачивание оригиналов и результатов.
- Удаление во время обработки с фоновой очисткой файлов.

Ограничения: входной файл — до 20 MiB, результат — до 128 MiB, изображение — до 25 MP и 10 000 px по стороне. Сторона миниатюры — 1–2048 px.

## Стек

- Go 1.27, `net/http`, `log/slog`.
- PostgreSQL 18, `pgx/v5`, `pgxpool`.
- Goose.
- Apache Kafka в режиме KRaft, franz-go.
- MinIO, minio-go/v7.
- `golang.org/x/image`.
- YAML, caarlos0/env.
- HTML, CSS, Vanilla JavaScript.
- Docker Compose, Make.

## Структура проекта

```text
cmd/
  server/           # Точка входа HTTP-сервера и команды миграций
  worker/           # Точка входа worker
internal/
  app/              # Сборка зависимостей и жизненный цикл
  config/           # Загрузка и валидация конфигурации
  domain/           # Сущности, операции и статусы
  logger/           # Настройка slog
  delivery/
    http/           # Router, handlers, DTO и middleware
    kafka/          # Consumer loop и worker pool
  service/
    image/          # Загрузка, чтение, удаление и cleanup
    processor/      # Движок обработки, задания, retry и recovery
    outbox/         # Публикация событий
  repository/
    postgres/       # SQL-репозитории и транзакции
  infra/            # Клиенты PostgreSQL, Kafka и MinIO
configs/            # YAML-конфигурация
migrations/         # SQL-миграции Goose
web/                # HTML, CSS и JavaScript
tests/integration/ # Изолированное окружение и интеграционные тесты
Dockerfile          # Сборка server и worker
Dockerfile.minio    # Сборка MinIO из закреплённого исходного кода
docker-compose.yml # Полный стек приложения
.dockerignore       # Состав Docker build context
.env.example        # Демонстрационные настройки окружения
.gitignore          # Исключения Git
Makefile            # Команды разработки
go.mod, go.sum     # Go-модуль и зависимости
```

## Запуск

Необходимы Git, Docker Engine с Compose v2 и Buildx, Make. Для сборки и тестов вне контейнеров нужен Go 1.27 или новее.

```sh
git clone https://github.com/akhmed9505/image-processor.git
cd image-processor
cp .env.example .env
```

Задайте в `.env` собственные `POSTGRES_USER`, `POSTGRES_PASSWORD`, `MINIO_ACCESS_KEY` и `MINIO_SECRET_KEY` вместо демонстрационных значений. Соберите и запустите приложение:

```sh
make up
```

Веб-интерфейс и API доступны по адресу [http://localhost:8080](http://localhost:8080).

Остановка приложения:

```sh
make down
```

## Конфигурация

ENV имеет приоритет над `configs/config.yml`. Compose читает `.env` и задаёт внутренние адреса сервисов. Данные доступа передаются только через ENV.

Основные переменные для запуска через Compose:

| Переменная | Назначение | По умолчанию |
|---|---|---|
| `HTTP_PORT` | Порт веб-интерфейса и API на хосте | `8080` |
| `POSTGRES_DB` | Имя базы | Требуется; в `.env.example` — `imageprocessor` |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | Пользователь и пароль PostgreSQL | Требуются |
| `POSTGRES_PORT` | Порт PostgreSQL на хосте | `55433` |
| `MINIO_BUCKET` | Приватный бакет | `images` |
| `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Ключи доступа MinIO | Требуются |
| `MINIO_PORT`, `MINIO_CONSOLE_PORT` | Порты MinIO API и консоли на хосте | `9000`, `9001` |
| `KAFKA_PORT` | Порт Kafka на хосте | `29092` |
| `LOG_LEVEL`, `LOG_FORMAT` | Уровень и формат логов | `info`, `json` |

Остальные настройки доступны в `.env.example` и `configs/config.yml`.

## API

| Метод и путь | Назначение | Успешный ответ |
|---|---|---|
| `POST /upload` | Загрузка файла и операций: multipart-поля `file` и `operations` | `202`, метаданные и задания |
| `GET /images?limit=20&cursor=…` | Список изображений, от новых к старым | `200`, `images` и необязательный `next_cursor` |
| `GET /image/{id}/metadata` | Метаданные и статусы заданий | `200`, JSON |
| `GET /image/{id}/status` | Метаданные с общим статусом обработки | `200`, JSON |
| `GET /image/{id}` | Оригинал; с `?operation=resize`, `thumbnail` или `watermark` — результат | `200`, JPEG/PNG |
| `GET /image/{id}/original` | Получение оригинала | `200`, JPEG/PNG |
| `DELETE /image/{id}` | Логическое удаление; повторное удаление того же изображения безопасно | `202`, `id` и `status: deleted` |

- `id` — UUID из ответа загрузки.
- `limit` — 1–100, по умолчанию 20. Для следующей страницы передайте полученный `next_cursor` в `cursor`.
- `width` и `height` — максимальные ширина и высота для `resize`, целые числа 1–10 000.
- `size` — сторона квадратной миниатюры для `thumbnail`, 1–2048. У `watermark` параметров нет.
- Каждый тип операции можно выбрать один раз. Размер тела запроса — до 21 MiB, поля `operations` — до 4 KiB.
- Общий статус: `pending`, `processing`, `completed`, `partial`, `failed`. Статусы заданий также включают `retrying` и `cancelled`.

**Загрузка изображения**

```sh
curl -sS http://localhost:8080/upload \
  -F 'file=@./photo.png' \
  -F 'operations=[{"type":"resize","width":1200,"height":800},{"type":"thumbnail","size":256},{"type":"watermark"}]'
```

Ожидается HTTP `202 Accepted`. Фрагмент ответа:

```json
{
  "id": "7b65a090-6135-4ccc-a55f-b43edc928ea0",
  "status": "pending"
}
```

**Получение статуса**

Подставьте `id` из ответа загрузки:

```sh
IMAGE_ID='<id из ответа загрузки>'
curl -sS "http://localhost:8080/image/$IMAGE_ID/status"
```

Ожидается HTTP `200 OK`. Фрагмент ответа после завершения обработки:

```json
{
  "id": "7b65a090-6135-4ccc-a55f-b43edc928ea0",
  "status": "completed"
}
```

**Список изображений**

```sh
curl -sS 'http://localhost:8080/images?limit=10'
```

Ожидается HTTP `200 OK`. Если изображений нет:

```json
{
  "images": []
}
```

**Скачивание оригинала**

```sh
curl -fS "http://localhost:8080/image/$IMAGE_ID/original" -o original.png
```

Ожидается HTTP `200 OK`, файл PNG для загруженного `photo.png`.

**Скачивание результата**

```sh
curl -fS "http://localhost:8080/image/$IMAGE_ID?operation=thumbnail" -o thumbnail.png
```

После завершения операции ожидается HTTP `200 OK`, файл PNG. Пока результат не готов — HTTP `409 Conflict`.

**Удаление изображения**

```sh
curl -sS -X DELETE "http://localhost:8080/image/$IMAGE_ID"
```

Ожидается HTTP `202 Accepted`:

```json
{
  "id": "7b65a090-6135-4ccc-a55f-b43edc928ea0",
  "status": "deleted"
}
```

## Makefile

| Команда | Назначение |
|---|---|
| `make build` | Сборка `bin/server` и `bin/worker` |
| `make vet` | Статический анализ Go-кода |
| `make test` | Обычные тесты |
| `make test-race` | Тесты с race detector |
| `make test-integration` | Интеграционные тесты с race detector в изолированном окружении |
| `make test-compose` | Сборка образов и Compose E2E |
| `make up` | Сборка и запуск стека |
| `make down` | Остановка стека |
| `make migrate` | Применение миграций через Compose |
