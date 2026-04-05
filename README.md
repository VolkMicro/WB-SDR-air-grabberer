# WB SDR Air Grabberer

Лёгкий 24/7 SDR-monitoring стек под Wiren Board 8/8.5 и RTL-SDR Blog V4.

Что делает MVP:
- последовательно сканирует профили через `rtl_power`
- детектирует активность по энергии и адаптивным порогам
- режет ложные срабатывания по blacklist, carrier persistence, duty cycle и dedup
- хранит события в SQLite
- держит локальный spool для Telegram
- показывает состояние через лёгкий FastAPI dashboard
- удаляет старые события, снапшоты и спул старше 24 часов
- умеет делать synthetic authorized self-test с Telegram audio attachment

Что намеренно не делает:
- не декодирует чужой трафик
- не пересылает содержимое чужих переговоров
- не делает STT
- аудиовложения разрешены только в `AUTHORIZED_AUDIO_MODE=true` и только для профилей с `authorized_audio: true`
- захват и отправка FM-радио или любых сторонних голосовых эфиров остаётся `UNSUPPORTED`

## Quick Start

1. Скопировать `.env.example` в `.env` и заполнить Telegram параметры.
2. Проверить, что RTL-SDR виден на хосте: `rtl_test -t`.
3. При необходимости поправить `profiles.yaml` под свои разрешённые диапазоны.
4. Запустить: `docker compose up -d --build`
5. Открыть dashboard: `http://<wb-ip>:8080`
6. Прогнать self-test: `docker compose exec event-engine python -m app.selftest`

## Сервисы

- `sdr-core` — циклический скан диапазонов и создание кандидатов событий
- `event-engine` — suppression, dedup, retention, очередь уведомлений
- `telegram-gateway` — отправка очереди и бот-команды
- `dashboard` — web UI и health

## Deploy To Wiren Board

1. Заполнить `WB_DEPLOY_HOST` в `.env` или передать через окружение.
2. Запустить `bash deploy/deploy_wb.sh`.
3. Скрипт скопирует проект на контроллер и выполнит `docker compose up -d --build` на нём.

## Диагностика

- `docker compose ps`
- `docker compose logs -f sdr-core`
- `docker compose logs -f event-engine`
- `docker compose logs -f telegram-gateway`
- `docker compose exec event-engine python -m app.selftest`
- `sqlite3 data/airgrabber.db 'select id, profile_id, frequency_hz, signal_db, suppression_flags_json from events order by id desc limit 20;'`
- `curl http://127.0.0.1:8080/healthz`

