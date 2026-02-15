# Knowledge Capture Bot

## Статус
- **v1.3** — Telegram sync (2ч) + LMS sync (6ч) + Дайджесты (2 раза/день) + Topic Extractor
- **URL**: https://knowledge-capture-bot-production.up.railway.app
- **Railway проект**: 570f1bc7-101e-4e69-8894-a7b4ebfb9ff7

## Что делает
Автоматически собирает контент из двух источников и записывает в Obsidian vault через Dropbox API:
1. **Telegram**: сообщения из чатов курса AI Mindset (инкрементально)
2. **LMS**: данные сессий из learn.aimindset.org (парсинг JS бандла)
3. **Topic Extractor**: извлекает темы из чатов через Claude API
4. **Дайджесты** (v1.3): 2 раза в день (09:00 + 21:00 Новосибирск) через бота Tvorets — статистика + AI-инсайт

## Архитектура
```
Telegram MTProto → telegram_source.py → md_formatter → obsidian_writer → Dropbox → Obsidian
                       ↕                                                      ↕
LMS JS bundle → lms_source.py → lms_formatter.py ─────────────────────────────┘
                       ↕                    ↕
                   state.json          topic_extractor → Pipeline/topics/
                       ↕
                   notifier.py → Claude Haiku → Telegram Bot API → Tvorets бот → Сергей
```

## Дайджесты (v1.3)
- **notifier.py**: аккумуляторы собирают данные синков + topic extraction
- **Расписание**: 09:00 и 21:00 по Новосибирску (`zoneinfo.ZoneInfo("Asia/Novosibirsk")`)
- **Claude Haiku** (`claude-haiku-4-5-20251001`): анализирует активность, пишет инсайт
- **Доставка**: прямой POST к `api.telegram.org` через токен Tvorets бота
- **Логика**: если нет новых данных — дайджест не отправляется
- **Fallback**: если Claude API недоступен — дайджест без инсайта (только статистика)
- **Endpoint**: `POST /digest` — ручной запуск для тестирования

## LMS Source (v1.1)
- learn.aimindset.org — React SPA, контент вшит в JS бандл
- Парсинг: скачиваем index.html → находим bundle URL → скачиваем JS → извлекаем объекты → парсим через Node.js
- 23 сессии: ws00-04, at01-05, bonus01-04, oh01-04, fs01-04, fos18
- Инкрементальный sync через content hash (SHA256)
- Файлы в `20 Projects/AI_Mindset/W26 Winter 2026/`
- Требуется Node.js на Railway (nixpacks.toml)

## Источники (TG_SOURCES)
| Key | Chat | Type | Topics |
|-----|------|------|--------|
| w26_main | AI Mindset {w26} | forum | 6 (general, support, intro, materials/org, ПО, ИИ) |
| w26_adv | AI Mindset {w26} {adv} | chat | — |
| channel | AI Mindset канал | channel | — |

## Endpoints
- `GET /health` — Railway health check (telegram, dropbox, lms, pipeline, digest)
- `POST /sync` — полный синк (Telegram + LMS)
- `POST /sync/telegram` — только Telegram
- `POST /sync/lms` — только LMS
- `POST /digest` — ручной дайджест
- `GET /topics` — список извлечённых тем из Pipeline
- `POST /topics/extract` — ручная экстракция тем
- `GET /status` — последний синк + state

## Environment Variables (Railway)
Credentials взяты из zoom-transcript-agent (Dropbox) и my.telegram.org (Telethon).
StringSession создана 2026-02-15 (отдельная от local_export_session).

| Variable | Описание |
|----------|----------|
| TELEGRAM_API_ID | 37231632 |
| TELEGRAM_API_HASH | Telethon API hash |
| TELEGRAM_SESSION_STRING | StringSession для Railway |
| DROPBOX_APP_KEY | Dropbox app credentials |
| DROPBOX_APP_SECRET | |
| DROPBOX_REFRESH_TOKEN | |
| TG_SOURCES | JSON array источников |
| ANTHROPIC_API_KEY | Для topic extraction + дайджест инсайтов |
| TVORETS_BOT_TOKEN | Токен бота Tvorets для отправки дайджестов |

## Что осталось сделать
- [x] LMS source (lms_source.py) — парсинг learn.aimindset.org
- [x] Topic Extractor (v1.2) — извлечение тем из чатов
- [x] Дайджесты (v1.3) — уведомления через Tvorets бота
- [ ] Cloud transcription на Railway (OpenAI Whisper API)
- [ ] Универсализация для X26 (апрель 2026)

## Технические заметки
1. **Dropbox scope**: Нельзя вызывать `users_get_current_account()` — нет scope `account_info.read`.
2. **Telethon StringSession**: Работает на Railway без проблем. Нельзя использовать одну session с двух IP одновременно (AuthKeyDuplicatedError убивает session).
3. **Инкрементальный синк TG**: `min_id` в `iter_messages()`.
4. **Форумные топики**: `GetForumTopicsRequest` из `telethon.tl.functions.messages`.
5. **LMS парсинг**: JS бандл содержит все данные сессий как JS object literals. Node.js eval надёжнее Python-парсера (шаблонные строки, одинарные кавычки в строках).
6. **nixpacks.toml**: `nodejs_20` для парсинга JS на Railway.
7. **Дайджест модель**: `claude-haiku-4-5-20251001` — НЕ `claude-haiku-4-20250414` (такой модели нет).
8. **Telegram сессии**: Для KCB на Railway — отдельная StringSession. Для локального экспорта — `local_export_session.session` в `~/Library/CloudStorage/Dropbox/Приложения/AI_Agents/`.
