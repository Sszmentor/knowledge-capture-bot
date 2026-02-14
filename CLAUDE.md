# Knowledge Capture Bot

## Статус
- **v1.1** — Telegram sync (2ч) + LMS sync (6ч) на Railway
- **URL**: https://agile-eagerness-production-3861.up.railway.app
- **Railway проект**: agile-eagerness

## Что делает
Автоматически собирает контент из двух источников и записывает в Obsidian vault через Dropbox API:
1. **Telegram**: сообщения из чатов курса AI Mindset (инкрементально)
2. **LMS**: данные сессий из learn.aimindset.org (парсинг JS бандла)

## Архитектура
```
Telegram MTProto → telegram_source.py → md_formatter → obsidian_writer → Dropbox → Obsidian
                       ↕                                                      ↕
LMS JS bundle → lms_source.py → lms_formatter.py ─────────────────────────────┘
                       ↕
                   state.json (в Dropbox — last_msg_id + content_hash)
```

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
- `GET /health` — Railway health check (telegram, dropbox, lms)
- `POST /sync` — полный синк (Telegram + LMS)
- `POST /sync/telegram` — только Telegram
- `POST /sync/lms` — только LMS
- `GET /status` — последний синк + state

## Environment Variables (Railway)
Credentials взяты из zoom-transcript-agent (Dropbox) и my.telegram.org (Telethon).
StringSession сгенерирована из aimindset_session.session.

## Что осталось сделать
- [x] LMS source (lms_source.py) — парсинг learn.aimindset.org
- [ ] Railway деплой LMS (образ собран, но контейнер не переключился автоматически)
- [ ] Cloud transcription на Railway (OpenAI Whisper API)
- [ ] Уведомления (по желанию)
- [ ] Универсализация для X26 (апрель 2026)

## Технические заметки
1. **Dropbox scope**: Нельзя вызывать `users_get_current_account()` — нет scope `account_info.read`.
2. **Telethon StringSession**: Работает на Railway без проблем.
3. **Инкрементальный синк TG**: `min_id` в `iter_messages()`.
4. **Форумные топики**: `GetForumTopicsRequest` из `telethon.tl.functions.messages`.
5. **LMS парсинг**: JS бандл содержит все данные сессий как JS object literals. Node.js eval надёжнее Python-парсера (шаблонные строки, одинарные кавычки в строках).
6. **nixpacks.toml**: `nodejs_20` для парсинга JS на Railway.
