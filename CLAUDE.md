# Knowledge Capture Bot

## Статус
- **v1.0** — работает на Railway, Telegram sync каждые 2 часа
- **URL**: https://agile-eagerness-production-3861.up.railway.app
- **Railway проект**: agile-eagerness

## Что делает
Автоматически собирает сообщения из Telegram чатов курса AI Mindset и записывает в Obsidian vault через Dropbox API.

## Архитектура
```
Telegram MTProto → telegram_source.py → md_formatter → obsidian_writer → Dropbox → Obsidian
                       ↕                                                      ↕
                   state.json (в Dropbox — last_msg_id per source)
```

## Источники (TG_SOURCES)
| Key | Chat | Type | Topics |
|-----|------|------|--------|
| w26_main | AI Mindset {w26} | forum | 6 (general, support, intro, materials/org, ПО, ИИ) |
| w26_adv | AI Mindset {w26} {adv} | chat | — |
| channel | AI Mindset канал | channel | — |

## Endpoints
- `GET /health` — Railway health check
- `POST /sync` — ручной запуск синка
- `GET /status` — последний синк + state

## Environment Variables (Railway)
Credentials взяты из zoom-transcript-agent (Dropbox) и my.telegram.org (Telethon).
StringSession сгенерирована из aimindset_session.session.

## Что осталось сделать
- [ ] LMS source (lms_source.py) — scraping learn.aimindset.org
- [ ] Cloud transcription на Railway (OpenAI Whisper API)
- [ ] Уведомления (по желанию)
- [ ] Универсализация для X26 (апрель 2026)

## Технические заметки
1. **Dropbox scope**: Нельзя вызывать `users_get_current_account()` — нет scope `account_info.read`. Используем `files_list_folder` для проверки подключения.
2. **Telethon StringSession**: Сгенерирована из файловой сессии `aimindset_session.session`, работает на Railway без проблем.
3. **Инкрементальный синк**: `min_id` в `iter_messages()` — получаем только новые сообщения.
4. **Форумные топики**: `GetForumTopicsRequest` из `telethon.tl.functions.messages` (НЕ channels!), `peer=` (НЕ channel!).
