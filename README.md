
# GW2 Raid/Event Discord Bot — Modular Refactor

A Discord bot for scheduling Guild Wars 2 events, handling sign-ups, automatically uploading Elite Insights logs to **dps.report**, and posting **analytics** summaries.

## ✨ Features

- **Event scheduling** 
- **Sign-ups** with **DM reminders** 15 minutes before start
- **Live log watcher**: scans `LOG_DIR` for `.zevtc`, `.evtc`, `.evtc.zip` during the event window
- **Reliable uploads** to **dps.report** 
- **Analytics enrichment** from EI JSON (downs, deaths, resurrects, boss DPS, mechanics)
- **End-of-event summary** (attempts, kills, total vs. wasted time)
- **Analytics embeds** (overall stats + per-encounter mechanics)

## Quick start
1. Copy your `.env` into this folder (or rename `.env.example`).
2. `pip install -r requirements.txt`
3. `python bot.py`

## Layout
- `bot.py` — tiny composition root
- `cogs/events_cog.py` — event creation, signups, reminders, start/end hooks
- `services/session.py` — per-event log watcher (uploads during the event window)
- `services/upload_service.py` — dps.report upload + persistent retry worker
- `repos/sqlite_repo.py` — SQLite tables + simple repos
- `ui/embeds.py` — end-of-event summary embed
- `ui/views.py` — buttons + modal
- `infra/scheduler.py` — global `AsyncIOScheduler`
- `analytics/` — your original `service.py` and `registry.py` kept intact

## Notes
- The pending uploads worker runs every minute inside `EventsCog`.
- Analytics enrich/embeds call your existing code.
- Add more cogs later (e.g., admin, manual ingest) without touching services.
