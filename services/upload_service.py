
from __future__ import annotations
import os, json, asyncio, datetime, logging, aiohttp, aiosqlite
from typing import Optional
from config import settings
log = logging.getLogger("uploads")
_PENDING_SEMA = asyncio.Semaphore(settings.PENDING_CONCURRENCY)

async def upload_to_dps_report(file_path: str) -> Optional[dict]:
    url = "https://dps.report/uploadContent?json=1"
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            with open(file_path, "rb") as f:
                data = aiohttp.FormData()
                data.add_field("file", f, filename=os.path.basename(file_path), content_type="application/octet-stream")
                async with session.post(url, data=data) as resp:
                    status = resp.status
                    body = await resp.read()
                    if status == 429 or status >= 500 or status != 200:
                        return None
                    try:
                        return json.loads(body.decode(errors="ignore"))
                    except Exception:
                        return None
    except Exception:
        return None

async def enqueue_pending_upload(file_path: str, event_name: str, channel_id: int, attempts: int, delay_seconds: int, err: str = ""):
    now = datetime.datetime.now(datetime.timezone.utc)
    next_retry = now + datetime.timedelta(seconds=delay_seconds)
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        await db.execute(
            "INSERT INTO pending_uploads (file_path,event_name,channel_id,attempts,next_retry,last_error,created_utc) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(file_path) DO UPDATE SET attempts=excluded.attempts, next_retry=excluded.next_retry, last_error=excluded.last_error",
            (file_path, event_name, channel_id, attempts, next_retry.isoformat(), err, now.isoformat()))
        await db.commit()

async def _delete_pending(id_: int):
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        await db.execute("DELETE FROM pending_uploads WHERE id = ?", (id_,))
        await db.commit()

async def _update_pending_retry(id_: int, attempts: int, delay_seconds: int, err: str):
    next_retry = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=delay_seconds)).isoformat()
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        await db.execute("UPDATE pending_uploads SET attempts=?, next_retry=?, last_error=? WHERE id=?",
                         (attempts, next_retry, err, id_))
        await db.commit()

async def _process_one_pending(row):
    from analytics.service import enrich_upload  # lazy import to avoid cycles
    id_, file_path, event_name, channel_id, attempts = row
    try:
        if not os.path.exists(file_path):
            await _delete_pending(id_); return
        async with _PENDING_SEMA:
            try:
                result = await asyncio.wait_for(upload_to_dps_report(file_path), timeout=95)
            except asyncio.TimeoutError:
                result = None
        if result:
            upload_id = None
            try:
                async with aiosqlite.connect(settings.SQLITE_PATH) as db:
                    cur = await db.execute(
                        "INSERT OR IGNORE INTO uploads (event_name,file_path,permalink,boss_id,boss_name,success,time_utc) VALUES (?,?,?,?,?,?,?)",
                        (event_name, file_path, result.get("permalink",""),
                         int(result.get("encounter",{}).get("bossId") or result.get("bossId") or -1),
                         str(result.get("encounter",{}).get("boss") or result.get("boss") or ""),
                         1 if (result.get("encounter",{}).get("success") or result.get("success")) else 0,
                         datetime.datetime.now(datetime.timezone.utc).isoformat()))
                    await db.commit()
                    if cur.lastrowid:
                        upload_id = cur.lastrowid
                    else:
                        cur = await db.execute("SELECT id FROM uploads WHERE file_path=?", (file_path,))
                        r = await cur.fetchone()
                        upload_id = r[0] if r else None
            except Exception:
                upload_id = None
            if upload_id:
                try:
                    await enrich_upload(upload_id, result)
                except Exception:
                    pass
            await _delete_pending(id_)
        else:
            attempts += 1
            if attempts >= settings.PENDING_MAX_ATTEMPTS:
                await _delete_pending(id_)
            else:
                backoff = settings.PENDING_BASE_BACKOFF * (2 ** (attempts - 1))
                await _update_pending_retry(id_, attempts, backoff, "upload failed")
    except Exception:
        pass

async def process_pending_uploads():
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = []
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        cur = await db.execute("SELECT id,file_path,event_name,channel_id,attempts FROM pending_uploads WHERE next_retry <= ?", (now_iso,))
        rows = await cur.fetchall()
    if not rows:
        return
    await asyncio.gather(*[_process_one_pending(r) for r in rows], return_exceptions=True)
