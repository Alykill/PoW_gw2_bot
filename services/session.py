
from __future__ import annotations
import os, asyncio, datetime, logging, aiosqlite
from typing import Optional, List
from .upload_service import upload_to_dps_report, enqueue_pending_upload
from analytics.service import enrich_upload
from config import settings
log = logging.getLogger("session")

class EventSession:
    def __init__(self, event_name: str, start: datetime.datetime, end: datetime.datetime, channel_id: int, log_dir: str):
        self.event_name = event_name
        self.start = start
        self.end = end
        self.channel_id = channel_id
        self.log_dir = log_dir

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

        self.seen: set[str] = set()            # uploaded (or permanently skipped) files
        self.results: List[dict] = []          # successful upload JSONs

        self._size_cache: dict[str, int] = {}  # last observed size
        self._last_seen: dict[str, datetime.datetime] = {}  # when size last changed
        self.retry_state: dict[str, dict] = {} # upload backoff per file

        self.max_attempts = 5
        self.base_backoff = 8
        self.concurrent_sema = asyncio.Semaphore(2)

        self.grace_minutes = settings.EVENT_GRACE_MINUTES
        self.settle_seconds = settings.FILE_SETTLE_SECONDS

    def start_task(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop_task(self):
        if self._task is not None:
            self._stop.set()
            try:
                await self._task
            except Exception:
                pass
            self._task = None

    def _within_event_window(self, mtime_utc: datetime.datetime) -> bool:
        return (mtime_utc >= self.start - datetime.timedelta(minutes=5)
                and mtime_utc <= self.end + datetime.timedelta(minutes=15))

    async def _try_upload(self, full: str):
        try:
            async with self.concurrent_sema:
                result = await asyncio.wait_for(upload_to_dps_report(full), timeout=95)
        except asyncio.TimeoutError:
            result = None
        if result:
            self.results.append(result)
            self.seen.add(full)
            self.retry_state.pop(full, None)
            try:
                async with aiosqlite.connect(settings.SQLITE_PATH) as db:
                    cur = await db.execute(
                        "INSERT OR IGNORE INTO uploads(event_name,file_path,permalink,boss_id,boss_name,success,time_utc) VALUES (?,?,?,?,?,?,?)",
                        (self.event_name, full, result.get("permalink",""),
                         int(result.get("encounter",{}).get("bossId") or result.get("bossId") or -1),
                         str(result.get("encounter",{}).get("boss") or result.get("boss") or ""),
                         1 if (result.get("encounter",{}).get("success") or result.get("success")) else 0,
                         datetime.datetime.now(datetime.timezone.utc).isoformat()))
                    await db.commit()
                    upload_id = cur.lastrowid
                    if not upload_id:
                        cur = await db.execute("SELECT id FROM uploads WHERE file_path=?", (full,))
                        row = await cur.fetchone()
                        upload_id = row[0] if row else None
                if upload_id:
                    await enrich_upload(upload_id, result)
            except Exception:
                pass
            return True
        else:
            st = self.retry_state.get(full, {"attempts": 0})
            st["attempts"] = st.get("attempts", 0) + 1
            if st["attempts"] >= self.max_attempts:
                self.seen.add(full)
                self.retry_state.pop(full, None)
                await enqueue_pending_upload(full, self.event_name, self.channel_id, 0, 60, "session max attempts reached")
            else:
                self.retry_state[full] = st
            return False

    async def _run(self):
        poll_seconds = 5
        suffixes = (".zevtc", ".evtc", ".evtc.zip")

        # seed seen with any existing files so we don't reprocess out-of-window
        if os.path.isdir(self.log_dir):
            for root, _dirs, files in os.walk(self.log_dir):
                for fn in files:
                    if fn.lower().endswith(suffixes):
                        self.seen.add(os.path.join(root, fn))

        while not self._stop.is_set():
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            # extend scanning until grace window expires
            if now_utc >= self.end + datetime.timedelta(minutes=self.grace_minutes):
                break
            try:
                if os.path.isdir(self.log_dir):
                    for root, _dirs, files in os.walk(self.log_dir):
                        for fn in files:
                            if not fn.lower().endswith(suffixes):
                                continue
                            full = os.path.join(root, fn)

                            # window filter
                            try:
                                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(full), tz=datetime.timezone.utc)
                            except Exception:
                                continue
                            if not self._within_event_window(mtime):
                                self.seen.add(full)
                                self.retry_state.pop(full, None)
                                continue

                            # stability check: unchanged for N seconds
                            size_now = os.path.getsize(full)
                            prev = self._size_cache.get(full)
                            if prev is None or prev != size_now:
                                self._size_cache[full] = size_now
                                self._last_seen[full] = now_utc
                                continue
                            settled_for = (now_utc - self._last_seen.get(full, now_utc)).total_seconds()
                            if settled_for < self.settle_seconds:
                                continue

                            if full in self.seen and full not in self.retry_state:
                                # already handled
                                continue

                            # try to upload
                            await self._try_upload(full)

            except Exception:
                pass

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                pass

        # FINAL SWEEP: anything in-window not uploaded → enqueue to persistent queue
        try:
            candidates = []
            if os.path.isdir(self.log_dir):
                for root, _dirs, files in os.walk(self.log_dir):
                    for fn in files:
                        if not fn.lower().endswith(suffixes):
                            continue
                        full = os.path.join(root, fn)
                        if full in self.seen:
                            continue
                        try:
                            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(full), tz=datetime.timezone.utc)
                        except Exception:
                            continue
                        if self._within_event_window(mtime):
                            candidates.append(full)
            for full in candidates:
                await enqueue_pending_upload(full, self.event_name, self.channel_id, 0, 60, "final sweep at session end")
        except Exception:
            pass
