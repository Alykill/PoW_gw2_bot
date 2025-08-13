import discord
from discord.ext import commands
from discord import app_commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiosqlite
import aiohttp
import asyncio
import datetime
import os
import re
from typing import Dict, List, Optional, Tuple
import logging
from dotenv import load_dotenv
import json

# ✅ Analytics imports
from analytics.service import (
    enrich_upload,                 # <-- added
    ensure_enriched_for_event,
    build_event_analytics_embeds,
)

load_dotenv()

# === Persistent retry config ===
PENDING_SCAN_MIN = int(os.getenv("PENDING_SCAN_MIN", "5"))          # scan every N minutes
PENDING_MAX_ATTEMPTS = int(os.getenv("PENDING_MAX_ATTEMPTS", "12")) # total retries before giving up
PENDING_BASE_BACKOFF = int(os.getenv("PENDING_BASE_BACKOFF", "60"))  # seconds, exponential backoff
PENDING_CONCURRENCY = int(os.getenv("PENDING_CONCURRENCY", "2"))     # parallel uploads in background worker

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("raidbot")
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "725430860421136404"))  # allow overriding via .env
LOG_DIR = os.getenv("LOG_DIR", "").strip()

# === Set up bot ===
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
scheduler = AsyncIOScheduler(timezone=datetime.timezone.utc)

# ---------- Helpers ----------
def safe_event_name(name: str) -> str:
    return re.sub(r"\s+", "_", name.strip())

def reminder_job_id(event_name: str, user_id: int) -> str:
    return f"reminder:{user_id}:{safe_event_name(event_name)}"

def start_job_id(event_name: str, channel_id: int) -> str:
    return f"start:{safe_event_name(event_name)}:{channel_id}"

def end_job_id(event_name: str, channel_id: int) -> str:
    return f"end:{safe_event_name(event_name)}:{channel_id}"

def resolve_text_channel(guild: discord.Guild, raw: str, fallback: discord.TextChannel) -> Optional[discord.TextChannel]:
    raw = (raw or "").strip()
    if not raw:
        return fallback
    m = re.match(r"<#(\d+)>", raw)
    if m:
        ch = guild.get_channel(int(m.group(1)))
        return ch if isinstance(ch, discord.TextChannel) else None
    if raw.isdigit():
        ch = guild.get_channel(int(raw))
        return ch if isinstance(ch, discord.TextChannel) else None
    for ch in guild.text_channels:
        if ch.name.lower() == raw.lstrip("#").lower():
            return ch
    return None

def local_str_to_utc(dt_str: str, fmt: str = "%Y-%m-%d %H:%M") -> datetime.datetime:
    """Parse a local (naive) datetime string and return an AWARE UTC datetime."""
    naive = datetime.datetime.strptime(dt_str, fmt)
    local_tz = datetime.datetime.now().astimezone().tzinfo
    local_aware = naive.replace(tzinfo=local_tz)
    return local_aware.astimezone(datetime.timezone.utc)

def fmt_duration(td: datetime.timedelta) -> str:
    """Return '2h30m' / '45m' / '3h' style strings."""
    total_minutes = int(td.total_seconds() // 60)
    h, m = divmod(total_minutes, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m or not parts:
        parts.append(f"{m}m")
    return "".join(parts)

async def is_owner_or_admin(user: discord.abc.User, guild_id: int, creator_id: int) -> bool:
    if user.id == creator_id:
        return True
    guild = bot.get_guild(guild_id) or await bot.fetch_guild(guild_id)
    try:
        member = guild.get_member(user.id) or await guild.fetch_member(user.id)
    except discord.NotFound:
        return False
    return bool(member.guild_permissions.administrator)

# dps.report link extractor
_DPS_RE = re.compile(r"https?://dps\.report/[^\s>]+")
def extract_dps_links(s: str) -> List[str]:
    if not s:
        return []
    return _DPS_RE.findall(s)

# === Duration Parser ===
def parse_duration(duration_str: str) -> Optional[datetime.timedelta]:
    duration_str = duration_str.lower().replace(" ", "")
    match = re.match(r"(?:(\d+)h)?(?:(\d+)m)?", duration_str)
    if not match:
        return None
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    if hours == 0 and minutes == 0:
        return None
    return datetime.timedelta(hours=hours, minutes=minutes)

# ---------- DB bootstrap / migration ----------
async def ensure_tables():
    async with aiosqlite.connect("events.db") as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                user_id INTEGER,
                channel_id INTEGER,
                start_time TEXT,
                end_time TEXT,
                message_id INTEGER
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pending_uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE,
                event_name TEXT,
                channel_id INTEGER,
                attempts INTEGER DEFAULT 0,
                next_retry TEXT,
                last_error TEXT,
                created_utc TEXT
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pending_next ON pending_uploads(next_retry)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT,
                user_id INTEGER
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT,
                file_path TEXT UNIQUE,
                permalink TEXT,
                boss_id INTEGER,
                boss_name TEXT,
                success INTEGER,
                time_utc TEXT
            )
        """)
        await db.commit()

# ---------- In-memory event sessions for log watching ----------
class EventSession:
    """
    Tracks new logs during an event window and uploads them to dps.report.
    Recurses subfolders; waits for file size to stabilize before upload.
    """
    def __init__(self, event_name: str, start: datetime.datetime, end: datetime.datetime, channel_id: int, log_dir: str):
        self.event_name = event_name
        self.start = start
        self.end = end
        self.channel_id = channel_id
        self.log_dir = log_dir
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self.seen: set[str] = set()
        self.results: List[dict] = []
        self._size_cache: dict[str, int] = {}
        self.retry_state: dict[str, dict] = {}  # {path: {"attempts": int, "next": datetime}}
        self.max_attempts = 5
        self.base_backoff = 8  # seconds, exponential
        self.concurrent_sema = asyncio.Semaphore(2)  # avoid slamming the site

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

    async def _run(self):
        poll = 5
        log_suffixes = (".zevtc", ".evtc", ".evtc.zip")

        log.info(f"[WATCH] Session start for '{self.event_name}' "
                 f"window {self.start.isoformat()}–{self.end.isoformat()}, dir={self.log_dir}")

        # Warm-up: mark all existing logs so we don't re-upload old ones
        if os.path.isdir(self.log_dir):
            for root, _dirs, files in os.walk(self.log_dir):
                for fn in files:
                    if fn.lower().endswith(log_suffixes):
                        full = os.path.join(root, fn)
                        self.seen.add(full)

        while not self._stop.is_set():
            now_utc = datetime.datetime.now(datetime.timezone.utc)  # aware UTC
            if now_utc >= self.end + datetime.timedelta(minutes=1):
                break

            try:
                if os.path.isdir(self.log_dir):
                    for root, _dirs, files in os.walk(self.log_dir):
                        for fn in files:
                            if not fn.lower().endswith(log_suffixes):
                                continue
                            full = os.path.join(root, fn)

                            # If we've already fully handled it (success or gave up), skip.
                            if full in self.seen and full not in self.retry_state:
                                continue

                            # mtime gate (only consider files in/near the event window)
                            try:
                                mtime = datetime.datetime.fromtimestamp(
                                    os.path.getmtime(full),
                                    tz=datetime.timezone.utc
                                )
                            except Exception:
                                mtime = now_utc

                            if mtime < self.start - datetime.timedelta(minutes=5) or \
                               mtime > self.end + datetime.timedelta(minutes=15):
                                self.seen.add(full)
                                self.retry_state.pop(full, None)
                                continue

                            # Stability gate (size unchanged across polls)
                            size_now = os.path.getsize(full)
                            prev = self._size_cache.get(full)
                            self._size_cache[full] = size_now
                            if prev is None or prev != size_now:
                                continue

                            # Retry gate
                            state = self.retry_state.get(full, {"attempts": 0, "next": now_utc})
                            if state["attempts"] > 0 and now_utc < state["next"]:
                                continue

                            # Try upload with bounded concurrency + global per-attempt timeout
                            try:
                                async with self.concurrent_sema:
                                    log.debug(f"[WATCH] Attempting upload (try {state['attempts'] + 1}) -> {full}")
                                    result = await asyncio.wait_for(
                                        upload_to_dps_report(full),
                                        timeout=95
                                    )
                            except asyncio.TimeoutError:
                                log.warning(f"[WATCH] Global timeout (wait_for) {full}")
                                result = None

                            if result:
                                # success → save upload row and enrich analytics now
                                self.results.append(result)
                                self.seen.add(full)
                                self.retry_state.pop(full, None)

                                try:
                                    async with aiosqlite.connect("events.db") as db:
                                        cur = await db.execute("""
                                            INSERT OR IGNORE INTO uploads
                                            (event_name, file_path, permalink, boss_id, boss_name, success, time_utc)
                                            VALUES (?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            self.event_name,
                                            full,
                                            result.get("permalink") or "",
                                            result.get("encounter", {}).get("bossId") or result.get("bossId") or -1,
                                            result.get("encounter", {}).get("boss") or result.get("boss") or "",
                                            1 if (result.get("encounter", {}).get("success") or result.get("success")) else 0,
                                            datetime.datetime.now(datetime.timezone.utc).isoformat()
                                        ))
                                        await db.commit()
                                        upload_id = cur.lastrowid
                                        if not upload_id:
                                            # find existing row id
                                            cur = await db.execute("SELECT id FROM uploads WHERE file_path = ?", (full,))
                                            row = await cur.fetchone()
                                            upload_id = row[0] if row else None
                                    if upload_id:
                                        # enrich with EI JSON (dict) returned by dps.report
                                        await enrich_upload(upload_id, result)
                                except Exception:
                                    log.exception("[WATCH] DB persist or analytics enrich failed")
                            else:
                                # failed -> schedule retry or give up (in-session)
                                attempts = state["attempts"] + 1
                                if attempts >= self.max_attempts:
                                    log.error(f"[WATCH] Max attempts reached in-session; queueing for persistent retry {full}")
                                    self.seen.add(full)
                                    self.retry_state.pop(full, None)
                                    await enqueue_pending_upload(
                                        file_path=full,
                                        event_name=self.event_name,
                                        channel_id=self.channel_id,
                                        attempts=0,
                                        delay_seconds=60,
                                        err="session max attempts reached"
                                    )
                                else:
                                    backoff = self.base_backoff * (2 ** (attempts - 1))  # 8,16,32,64,128…
                                    next_time = now_utc + datetime.timedelta(seconds=backoff)
                                    self.retry_state[full] = {"attempts": attempts, "next": next_time}
                                    log.warning(f"[WATCH] Upload failed; will retry #{attempts} in {backoff}s for {full}")

            except Exception:
                log.exception("[WATCH] Loop error")

            # Sleep/poll
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=poll)
            except asyncio.TimeoutError:
                pass

        # Anything still waiting in retry_state goes to persistent queue
        for full, st in list(self.retry_state.items()):
            self.seen.add(full)
            self.retry_state.pop(full, None)
            delay = max(30, self.base_backoff)
            await enqueue_pending_upload(
                file_path=full,
                event_name=self.event_name,
                channel_id=self.channel_id,
                attempts=st.get("attempts", 0),
                delay_seconds=delay,
                err="moved to persistent at session end"
            )

        log.info(f"[WATCH] Session end for '{self.event_name}'. Uploaded {len(self.results)} logs. Pending retries: {len(self.retry_state)}")

# Track active sessions by (event_name, channel_id)
active_sessions: Dict[Tuple[str, int], EventSession] = {}

# ---------- dps.report upload ----------
async def upload_to_dps_report(file_path: str) -> Optional[dict]:
    url = "https://dps.report/uploadContent?json=1"
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            with open(file_path, "rb") as f:
                data = aiohttp.FormData()
                data.add_field("file", f, filename=os.path.basename(file_path), content_type="application/octet-stream")

                log.debug(f"[UPLOAD] POST {url} -> {os.path.basename(file_path)}")
                async with session.post(url, data=data) as resp:
                    status = resp.status
                    body = await resp.read()
                    preview = body[:300].decode(errors="ignore")
                    log.debug(f"[UPLOAD] Response {status}: {preview}")

                    if status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        delay = int(retry_after) if (retry_after and retry_after.isdigit()) else 30
                        raise RuntimeError(f"429 rate limited; retry after {delay}s")
                    if 500 <= status < 600:
                        raise RuntimeError(f"{status} server error")
                    if status != 200:
                        log.warning(f"[UPLOAD] Non-OK {status}: {preview}")
                        return None

                    try:
                        return json.loads(body.decode(errors="ignore"))
                    except Exception:
                        log.exception("[UPLOAD] Failed to parse JSON")
                        return None
    except asyncio.TimeoutError:
        log.warning(f"[UPLOAD] Timeout while uploading {file_path}")
        return None
    except Exception as e:
        log.warning(f"[UPLOAD] Exception for {file_path}: {e}")
        return None

# ---------- Pending uploads worker ----------
async def enqueue_pending_upload(file_path: str, event_name: str, channel_id: int,
                                 attempts: int, delay_seconds: int, err: str = ""):
    """Insert or update a pending upload with next_retry in the future."""
    now = datetime.datetime.now(datetime.timezone.utc)
    next_retry = now + datetime.timedelta(seconds=delay_seconds)
    async with aiosqlite.connect("events.db") as db:
        await db.execute("""
            INSERT INTO pending_uploads (file_path, event_name, channel_id, attempts, next_retry, last_error, created_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_path) DO UPDATE SET
                attempts = excluded.attempts,
                next_retry = excluded.next_retry,
                last_error = excluded.last_error
        """, (file_path, event_name, channel_id, attempts, next_retry.isoformat(), err, now.isoformat()))
        await db.commit()
    log.info(f"[PENDING] queued {os.path.basename(file_path)} (attempts={attempts}, retry in {delay_seconds}s)")

_pending_sema = asyncio.Semaphore(PENDING_CONCURRENCY)

async def process_pending_uploads():
    """Periodic worker: retries failed uploads whose next_retry has arrived."""
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = []
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute("""
            SELECT id, file_path, event_name, channel_id, attempts
            FROM pending_uploads
            WHERE next_retry <= ?
        """, (now,))
        rows = await cur.fetchall()

    if not rows:
        return

    log.info(f"[PENDING] processing {len(rows)} item(s)")
    tasks = [_process_one_pending(row) for row in rows]
    await asyncio.gather(*tasks, return_exceptions=True)

async def _process_one_pending(row):
    id_, file_path, event_name, channel_id, attempts = row
    try:
        if not os.path.exists(file_path):
            await _delete_pending(id_)
            log.warning(f"[PENDING] missing file, dropping: {file_path}")
            return

        async with _pending_sema:
            try:
                result = await asyncio.wait_for(upload_to_dps_report(file_path), timeout=95)
            except asyncio.TimeoutError:
                result = None

        if result:
            # Save to uploads & enrich analytics
            upload_id = None
            try:
                async with aiosqlite.connect("events.db") as db:
                    cur = await db.execute("""
                        INSERT OR IGNORE INTO uploads
                        (event_name, file_path, permalink, boss_id, boss_name, success, time_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event_name,
                        file_path,
                        result.get("permalink") or "",
                        result.get("encounter", {}).get("bossId") or result.get("bossId") or -1,
                        result.get("encounter", {}).get("boss") or result.get("boss") or "",
                        1 if (result.get("encounter", {}).get("success") or result.get("success")) else 0,
                        datetime.datetime.now(datetime.timezone.utc).isoformat()
                    ))
                    await db.commit()
                    upload_id = cur.lastrowid
                    if not upload_id:
                        cur = await db.execute("SELECT id FROM uploads WHERE file_path = ?", (file_path,))
                        r = await cur.fetchone()
                        upload_id = r[0] if r else None
            except Exception:
                log.exception("[PENDING] persist to uploads failed")

            if upload_id:
                try:
                    await enrich_upload(upload_id, result)
                except Exception:
                    log.exception("[PENDING] analytics enrich failed")

            await _delete_pending(id_)
            log.info(f"[PENDING] uploaded ok: {os.path.basename(file_path)}")
        else:
            attempts += 1
            if attempts >= PENDING_MAX_ATTEMPTS:
                await _delete_pending(id_)
                log.error(f"[PENDING] max attempts reached; dropping {file_path}")
            else:
                backoff = PENDING_BASE_BACKOFF * (2 ** (attempts - 1))
                await _update_pending_retry(id_, attempts, backoff, "upload failed")
                log.warning(f"[PENDING] will retry ({attempts}) in {backoff}s: {file_path}")
    except Exception:
        log.exception("[PENDING] error while processing one item")

async def _delete_pending(id_: int):
    async with aiosqlite.connect("events.db") as db:
        await db.execute("DELETE FROM pending_uploads WHERE id = ?", (id_,))
        await db.commit()

async def _update_pending_retry(id_: int, attempts: int, delay_seconds: int, err: str):
    next_retry = (datetime.datetime.now(datetime.timezone.utc)
                  + datetime.timedelta(seconds=delay_seconds)).isoformat()
    async with aiosqlite.connect("events.db") as db:
        await db.execute("""
            UPDATE pending_uploads
            SET attempts = ?, next_retry = ?, last_error = ?
            WHERE id = ?
        """, (attempts, next_retry, err, id_))
        await db.commit()

# ---------- Embed update ----------
async def update_event_message(event_name: str, channel_id: int, message_id: int):
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    try:
        message = await channel.fetch_message(message_id)
        guild = message.guild

        async with aiosqlite.connect("events.db") as db:
            cursor = await db.execute("SELECT user_id FROM signups WHERE event_name = ? ORDER BY id ASC", (event_name,))
            rows = await cursor.fetchall()

        names = []
        for (user_id,) in rows:
            member = guild.get_member(user_id) or await guild.fetch_member(user_id)
            if member:
                names.append(member.display_name)
            else:
                user = await bot.fetch_user(user_id)
                names.append(user.global_name or user.name)

        list_text = "\n".join(f"• {n}" for n in names) if names else "_No one has signed up yet._"

        if message.embeds:
            em = message.embeds[0]
            idx = None
            for i, f in enumerate(em.fields):
                if f.name == "🧑‍🤝‍🧑 Sign-ups":
                    idx = i
                    break
            if idx is None:
                em.add_field(name="🧑‍🤝‍🧑 Sign-ups", value=list_text, inline=False)
            else:
                em.set_field_at(idx, name="🧑‍🤝‍🧑 Sign-ups", value=list_text, inline=False)
            await message.edit(embed=em)
    except Exception:
        log.exception("Failed to update event message")

# ---------- DM Reminder ----------
async def send_dm_reminder(user_id, event_name, event_start_utc):
    try:
        user = await bot.fetch_user(user_id)
        if user:
            ts = int(event_start_utc.timestamp())
            await user.send(f"⏰ Reminder: **{event_name}** starts at <t:{ts}:f> (**<t:{ts}:R>**)")
    except discord.Forbidden:
        log.info(f"⚠️ Could not DM user {user_id} — DMs disabled.")

# ---------- Disabled View for canceled events ----------
class DisabledSignupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="✅ Sign Up", style=discord.ButtonStyle.success, disabled=True))
        self.add_item(discord.ui.Button(label="❌ Sign Out", style=discord.ButtonStyle.danger, disabled=True))

# ---------- Create Event Modal & Panel (guild-side quick creator) ----------
class CreateEventModal(discord.ui.Modal, title="Create Event"):
    def __init__(self):
        super().__init__(timeout=None)
        self.name_input = discord.ui.TextInput(label="Event name", placeholder="Raid Full Clear", required=True, max_length=100)
        self.start_input = discord.ui.TextInput(label="Start (YYYY-MM-DD HH:MM)", placeholder="2025-08-12 18:00", required=True)
        self.duration_input = discord.ui.TextInput(label="Duration (e.g., 2h30m, 45m)", placeholder="2h30m", required=True)
        self.channel_input = discord.ui.TextInput(label="Channel (mention, ID, or name)", placeholder="#raid-planning or 123456789012345678", required=False)
        self.add_item(self.name_input)
        self.add_item(self.start_input)
        self.add_item(self.duration_input)
        self.add_item(self.channel_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            if not LOG_DIR:
                await interaction.response.send_message("❌ `LOG_DIR` is not set in `.env`.", ephemeral=True)
                return

            target = resolve_text_channel(interaction.guild, self.channel_input.value or "", interaction.channel)
            if target is None:
                await interaction.response.send_message("❌ I couldn’t find that channel, or it’s not a text channel.", ephemeral=True)
                return

            created_name = await create_event_common(
                interaction,
                self.name_input.value.strip(),
                self.start_input.value.strip(),
                self.duration_input.value.strip(),
                target_channel=target
            )
            await interaction.response.send_message(f"✅ Event **{created_name}** created in {target.mention}.", ephemeral=True)
        except ValueError as e:
            await interaction.response.send_message(f"❌ {str(e)}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Unexpected error: {str(e)}", ephemeral=True)

class EventCreatorView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📅 Create Event", style=discord.ButtonStyle.primary, custom_id="open_create_event_modal")
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CreateEventModal())

# ---------- Sign-up View ----------
class SignupView(discord.ui.View):
    def __init__(self, event_name: str, event_start: datetime.datetime, channel_id: int, message_id: int):
        super().__init__(timeout=None)
        self.event_name = event_name
        self.event_start = event_start
        self.channel_id = channel_id
        self.message_id = message_id

    @discord.ui.button(label="✅ Sign Up", style=discord.ButtonStyle.success, custom_id="signup_button")
    async def signup(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        async with aiosqlite.connect("events.db") as db:
            cur = await db.execute("SELECT 1 FROM signups WHERE event_name = ? AND user_id = ?", (self.event_name, user_id))
            if await cur.fetchone():
                await interaction.response.defer()
                await update_event_message(self.event_name, self.channel_id, self.message_id)
                return
            await db.execute("INSERT INTO signups (event_name, user_id) VALUES (?, ?)", (self.event_name, user_id))
            await db.commit()

        # schedule DM
        job_id = reminder_job_id(self.event_name, user_id)
        now = datetime.datetime.now(datetime.timezone.utc)
        reminder_time = self.event_start - datetime.timedelta(minutes=15)
        if reminder_time > now:
            scheduler.add_job(send_dm_reminder, 'date', id=job_id, replace_existing=True,
                              run_date=reminder_time, args=[user_id, self.event_name, self.event_start])
        else:
            await send_dm_reminder(user_id, self.event_start, self.event_start)

        await interaction.response.defer()
        await update_event_message(self.event_name, self.channel_id, self.message_id)

    @discord.ui.button(label="❌ Sign Out", style=discord.ButtonStyle.danger, custom_id="signout_button")
    async def signout(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        async with aiosqlite.connect("events.db") as db:
            await db.execute("DELETE FROM signups WHERE event_name = ? AND user_id = ?", (self.event_name, user_id))
            await db.commit()

        # cancel reminder
        job_id = reminder_job_id(self.event_name, user_id)
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

        await interaction.response.defer()
        await update_event_message(self.event_name, self.channel_id, self.message_id)

# ---------- Shared create function ----------
async def create_event_common(
    interaction: discord.Interaction,
    name: str,
    start_time: str,
    duration_str: str,
    target_channel: Optional[discord.TextChannel] = None
):
    await ensure_tables()
    event_start_utc = local_str_to_utc(start_time, "%Y-%m-%d %H:%M")  # aware UTC
    duration_td = parse_duration(duration_str)
    if duration_td is None:
        raise ValueError("Invalid duration format. Use `2h30m`, `45m`, etc.")
    event_end_utc = event_start_utc + duration_td

    ts = int(event_start_utc.timestamp())   # ✅ show the START time
    embed = discord.Embed(
        title=f"📅 Event Scheduled: {name}",
        description=f"**Start:** <t:{ts}:f>\n**Duration:** {duration_str}",
        color=discord.Color.blue(),
    )
    embed.add_field(name="🧑‍🤝‍🧑 Sign-ups", value="_No one has signed up yet._", inline=False)
    embed.set_footer(text="Click the button to sign up. You’ll get a DM 15 minutes before start!")

    channel = target_channel or interaction.channel
    perms = channel.permissions_for(channel.guild.me)
    if not (perms.view_channel and perms.send_messages):
        raise ValueError(f"I don’t have permission to post in {channel.mention}.")

    # Send without view to get message id, then attach the view
    msg = await channel.send(embed=embed, view=None)
    view = SignupView(name, event_start_utc, channel.id, msg.id)  # ✅ pass event_start_utc
    await msg.edit(view=view)

    # Store as ISO UTC
    async with aiosqlite.connect("events.db") as db:
        await db.execute("""
            INSERT INTO events (name, user_id, channel_id, start_time, end_time, message_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, interaction.user.id, channel.id,
              event_start_utc.isoformat(), event_end_utc.isoformat(), msg.id))
        await db.commit()

    # Schedule jobs with aware UTC
    scheduler.add_job(
        start_event, 'date',
        id=start_job_id(name, channel.id),
        replace_existing=True,
        run_date=event_start_utc,
        args=[name, channel.id]
    )
    scheduler.add_job(
        end_event, 'date',
        id=end_job_id(name, channel.id),
        replace_existing=True,
        run_date=event_end_utc,
        args=[name, channel.id]
    )

    # Prepare watcher session
    if LOG_DIR and os.path.isdir(LOG_DIR):
        active_sessions[(name, channel.id)] = EventSession(name, event_start_utc, event_end_utc, channel.id, LOG_DIR)

    return name

# ---------- Start/End Handlers ----------
async def start_event(name, channel_id):
    ch = bot.get_channel(channel_id)
    if ch:
        await ch.send(f"🚀 **Event '{name}'** has started! Start logging those kills!")
    # Start log watcher if we created a session
    sess = active_sessions.get((name, channel_id))
    if sess:
        sess.start_task()

async def end_event(name, channel_id):
    ch = bot.get_channel(channel_id)
    # Stop watcher and compile summary
    sess = active_sessions.pop((name, channel_id), None)
    if sess:
        await sess.stop_task()
        log.info(f"[WATCH] {name}: collected {len(sess.results)} upload result(s)")  # 👈 debug visibility
        summary_embed = await build_summary_embed(name, sess.results, event_start_utc=sess.start)
        if ch:
            await ch.send(f"✅ **Event '{name}'** has ended. Processing logs and generating summary...")
            await ch.send(embed=summary_embed)

            # ✅ Ensure all uploads for this event are enriched before analytics
            repaired = await ensure_enriched_for_event(name)
            if repaired:
                print(f"[WATCH] Repaired {repaired} missing analytics uploads for {name}")

            # Auto-post analytics for the event
            try:
                embeds = await build_event_analytics_embeds(name)
                if embeds:
                    for em in embeds:
                        await ch.send(embed=em)
                else:
                    await ch.send("ℹ️ No analytics to show yet. Use `/ingest_logs` to add dps.report links, then `/post_analytics`.")
            except Exception as e:
                await ch.send(f"⚠️ Analytics failed: `{e}`")
        return
    # Fallback if no session
    if ch:
        await ch.send(f"✅ **Event '{name}'** has ended. (No logs collected or LOG_DIR not set.)")

        # 🔧 Make sure uploads for this event are enriched before analytics
        repaired = await ensure_enriched_for_event(name)
        if repaired:
            print(f"[WATCH] Repaired {repaired} missing analytics uploads for {name}")

        # Still try to post analytics if available
        try:
            embeds = await build_event_analytics_embeds(name)
            if embeds:
                for em in embeds:
                    await ch.send(embed=em)
        except Exception as e:
            await ch.send(f"⚠️ Analytics failed: `{e}`")

# ---------- Build summary ----------
def _parse_dt_any(s: Optional[str]) -> Optional[datetime.datetime]:
    """Parse common dps.report timestamp shapes as aware UTC datetimes if possible."""
    if not s:
        return None
    # EI typically uses ISO 8601 (UTC). We try a couple of lenient paths.
    try:
        # Handles "2025-08-13T11:22:33Z" or with offset
        dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        pass
    # Fallback: try stripping ms or weird fractions
    try:
        base = s.split('.')[0]  # "2025-08-13T11:22:33"
        dt = datetime.datetime.fromisoformat(base)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None

def _fmt_td_hms(td: datetime.timedelta) -> str:
    secs = int(td.total_seconds())
    if secs < 0:
        secs = 0
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

async def build_summary_embed(event_name: str, results: List[dict], event_start_utc: Optional[datetime.datetime] = None) -> discord.Embed:
    """
    Group attempts by encounter, compute Total/Wasted time, and render encounters in 1–3 columns.
    Total time: event_start -> last log end (if available).
    Wasted time: sum of gaps between consecutive attempts (start[i] - end[i-1], clipped at >=0).
    """
    # Build attempt buckets per boss
    attempts: Dict[Tuple[int, str], List[dict]] = {}
    for r in results:
        enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
        boss_id = enc.get("bossId") or r.get("bossId") or -1
        boss_name = enc.get("boss") or r.get("boss") or "Unknown Encounter"
        key = (int(boss_id), str(boss_name))
        attempts.setdefault(key, []).append(r)

    # Sort logs in each bucket by end (then start) so our links/readout are stable
    def sort_key(x):
        return (x.get("timeEnd") or "", x.get("timeStart") or x.get("time") or "")
    for key in attempts:
        attempts[key].sort(key=sort_key)

    # Compute overall timing
    spans: List[Tuple[Optional[datetime.datetime], Optional[datetime.datetime]]] = []
    for logs in attempts.values():
        for r in logs:
            start_dt = _parse_dt_any(r.get("timeStart") or r.get("time"))
            end_dt   = _parse_dt_any(r.get("timeEnd"))
            spans.append((start_dt, end_dt))

    # last_end for Total time
    last_end: Optional[datetime.datetime] = None
    for _s, e in spans:
        if e and (last_end is None or e > last_end):
            last_end = e

    # Total time: from event_start_utc (if given), else earliest start we have, to last_end
    earliest_start: Optional[datetime.datetime] = None
    for s, _e in spans:
        if s and (earliest_start is None or s < earliest_start):
            earliest_start = s
    anchor_start = event_start_utc or earliest_start

    # Only compute total if both ends exist
    total_time_td = last_end - anchor_start if (anchor_start and last_end) else None

    # Wasted time: sum of gaps between sorted attempts by their *per-log* start/end
    flat_logs = []
    for logs in attempts.values():
        for r in logs:
            s = _parse_dt_any(r.get("timeStart") or r.get("time"))
            e = _parse_dt_any(r.get("timeEnd"))
            if s or e:
                flat_logs.append((s, e))
    flat_logs.sort(key=lambda se: (se[0] or se[1] or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)))

    wasted = datetime.timedelta(0)
    prev_end: Optional[datetime.datetime] = None
    positive_gap_found = False
    for s, e in flat_logs:
        if prev_end and s:
            gap = s - prev_end
            if gap.total_seconds() > 0:
                wasted += gap
                positive_gap_found = True
        if e and ((prev_end is None) or (e > prev_end)):
            prev_end = e

    # Build encounter lines
    lines: List[str] = []
    for (bid, bname), logs in attempts.items():
        # first success?
        success_index = None
        success_log = None
        for i, r in enumerate(logs):
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            success = enc.get("success", r.get("success", False))
            if success:
                success_index = i
                success_log = r
                break

        # CM flag (if any)
        cm_flag = None
        for r in logs:
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if "isCM" in enc:
                cm_flag = enc.get("isCM")
                break
            if "isCM" in r:
                cm_flag = r.get("isCM")
                break
        cm_text = " [CM]" if cm_flag else ""

        if success_log:
            attempts_count = success_index + 1
            url = success_log.get("permalink", "")
            lines.append(f"• **{bname}{cm_text}** — attempts: **{attempts_count}** — [kill log]({url})")
        else:
            latest = logs[-1]
            url = latest.get("permalink", "")
            tries = len(logs)
            lines.append(f"• **{bname}{cm_text}** — attempts: **{tries}** — _no kill_ — [latest log]({url})")

    # Header with timing — show em-dash (—) when unknown or trivial instead of "0s"
    def td_or_dash(td: Optional[datetime.timedelta], show_zero: bool = False) -> str:
        if not td:
            return "—"
        if not show_zero and int(td.total_seconds()) == 0:
            return "—"
        return _fmt_td_hms(td)

    total_text = td_or_dash(total_time_td)
    wasted_text = td_or_dash(wasted if positive_gap_found else None)  # only show if we had real gaps
    header_text = f"**Total time:** {total_text} • **Wasted time:** {wasted_text}"

    # Create the embed
    color = discord.Color.green() if any("kill log" in ln for ln in lines) else discord.Color.orange()
    em = discord.Embed(title=f"📊 Event Summary — {event_name}", description=header_text, color=color)
    em.set_footer(text="Kill: link to the successful log. No kill: link to the latest attempt.")

    # Multi-column encounter list: up to 3 columns (inline fields)
    if not lines:
        em.add_field(name="\u200b", value="_No encounters recorded in this event window._", inline=False)
        return em

    n = len(lines)
    cols = 3 if n >= 9 else (2 if n >= 4 else 1)
    per_col = (n + cols - 1) // cols  # ceiling

    for i in range(cols):
        chunk = lines[i*per_col:(i+1)*per_col]
        if not chunk:
            continue
        em.add_field(name="\u200b", value="\n".join(chunk), inline=True)

    return em

# ---------- CANCEL EVENT (slash, guild) ----------
@bot.tree.command(name="cancel_event", description="Cancel an event (creator or admin)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(name="Event name to cancel")
async def cancel_event_cmd(interaction: discord.Interaction, name: str):
    # find event
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute("SELECT user_id, channel_id, message_id FROM events WHERE name = ?", (name,))
        row = await cur.fetchone()

    if not row:
        await interaction.response.send_message(f"❌ Event **{name}** not found.", ephemeral=True)
        return

    creator_id, channel_id, message_id = row

    # permissions: creator or admin
    if (interaction.user.id != creator_id) and (not interaction.user.guild_permissions.administrator):
        await interaction.response.send_message("❌ Only the event creator or an admin can cancel this event.", ephemeral=True)
        return

    await _cancel_event_core(name, channel_id, message_id)
    await interaction.response.send_message(f"🛑 Event **{name}** has been canceled.", ephemeral=True)

async def _cancel_event_core(name: str, channel_id: int, message_id: int):
    # cancel start/end jobs
    for jid in [start_job_id(name, channel_id), end_job_id(name, channel_id)]:
        try:
            scheduler.remove_job(jid)
        except Exception:
            pass

    # cancel all reminders
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute("SELECT user_id FROM signups WHERE event_name = ?", (name,))
        user_rows = await cur.fetchall()
    for (uid,) in user_rows:
        try:
            scheduler.remove_job(reminder_job_id(name, uid))
        except Exception:
            pass

    # update message: mark canceled + disable buttons
    channel = bot.get_channel(channel_id)
    if channel and message_id:
        try:
            msg = await channel.fetch_message(message_id)
            if msg.embeds:
                em = msg.embeds[0]
                em.title = f"❌ CANCELED — {em.title}"
                em.color = discord.Color.red()
                em.set_footer(text="This event has been canceled.")
                await msg.edit(embed=em, view=DisabledSignupView())
        except Exception as e:
            log.info(f"⚠️ Could not edit event message: {e}")

# ---------- Admin cleanup ----------
@bot.tree.command(name="admin_cleanup_commands", description="(Admin) Remove old duplicate slash commands")
@app_commands.guilds(discord.Object(id=GUILD_ID))
async def admin_cleanup_commands(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admins only.", ephemeral=True)
        return
    bot.tree.clear_commands(guild=None)
    await bot.tree.sync()  # global empty
    guild_obj = discord.Object(id=GUILD_ID)
    bot.tree.clear_commands(guild=guild_obj)
    synced = await bot.tree.sync(guild=guild_obj)
    await interaction.response.send_message(
        f"🧹 Cleaned up old commands. Re-synced {len(synced)} command(s) to this guild.",
        ephemeral=True
    )

# ======================
# DM EVENT MANAGER (NEW)
# ======================

# 1) Slash to open DM manager
@bot.tree.command(name="manage_events", description="Open your Event Manager in DM")
@app_commands.guilds(discord.Object(id=GUILD_ID))
async def manage_events(interaction: discord.Interaction):
    try:
        await interaction.user.send("📬 Opening your Event Manager…", view=EventManagerDMView())
        await interaction.response.send_message("✅ Check your DMs!", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("❌ I can’t DM you. Please enable DMs from this server.", ephemeral=True)

# 2) DM Manager panel
class EventManagerDMView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="➕ Create Event", style=discord.ButtonStyle.primary, custom_id="dm_create_event")
    async def dm_create(self, interaction: discord.Interaction, btn: discord.ui.Button):
        guild = bot.get_guild(GUILD_ID) or await bot.fetch_guild(GUILD_ID)
        options: List[discord.SelectOption] = []
        for ch in (guild.text_channels if guild else []):
            perms = ch.permissions_for(ch.guild.me)
            if perms.view_channel and perms.send_messages:
                label = f"#{ch.name}"[:100]
                options.append(discord.SelectOption(label=label, value=str(ch.id)))
        if not options:
            await interaction.response.send_message("❌ I don’t have a text channel I can post to.")
            return
        await interaction.response.send_message("Choose a channel for the event:", view=ChannelPickThenCreateModal(options))

    @discord.ui.button(label="📂 My Events", style=discord.ButtonStyle.secondary, custom_id="dm_my_events")
    async def dm_my_events(self, interaction: discord.Interaction, btn: discord.ui.Button):
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        async with aiosqlite.connect("events.db") as db:
            # Upcoming first: soonest at the top
            cur = await db.execute("""
                SELECT id, name, start_time, channel_id, message_id, user_id
                FROM events
                WHERE user_id = ? AND datetime(start_time) >= datetime(?)
                ORDER BY datetime(start_time) ASC
            """, (interaction.user.id, now_iso))
            upcoming = await cur.fetchall()

            # Then recent past: newest first
            cur = await db.execute("""
                SELECT id, name, start_time, channel_id, message_id, user_id
                FROM events
                WHERE user_id = ? AND datetime(start_time) < datetime(?)
                ORDER BY datetime(start_time) DESC
            """, (interaction.user.id, now_iso))
            past = await cur.fetchall()

        rows = list(upcoming) + list(past)
        if not rows:
            await interaction.response.send_message("You don’t have any events yet.")
            return

        rows = rows[:25]

        options: List[discord.SelectOption] = []
        for (eid, name, start_iso, ch_id, msg_id, creator_id) in rows:
            dt = datetime.datetime.fromisoformat(start_iso)
            label = f"{name} – {dt.strftime('%Y-%m-%d %H:%M')}"
            options.append(discord.SelectOption(label=label[:100], value=str(eid)))

        await interaction.response.send_message("Select an event:", view=MyEventsSelectView(options))

# 3) Channel selection ➜ Create Event (modal)
class ChannelPickThenCreateModal(discord.ui.View):
    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(timeout=60)
        self.add_item(ChannelChoice(options))

class ChannelChoice(discord.ui.Select):
    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(placeholder="Select a channel…", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        channel_id = int(self.values[0])
        await interaction.response.send_modal(CreateFromDMModal(channel_id))

class CreateFromDMModal(discord.ui.Modal, title="Create Event"):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id
        self.name_input = discord.ui.TextInput(label="Event name", placeholder="Raid Full Clear", max_length=100)
        self.start_input = discord.ui.TextInput(label="Start (YYYY-MM-DD HH:MM)", placeholder="2025-08-12 18:00")
        self.duration_input = discord.ui.TextInput(label="Duration (e.g., 2h30m, 45m)", placeholder="2h30m")
        self.add_item(self.name_input)
        self.add_item(self.start_input)
        self.add_item(self.duration_input)

    async def on_submit(self, interaction: discord.Interaction):
        guild = bot.get_guild(GUILD_ID) or await bot.fetch_guild(GUILD_ID)
        channel = guild.get_channel(self.channel_id)
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("❌ That channel is unavailable.")
            return

        class FakeInteraction:
            def __init__(self, user, channel, guild):
                self.user = user
                self.channel = channel
                self.guild = guild
        fake = FakeInteraction(interaction.user, channel, guild)

        try:
            created_name = await create_event_common(
                interaction=fake,
                name=self.name_input.value.strip(),
                start_time=self.start_input.value.strip(),
                duration_str=self.duration_input.value.strip(),
                target_channel=channel
            )
            await interaction.response.send_message(f"✅ Created **{created_name}** in {channel.mention}.")
        except Exception as e:
            await interaction.response.send_message(f"❌ {e}")

# 4) “My Events” ➜ manage one (Edit/Cancel)
class MyEventsSelectView(discord.ui.View):
    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(MyEventsSelect(options))

class MyEventsSelect(discord.ui.Select):
    def __init__(self, options: List[discord.SelectOption]):
        super().__init__(placeholder="Pick an event…", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        event_id = int(self.values[0])
        # Load event
        async with aiosqlite.connect("events.db") as db:
            cur = await db.execute("""
                SELECT id, name, user_id, channel_id, start_time, end_time, message_id
                FROM events WHERE id = ?
            """, (event_id,))
            row = await cur.fetchone()
        if not row:
            await interaction.response.send_message("❌ Event not found.")
            return

        eid, name, creator_id, channel_id, start_iso, end_iso, message_id = row
        if not await is_owner_or_admin(interaction.user, GUILD_ID, creator_id):
            await interaction.response.send_message("❌ You can’t manage this event.")
            return

        await interaction.response.send_message(
            f"**{name}** — What do you want to do?",
            view=ManageOneEventView(event_id=eid, name=name, channel_id=channel_id, message_id=message_id)
        )

class ManageOneEventView(discord.ui.View):
    def __init__(self, event_id: int, name: str, channel_id: int, message_id: int):
        super().__init__(timeout=120)
        self.event_id = event_id
        self.name = name
        self.channel_id = channel_id
        self.message_id = message_id

    @discord.ui.button(label="✏️ Edit Name", style=discord.ButtonStyle.secondary, custom_id="edit_name")
    async def edit_name(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(EditNameModal(self.event_id))

    @discord.ui.button(label="🕒 Edit Start", style=discord.ButtonStyle.secondary, custom_id="edit_start")
    async def edit_start(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(EditStartModal(self.event_id))

    @discord.ui.button(label="⏱️ Edit Duration", style=discord.ButtonStyle.secondary, custom_id="edit_duration")
    async def edit_duration(self, interaction: discord.Interaction, _):
        await interaction.response.send_modal(EditDurationModal(self.event_id))

    @discord.ui.button(label="🛑 Cancel Event", style=discord.ButtonStyle.danger, custom_id="cancel_event_dm")
    async def cancel_event_dm(self, interaction: discord.Interaction, _):
        async with aiosqlite.connect("events.db") as db:
            cur = await db.execute("SELECT name, user_id FROM events WHERE id = ?", (self.event_id,))
            r = await cur.fetchone()
        if not r:
            await interaction.response.send_message("❌ Event not found.")
            return
        name, creator_id = r
        if not await is_owner_or_admin(interaction.user, GUILD_ID, creator_id):
            await interaction.response.send_message("❌ You can’t cancel this event.")
            return

        await _cancel_event_core(name, self.channel_id, self.message_id)
        await interaction.response.send_message("✅ Canceled.")

# 5) Edit modals + helpers
async def _refresh_event_message(eid: int):
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute("SELECT name, channel_id, message_id, start_time, end_time FROM events WHERE id = ?", (eid,))
        row = await cur.fetchone()
    if not row:
        return False
    name, channel_id, message_id, start_iso, end_iso = row
    ch = bot.get_channel(channel_id)
    if not ch:
        return False
    try:
        msg = await ch.fetch_message(message_id)
    except Exception:
        return False

    start_dt = datetime.datetime.fromisoformat(start_iso)
    end_dt = datetime.datetime.fromisoformat(end_iso)
    ts = int(start_dt.timestamp())
    # rebuild embed
    em = discord.Embed(
        title=f"📅 Event Scheduled: {name}",
        description=f"**Start:** <t:{ts}:f>\n**Duration:** {fmt_duration(end_dt - start_dt)}",
        color=discord.Color.blue(),
    )
    # preserve sign-ups field if present
    if msg.embeds:
        old = msg.embeds[0]
        for f in old.fields:
            if f.name == "🧑‍🤝‍🧑 Sign-ups":
                em.add_field(name=f.name, value=f.value, inline=False)
    await msg.edit(embed=em, view=SignupView(name, start_dt, channel_id, message_id))
    return True

async def _reschedule_jobs(old_name: str, new_name: str, channel_id: int, start_dt: datetime.datetime, end_dt: datetime.datetime):
    if new_name != old_name:
        for jid in [start_job_id(old_name, channel_id), end_job_id(old_name, channel_id)]:
            try:
                scheduler.remove_job(jid)
            except Exception:
                pass
    scheduler.add_job(start_event, 'date', id=start_job_id(new_name, channel_id),
                      replace_existing=True, run_date=start_dt, args=[new_name, channel_id])
    scheduler.add_job(end_event, 'date', id=end_job_id(new_name, channel_id),
                      replace_existing=True, run_date=end_dt, args=[new_name, channel_id])

class EditNameModal(discord.ui.Modal, title="Edit Event Name"):
    def __init__(self, event_id: int):
        super().__init__(timeout=None)
        self.event_id = event_id
        self.name_input = discord.ui.TextInput(label="New name", max_length=100)
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction):
        async with aiosqlite.connect("events.db") as db:
            cur = await db.execute("SELECT name, channel_id, start_time, end_time FROM events WHERE id = ?", (self.event_id,))
            row = await cur.fetchone()
        if not row:
            await interaction.response.send_message("❌ Event not found.")
            return
        old_name, channel_id, start_iso, end_iso = row
        new_name = self.name_input.value.strip()
        if not new_name:
            await interaction.response.send_message("❌ Name can’t be empty.")
            return
        async with aiosqlite.connect("events.db") as db:
            await db.execute("UPDATE events SET name = ? WHERE id = ?", (new_name, self.event_id))
            await db.execute("UPDATE signups SET event_name = ? WHERE event_name = ?", (new_name, old_name))
            await db.commit()
        await _reschedule_jobs(old_name, new_name, channel_id,
                               datetime.datetime.fromisoformat(start_iso),
                               datetime.datetime.fromisoformat(end_iso))
        await _refresh_event_message(self.event_id)
        await interaction.response.send_message("✅ Name updated.")

class EditStartModal(discord.ui.Modal, title="Edit Start Time"):
    def __init__(self, event_id: int):
        super().__init__(timeout=None)
        self.event_id = event_id
        self.start_input = discord.ui.TextInput(label="Start (YYYY-MM-DD HH:MM)")
        self.add_item(self.start_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            new_start = local_str_to_utc(self.start_input.value.strip(), "%Y-%m-%d %H:%M")
        except Exception:
            await interaction.response.send_message("❌ Invalid datetime format.")
            return

        async with aiosqlite.connect("events.db") as db:
            cur = await db.execute("SELECT name, channel_id, end_time FROM events WHERE id = ?", (self.event_id,))
            row = await cur.fetchone()
        if not row:
            await interaction.response.send_message("❌ Event not found.")
            return
        name, channel_id, end_iso = row
        end_dt = datetime.datetime.fromisoformat(end_iso)
        if new_start >= end_dt:
            await interaction.response.send_message("❌ Start must be before end.")
            return
        async with aiosqlite.connect("events.db") as db:
            await db.execute("UPDATE events SET start_time = ? WHERE id = ?", (new_start.isoformat(), self.event_id))
            await db.commit()
        await _reschedule_jobs(name, name, channel_id, new_start, end_dt)
        await _refresh_event_message(self.event_id)
        await interaction.response.send_message("✅ Start updated.")

class EditDurationModal(discord.ui.Modal, title="Edit Duration"):
    def __init__(self, event_id: int):
        super().__init__(timeout=None)
        self.event_id = event_id
        self.duration_input = discord.ui.TextInput(label="Duration (e.g., 2h30m)")
        self.add_item(self.duration_input)

    async def on_submit(self, interaction: discord.Interaction):
        td = parse_duration(self.duration_input.value.strip())
        if not td:
            await interaction.response.send_message("❌ Invalid duration.")
            return
        async with aiosqlite.connect("events.db") as db:
            cur = await db.execute("SELECT name, channel_id, start_time FROM events WHERE id = ?", (self.event_id,))
            row = await cur.fetchone()
        if not row:
            await interaction.response.send_message("❌ Event not found.")
            return
        name, channel_id, start_iso = row
        start_dt = datetime.datetime.fromisoformat(start_iso)
        end_dt = start_dt + td
        async with aiosqlite.connect("events.db") as db:
            await db.execute("UPDATE events SET end_time = ? WHERE id = ?", (end_dt.isoformat(), self.event_id))
            await db.commit()
        await _reschedule_jobs(name, name, channel_id, start_dt, end_dt)
        await _refresh_event_message(self.event_id)
        await interaction.response.send_message("✅ Duration updated.")

# ---------- Post creator panel (guild) ----------
@bot.tree.command(name="post_event_creator", description="Post a button to create events via a modal")
@app_commands.guilds(discord.Object(id=GUILD_ID))
async def post_event_creator(interaction: discord.Interaction):
    view = EventCreatorView()
    await interaction.channel.send("Click the button to create a new event:", view=view)
    await interaction.response.send_message("✅ Posted event creator.", ephemeral=True)

# ---------- Optional fallback signup command ----------
@bot.tree.command(name="signup_event", description="(Fallback) Sign up for an event by name")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(name="Event name")
async def signup_event(interaction: discord.Interaction, name: str):
    user_id = interaction.user.id
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute("SELECT start_time, channel_id, message_id FROM events WHERE name = ?", (name,))
        row = await cur.fetchone()
        if not row:
            await interaction.response.send_message(f"❌ Event **{name}** not found.", ephemeral=True)
            return
        start_time_iso, channel_id, message_id = row
        start_time = datetime.datetime.fromisoformat(start_time_iso)

        cur = await db.execute("SELECT 1 FROM signups WHERE event_name = ? AND user_id = ?", (name, user_id))
        if await cur.fetchone():
            await interaction.response.send_message("❗ You’re already signed up.", ephemeral=True)
            return

        await db.execute("INSERT INTO signups (event_name, user_id) VALUES (?, ?)", (name, user_id))
        await db.commit()

    await interaction.response.send_message(f"✅ Signed up for **{name}**.", ephemeral=True)

    if channel_id and message_id:
        await update_event_message(name, channel_id, message_id)

    # Schedule DM
    now = datetime.datetime.now(datetime.timezone.utc)  # ✅ aware
    reminder_time = start_time - datetime.timedelta(minutes=15)
    job_id = reminder_job_id(name, user_id)
    if reminder_time > now:
        scheduler.add_job(send_dm_reminder, 'date', id=job_id, replace_existing=True,
                          run_date=reminder_time, args=[user_id, name, start_time])
    else:
        await send_dm_reminder(user_id, name, start_time)

# ---------- Analytics slash commands ----------
@bot.tree.command(name="ingest_logs", description="Attach dps.report links to an event and ingest analytics")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(event="Event name", links="One or more dps.report links separated by spaces/newlines")
async def ingest_logs(interaction: discord.Interaction, event: str, links: str):
    await ensure_tables()
    urls = extract_dps_links(links)
    if not urls:
        await interaction.response.send_message("❌ No valid dps.report links found.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    ingested = 0
    errors: List[str] = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    async with aiosqlite.connect("events.db") as db:
        for url in urls:
            try:
                cur = await db.execute("""
                    INSERT OR IGNORE INTO uploads(event_name, file_path, permalink, boss_id, boss_name, success, time_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (event, url, url, -1, "", 0, now_iso))
                await db.commit()
                upload_id = cur.lastrowid
                if not upload_id:
                    cur = await db.execute("SELECT id FROM uploads WHERE file_path = ?", (url,))
                    row = await cur.fetchone()
                    if not row:
                        raise RuntimeError("Could not create or fetch uploads row")
                    upload_id = row[0]

                # Enrich directly from the permalink (service will fetch EI JSON)
                await enrich_upload(upload_id, url)
                ingested += 1
            except Exception as e:
                errors.append(f"{url} — {e!s}")

    msg = f"✅ Ingested {ingested}/{len(urls)} link(s) into **{event}**."
    if errors:
        msg += "\n\n⚠️ Errors:\n" + "\n".join(f"• {e}" for e in errors[:8])
        if len(errors) > 8:
            msg += f"\n(and {len(errors)-8} more…)"
    await interaction.followup.send(msg, ephemeral=True)

@bot.tree.command(name="post_analytics", description="Post analytics embeds for an event")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(event="Event name")
async def post_analytics(interaction: discord.Interaction, event: str):
    await interaction.response.defer(thinking=True)
    try:
        embeds = await build_event_analytics_embeds(event)
        if not embeds:
            await interaction.followup.send(f"❌ No analytics found for **{event}** yet.", ephemeral=True)
            return
        for em in embeds:
            await interaction.channel.send(embed=em)
        await interaction.followup.send(f"✅ Posted analytics for **{event}**.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to build/post analytics: {e}", ephemeral=True)

# ---------- On Ready ----------
@bot.event
async def on_ready():
    await ensure_tables()
    # Register persistent views so custom_ids survive restarts
    bot.add_view(EventCreatorView())    # guild quick-creator
    bot.add_view(EventManagerDMView())  # DM manager
    guild = discord.Object(id=GUILD_ID)
    synced = await bot.tree.sync(guild=guild)
    scheduler.start()
    scheduler.add_job(
        process_pending_uploads,
        "interval",
        minutes=PENDING_SCAN_MIN,
        id="pending_uploader",
        replace_existing=True
    )
    log.info(f"[PENDING] background worker scheduled every {PENDING_SCAN_MIN} min")
    log.info(f"Bot is ready as {bot.user} — Synced {len(synced)} commands to GUILD.")

# ---------- Run bot ----------
bot.run(TOKEN)
