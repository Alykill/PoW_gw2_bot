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
GUILD_ID = 725430860421136404
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
                                # outside window → don’t upload, mark as seen, clear retry if any
                                self.seen.add(full)
                                self.retry_state.pop(full, None)
                                continue

                            # Stability gate (size unchanged across polls)
                            size_now = os.path.getsize(full)
                            prev = self._size_cache.get(full)
                            self._size_cache[full] = size_now
                            if prev is None or prev != size_now:
                                # still growing; check next poll
                                continue

                            # Retry gate
                            state = self.retry_state.get(full, {"attempts": 0, "next": now_utc})
                            if state["attempts"] > 0 and now_utc < state["next"]:
                                # not time yet to retry
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
                                # success
                                self.results.append(result)
                                self.seen.add(full)
                                self.retry_state.pop(full, None)

                                # persist to DB
                                try:
                                    async with aiosqlite.connect("events.db") as db:
                                        await db.execute("""
                                            INSERT OR IGNORE INTO uploads
                                            (event_name, file_path, permalink, boss_id, boss_name, success, time_utc)
                                            VALUES (?, ?, ?, ?, ?, ?, ?)
                                        """, (
                                            self.event_name,
                                            full,
                                            result.get("permalink") or "",
                                            result.get("encounter", {}).get("bossId") or result.get("bossId") or -1,
                                            result.get("encounter", {}).get("boss") or result.get("boss") or "",
                                            1 if (result.get("encounter", {}).get("success") or result.get(
                                                "success")) else 0,
                                            datetime.datetime.now(datetime.timezone.utc).isoformat()
                                        ))
                                        await db.commit()
                                except Exception:
                                    log.exception("[WATCH] DB persist failed")
                            else:
                                # failed -> schedule retry or give up (in-session)
                                attempts = state["attempts"] + 1
                                if attempts >= self.max_attempts:
                                    log.error(
                                        f"[WATCH] Max attempts reached in-session; queueing for persistent retry {full}")
                                    self.seen.add(full)  # stop the watcher re-checking this
                                    self.retry_state.pop(full, None)
                                    await enqueue_pending_upload(
                                        file_path=full,
                                        event_name=self.event_name,
                                        channel_id=self.channel_id,
                                        attempts=0,
                                        delay_seconds=60,  # first persistent retry in ~1 min
                                        err="session max attempts reached"
                                    )
                                else:
                                    backoff = self.base_backoff * (2 ** (attempts - 1))  # 8,16,32,64,128…
                                    next_time = now_utc + datetime.timedelta(seconds=backoff)
                                    self.retry_state[full] = {"attempts": attempts, "next": next_time}
                                    log.warning(
                                        f"[WATCH] Upload failed; will retry #{attempts} in {backoff}s for {full}")

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
            delay = max(30, self.base_backoff)  # small initial delay
            await enqueue_pending_upload(
                file_path=full,
                event_name=self.event_name,
                channel_id=self.channel_id,
                attempts=st.get("attempts", 0),
                delay_seconds=delay,
                err="moved to persistent at session end"
            )

        log.info(f"[WATCH] Session end for '{self.event_name}'. "
                 f"Uploaded {len(self.results)} logs. Pending retries: {len(self.retry_state)}")

# Track active sessions by (event_name, channel_id)
active_sessions: Dict[Tuple[str, int], EventSession] = {}

# ---------- dps.report upload ----------
import json

async def upload_to_dps_report(file_path: str) -> Optional[dict]:
    url = "https://dps.report/uploadContent?json=1"
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            with open(file_path, "rb") as f:
                data = aiohttp.FormData()
                data.add_field("file", f, filename=os.path.basename(file_path), content_type="application/octet-stream")
                # data.add_field("generator", "ei")  # optional

                log.debug(f"[UPLOAD] POST {url} -> {os.path.basename(file_path)}")
                async with session.post(url, data=data) as resp:
                    status = resp.status
                    body = await resp.read()  # <-- read once
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

async def enqueue_pending_upload(file_path: str, event_name: str, channel_id: int,
                                 attempts: int, delay_seconds: int, err: str = ""):
    """Insert or update a pending upload with next_retry in the future."""
    now = datetime.datetime.now(datetime.timezone.utc)
    next_retry = now + datetime.timedelta(seconds=delay_seconds)
    async with aiosqlite.connect("events.db") as db:
        # Insert or bump attempts + next_retry if already there
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
    tasks = [ _process_one_pending(row) for row in rows ]
    await asyncio.gather(*tasks, return_exceptions=True)

async def _process_one_pending(row):
    id_, file_path, event_name, channel_id, attempts = row
    try:
        if not os.path.exists(file_path):
            # File vanished — drop the row
            await _delete_pending(id_)
            log.warning(f"[PENDING] missing file, dropping: {file_path}")
            return

        async with _pending_sema:
            try:
                result = await asyncio.wait_for(upload_to_dps_report(file_path), timeout=95)
            except asyncio.TimeoutError:
                result = None

        if result:
            # Save to uploads table (same as in watcher)
            try:
                async with aiosqlite.connect("events.db") as db:
                    await db.execute("""
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
            except Exception:
                log.exception("[PENDING] persist to uploads failed")

            await _delete_pending(id_)
            log.info(f"[PENDING] uploaded ok: {os.path.basename(file_path)}")
        else:
            # backoff and retry or give up
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


# ---------- On Ready ----------
@bot.event
async def on_ready():
    await ensure_tables()
    guild = discord.Object(id=GUILD_ID)
    synced = await bot.tree.sync(guild=guild)
    bot.add_view(EventCreatorView())
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

# ---------- Create Event Modal & Panel ----------
class CreateEventModal(discord.ui.Modal, title="Create Event"):
    def __init__(self):
        super().__init__(timeout=None)
        self.name_input = discord.ui.TextInput(label="Event name", placeholder="Raid Full Clear", required=True, max_length=100)
        self.start_input = discord.ui.TextInput(label="Start (YYYY-MM-DD HH:MM)", placeholder="2025-08-12 18:00", required=True)
        self.duration_input = discord.ui.TextInput(label="Duration (e.g., 2h30m, 45m, 3h)", placeholder="2h30m", required=True)
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

class EndedSignupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="✅ Sign Up", style=discord.ButtonStyle.success, disabled=True))
        self.add_item(discord.ui.Button(label="❌ Sign Out", style=discord.ButtonStyle.danger, disabled=True))

async def mark_event_ended_in_message(channel_id: int, message_id: int):
    channel = bot.get_channel(channel_id)
    if not channel or not message_id:
        return
    try:
        msg = await channel.fetch_message(message_id)
        if msg.embeds:
            em = msg.embeds[0]
            em.title = f"🏁 ENDED — {em.title}"
            em.color = discord.Color.greyple()
            em.set_footer(text="This event ended early.")
            await msg.edit(embed=em, view=EndedSignupView())
    except Exception:
        log.exception("Could not mark event ended")

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
            await send_dm_reminder(user_id, self.event_name, self.event_start)

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


# ---------- Post creator panel ----------
@bot.tree.command(name="post_event_creator", description="Post a button to create events via a modal")
@app_commands.guilds(discord.Object(id=GUILD_ID))
async def post_event_creator(interaction: discord.Interaction):
    view = EventCreatorView()
    await interaction.channel.send("Click the button to create a new event:", view=view)
    await interaction.response.send_message("✅ Posted event creator.", ephemeral=True)

@bot.tree.command(name="end_event_now", description="End an event early (creator or admin)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(name="Event name to end now")
async def end_event_now(interaction: discord.Interaction, name: str):
    # Look up the event
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute(
            "SELECT user_id, channel_id, message_id FROM events WHERE name = ?",
            (name,)
        )
        row = await cur.fetchone()

    if not row:
        await interaction.response.send_message(f"❌ Event **{name}** not found.", ephemeral=True)
        return

    creator_id, channel_id, message_id = row

    # Permissions: creator or admin
    if (interaction.user.id != creator_id) and (not interaction.user.guild_permissions.administrator):
        await interaction.response.send_message("❌ Only the event creator or an admin can end this event early.", ephemeral=True)
        return

    # Cancel the scheduled end job (if any)
    try:
        scheduler.remove_job(end_job_id(name, channel_id))
    except Exception:
        pass

    # Stop watcher and compile summary (reuse your existing logic)
    ch = bot.get_channel(channel_id)
    sess = active_sessions.pop((name, channel_id), None)
    if sess:
        await sess.stop_task()
        summary_embed = await build_summary_embed(name, sess.results)
        if ch:
            await ch.send(f"🏁 **Event '{name}'** ended early. Generating summary…")
            await ch.send(embed=summary_embed)
    else:
        if ch:
            await ch.send(f"🏁 **Event '{name}'** ended early. (No logs collected or LOG_DIR not set.)")

    # Mark the original event message as ended and disable buttons
    await mark_event_ended_in_message(channel_id, message_id)

    await interaction.response.send_message(f"✅ Ended **{name}** now.", ephemeral=True)


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
        summary_embed = await build_summary_embed(name, sess.results)
        if ch:
            await ch.send(f"✅ **Event '{name}'** has ended. Processing logs and generating summary...")
            await ch.send(embed=summary_embed)
        return
    # Fallback if no session
    if ch:
        await ch.send(f"✅ **Event '{name}'** has ended. (No logs collected or LOG_DIR not set.)")

# ---------- Build summary ----------
async def build_summary_embed(event_name: str, results: List[dict]) -> discord.Embed:
    """
    Group attempts by encounter. If no success, show attempts + link the most recent attempt.
    If success exists, show number of attempts until first success + link the kill.
    """
    attempts: Dict[Tuple[int, str], List[dict]] = {}
    for r in results:
        enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
        boss_id = enc.get("bossId") or r.get("bossId") or -1
        boss_name = enc.get("boss") or r.get("boss") or "Unknown Encounter"
        key = (int(boss_id), str(boss_name))
        attempts.setdefault(key, []).append(r)

    # sort attempts by end-time or fallback key
    def sort_key(x):
        return x.get("timeEnd") or x.get("time") or ""
    for key in attempts:
        attempts[key].sort(key=sort_key)

    lines = []
    for (bid, bname), logs in attempts.items():
        # find first success
        success_index = None
        success_log = None
        for i, r in enumerate(logs):
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            success = enc.get("success", r.get("success", False))
            if success:
                success_index = i
                success_log = r
                break

        cm_flag = None
        # try to read CM from either structure (won't exist for golems)
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
            # No success: show number of attempts, link the latest attempt
            latest = logs[-1]
            url = latest.get("permalink", "")
            tries = len(logs)
            lines.append(f"• **{bname}{cm_text}** — attempts: **{tries}** — _no kill_ — [latest log]({url})")

    desc = "\n".join(lines) if lines else "No encounters recorded in this event window."

    em = discord.Embed(
        title=f"📊 Event Summary — {event_name}",
        description=desc,
        color=discord.Color.green() if any("kill log" in ln for ln in lines) else discord.Color.orange()
    )
    em.set_footer(text="Kill: link to the successful log. No kill: link to the latest attempt.")
    return em

# ---------- CANCEL EVENT ----------
@bot.tree.command(name="cancel_event", description="Cancel an event (creator or admin)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(name="Event name to cancel")
async def cancel_event(interaction: discord.Interaction, name: str):
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

    await interaction.response.send_message(f"🛑 Event **{name}** has been canceled.", ephemeral=True)

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

# ---------- Run bot ----------
bot.run(TOKEN)
