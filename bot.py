import discord
from discord.ext import commands
from discord import app_commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiosqlite
import datetime
import os
from dotenv import load_dotenv
import re

# === Load .env config ===
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = 725430860421136404  # your server

# === Set up bot ===
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
scheduler = AsyncIOScheduler()

# ---------- Helpers ----------

def safe_event_name(name: str) -> str:
    return re.sub(r"\s+", "_", name.strip())

def reminder_job_id(event_name: str, user_id: int) -> str:
    return f"reminder:{user_id}:{safe_event_name(event_name)}"

def start_job_id(event_name: str, channel_id: int) -> str:
    return f"start:{safe_event_name(event_name)}:{channel_id}"

def end_job_id(event_name: str, channel_id: int) -> str:
    return f"end:{safe_event_name(event_name)}:{channel_id}"

# === Duration Parser ===
def parse_duration(duration_str: str) -> datetime.timedelta | None:
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
                end_time TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT,
                user_id INTEGER
            )
        """)
        # add message_id if not present (SQLite allows ADD COLUMN)
        try:
            await db.execute("ALTER TABLE events ADD COLUMN message_id INTEGER")
        except Exception:
            pass
        await db.commit()

# ---------- On Ready ----------
@bot.event
async def on_ready():
    await ensure_tables()
    guild = discord.Object(id=GUILD_ID)
    synced = await bot.tree.sync(guild=guild)
    # persistent creator panel button
    bot.add_view(EventCreatorView())
    scheduler.start()
    print(f"Bot is ready as {bot.user} — Synced {len(synced)} commands to GUILD.")

# ---------- Embed update ----------
async def update_event_message(event_name: str, channel_id: int, message_id: int):
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    try:
        message = await channel.fetch_message(message_id)
        guild = message.guild

        async with aiosqlite.connect("events.db") as db:
            cursor = await db.execute(
                "SELECT user_id FROM signups WHERE event_name = ? ORDER BY id ASC",
                (event_name,)
            )
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
            # find field
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
    except Exception as e:
        print(f"❌ Failed to update event message: {e}")

# ---------- DM Reminder ----------
async def send_dm_reminder(user_id, event_name, event_start):
    try:
        user = await bot.fetch_user(user_id)
        if user:
            ts = int(event_start.timestamp())
            await user.send(f"⏰ Reminder: **{event_name}** starts at <t:{ts}:f> (**<t:{ts}:R>**)")
    except discord.Forbidden:
        print(f"⚠️ Could not DM user {user_id} — DMs disabled.")

# ---------- Disabled View for canceled events ----------
class DisabledSignupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        # add disabled clones of the two buttons for clarity
        self.add_item(discord.ui.Button(label="✅ Sign Up", style=discord.ButtonStyle.success, disabled=True))
        self.add_item(discord.ui.Button(label="❌ Sign Out", style=discord.ButtonStyle.danger, disabled=True))

# ---------- Create Event Modal & Panel ----------
class CreateEventModal(discord.ui.Modal, title="Create Event"):
    def __init__(self):
        super().__init__(timeout=None)
        self.name_input = discord.ui.TextInput(label="Event name", placeholder="Raid Full Clear", required=True, max_length=100)
        self.start_input = discord.ui.TextInput(label="Start (YYYY-MM-DD HH:MM)", placeholder="2025-08-12 18:00", required=True)
        self.duration_input = discord.ui.TextInput(label="Duration (e.g., 2h30m, 45m, 3h)", placeholder="2h30m", required=True)
        self.add_item(self.name_input)
        self.add_item(self.start_input)
        self.add_item(self.duration_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            created_name = await create_event_common(
                interaction,
                self.name_input.value.strip(),
                self.start_input.value.strip(),
                self.duration_input.value.strip()
            )
            await interaction.response.send_message(f"✅ Event **{created_name}** created.", ephemeral=True)
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
            cur = await db.execute(
                "SELECT 1 FROM signups WHERE event_name = ? AND user_id = ?",
                (self.event_name, user_id),
            )
            if await cur.fetchone():
                await interaction.response.defer()
                await update_event_message(self.event_name, self.channel_id, self.message_id)
                return
            await db.execute(
                "INSERT INTO signups (event_name, user_id) VALUES (?, ?)",
                (self.event_name, user_id),
            )
            await db.commit()

        # schedule DM
        job_id = reminder_job_id(self.event_name, user_id)
        now = datetime.datetime.utcnow()
        reminder_time = self.event_start - datetime.timedelta(minutes=15)
        if reminder_time > now:
            scheduler.add_job(
                send_dm_reminder,
                'date',
                id=job_id,
                replace_existing=True,
                run_date=reminder_time,
                args=[user_id, self.event_name, self.event_start],
            )
        else:
            await send_dm_reminder(user_id, self.event_name, self.event_start)

        await interaction.response.defer()
        await update_event_message(self.event_name, self.channel_id, self.message_id)

    @discord.ui.button(label="❌ Sign Out", style=discord.ButtonStyle.danger, custom_id="signout_button")
    async def signout(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        async with aiosqlite.connect("events.db") as db:
            await db.execute(
                "DELETE FROM signups WHERE event_name = ? AND user_id = ?",
                (self.event_name, user_id),
            )
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
async def create_event_common(interaction: discord.Interaction, name: str, start_time: str, duration_str: str):
    await ensure_tables()
    event_start = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M")
    duration_td = parse_duration(duration_str)
    if duration_td is None:
        raise ValueError("Invalid duration format. Use `2h30m`, `45m`, etc.")
    event_end = event_start + duration_td

    ts = int(event_start.timestamp())
    embed = discord.Embed(
        title=f"📅 Event Scheduled: {name}",
        description=f"**Start:** <t:{ts}:f>\n**Duration:** {duration_str}",
        color=discord.Color.blue(),
    )
    embed.add_field(name="🧑‍🤝‍🧑 Sign-ups", value="_No one has signed up yet._", inline=False)
    embed.set_footer(text="Click the button to sign up. You’ll get a DM 15 minutes before start!")

    # Send without view to get message id, then attach the view
    msg = await interaction.channel.send(embed=embed, view=None)
    view = SignupView(name, event_start, interaction.channel_id, msg.id)
    await msg.edit(view=view)

    # Store the event with message_id (migration-safe)
    async with aiosqlite.connect("events.db") as db:
        # ensure message_id column exists (migration)
        try:
            await db.execute("ALTER TABLE events ADD COLUMN message_id INTEGER")
        except Exception:
            pass
        await db.execute("""
            INSERT INTO events (name, user_id, channel_id, start_time, end_time, message_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, interaction.user.id, interaction.channel_id, event_start.isoformat(), event_end.isoformat(), msg.id))
        await db.commit()

    # Schedule announcements with predictable IDs
    scheduler.add_job(
        start_event, 'date',
        id=start_job_id(name, interaction.channel_id),
        replace_existing=True,
        run_date=event_start,
        args=[name, interaction.channel_id]
    )
    scheduler.add_job(
        end_event, 'date',
        id=end_job_id(name, interaction.channel_id),
        replace_existing=True,
        run_date=event_end,
        args=[name, interaction.channel_id]
    )

    return name

# ---------- Post creator panel ----------
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

        cur = await db.execute(
            "SELECT 1 FROM signups WHERE event_name = ? AND user_id = ?",
            (name, user_id),
        )
        if await cur.fetchone():
            await interaction.response.send_message("❗ You’re already signed up.", ephemeral=True)
            return

        await db.execute("INSERT INTO signups (event_name, user_id) VALUES (?, ?)", (name, user_id))
        await db.commit()

    await interaction.response.send_message(f"✅ Signed up for **{name}**.", ephemeral=True)

    # Update list if we have message/channel
    if channel_id and message_id:
        await update_event_message(name, channel_id, message_id)

    # Schedule DM
    now = datetime.datetime.utcnow()
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
        await ch.send(f"🚀 **Event '{name}'** has started!")

async def end_event(name, channel_id):
    ch = bot.get_channel(channel_id)
    if ch:
        await ch.send(f"✅ **Event '{name}'** has ended. Processing logs and generating summary...")

# ---------- CANCEL EVENT ----------
@bot.tree.command(name="cancel_event", description="Cancel an event (creator or admin)")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(name="Event name to cancel")
async def cancel_event(interaction: discord.Interaction, name: str):
    # find event
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
            print(f"⚠️ Could not edit event message: {e}")

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
