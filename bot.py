import discord
from discord.ext import commands
from discord import app_commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import aiosqlite
import datetime
import os
from dotenv import load_dotenv
import re

# === Load .env config ===
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = 725430860421136404  # Replace with your real server ID

# === Set up bot ===
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
scheduler = AsyncIOScheduler()

# === Duration Parser ===
def parse_duration(duration_str: str) -> datetime.timedelta | None:
    duration_str = duration_str.lower().replace(" ", "")
    pattern = r"(?:(\d+)h)?(?:(\d+)m)?"
    match = re.match(pattern, duration_str)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0

    if hours == 0 and minutes == 0:
        return None

    return datetime.timedelta(hours=hours, minutes=minutes)

# === On bot ready ===
@bot.event
async def on_ready():
    guild = discord.Object(id=GUILD_ID)
    synced = await bot.tree.sync(guild=guild)
    scheduler.start()
    print(f"Bot is ready as {bot.user} — Synced {len(synced)} commands to GUILD.")

# === View with Sign Up Button ===
class SignupView(discord.ui.View):
    def __init__(self, event_name: str, event_start: datetime.datetime):
        super().__init__(timeout=None)
        self.event_name = event_name
        self.event_start = event_start

    @discord.ui.button(label="✅ Sign Up", style=discord.ButtonStyle.success, custom_id="signup_button")
    async def signup(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id

        async with aiosqlite.connect("events.db") as db:
            # Check if already signed up
            cursor = await db.execute(
                "SELECT * FROM signups WHERE event_name = ? AND user_id = ?",
                (self.event_name, user_id)
            )
            if await cursor.fetchone():
                await interaction.response.send_message(
                    "❗ You are already signed up for this event.",
                    ephemeral=True
                )
                return

            # Register user
            await db.execute(
                "INSERT INTO signups (event_name, user_id) VALUES (?, ?)",
                (self.event_name, user_id)
            )
            await db.commit()

        await interaction.response.send_message(
            f"✅ You’ve signed up for **{self.event_name}**!",
            ephemeral=True
        )

        # Schedule DM reminder
        now = datetime.datetime.utcnow()
        reminder_time = self.event_start - datetime.timedelta(minutes=15)
        if reminder_time > now:
            scheduler.add_job(
                send_dm_reminder,
                'date',
                run_date=reminder_time,
                args=[user_id, self.event_name, self.event_start]
            )
        else:
            await send_dm_reminder(user_id, self.event_name, self.event_start)

# === Schedule Event Command ===
@bot.tree.command(name="schedule_event", description="Schedule a raid event with flexible duration")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(
    name="Name of the event",
    start_time="When to start (e.g., 2025-08-08 19:00)",
    duration_str="Duration (e.g., 10m, 2h, 1h30m)"
)
async def schedule_event(interaction: discord.Interaction, name: str, start_time: str, duration_str: str):
    try:
        event_start = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M")
        duration_td = parse_duration(duration_str)
        if duration_td is None:
            raise ValueError("Invalid duration format. Use formats like `2h30m`, `45m`, etc.")
        event_end = event_start + duration_td

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
            await db.execute("""
                INSERT INTO events (name, user_id, channel_id, start_time, end_time)
                VALUES (?, ?, ?, ?, ?)
            """, (name, interaction.user.id, interaction.channel_id, event_start.isoformat(), event_end.isoformat()))
            await db.commit()

        unix_ts = int(event_start.timestamp())
        embed = discord.Embed(
            title=f"📅 Event Scheduled: {name}",
            description=(
                f"**Start:** <t:{unix_ts}:f>\n"
                f"**Duration:** {duration_str}\n\n"
                f"Click the button below to sign up for a reminder!"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="You will receive a DM 15 minutes before it starts.")

        view = SignupView(name, event_start)

        await interaction.channel.send(embed=embed, view=view)
        await interaction.response.send_message(
            f"✅ Event **{name}** created and sign-up message sent.",
            ephemeral=True
        )

        scheduler.add_job(start_event, 'date', run_date=event_start, args=[name, interaction.channel_id])
        scheduler.add_job(end_event, 'date', run_date=event_end, args=[name, interaction.channel_id])

    except ValueError as e:
        await interaction.response.send_message(f"❌ {str(e)}", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Unexpected error: {str(e)}", ephemeral=True)

# === Manual Signup Command ===
@bot.tree.command(name="signup_event", description="Sign up for a scheduled event to receive a reminder")
@app_commands.guilds(discord.Object(id=GUILD_ID))
@app_commands.describe(name="Name of the event to join")
async def signup_event(interaction: discord.Interaction, name: str):
    user_id = interaction.user.id

    async with aiosqlite.connect("events.db") as db:
        cursor = await db.execute("SELECT start_time FROM events WHERE name = ?", (name,))
        row = await cursor.fetchone()
        if not row:
            await interaction.response.send_message(f"❌ Event **{name}** not found.", ephemeral=True)
            return

        start_time = datetime.datetime.fromisoformat(row[0])

        # Check if already signed up
        cursor = await db.execute(
            "SELECT * FROM signups WHERE event_name = ? AND user_id = ?", (name, user_id)
        )
        if await cursor.fetchone():
            await interaction.response.send_message("❗ You are already signed up for this event.", ephemeral=True)
            return

        await db.execute("INSERT INTO signups (event_name, user_id) VALUES (?, ?)", (name, user_id))
        await db.commit()

    await interaction.response.send_message(f"✅ You have signed up for **{name}**!", ephemeral=True)

    # Schedule DM Reminder
    now = datetime.datetime.utcnow()
    delay_until = start_time - datetime.timedelta(minutes=15)
    if delay_until > now:
        scheduler.add_job(send_dm_reminder, 'date', run_date=delay_until, args=[user_id, name, start_time])
    else:
        await send_dm_reminder(user_id, name, start_time)

# === DM Reminder ===
async def send_dm_reminder(user_id, event_name, event_start):
    try:
        user = await bot.fetch_user(user_id)
        if user:
            event_unix = int(event_start.timestamp())
            await user.send(
                f"⏰ Reminder: **{event_name}** starts at <t:{event_unix}:f> (**<t:{event_unix}:R>**)"
            )
    except discord.Forbidden:
        print(f"⚠️ Could not DM user {user_id} — they may have DMs disabled.")

# === Start Event Handler ===
async def start_event(name, channel_id):
    channel = bot.get_channel(channel_id)
    if channel:
        try:
            await channel.send(f"🚀 **Event '{name}'** has started! Start logging those kills!")
        except discord.Forbidden:
            print(f"❌ Missing access to channel {channel_id} for start_event.")

# === End Event Handler ===
async def end_event(name, channel_id):
    channel = bot.get_channel(channel_id)
    if channel:
        try:
            await channel.send(f"✅ **Event '{name}'** has ended. Processing logs and generating summary...")
        except discord.Forbidden:
            print(f"❌ Missing access to channel {channel_id} for end_event.")

# === Run the bot ===
bot.run(TOKEN)
