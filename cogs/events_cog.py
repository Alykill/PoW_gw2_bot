from __future__ import annotations
import os, re, datetime, logging, discord
from discord import app_commands
from discord.ext import commands, tasks
from typing import Optional, Tuple, Dict, List
from config import settings
from infra.scheduler import scheduler
import aiosqlite
from ui.views import CreateEventModal, EventCreatorView, EditEventModal, EventMessageView, EventFinalizeView
from ui.embeds import build_summary_embed
from repos.sqlite_repo import ensure_tables, SqliteEventRepo, SqliteSignupRepo
from services.session import EventSession
from services.upload_service import process_pending_uploads
from analytics.service import (
    ensure_enriched_for_event,
    build_event_analytics_embeds,
    _coerce_payload_to_json,
    enrich_upload,
)

log = logging.getLogger("events")

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
    naive = datetime.datetime.strptime(dt_str, fmt)
    local_tz = datetime.datetime.now().astimezone().tzinfo
    local_aware = naive.replace(tzinfo=local_tz)
    return local_aware.astimezone(datetime.timezone.utc)

def parse_duration(duration_str: str) -> Optional[datetime.timedelta]:
    duration_str = duration_str.lower().replace(" ", "")
    match = re.match(r"(?:(\d+)h)?(?:(\d+)m)?", duration_str)
    if not match:
        return None
    h = int(match.group(1)) if match.group(1) else 0
    m = int(match.group(2)) if match.group(2) else 0
    if h == 0 and m == 0:
        return None
    return datetime.timedelta(hours=h, minutes=m)

def is_metrics_role(*, role_ids: set[int] = set(), role_names: set[str] = {"raid leader"}):
    """
    Passes only if the invoker has ANY of the given roles.
    No Administrator bypass.
    Role-name checks are case-insensitive.
    """
    lowered = {n.lower() for n in role_names}

    def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("Run this in a server, not DMs.")
        roles = getattr(interaction.user, "roles", [])
        if any(r.id in role_ids for r in roles):
            return True
        if any((r.name or "").lower() in lowered for r in roles):
            return True
        raise app_commands.MissingPermissions(["required role"])
    return app_commands.check(predicate)

# active sessions keyed by (event_name, channel_id)
active_sessions: Dict[Tuple[str, int], EventSession] = {}

async def _get_event_row(db_path: str, name: str, channel_id: int):
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            "SELECT id, name, user_id, channel_id, start_time, end_time, message_id FROM events WHERE name=? AND channel_id=?",
            (name, channel_id),
        )
        return await cur.fetchone()

class EventsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.event_repo = SqliteEventRepo(settings.SQLITE_PATH)
        self.signup_repo = SqliteSignupRepo(settings.SQLITE_PATH)
        self._pending_worker.start()

    def cog_unload(self):
        self._pending_worker.cancel()

    @tasks.loop(seconds=settings.PENDING_SCAN_SECONDS)
    async def _pending_worker(self):
        await process_pending_uploads()

    @_pending_worker.before_loop
    async def _before(self):
        await self.bot.wait_until_ready()
        await ensure_tables(settings.SQLITE_PATH)

    @commands.Cog.listener()
    async def on_ready(self):
        log.info("EventsCog ready")
        try:
            await self._rehydrate_jobs()
            await self._rehydrate_views()
            log.info("Rehydrated scheduled jobs and views")
        except Exception as e:
            log.exception(f"Rehydrate failed: {e}")

    # -------- Slash commands --------

    @app_commands.command(name="manage_events", description="Open the event management panel")
    async def slash_manage_events(self, interaction: discord.Interaction):
        await self._send_event_panel(interaction)

    @app_commands.command(name="event_panel", description="Open the event creation panel")
    async def slash_event_panel(self, interaction: discord.Interaction):
        await self._send_event_panel(interaction)

    @app_commands.command(name="reprocess_pending", description="Process pending uploads now")
    async def reprocess_pending(self, interaction: discord.Interaction):
        await process_pending_uploads()
        await interaction.response.send_message("Pending uploads processed.", ephemeral=True)

    # ---- /import_log_event ----
    @app_commands.guild_only()
    @is_metrics_role(role_names={"Raid Leader"})  # role-only
    @app_commands.command(
        name="import_log_event",
        description="Attach one or more dps.report links to an event and post Summary & Analytics."
    )
    async def import_log_event(
            self,
            interaction: discord.Interaction,
            event_name: str,
            permalinks: str,  # paste one or many URLs separated by spaces/commas/newlines
            channel: Optional[discord.TextChannel] = None,
            use_log_time: bool = True,
    ):
        """Create (or reuse) an event, import EI reports into it, enrich, and post embeds."""
        await interaction.response.defer(ephemeral=True)

        # 0) Parse URLs
        urls = [u for u in re.split(r"[,\s]+", (permalinks or "").strip()) if u]
        urls = list(dict.fromkeys(urls))  # de-dupe keep order
        if not urls:
            await interaction.followup.send("❌ No valid URLs provided.", ephemeral=True)
            return

        imported: list[tuple[str, int]] = []  # (boss_name, upload_id)
        boss_names: list[str] = []
        earliest: Optional[datetime.datetime] = None
        latest: Optional[datetime.datetime] = None

        # helper to parse EI ISO-ish timestamps safely
        def _parse_ts(s: Optional[str]) -> Optional[datetime.datetime]:
            if not s:
                return None
            s = s.replace("Z", "+00:00").replace(" UTC", "+00:00")
            try:
                dt = datetime.datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt.astimezone(datetime.timezone.utc)
            except Exception:
                return None

        # 1) Ensure event row exists (message_id=0 is fine)
        ch = channel or interaction.channel  # type: ignore
        assert isinstance(ch, discord.TextChannel)
        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO events (name, user_id, channel_id, start_time, end_time, message_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (event_name, interaction.user.id, ch.id,
                 (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)).isoformat(),
                 (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=8)).isoformat(),
                 0),
            )
            await db.commit()

        # 2) For each permalink: fetch JSON, extract info, insert/attach upload
        for url in urls:
            try:
                j = await _coerce_payload_to_json(url)
            except Exception as e:
                boss_names.append(f"(load failed: {url})")
                continue

            enc = (j.get("encounter") or {})
            boss_name = str(enc.get("boss") or j.get("boss") or j.get("fightName") or "Unknown Encounter")
            boss_id = int(enc.get("bossId") or -1)
            success = 1 if bool(enc.get("success")) else 0

            start_dt = _parse_ts(j.get("timeStartStd")) or _parse_ts(j.get("timeStart"))
            end_dt = _parse_ts(j.get("timeEndStd")) or _parse_ts(j.get("timeEnd"))
            # fallback times if EI didn’t provide them
            if not start_dt:
                start_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
            if not end_dt:
                end_dt = start_dt + datetime.timedelta(minutes=8)

            earliest = min(earliest, start_dt) if earliest else start_dt
            latest = max(latest, end_dt) if latest else end_dt

            async with aiosqlite.connect(settings.SQLITE_PATH) as db:
                # avoid duplicate uploads by permalink
                cur = await db.execute("SELECT id FROM uploads WHERE permalink = ?", (url,))
                row = await cur.fetchone()
                if row:
                    upload_id = int(row[0])
                    await db.execute("UPDATE uploads SET event_name = ? WHERE id = ?", (event_name, upload_id))
                else:
                    cur = await db.execute(
                        """
                        INSERT INTO uploads (event_name, boss_id, boss_name, success, permalink, file_path, time_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (event_name, boss_id, boss_name, success, url, None, start_dt.isoformat()),
                    )
                    upload_id = int(cur.lastrowid or 0)
                await db.commit()

            imported.append((boss_name, upload_id))
            boss_names.append(boss_name)

        # 3) Expand event time range to cover all logs (optional)
        if use_log_time and (earliest and latest):
            try:
                async with aiosqlite.connect(settings.SQLITE_PATH) as db:
                    await db.execute(
                        """
                        UPDATE events
                        SET start_time = CASE
                            WHEN start_time IS NULL THEN ?
                            WHEN datetime(start_time) > datetime(?) THEN ?
                            ELSE start_time END,
                            end_time = CASE
                            WHEN end_time IS NULL THEN ?
                            WHEN datetime(end_time) < datetime(?) THEN ?
                            ELSE end_time END
                        WHERE name = ? AND channel_id = ?
                        """,
                        (earliest.isoformat(), earliest.isoformat(), earliest.isoformat(),
                         latest.isoformat(), latest.isoformat(), latest.isoformat(),
                         event_name, ch.id)
                    )
                    await db.commit()
            except Exception:
                pass

        # 4) Enrich newly added uploads (only those with no metrics)
        try:
            _ = await ensure_enriched_for_event(event_name)
        except Exception as e:
            await interaction.followup.send(f"⚠️ Enrichment warning: `{e}`", ephemeral=True)

        # 5) Post Summary & Analytics
        try:
            results = await self._load_results_for_summary(event_name)
            summary = await build_summary_embed(
                event_name,
                results,
                event_start_utc=earliest or datetime.datetime.now(datetime.timezone.utc),
                event_end_utc=latest or (earliest or datetime.datetime.now(datetime.timezone.utc)) + datetime.timedelta(
                    minutes=8),
            )
            await ch.send(embed=summary)

            analytics = await build_event_analytics_embeds(event_name)
            for em in analytics:
                await ch.send(embed=em)
        except Exception as e:
            await ch.send(f"⚠️ Posting failed: `{e}`")

        # 6) Ephemeral ack
        ok = sum(1 for _, uid in imported if uid)
        await interaction.followup.send(
            f"✅ Imported **{ok}/{len(urls)}** logs into **{event_name}**.\n"
            f"Bosses: {', '.join(boss_names)}",
            ephemeral=True
        )

    # ---- /rebuild_metrics ----
    @app_commands.guild_only()
    @is_metrics_role(role_names={"Raid Leader"})  # role-only
    @app_commands.command(
        name="rebuild_metrics",
        description="Re-enrich EI metrics for an event and (optionally) repost Summary & Analytics."
    )
    async def rebuild_metrics(
        self,
        interaction: discord.Interaction,
        event_name: str,
        repost: bool = True,
        channel: Optional[discord.TextChannel] = None,
    ):
        await interaction.response.defer(ephemeral=True)

        # Re-enrich ALL uploads for this event
        processed = 0
        failed = 0
        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            cur = await db.execute("""
                SELECT id, COALESCE(NULLIF(permalink, ''), file_path) AS src
                FROM uploads
                WHERE event_name = ?
                ORDER BY datetime(time_utc) ASC
            """, (event_name,))
            rows = await cur.fetchall()

        for upload_id, src in rows:
            try:
                await enrich_upload(int(upload_id), str(src or ""))
                processed += 1
            except Exception:
                failed += 1

        posted = False
        if repost:
            try:
                target = channel or interaction.channel  # type: ignore
                results = await self._load_results_for_summary(event_name)
                summary_embed = await build_summary_embed(event_name, results)
                if summary_embed and isinstance(target, discord.TextChannel):
                    await target.send(embed=summary_embed)

                analytics_embeds = await build_event_analytics_embeds(event_name)
                if analytics_embeds and isinstance(target, discord.TextChannel):
                    for em in analytics_embeds:
                        await target.send(embed=em)
                posted = True
            except Exception as e:
                await interaction.followup.send(
                    f"✅ Rebuilt metrics for **{event_name}**. Re-enriched {processed} upload(s)"
                    + (f", {failed} failed" if failed else "")
                    + f". ⚠️ Couldn’t repost Summary/Analytics: `{e}`",
                    ephemeral=True,
                )
                return

        msg = f"✅ Rebuilt metrics for **{event_name}**. Re-enriched {processed} upload(s)"
        if failed:
            msg += f", {failed} failed"
        if repost:
            msg += " — posted updated Summary & Analytics." if posted else " — nothing to repost."
        await interaction.followup.send(msg, ephemeral=True)

    # -------- Legacy prefix command (optional) --------
    @commands.command(name="event_panel")
    async def event_panel(self, ctx: commands.Context):
        await ctx.send("Use the button to create a new event:", view=EventCreatorView(self._open_modal))

    # -------- Internal handlers --------
    async def _load_results_for_summary(self, event_name: str) -> List[dict]:
        """
        Build a minimal 'results' array from DB so build_summary_embed works
        even if the process was rebooted (no in-memory EventSession).
        """
        rows: List[tuple] = []
        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            cur = await db.execute("""
                SELECT boss_id, boss_name, success, COALESCE(NULLIF(permalink,''), file_path), time_utc
                FROM uploads
                WHERE event_name = ?
                ORDER BY datetime(time_utc) ASC
            """, (event_name,))
            rows = await cur.fetchall()

        results: List[dict] = []
        for boss_id, boss_name, success, src, time_utc in rows:
            results.append({
                "encounter": {
                    "bossId": int(boss_id or -1),
                    "boss": boss_name or "Unknown Encounter",
                    "success": bool(success),
                },
                "permalink": src or "",
                "timeStart": time_utc or "",
            })
        return results

    def _parse_iso_utc(self, s: str | None) -> datetime.datetime | None:
        if not s:
            return None
        try:
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        except Exception:
            return None

    async def _rehydrate_jobs(self):
        """
        On bot startup, reconstruct start/end jobs, resume active sessions for
        in-progress events, and (re)schedule T-15 reminders for signups.
        """
        await ensure_tables(settings.SQLite_PATH if hasattr(settings, "SQLite_PATH") else settings.SQLITE_PATH)
        now = datetime.datetime.now(datetime.timezone.utc)

        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            cur = await db.execute("""
                SELECT name, user_id, channel_id, start_time, end_time, message_id
                FROM events
            """)
            rows = await cur.fetchall()

            for name, creator_id, channel_id, start_iso, end_iso, message_id in rows:
                start_dt = self._parse_iso_utc(start_iso)
                end_dt = self._parse_iso_utc(end_iso)
                if not start_dt or not end_dt:
                    continue
                if end_dt <= now:
                    continue

                try:
                    scheduler.remove_job(f"end:{name}:{channel_id}")
                except Exception:
                    pass
                scheduler.add_job(
                    self._end_event, 'date',
                    id=f"end:{name}:{channel_id}", replace_existing=True,
                    run_date=end_dt, args=[name, channel_id]
                )

                if now < start_dt:
                    try:
                        scheduler.remove_job(f"start:{name}:{channel_id}")
                    except Exception:
                        pass
                    scheduler.add_job(
                        self._start_event, 'date',
                        id=f"start:{name}:{channel_id}", replace_existing=True,
                        run_date=start_dt, args=[name, channel_id]
                    )

                    cur2 = await db.execute("SELECT user_id FROM signups WHERE event_name = ?", (name,))
                    for (uid,) in await cur2.fetchall():
                        remind_at = start_dt - datetime.timedelta(minutes=15)
                        if remind_at > now:
                            scheduler.add_job(
                                send_dm_reminder, 'date',
                                id=f"reminder:{uid}:{name}", replace_existing=True,
                                run_date=remind_at,
                                args=[self.bot, int(uid), name, start_dt]
                            )
                else:
                    if settings.LOG_DIR and os.path.isdir(settings.LOG_DIR):
                        active_sessions[(name, channel_id)] = EventSession(
                            name, start_dt, end_dt, channel_id, settings.LOG_DIR
                        )
                        active_sessions[(name, channel_id)].start_task()

    async def _rehydrate_views(self):
        """Re-register persistent views for all not-yet-ended events so buttons work after reboot."""
        now = datetime.datetime.now(datetime.timezone.utc)
        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            cur = await db.execute(
                "SELECT name, channel_id, message_id, start_time, end_time "
                "FROM events WHERE end_time > ?", (now.isoformat(),)
            )
            rows = await cur.fetchall()

        for name, channel_id, message_id, start_iso, end_iso in rows:
            start_dt = self._parse_iso_utc(start_iso) or now
            view = EventMessageView(
                name, start_dt, channel_id, message_id,
                self._on_signup, self._on_signout,
                self._on_edit_request, self._on_cancel, self._on_end_now
            )
            self.bot.add_view(view, message_id=message_id)

    async def _open_modal(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateEventModal(self._handle_create_submit))

    async def _handle_create_submit(self, interaction: discord.Interaction, name: str, start: str, duration: str):
        try:
            if not settings.LOG_DIR:
                await interaction.response.send_message("❌ `LOG_DIR` is not set in `.env`.", ephemeral=True)
                return

            await interaction.response.send_message(
                f"Pick a channel to post **{name}**:",
                view=EventFinalizeView(name, start, duration, self._finalize_event_creation),
                ephemeral=True
            )
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Unexpected error: {e}", ephemeral=True)

    async def _finalize_event_creation(self, interaction: discord.Interaction, view: EventFinalizeView):
        ch = interaction.client.get_channel(view.selected_channel_id) or await interaction.client.fetch_channel(view.selected_channel_id)
        if not isinstance(ch, discord.TextChannel):
            await interaction.followup.send("Selected channel is not a text channel.", ephemeral=True)
            return

        created = await self._create_event_common(
            interaction,
            view.name,
            view.start,
            view.duration,
            target_channel=ch
        )

        await interaction.followup.send(f"✅ Event **{created}** created in {ch.mention}.", ephemeral=True)

    async def _create_event_common(self, interaction: discord.Interaction, name: str, start_time: str, duration_str: str, target_channel: Optional[discord.TextChannel] = None):
        await ensure_tables(settings.SQLITE_PATH)
        event_start_utc = local_str_to_utc(start_time, "%Y-%m-%d %H:%M")
        duration_td = parse_duration(duration_str)
        if duration_td is None:
            raise ValueError("Invalid duration format. Use `2h30m`, `45m`, etc.")
        event_end_utc = event_start_utc + duration_td

        ts = int(event_start_utc.timestamp())
        embed = discord.Embed(
            title=f"📅 Event Scheduled: {name}",
            description=f"**Start:** <t:{ts}:f>\n**Duration:** {duration_str}",
            color=discord.Color.blue(),
        )
        embed.add_field(name="🧑‍🤝‍🧑 Sign-ups", value="_No one has signed up yet._", inline=False)
        embed.set_footer(text="Click the button to sign up. You’ll get a DM 15 minutes before start!")

        channel = target_channel or interaction.channel
        msg = await channel.send(embed=embed, view=None)
        view = EventMessageView(
            name, event_start_utc, channel.id, msg.id,
            self._on_signup, self._on_signout,
            self._on_edit_request, self._on_cancel, self._on_end_now
        )
        await msg.edit(view=view)
        self.bot.add_view(view, message_id=msg.id)

        await self.event_repo.create(name, interaction.user.id, channel.id, event_start_utc.isoformat(), event_end_utc.isoformat(), msg.id)

        scheduler.add_job(self._start_event, 'date', id=f"start:{name}:{channel.id}", replace_existing=True, run_date=event_start_utc, args=[name, channel.id])
        scheduler.add_job(self._end_event,   'date', id=f"end:{name}:{channel.id}",   replace_existing=True, run_date=event_end_utc,   args=[name, channel.id])

        if settings.LOG_DIR and os.path.isdir(settings.LOG_DIR):
            active_sessions[(name, channel.id)] = EventSession(name, event_start_utc, event_end_utc, channel.id, settings.LOG_DIR)
        return name

    async def _update_event_message(self, event_name: str, channel_id: int, message_id: int):
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        try:
            message = await channel.fetch_message(message_id)
            guild = message.guild
            user_ids = await self.signup_repo.list_names(event_name)
            names = []
            for uid in user_ids:
                member = guild.get_member(uid) or await guild.fetch_member(uid)
                if member:
                    names.append(member.display_name)
                else:
                    user = await self.bot.fetch_user(uid)
                    names.append(user.global_name or user.name)
            list_text = "\n".join(f"• {n}" for n in names) if names else "_No one has signed up yet._"
            if message.embeds:
                em = message.embeds[0]; idx = None
                for i, f in enumerate(em.fields):
                    if f.name == "🧑‍🤝‍🧑 Sign-ups":
                        idx = i; break
                if idx is None:
                    em.add_field(name="🧑‍🤝‍🧑 Sign-ups", value=list_text, inline=False)
                else:
                    em.set_field_at(idx, name="🧑‍🤝‍🧑 Sign-ups", value=list_text, inline=False)
                await message.edit(embed=em)
        except Exception:
            pass

    async def _on_signup(self, interaction: discord.Interaction, view: EventMessageView):
        await self.signup_repo.add(view.event_name, interaction.user.id)

        row = await _get_event_row(settings.SQLITE_PATH, view.event_name, view.channel_id)
        if row:
            start_iso = row[4]
            event_start = datetime.datetime.fromisoformat(start_iso)
            if event_start.tzinfo is None:
                event_start = event_start.replace(tzinfo=datetime.timezone.utc)
        else:
            event_start = view.event_start

        now = datetime.datetime.now(datetime.timezone.utc)
        reminder_time = event_start - datetime.timedelta(minutes=15)
        job_id = f"reminder:{interaction.user.id}:{view.event_name}"

        if reminder_time > now:
            scheduler.add_job(
                send_dm_reminder, 'date', id=job_id, replace_existing=True,
                run_date=reminder_time,
                args=[self.bot, interaction.user.id, view.event_name, event_start]
            )
        else:
            try:
                user = await self.bot.fetch_user(interaction.user.id)
                if user:
                    ts = int(event_start.timestamp())
                    await user.send(f"🔔 You’re signed up for **{view.event_name}**. It starts <t:{ts}:R>.")
            except discord.Forbidden:
                pass

        await interaction.response.defer()
        await self._update_event_message(view.event_name, view.channel_id, view.message_id)

    async def _on_signout(self, interaction: discord.Interaction, view: EventMessageView):
        await self.signup_repo.remove(view.event_name, interaction.user.id)
        try:
            scheduler.remove_job(f"reminder:{interaction.user.id}:{view.event_name}")
        except Exception:
            pass
        await interaction.response.defer()
        await self._update_event_message(view.event_name, view.channel_id, view.message_id)

    async def _start_event(self, name: str, channel_id: int):
        ch = self.bot.get_channel(channel_id)
        if ch:
            await ch.send(f"📢 **Event '{name}'** has started!")

        try:
            user_ids = await self.signup_repo.list_names(name)
            for uid in user_ids:
                try:
                    user = await self.bot.fetch_user(uid)
                    if user:
                        await user.send(f"▶️ **{name}** is starting now.")
                except discord.Forbidden:
                    pass
        except Exception:
            pass

        sess = active_sessions.get((name, channel_id))
        if not sess:
            row = await _get_event_row(settings.SQLITE_PATH, name, channel_id)
            if row:
                _, _n, _creator, _ch, start_iso, end_iso, _msg = row
                start_dt = self._parse_iso_utc(start_iso)
                end_dt = self._parse_iso_utc(end_iso)
                if start_dt and end_dt and settings.LOG_DIR and os.path.isdir(settings.LOG_DIR):
                    sess = EventSession(name, start_dt, end_dt, channel_id, settings.LOG_DIR)
                    active_sessions[(name, channel_id)] = sess

        if sess:
            sess.start_task()

    async def _send_event_panel(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Use the button to create a new event:",
            view=EventCreatorView(self._open_modal),
            ephemeral=True
        )

    async def _end_event(self, name: str, channel_id: int, end_override_utc: datetime.datetime | None = None):
        ch = self.bot.get_channel(channel_id)
        sess = active_sessions.pop((name, channel_id), None)

        row = await _get_event_row(settings.SQLITE_PATH, name, channel_id)
        start_dt = end_dt = None
        if row:
            _, _n, _creator, _ch, start_iso, end_iso, _msg = row
            start_dt = self._parse_iso_utc(start_iso)
            end_dt = self._parse_iso_utc(end_iso)

        if end_override_utc:
            end_dt = end_override_utc

        if ch:
            await ch.send(f"✅ **Event '{name}'** has ended. I’m finalizing uploads… (a few minutes)")

        try:
            await ensure_tables(settings.SQLITE_PATH)
        except Exception as e:
            if ch:
                await ch.send(f"⚠️ DB init failed: `{e}`")

        try:
            await process_pending_uploads()
        except Exception:
            pass

        try:
            await ensure_enriched_for_event(name)
        except Exception as e:
            if ch:
                await ch.send(f"⚠️ Analytics bootstrap failed: `{e}`")

        try:
            if sess:
                await sess.stop_task()
                summary_embed = await build_summary_embed(
                    name,
                    sess.results,
                    event_start_utc=start_dt or sess.start,
                    event_end_utc=end_dt or sess.end,
                )
            else:
                results = await self._load_results_for_summary(name)
                safe_end = end_dt or datetime.datetime.now(datetime.timezone.utc)
                summary_embed = await build_summary_embed(
                    name,
                    results,
                    event_start_utc=start_dt,
                    event_end_utc=safe_end,
                )

            if ch and summary_embed:
                await ch.send(embed=summary_embed)
        except Exception as e:
            if ch:
                await ch.send(f"⚠️ Summary failed: `{e}`")

        try:
            embeds = await build_event_analytics_embeds(name)
            if ch and embeds:
                for em in embeds:
                    await ch.send(embed=em)
        except Exception as e:
            if ch:
                await ch.send(f"⚠️ Analytics failed: `{e}`")

    async def _is_owner_or_admin(self, interaction: discord.Interaction, event_creator_id: int) -> bool:
        if interaction.user.id == event_creator_id:
            return True
        try:
            member = interaction.guild.get_member(interaction.user.id) or await interaction.guild.fetch_member(interaction.user.id)
        except discord.NotFound:
            return False
        return bool(member.guild_permissions.administrator)

    async def _on_edit_request(self, interaction: discord.Interaction, view: EventMessageView):
        row = await _get_event_row(settings.SQLITE_PATH, view.event_name, view.channel_id)
        if not row:
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return
        _, _name, creator_id, _ch, start_iso, end_iso, _msg = row

        if not await self._is_owner_or_admin(interaction, creator_id):
            await interaction.response.send_message("You don’t have permission to edit this event.", ephemeral=True)
            return

        now = datetime.datetime.now(datetime.timezone.utc)
        start_dt = datetime.datetime.fromisoformat(start_iso)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=datetime.timezone.utc)
        if now >= start_dt:
            await interaction.response.send_message("This event has already started. Use **End Now** or **Cancel**.", ephemeral=True)
            return

        try:
            end_dt = datetime.datetime.fromisoformat(end_iso)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=datetime.timezone.utc)
            dur_td = end_dt - start_dt
            h = dur_td.seconds // 3600
            m = (dur_td.seconds % 3600) // 60
            dur_str = (f"{h}h" if h else "") + (f"{m}m" if m else ("0m" if not h else ""))
        except Exception:
            dur_str = "1h"

        start_str_local = start_dt.astimezone().strftime("%Y-%m-%d %H:%M")
        await interaction.response.send_modal(EditEventModal(view.event_name, start_str_local, dur_str, self._on_edit_submit))

    async def _on_edit_submit(self, interaction: discord.Interaction, event_name: str, new_start_str: str, new_duration_str: str):
        await interaction.response.defer(ephemeral=True)

        def _parse_duration(s: str) -> datetime.timedelta | None:
            s = s.lower().replace(" ", "")
            m = re.match(r"(?:(\d+)h)?(?:(\d+)m)?", s)
            if not m:
                return None
            h = int(m.group(1)) if m.group(1) else 0
            mnts = int(m.group(2)) if m.group(2) else 0
            if h == 0 and mnts == 0:
                return None
            return datetime.timedelta(hours=h, minutes=mnts)

        row = await _get_event_row(settings.SQLITE_PATH, event_name, interaction.channel.id)
        if not row:
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return
        _, _name, creator_id, channel_id, _start_iso_old, _end_iso_old, message_id = row

        if not await self._is_owner_or_admin(interaction, creator_id):
            await interaction.response.send_message("You don’t have permission to edit this event.", ephemeral=True)
            return

        try:
            naive = datetime.datetime.strptime(new_start_str, "%Y-%m-%d %H:%M")
            local_tz = datetime.datetime.now().astimezone().tzinfo
            new_start_utc = naive.replace(tzinfo=local_tz).astimezone(datetime.timezone.utc)
        except Exception:
            await interaction.response.send_message("Invalid start format. Use `YYYY-MM-DD HH:MM`.", ephemeral=True)
            return

        dur = _parse_duration(new_duration_str)
        if dur is None:
            await interaction.response.send_message("Invalid duration. Use `2h30m`, `45m`, etc.", ephemeral=True)
            return
        new_end_utc = new_start_utc + dur

        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            await db.execute("UPDATE events SET start_time=?, end_time=? WHERE name=? AND channel_id=?",
                             (new_start_utc.isoformat(), new_end_utc.isoformat(), event_name, channel_id))
            await db.commit()

        try:
            scheduler.remove_job(f"start:{event_name}:{channel_id}")
        except Exception:
            pass
        try:
            scheduler.remove_job(f"end:{event_name}:{channel_id}")
        except Exception:
            pass
        scheduler.add_job(self._start_event, 'date', id=f"start:{event_name}:{channel_id}",
                          replace_existing=True, run_date=new_start_utc, args=[event_name, channel_id])
        scheduler.add_job(self._end_event, 'date', id=f"end:{event_name}:{channel_id}",
                          replace_existing=True, run_date=new_end_utc, args=[event_name, channel_id])

        channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
        try:
            message = await channel.fetch_message(message_id)
            if message.embeds:
                em = message.embeds[0]
                ts = int(new_start_utc.timestamp())
                em.description = f"**Start:** <t:{ts}:f>\n**Duration:** {new_duration_str}"
                await message.edit(embed=em, view=EventMessageView(
                    event_name, new_start_utc, channel_id, message_id,
                    self._on_signup, self._on_signout, self._on_edit_request, self._on_cancel, self._on_end_now
                ))
        except Exception:
            pass

        await interaction.followup.send("✅ Event updated.", ephemeral=True)

    async def _on_cancel(self, interaction: discord.Interaction, view: EventMessageView):
        await interaction.response.defer(ephemeral=True)
        row = await _get_event_row(settings.SQLITE_PATH, view.event_name, view.channel_id)
        if not row:
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return
        _, _name, creator_id, _ch, _s, _e, message_id = row
        if not await self._is_owner_or_admin(interaction, creator_id):
            await interaction.response.send_message("You don’t have permission to cancel this event.", ephemeral=True)
            return

        for jid in (f"start:{view.event_name}:{view.channel_id}", f"end:{view.event_name}:{view.channel_id}"):
            try:
                scheduler.remove_job(jid)
            except Exception:
                pass

        sess = active_sessions.pop((view.event_name, view.channel_id), None)
        if sess:
            await sess.stop_task()

        channel = self.bot.get_channel(view.channel_id) or await self.bot.fetch_channel(view.channel_id)
        try:
            message = await channel.fetch_message(message_id)
            if message.embeds:
                em = message.embeds[0]
                em.title = f"❌ Cancelled: {view.event_name}"
                em.set_footer(text="Event was cancelled by an admin.")
                await message.edit(embed=em, view=None)
        except Exception:
            pass

        await interaction.followup.send("🛑 Event cancelled.", ephemeral=True)

    async def _on_end_now(self, interaction: discord.Interaction, view: EventMessageView):
        await interaction.response.defer(ephemeral=True)
        row = await _get_event_row(settings.SQLITE_PATH, view.event_name, view.channel_id)
        if not row:
            await interaction.response.send_message("Event not found.", ephemeral=True)
            return
        _, _name, creator_id, _ch, _s, _e, _msg = row
        if not await self._is_owner_or_admin(interaction, creator_id):
            await interaction.response.send_message("You don’t have permission to end this event.", ephemeral=True)
            return

        try:
            scheduler.remove_job(f"end:{view.event_name}:{view.channel_id}")
        except Exception:
            pass

        end_now = datetime.datetime.now(datetime.timezone.utc)
        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            await db.execute("UPDATE events SET end_time=? WHERE name=? AND channel_id=?",
                             (end_now.isoformat(), view.event_name, view.channel_id))
            await db.commit()

        await self._end_event(view.event_name, view.channel_id, end_override_utc=end_now)
        await interaction.followup.send("⏹️ Event ended.", ephemeral=True)

    async def _post_finalize_from_db(self, name: str, channel_id: int):
        from repos.sqlite_repo import SqliteUploadRepo
        upload_repo = SqliteUploadRepo(settings.SQLITE_PATH)
        rows = await upload_repo.list_for_event(name)
        if not rows:
            return
        by_boss = {}
        for r in rows:
            key = (r["boss_id"], r["boss_name"])
            by_boss.setdefault(key, []).append(r)
        lines = []
        for (bid, bname), logs in by_boss.items():
            success = any(l["success"] for l in logs)
            attempts = len(logs)
            mark = "✅" if success else "❌"
            url = next((l["permalink"] for l in logs if l.get("permalink")), "")
            lines.append(f"• **{bname or 'Unknown'}** — {attempts} attempt(s) {mark}  {url}")
        desc = "\n".join(lines)
        em = discord.Embed(title=f"📊 Finalized Summary — {name}", description=desc, color=discord.Color.blurple())
        ch = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
        await ch.send(embed=em)

async def send_dm_reminder(bot: commands.Bot, user_id: int, event_name: str, event_start_utc):
    try:
        user = await bot.fetch_user(user_id)
        if user:
            ts = int(event_start_utc.timestamp())
            await user.send(f"⏰ Reminder: **{event_name}** starts at <t:{ts}:f> (**<t:{ts}:R>**)")
    except discord.Forbidden:
        pass
