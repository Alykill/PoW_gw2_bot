# debug_service.py
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import List, Dict, Any, Optional

import aiosqlite
import discord

from config import settings, setup_logging
from analytics.service import ensure_enriched_for_event, build_event_analytics_embeds
from ui.embeds import build_summary_embed


async def _fetch_event_times(event_name: str, channel_id: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (start_iso, end_iso) for the most recent event with this name.
    If channel_id is provided, it filters by channel.
    """
    where = "WHERE name = ?"
    params = [event_name]
    if channel_id:
        where += " AND channel_id = ?"
        params.append(channel_id)
    query = f"""
        SELECT start_time, end_time
        FROM events
        {where}
        ORDER BY id DESC
        LIMIT 1
    """
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        cur = await db.execute(query, tuple(params))
        row = await cur.fetchone()
        return (row[0], row[1]) if row else (None, None)


async def _fetch_uploads_for_event(event_name: str) -> List[Dict[str, Any]]:
    """
    Build a minimal 'results' list compatible with build_summary_embed(),
    using rows already in the uploads table.
    """
    rows: List[Dict[str, Any]] = []
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        cur = await db.execute(
            """
            SELECT boss_name, success, boss_id, permalink, time_utc
            FROM uploads
            WHERE event_name = ?
            ORDER BY datetime(time_utc) ASC
            """,
            (event_name,),
        )
        fetched = await cur.fetchall()

    for boss_name, success, boss_id, permalink, time_utc in fetched:
        # Minimal shape; duration will be fetched lazily by build_summary_embed when needed
        rows.append({
            "encounter": {
                "boss": boss_name or "Unknown Encounter",
                "bossId": int(boss_id or -1),
                "success": bool(success) if isinstance(success, (int, bool)) else str(success).lower() in ("1", "true", "t", "yes", "y"),
            },
            "permalink": permalink or "",
            "time": time_utc,   # used as a weak fallback for ordering if needed
        })
    return rows


async def run(event_name: str, channel_id: int, include_summary: bool):
    # 1) Enrich any missing metrics for this event
    repaired = await ensure_enriched_for_event(event_name)
    logging.info("Re-enriched %s upload(s) for event '%s'", repaired, event_name)

    # 2) Build embeds
    analytics_embeds = await build_event_analytics_embeds(event_name)

    summary_embed = None
    if include_summary:
        res = await _fetch_uploads_for_event(event_name)
        start_iso, end_iso = await _fetch_event_times(event_name, channel_id)
        # parse ISO to aware datetimes (ui/embeds.py can cope with None too)
        from datetime import datetime, timezone
        start_dt = datetime.fromisoformat(start_iso).astimezone(timezone.utc) if start_iso else None
        end_dt = datetime.fromisoformat(end_iso).astimezone(timezone.utc) if end_iso else None
        summary_embed = await build_summary_embed(event_name, res, start_dt, end_dt)

    # 3) Send to Discord
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        try:
            ch = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
            if summary_embed:
                await ch.send(embed=summary_embed)
            if analytics_embeds:
                # send as a small batch; Discord allows multiple embeds per message but keep it simple
                for em in analytics_embeds:
                    await ch.send(embed=em)
            else:
                await ch.send(f"ℹ️ No analytics available for **{event_name}**.")
        finally:
            await client.close()

    await client.start(settings.DISCORD_TOKEN)


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Post analytics (and optional summary) for an existing event.")
    parser.add_argument("--event", required=True, help="Event name as stored in DB (events.name / uploads.event_name).")
    parser.add_argument("--channel", required=True, type=int, help="Target Discord text channel ID to post embeds.")
    parser.add_argument("--summary", action="store_true", help="Also post the Event Summary embed.")
    args = parser.parse_args()

    asyncio.run(run(args.event, args.channel, args.summary))


if __name__ == "__main__":
    main()