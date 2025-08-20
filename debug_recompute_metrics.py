# debug_recompute_metrics.py
import asyncio, aiosqlite
from config import settings
from analytics.service import ensure_enriched_for_event

async def main(event_name: str):
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        await db.execute("""
            DELETE FROM metrics
            WHERE upload_id IN (SELECT id FROM uploads WHERE event_name = ?)
        """, (event_name,))
        await db.commit()
    repaired = await ensure_enriched_for_event(event_name)
    print(f"Re-enriched {repaired} upload(s) for event '{event_name}'")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python debug_recompute_metrics.py \"Event Name\"")
        raise SystemExit(2)
    asyncio.run(main(sys.argv[1]))