
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import datetime
scheduler = AsyncIOScheduler(timezone=datetime.timezone.utc)