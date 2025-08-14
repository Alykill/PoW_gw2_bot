
import aiosqlite
from typing import Tuple
from .base import EventRepo, SignupRepo, UploadRepo

CREATE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        user_id INTEGER,
        channel_id INTEGER,
        start_time TEXT,
        end_time TEXT,
        message_id INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        user_id INTEGER
    )
    """,
    """
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
    """,
    """
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
    """
]

async def ensure_tables(db_path: str = "events.db") -> None:
    async with aiosqlite.connect(db_path) as db:
        for s in CREATE_SQL:
            await db.execute(s)
        await db.commit()

class SqliteEventRepo(EventRepo):
    def __init__(self, db_path: str = "events.db"):
        self.db_path = db_path
    async def create(self, name: str, user_id: int, channel_id: int, start_iso: str, end_iso: str, message_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "INSERT INTO events(name,user_id,channel_id,start_time,end_time,message_id) VALUES (?,?,?,?,?,?)",
                (name, user_id, channel_id, start_iso, end_iso, message_id))
            await db.commit()
            return cur.lastrowid
    async def get_message_ref(self, name: str, channel_id: int) -> Tuple[int, int] | None:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT channel_id,message_id FROM events WHERE name=? AND channel_id=?",
                                   (name, channel_id))
            row = await cur.fetchone()
            return (row[0], row[1]) if row else None

class SqliteSignupRepo(SignupRepo):
    def __init__(self, db_path: str = "events.db"):
        self.db_path = db_path
    async def add(self, event_name: str, user_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO signups(event_name,user_id) VALUES(?,?)", (event_name, user_id))
            await db.commit()
    async def remove(self, event_name: str, user_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM signups WHERE event_name=? AND user_id=?", (event_name, user_id))
            await db.commit()
    async def list_names(self, event_name: str) -> list[int]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT user_id FROM signups WHERE event_name=? ORDER BY id ASC", (event_name,))
            rows = await cur.fetchall()
            return [r[0] for r in rows]

class SqliteUploadRepo(UploadRepo):
    def __init__(self, db_path: str = "events.db"):
        self.db_path = db_path
    async def add_upload(self, event_name: str, file_path: str, permalink: str, boss_id: int, boss_name: str, success: int, time_iso: str) -> int | None:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "INSERT OR IGNORE INTO uploads(event_name,file_path,permalink,boss_id,boss_name,success,time_utc) VALUES (?,?,?,?,?,?,?)",
                (event_name, file_path, permalink, boss_id, boss_name, success, time_iso))
            await db.commit()
            if cur.lastrowid:
                return cur.lastrowid
            cur = await db.execute("SELECT id FROM uploads WHERE file_path=?", (file_path,))
            r = await cur.fetchone()
            return r[0] if r else None
    async def list_for_event(self, event_name: str) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT id,file_path,permalink,boss_id,boss_name,success,time_utc FROM uploads WHERE event_name=?",
                                   (event_name,))
            rows = await cur.fetchall()
            return [{
                "id": r[0], "file_path": r[1], "permalink": r[2], "boss_id": r[3], "boss_name": r[4], "success": r[5], "time_utc": r[6]
            } for r in rows]
