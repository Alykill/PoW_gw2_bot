# ingest_and_preview.py
import argparse
import asyncio
import datetime
import glob
import json
import os
from typing import List, Tuple, Optional, Any
import traceback

import aiohttp
import aiosqlite

from analytics.service import enrich_upload, build_event_analytics_embeds

# ----------------------------
# DB bootstrap (minimal tables)
# ----------------------------
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
    """,
]

# ----------------------------
# Normalize helpers + preview
# ----------------------------
def _preview(data: Any, limit: int = 220) -> str:
    try:
        if isinstance(data, (dict, list)):
            s = json.dumps(data)[:limit]
        else:
            s = str(data)[:limit]
    except Exception:
        s = str(type(data))
    return s + ("..." if len(s) == limit else "")

def normalize_ei_json(obj: Any) -> dict:
    """
    Convert dps.report responses (dict OR list) to a single EI dict.
    - dict: returned as-is
    - list: pick first element that looks like EI (has 'players' or 'encounter')
            else if first element is a dict, return it
            else raise
    """
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        for el in obj:
            if isinstance(el, dict) and (isinstance(el.get("players"), list) or isinstance(el.get("encounter"), dict)):
                return el
        if obj and isinstance(obj[0], dict):
            return obj[0]
        raise TypeError("List did not contain a usable EI object. First element preview: " + _preview(obj[:1]))
    raise TypeError(f"Unsupported EI JSON type: {type(obj).__name__} | Preview: {_preview(obj)}")

# ----------------------------
# Fetch EI JSON from dps.report
# ----------------------------
async def fetch_ei_json(url: str) -> dict:
    """
    Try common EI JSON endpoints for a dps.report permalink, plus:
      https://dps.report/getJson?permalink=<ID>
    Returns a normalized dict or raises.
    """
    import re
    base = url.rstrip("/")
    m = re.search(r"dps\.report/([^/?#]+)", base)
    permalink_id = m.group(1) if m else None

    candidates = []
    if base.lower().endswith(".json"):
        candidates = [base]
    else:
        candidates = [
            base + "/ei.json",
            base + "/json",
            base + "/report.json",
            base + "/index.json",
        ]
        if permalink_id:
            candidates.append(f"https://dps.report/getJson?permalink={permalink_id}")

    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; gw2-raidbot/1.0)"}
    tried = []
    last_err = None

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as sess:
        for cand in candidates:
            tried.append(cand)
            try:
                async with sess.get(cand, allow_redirects=True) as resp:
                    status = resp.status
                    text = await resp.text()
                    if status != 200:
                        last_err = RuntimeError(f"{cand} => HTTP {status} | body preview: {_preview(text)}")
                        continue
                    try:
                        raw = json.loads(text)
                    except Exception as je:
                        last_err = RuntimeError(f"{cand} => bad JSON parse: {je} | body preview: {_preview(text)}")
                        continue

                    # Debug shapes
                    print(f"[DEBUG] fetched {cand} => type={type(raw).__name__} preview={_preview(raw)}")
                    try:
                        norm = normalize_ei_json(raw)
                        print(f"[DEBUG] normalized => type={type(norm).__name__} has players? {isinstance(norm.get('players'), list)}")
                        return norm
                    except Exception as ne:
                        last_err = ne
                        continue
            except Exception as e:
                last_err = e
                continue

    raise RuntimeError(
        "Failed to fetch EI JSON. Tried:\n  - " + "\n  - ".join(tried) +
        (f"\nLast error: {last_err}" if last_err else "")
    )

async def ensure_tables():
    async with aiosqlite.connect("events.db") as db:
        for s in CREATE_SQL:
            await db.execute(s)
        await db.commit()

# ----------------------------
# Utils
# ----------------------------
def is_url(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))

def expand_inputs(args_logs: List[str]) -> List[str]:
    items: List[str] = []
    for item in args_logs:
        item = item.strip()
        if not item:
            continue
        if is_url(item):
            items.append(item)
            continue
        matched = glob.glob(item, recursive=True)
        if matched:
            items.extend(matched)
        else:
            if os.path.isdir(item):
                for ext in (".zevtc", ".evtc", ".evtc.zip"):
                    items.extend(glob.glob(os.path.join(item, f"**/*{ext}"), recursive=True))
            else:
                low = item.lower()
                if low.endswith((".zevtc", ".evtc", ".evtc.zip")):
                    items.append(item)
    seen, out = set(), []
    for x in items:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

# ----------------------------
# dps.report upload for files
# ----------------------------
async def upload_to_dps_report(file_path: str) -> Optional[dict]:
    url = "https://dps.report/uploadContent?json=1"
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            with open(file_path, "rb") as f:
                data = aiohttp.FormData()
                data.add_field("file", f, filename=os.path.basename(file_path), content_type="application/octet-stream")
                async with session.post(url, data=data) as resp:
                    status = resp.status
                    body = await resp.read()
                    if status != 200:
                        preview = body[:200].decode(errors="ignore")
                        print(f"   ⚠️ upload non-200 ({status}): {preview}")
                        return None
                    try:
                        return json.loads(body.decode(errors="ignore"))
                    except Exception:
                        print("   ⚠️ upload JSON parse failed")
                        return None
    except Exception as e:
        print(f"   ⚠️ upload exception for {file_path}: {e}")
        return None

# ----------------------------
# Ingest
# ----------------------------
async def ingest(event_name: str, items: List[str]) -> Tuple[int, int]:
    await ensure_tables()
    ingested, total = 0, len(items)

    for item in items:
        try:
            if is_url(item):
                # Fetch & normalize (handles dict or list)
                ei = await fetch_ei_json(item)
                if not isinstance(ei, dict) or not isinstance(ei.get("players"), list):
                    raise ValueError("Fetched EI JSON did not look like an EI object with 'players'")

                enc = ei.get("encounter")
                enc = enc if isinstance(enc, dict) else {}

                boss_name = (enc.get("boss") or ei.get("boss") or ei.get("fightName") or "Unknown").strip()
                boss_id = int(enc.get("bossId") or ei.get("triggerID") or -1)
                success = 1 if ((enc.get("success") if enc else None) or ei.get("success")) else 0

                async with aiosqlite.connect("events.db") as db:
                    cur = await db.execute(
                        """
                        INSERT OR IGNORE INTO uploads(event_name, file_path, permalink, boss_id, boss_name, success, time_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (event_name, item, item, boss_id, boss_name, success, datetime.datetime.utcnow().isoformat()),
                    )
                    await db.commit()
                    upload_id = cur.lastrowid
                    if not upload_id:
                        cur = await db.execute("SELECT id FROM uploads WHERE file_path = ?", (item,))
                        row = await cur.fetchone()
                        if not row:
                            raise RuntimeError("Failed to retrieve existing upload row after IGNORE")
                        upload_id = row[0]

                print(
                    f"[INGEST] calling enrich_upload with EI dict: has players? {isinstance(ei.get('players'), list)}")
                await enrich_upload(upload_id, ei)
                ingested += 1

            else:
                low = item.lower()
                if not low.endswith((".zevtc", ".evtc", ".evtc.zip")):
                    print(f"  Skipping unsupported file: {item}")
                    continue

                resp = await upload_to_dps_report(item)
                if not isinstance(resp, dict):
                    print(f"   ⚠️ upload failed: {item}")
                    continue

                permalink = resp.get("permalink") or resp.get("permaLink") or ""
                enc = resp.get("encounter")
                enc = enc if isinstance(enc, dict) else {}

                boss_name = (enc.get("boss") or resp.get("boss") or resp.get("fightName") or "Unknown").strip()
                boss_id = int(enc.get("bossId") or resp.get("triggerID") or -1)
                success = 1 if ((enc.get("success") if enc else None) or resp.get("success")) else 0

                async with aiosqlite.connect("events.db") as db:
                    cur = await db.execute(
                        """
                        INSERT OR IGNORE INTO uploads(event_name, file_path, permalink, boss_id, boss_name, success, time_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (event_name, item, permalink, boss_id, boss_name, success, datetime.datetime.utcnow().isoformat()),
                    )
                    await db.commit()
                    upload_id = cur.lastrowid
                    if not upload_id:
                        cur = await db.execute("SELECT id FROM uploads WHERE file_path = ?", (item,))
                        row = await cur.fetchone()
                        upload_id = row[0] if row else None

                print(f"[INGEST] calling enrich_upload with upload response dict: keys={list(resp.keys())[:8]}")
                await enrich_upload(upload_id, resp)
                ingested += 1


        except Exception as e:
            print(f"   ⚠️ failed to ingest {item}: {e}")
            traceback.print_exc()

    return ingested, total

# ----------------------------
# Console "embed" preview
# ----------------------------
def print_embeds(embeds):
    if not embeds:
        print("\n===== Discord Embed Preview: (no data) =====")
        return

    print("\n===== Discord Embed Preview: =====\n")
    for idx, em in enumerate(embeds, 1):
        try:
            d = em.to_dict()
        except Exception:
            d = {"title": getattr(em, "title", ""), "fields": []}

        title = d.get("title") or "(no title)"
        print(f"--- Embed #{idx} ---")
        print(f"Title: {title}\n")

        fields = d.get("fields") or []
        for f in fields:
            name = f.get("name") or ""
            value = f.get("value") or ""
            print(f"[{name}]")
            print(value)
            print()
        print()

# ----------------------------
# CLI
# ----------------------------
async def main():
    parser = argparse.ArgumentParser(description="Ingest logs or permalinks and preview analytics.")
    parser.add_argument("--event", required=True, help="Event name to attribute uploads to")
    parser.add_argument(
        "--logs",
        required=True,
        nargs="+",
        help=(
            "One or more items. Each may be:\n"
            "- a dps.report permalink (will fetch /ei.json), or\n"
            "- a file path/glob to .zevtc/.evtc/.evtc.zip (uploaded to dps.report)\n"
            "Examples:\n"
            '  --logs "C:/.../arcdps.cbtlogs/**/*.zevtc"\n'
            '  --logs "https://dps.report/XXXX-YYYY_boss"\n'
        ),
    )
    args = parser.parse_args()

    items = expand_inputs(args.logs)
    if not items:
        print("❌ No files or URLs matched your --logs input.")
        return

    print(f"📦 Ingesting {len(items)} item(s) into event '{args.event}'")
    ok, total = await ingest(args.event, items)
    print(f"✅ Ingested {ok}/{total} item(s)")

    embeds = await build_event_analytics_embeds(args.event)
    print_embeds(embeds)

if __name__ == "__main__":
    asyncio.run(main())
