# ui/embeds.py
import datetime, discord, re, asyncio, aiosqlite  # <-- add aiosqlite here
from typing import List, Dict, Tuple, Optional, Any
from config import settings

# --- datetime helpers ---
def _parse_dt_any(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s: return None
    try:
        dt = datetime.datetime.fromisoformat(str(s).replace("Z","+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        try:
            base = str(s).split('.')[0]
            dt = datetime.datetime.fromisoformat(base)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        except Exception:
            return None

def _fmt_uniform(td: Optional[datetime.timedelta]) -> str:
    if td is None:
        return "—"
    secs = max(0, int(td.total_seconds()))
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"

# --- duration parsing ---
def _parse_colon_duration(text: str) -> Optional[datetime.timedelta]:
    """
    Supports H:MM:SS(.mmm), MM:SS(.mmm), SS(.mmm)
    """
    t = str(text).strip()
    if not t:
        return None
    # SS(.mmm)
    if ":" not in t:
        m = re.match(r"^(\d+)(?:\.(\d{1,3}))?$", t)
        if not m:
            return None
        s = int(m.group(1))
        ms = int(m.group(2)) if m.group(2) else 0
        return datetime.timedelta(seconds=s, milliseconds=ms)
    parts = t.split(":")
    try:
        if len(parts) == 2:
            # MM:SS(.mmm)
            mm = int(parts[0])
            sec_part = parts[1]
            if "." in sec_part:
                s, ms = sec_part.split(".", 1)
                return datetime.timedelta(minutes=mm, seconds=int(s), milliseconds=int(ms[:3]))
            else:
                return datetime.timedelta(minutes=mm, seconds=int(sec_part))
        elif len(parts) == 3:
            # H:MM:SS(.mmm)
            hh = int(parts[0]); mm = int(parts[1]); sec_part = parts[2]
            if "." in sec_part:
                s, ms = sec_part.split(".", 1)
                return datetime.timedelta(hours=hh, minutes=mm, seconds=int(s), milliseconds=int(ms[:3]))
            else:
                return datetime.timedelta(hours=hh, minutes=mm, seconds=int(sec_part))
    except ValueError:
        return None
    return None

def _extract_duration_td_from_result(r: dict) -> Optional[datetime.timedelta]:
    """
    Try to read duration directly from the upload result dict.
    We look at r['encounter'] first, then top-level; and also support a cached __dur_ms field.
    """
    enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
    # Cached by our enrichment
    ms_cached = r.get("__dur_ms")
    if ms_cached is not None:
        try:
            return datetime.timedelta(milliseconds=int(ms_cached))
        except Exception:
            pass
    # Numeric milliseconds
    for key in ("durationMS", "durationMs", "duration_ms"):
        for obj in (enc, r):
            ms = obj.get(key)
            if ms is not None:
                try: return datetime.timedelta(milliseconds=int(ms))
                except Exception: pass
    # Text with units
    for key in ("duration", "durationText", "fightDuration"):
        for obj in (enc, r):
            txt = obj.get(key)
            if isinstance(txt, str) and txt:
                m = re.search(r"(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*(?:(\d+)\s*s)?\s*(?:(\d+)\s*ms)?", txt.lower())
                if m and any(m.groups()):
                    h = int(m.group(1) or 0); mn = int(m.group(2) or 0); s = int(m.group(3) or 0); ms = int(m.group(4) or 0)
                    return datetime.timedelta(hours=h, minutes=mn, seconds=s, milliseconds=ms)
                cd = _parse_colon_duration(txt)
                if cd:
                    return cd
    # Fallback to start/end timestamps if present
    s = _parse_dt_any(r.get("timeStart") or r.get("time"))
    e = _parse_dt_any(r.get("timeEnd"))
    if s and e and e >= s:
        return e - s
    return None

async def _enrich_result_with_ei_duration(r: dict) -> None:
    """
    If no duration can be extracted from the upload result, fetch EI JSON via permalink
    and cache the duration into r['__dur_ms'] (and also mirror common keys on r['encounter']).
    """
    if _extract_duration_td_from_result(r):
        return
    link = r.get("permalink") or r.get("permaLink") or r.get("id")
    if not isinstance(link, str) or not link:
        return
    try:
        # Lazy import to avoid module import cycles
        from analytics.service import _coerce_payload_to_json  # type: ignore
        j = await _coerce_payload_to_json(link)
        if not isinstance(j, dict):
            return
        enc = j.get("encounter", {}) if isinstance(j.get("encounter"), dict) else {}
        dur_ms = None
        for key in ("durationMS", "durationMs", "duration_ms"):
            val = enc.get(key) or j.get(key)
            if val is not None:
                try:
                    dur_ms = int(val); break
                except Exception:
                    pass
        if dur_ms is None:
            for key in ("duration", "durationText", "fightDuration"):
                val = enc.get(key) or j.get(key)
                if isinstance(val, str):
                    td = _parse_colon_duration(val)
                    if not td:
                        m = re.search(r"(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*(?:(\d+)\s*s)?\s*(?:(\d+)\s*ms)?", val.lower())
                        if m and any(m.groups()):
                            h = int(m.group(1) or 0); mn = int(m.group(2) or 0); s = int(m.group(3) or 0); ms = int(m.group(4) or 0)
                            td = datetime.timedelta(hours=h, minutes=mn, seconds=s, milliseconds=ms)
                    if td:
                        dur_ms = int(td.total_seconds() * 1000)
                        break
        if dur_ms is not None:
            r["__dur_ms"] = dur_ms
            # Mirror onto encounter for downstream readability
            if not isinstance(r.get("encounter"), dict):
                r["encounter"] = {}
            r["encounter"].setdefault("durationMS", dur_ms)
            # Also surface an ISO-ish fightDuration string for display if needed
            secs = dur_ms / 1000.0
            h = int(secs // 3600); m = int((secs % 3600) // 60); s = int(secs % 60)
            r["encounter"].setdefault("fightDuration", f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}")
    except Exception:
        # Silent: summary should not explode because a single fetch failed
        return

async def _ensure_durations(results: List[dict]) -> None:
    # Enrich in parallel with modest concurrency
    sem = asyncio.Semaphore(4)
    async def one(r):
        async with sem:
            await _enrich_result_with_ei_duration(r)
    await asyncio.gather(*(one(r) for r in results))

# --- main builder ---
def _fmt_dps_apostrophe(n: float | int | None) -> str:
    if n is None:
        return "0"
    try:
        return f"{int(round(float(n))):,}".replace(",", "'")
    except Exception:
        return str(n)

async def _fetch_top_dps_by_permalink(permalinks: List[str]) -> Dict[str, Tuple[str, float]]:
    """
    Return {permalink -> (actor, dps)} for the top DPS on that upload.
    """
    result: Dict[str, Tuple[str, float]] = {}
    if not permalinks:
        return result

    # Dedup to reduce queries
    unique = list(dict.fromkeys([p for p in permalinks if p]))

    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        for pl in unique:
            try:
                cur = await db.execute(
                    """
                    SELECT m.actor, m.value
                    FROM uploads u
                    JOIN metrics m ON m.upload_id = u.id
                    WHERE u.permalink = ? AND m.metric_key = 'boss_dps'
                    ORDER BY m.value DESC
                    LIMIT 1
                    """,
                    (pl,),
                )
                row = await cur.fetchone()
                if row:
                    result[pl] = (row[0], float(row[1]))
            except Exception:
                # ignore this one and continue
                pass
    return result

async def build_summary_embed(
    event_name: str,
    results: List[dict],
    event_start_utc: Optional[datetime.datetime] = None,
    event_end_utc: Optional[datetime.datetime] = None,
) -> discord.Embed:
    # Ensure we can read durations for Fight/links
    await _ensure_durations(results)

    # Group logs per boss
    attempts: Dict[Tuple[int, str], List[dict]] = {}
    for r in results:
        enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
        boss_id = enc.get("bossId") or r.get("bossId") or -1
        boss_name = enc.get("boss") or r.get("boss") or "Unknown Encounter"
        key = (int(boss_id), str(boss_name))
        attempts.setdefault(key, []).append(r)
    for k in attempts:
        attempts[k].sort(key=lambda x: (x.get("timeEnd") or "", x.get("timeStart") or x.get("time") or ""))

    # Sum fight time
    fight_total = datetime.timedelta(0)
    for logs in attempts.values():
        for r in logs:
            td = _extract_duration_td_from_result(r)
            if td:
                fight_total += td

    # Pre-fetch top DPS per successful permalink
    success_permalinks: List[str] = []
    success_index_by_pl: Dict[str, int] = {}
    for (_bid, _bname), logs in attempts.items():
        for i, r in enumerate(logs):
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if enc.get("success", r.get("success", False)):
                pl = r.get("permalink", "")
                if pl:
                    success_permalinks.append(pl)
                    success_index_by_pl[pl] = i
                break  # only the first success matters for the line
    top_dps_map = await _fetch_top_dps_by_permalink(success_permalinks)

    # Total & Wait based on event window
    start_anchor = event_start_utc
    end_anchor = event_end_utc
    total_time = (end_anchor - start_anchor) if (start_anchor and end_anchor) else None
    wait_time = (total_time - fight_total) if total_time is not None else None
    if wait_time is not None and wait_time.total_seconds() < 0:
        wait_time = datetime.timedelta(0)

    # Build boss lines (include Top DPS after success hyperlink)
    boss_lines: List[str] = []
    for (bid, bname), logs in attempts.items():
        success_idx = None; success_log = None; cm_flag = None
        for i, r in enumerate(logs):
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if enc.get("success", r.get("success", False)):
                success_idx = i; success_log = r; break
        for r in logs:
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if "isCM" in enc: cm_flag = enc.get("isCM"); break
            if "isCM" in r: cm_flag = r.get("isCM"); break
        cm_text = " [CM]" if cm_flag else ""

        if success_log:
            dur_td = _extract_duration_td_from_result(success_log)
            dur_txt = _fmt_uniform(dur_td) if dur_td else "success"
            url = success_log.get("permalink", "") or ""
            suffix = ""
            if url and url in top_dps_map:
                actor, dps = top_dps_map[url]
                suffix = f" - 👑 **{actor}** - {_fmt_dps_apostrophe(dps)} ⚔️"
            line = f"• **{bname}{cm_text}** — {success_idx + 1} attempt(s) ✅ [{dur_txt}]({url}){suffix}"
        else:
            line = f"• **{bname}{cm_text}** — {len(logs)} attempt(s) ❌"
        boss_lines.append(line)

    # Two columns
    left_lines, right_lines = [], []
    for i, ln in enumerate(boss_lines):
        (left_lines if i % 2 == 0 else right_lines).append(ln)
    left_text = "\n".join(left_lines) if left_lines else "\u200b"
    right_text = "\n".join(right_lines) if right_lines else "\u200b"

    # Assemble embed
    attempts_count = sum(len(v) for v in attempts.values())
    desc_top = f"You did **{attempts_count}** boss {'try' if attempts_count == 1 else 'tries'} during this event."
    em = discord.Embed(title=f"📊 Event Summary — {event_name}", description=desc_top, color=discord.Color.green())

    # Column 1: Session
    if start_anchor and end_anchor:
        em.add_field(
            name="Session",
            value=f"Start: <t:{int(start_anchor.timestamp())}:f>\nEnd: <t:{int(end_anchor.timestamp())}:f>",
            inline=True
        )
    else:
        em.add_field(name="Session", value="Start: —\nEnd: —", inline=True)

    # Column 2: Distribution
    em.add_field(
        name="Distribution",
        value=f"Total: {_fmt_uniform(total_time)}\nFight: {_fmt_uniform(fight_total)}\nWait: {_fmt_uniform(wait_time)}",
        inline=True
    )

    # Spacer + "Raid wing" section (two columns of boss lines)
    em.add_field(name="\u200b", value="\u200b", inline=False)
    em.add_field(name="Raid wing", value=left_text, inline=True)
    em.add_field(name="\u200b", value=right_text, inline=True)

    return em