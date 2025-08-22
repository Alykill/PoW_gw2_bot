# ui/embeds.py
from __future__ import annotations
import datetime, re, asyncio
from typing import List, Dict, Tuple, Optional, Any
from collections import defaultdict
import discord
from config import settings
import aiosqlite

# ---- Wing titles used for grouping in the Summary ----
WING_TITLES = {
    "W1": "Wing 1 — Spirit Vale",
    "W2": "Wing 2 — Salvation Pass",
    "W3": "Wing 3 — Stronghold of the Faithful",
    "W4": "Wing 4 — Bastion of the Penitent",
    "W5": "Wing 5 — Hall of Chains",
    "W6": "Wing 6 — Mythwright Gambit",
    "W7": "Wing 7 — The Key of Ahdashim",
    "W8": "Wing 8 — Mount Balrior",
    "Other": "Other",
}

# ---------- datetime / formatting helpers ----------
def _parse_dt_any(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        try:
            base = str(s).split(".")[0]
            dt = datetime.datetime.fromisoformat(base)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
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

def _fmt_dps_apostrophe(n: float | int | None) -> str:
    if n is None:
        return "0"
    try:
        return f"{int(round(float(n))):,}".replace(",", "'")
    except Exception:
        return str(n)

def _format_encounter_entry(
    boss: str,
    attempts: int,
    success_url: str | None,
    success_label: str | None,
    top_actor: str | None,
    top_dps: float | None,
) -> str:
    first = f"• {boss} — {attempts} pull(s)"
    if success_url and success_label:
        first += f" ➡️ [{success_label}]({success_url})"
    second = ""
    if top_actor and (top_dps is not None):
        second = f"\n└ 💪 {_short_actor(top_actor)} — {_fmt_dps_apostrophe(top_dps)}"
    return first + second

# shorten "Name.1234" -> "Name"
_NAME_TAG_RE = re.compile(r"\.\d{3,5}$")
def _short_actor(name: str | None) -> str:
    s = str(name or "")
    return _NAME_TAG_RE.sub("", s)

def _chunk_by_lines(text: str, limit: int = 1024) -> List[str]:
    """
    Split a long block into <=limit chunks on newline boundaries.
    """
    lines = text.split("\n")
    chunks, buf = [], ""
    for ln in lines:
        add = ln if not buf else "\n" + ln
        if len(buf) + len(add) > limit:
            if buf:
                chunks.append(buf)
            buf = ln
        else:
            buf += add
    if buf:
        chunks.append(buf)
    return chunks

async def _worst_player_for_event(event_name: str) -> tuple[str, int] | None:
    """
    Return (actor, total_fail_score) where score = deaths + downs + all failed mechanic counts.
    """
    try:
        from analytics.registry import ENCOUNTER_MECHANICS  # type: ignore
        mech_keys = [spec["key"] for specs in ENCOUNTER_MECHANICS.values() for spec in specs if "key" in spec]
    except Exception:
        mech_keys = []

    base_sql = """
        SELECT m.actor, SUM(m.value) AS total
        FROM metrics m
        JOIN uploads u ON u.id = m.upload_id
        WHERE u.event_name = ?
          AND (
                m.metric_key IN ('deaths')
                {extra_clause}
              )
        GROUP BY m.actor
        ORDER BY total DESC, m.actor ASC
        LIMIT 1
    """

    params = [event_name]
    if mech_keys:
        placeholders = ",".join("?" for _ in mech_keys)
        extra_clause = f"OR m.metric_key IN ({placeholders})"
        sql = base_sql.format(extra_clause=f" {extra_clause} ")
        params.extend(mech_keys)
    else:
        sql = base_sql.format(extra_clause="")

    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        cur = await db.execute(sql, params)
        row = await cur.fetchone()
        if row:
            actor, total = row[0], int(row[1] or 0)
            return actor, total
    return None

def _is_cm_from_result(r: dict) -> bool:
    enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}

    def truthy(v):
        if isinstance(v, bool): return v
        if isinstance(v, (int, float)): return int(v) != 0
        if isinstance(v, str): return v.strip().lower() in {"true","t","yes","y","1","challenge","cm","challenge mode"}
        return False

    for v in (
        enc.get("isCM"), enc.get("isCm"), enc.get("cm"), enc.get("challengeMode"),
        r.get("isCM"),   r.get("isCm"),   r.get("cm"),   r.get("challengeMode"),
    ):
        if v is not None and truthy(v):
            return True

    mode = enc.get("mode") or r.get("mode")
    if isinstance(mode, str) and "challenge" in mode.lower():
        return True

    tags = enc.get("tags") or r.get("tags")
    if isinstance(tags, list) and any(isinstance(t, str) and "challenge" in t.lower() for t in tags):
        return True

    return False

# ---------- duration parsing ----------
def _parse_colon_duration(text: str) -> Optional[datetime.timedelta]:
    t = str(text).strip()
    if not t:
        return None
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
            mm = int(parts[0])
            sec_part = parts[1]
            if "." in sec_part:
                s, ms = sec_part.split(".", 1)
                return datetime.timedelta(minutes=mm, seconds=int(s), milliseconds=int(ms[:3]))
            else:
                return datetime.timedelta(minutes=mm, seconds=int(sec_part))
        elif len(parts) == 3:
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
    enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
    ms_cached = r.get("__dur_ms")
    if ms_cached is not None:
        try:
            return datetime.timedelta(milliseconds=int(ms_cached))
        except Exception:
            pass
    for key in ("durationMS", "durationMs", "duration_ms"):
        for obj in (enc, r):
            ms = obj.get(key)
            if ms is not None:
                try:
                    return datetime.timedelta(milliseconds=int(ms))
                except Exception:
                    pass
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
    s = _parse_dt_any(r.get("timeStart") or r.get("time"))
    e = _parse_dt_any(r.get("timeEnd"))
    if s and e and e >= s:
        return e - s
    return None

async def _enrich_result_with_ei_duration(r: dict) -> None:
    if _extract_duration_td_from_result(r):
        return
    link = r.get("permalink") or r.get("permaLink") or r.get("id")
    if not isinstance(link, str) or not link:
        return
    try:
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
            if not isinstance(r.get("encounter"), dict):
                r["encounter"] = {}
            r["encounter"].setdefault("durationMS", dur_ms)
            secs = dur_ms / 1000.0
            h = int(secs // 3600); m = int((secs % 3600) // 60); s = int(secs % 60)
            r["encounter"].setdefault("fightDuration", f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}")
    except Exception:
        return

async def _ensure_durations(results: List[dict]) -> None:
    sem = asyncio.Semaphore(4)
    async def one(r):
        async with sem:
            await _enrich_result_with_ei_duration(r)
    await asyncio.gather(*(one(r) for r in results))

async def _top_actor_for_metric(event_name: str, metric_key: str) -> str | None:
    async with aiosqlite.connect(settings.SQLITE_PATH) as db:
        cur = await db.execute(
            """
            SELECT actor, SUM(value) AS v
            FROM metrics
            WHERE metric_key = ?
              AND upload_id IN (SELECT id FROM uploads WHERE event_name = ?)
            GROUP BY actor
            ORDER BY v DESC, actor ASC
            LIMIT 1
            """,
            (metric_key, event_name),
        )
        row = await cur.fetchone()
        return row[0] if row else None

async def _top_actors_for_metric(event_name: str, metric_key: str) -> tuple[list[str], int]:
    """
    Return (actors_tied_for_max, max_total). If metrics table is missing/empty, returns ([], 0).
    """
    try:
        async with aiosqlite.connect(settings.SQLITE_PATH) as db:
            cur = await db.execute(
                """
                SELECT m.actor, SUM(m.value) AS total
                FROM metrics m
                JOIN uploads u ON u.id = m.upload_id
                WHERE u.event_name = ? AND m.metric_key = ?
                GROUP BY m.actor
                """,
                (event_name, metric_key),
            )
            rows = await cur.fetchall()
    except Exception:
        return [], 0

    if not rows:
        return [], 0
    max_total = max(int(r[1] or 0) for r in rows)
    top = [str(r[0]) for r in rows if int(r[1] or 0) == max_total]
    return top, max_total

# ---------- DB helper for Top DPS ----------
async def _fetch_top_dps_by_permalink(permalinks: List[str]) -> Dict[str, Tuple[str, float]]:
    result: Dict[str, Tuple[str, float]] = {}
    if not permalinks:
        return result
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
                pass
    return result


# ---------- Main: Summary embed ----------
async def build_summary_embed(
    event_name: str,
    results: List[dict],
    event_start_utc: Optional[datetime.datetime] = None,
    event_end_utc: Optional[datetime.datetime] = None,
) -> discord.Embed:
    # Make sure each result has a duration we can read
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

    # Sum fight time from per-encounter durations
    fight_total = datetime.timedelta(0)
    for logs in attempts.values():
        for r in logs:
            td = _extract_duration_td_from_result(r)
            if td:
                fight_total += td

    # Pre-fetch top DPS for first success per boss
    success_permalinks: List[str] = []
    for (_bid, _bname), logs in attempts.items():
        for i, r in enumerate(logs):
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if enc.get("success", r.get("success", False)):
                pl = r.get("permalink", "")
                if pl:
                    success_permalinks.append(pl)
                break
    top_dps_map = await _fetch_top_dps_by_permalink(success_permalinks)

    # Total & Wait based on event window
    start_anchor = event_start_utc
    end_anchor = event_end_utc
    total_time = (end_anchor - start_anchor) if (start_anchor and end_anchor) else None
    wait_time = (total_time - fight_total) if total_time is not None else None
    if wait_time is not None and wait_time.total_seconds() < 0:
        wait_time = datetime.timedelta(0)

    # Build per-wing lines
    raw_by_wing: Dict[str, List[str]] = defaultdict(list)

    try:
        from analytics.registry import ENCOUNTER_WINGS_BY_NAME  # type: ignore
    except Exception:
        ENCOUNTER_WINGS_BY_NAME = {}

    def wing_code_for(boss_name: str) -> str:
        bn = (boss_name or "").lower()
        code = ENCOUNTER_WINGS_BY_NAME.get(bn)
        if code:
            return code
        for k, v in ENCOUNTER_WINGS_BY_NAME.items():
            if k in bn:
                return v
        return "Other"

    for (bid, bname), logs in attempts.items():
        success_idx = None
        success_log = None
        cm_flag = None

        for i, r in enumerate(logs):
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if enc.get("success", r.get("success", False)):
                success_idx = i
                success_log = r
                break

        for r in logs:
            enc = r.get("encounter", {}) if isinstance(r.get("encounter"), dict) else {}
            if "isCM" in enc:
                cm_flag = enc.get("isCM")
                break
            if "isCM" in r:
                cm_flag = r.get("isCM")
                break

        cm_text = " 🔴" if cm_flag else ""

        if success_log:
            dur_td = _extract_duration_td_from_result(success_log)
            dur_txt = _fmt_uniform(dur_td) if dur_td else "success"
            url = success_log.get("permalink") or ""
            top_actor = None
            top_dps = None
            if url and url in top_dps_map:
                top_actor, top_dps = top_dps_map[url]

            line = _format_encounter_entry(
                boss=f"**{bname}{cm_text}**",
                attempts=(success_idx + 1) if success_idx is not None else len(logs),
                success_url=url if url else None,
                success_label=dur_txt,
                top_actor=top_actor,
                top_dps=top_dps,
            )
        else:
            line = f"• **{bname}{cm_text}** — {len(logs)} pull(s) ❌"

        raw_by_wing[wing_code_for(bname)].append(line)

    # Assemble embed
    attempts_count = sum(len(v) for v in attempts.values())
    desc_top = f"You did **{attempts_count}** boss {'try' if attempts_count == 1 else 'tries'} during this event."
    em = discord.Embed(
        title=f"📊 Event Summary — {event_name}",
        description=desc_top,
        color=discord.Color.green()
    )

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
        value=f"⌛ Total: {_fmt_uniform(total_time)}\n⚔️ Fight: {_fmt_uniform(fight_total)}\n😴 Wait: {_fmt_uniform(wait_time)}",
        inline=True
    )

    # --- Worst player mention (before Session stars) ---
    worst = await _worst_player_for_event(event_name)
    if worst:
        worst_actor, worst_score = worst
        em.add_field(
            name="🏆 Pug of the week 🏆",
            value=f"{_short_actor(worst_actor)} — {worst_score} (deaths + failed mechanics)",
            inline=False
        )
        em.add_field(name="\u200b", value="\u200b", inline=False)

    # --- Awards (with ties + 'none') ---
    deaths_tied, deaths_max = await _top_actors_for_metric(event_name, "deaths")
    downs_tied, downs_max = await _top_actors_for_metric(event_name, "downs")
    medic_tied, medic_max = await _top_actors_for_metric(event_name, "resurrects")

    def _fmt_award(label: str, actors: list[str], mx: int) -> str:
        if mx <= 0 or not actors:
            return f"**{label}** — none"
        # Show all tied names
        names = ", ".join(_short_actor(a) for a in actors)
        return f"**{label}** — {names}"

    award_lines = [
        _fmt_award("🪦 Pleb", deaths_tied, deaths_max),
        _fmt_award("🫦 Chinese hooker", downs_tied, downs_max),
        _fmt_award("💉 Medic", medic_tied, medic_max),
    ]
    em.add_field(name="✨ Session stars ✨", value="\n".join(award_lines), inline=False)

    # Spacer
    em.add_field(name="\u200b", value="\u200b", inline=False)

    # --- Encounters per wing, chunked to <=1024 each ---
    MAX_FIELDS = 25

    def _remaining_fields() -> int:
        return MAX_FIELDS - len(em.fields)

    wing_order = [f"W{i}" for i in range(1, 9)] + ["Other"]
    for code in wing_order:
        lines = raw_by_wing.get(code)
        if not lines:
            continue
        title = WING_TITLES.get(code, code)
        wing_text = "\n".join(lines)
        chunks = _chunk_by_lines(wing_text, 1024)
        for idx, chunk in enumerate(chunks):
            # If we are about to run out of field slots, truncate the last chunk
            if _remaining_fields() <= 0:
                break
            name = title if idx == 0 else f"{title} (cont.)"
            # ensure value length
            if len(chunk) > 1024:
                chunk = chunk[:1021] + "…"
            em.add_field(name=name, value=chunk, inline=False)
        if _remaining_fields() <= 0:
            break

    em.colour = discord.Colour(0x2ecc71)
    return em
