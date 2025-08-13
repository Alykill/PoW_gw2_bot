# analytics/service.py
from __future__ import annotations

import os
import json
import aiohttp
import aiosqlite
from typing import Dict, List, Tuple, Any, DefaultDict, Union, Optional
from collections import defaultdict
import discord
import re
import asyncio

from .registry import ENCOUNTER_MECHANICS

# ---------- Storage ----------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  upload_id INTEGER,
  boss_name TEXT,
  actor TEXT,
  metric_key TEXT,
  value REAL
);
CREATE INDEX IF NOT EXISTS idx_metrics_upload ON metrics(upload_id);
CREATE INDEX IF NOT EXISTS idx_metrics_event_actor ON metrics(actor, metric_key);
"""

async def _ensure_tables():
    async with aiosqlite.connect("events.db") as db:
        for stmt in CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                await db.execute(s)
        await db.commit()

# ---------- Helpers (EI JSON shape) ----------
def _encounter_dict(j: Dict[str, Any]) -> Dict[str, Any]:
    enc = j.get("encounter")
    return enc if isinstance(enc, dict) else {}

def _boss_name(j: Dict[str, Any]) -> str:
    enc = _encounter_dict(j)
    boss = enc.get("boss") or j.get("boss") or j.get("fightName") or "Unknown Encounter"
    return str(boss).strip()

def _players(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    ps = j.get("players")
    return ps if isinstance(ps, list) else []

def _actor_name(p: Dict[str, Any]) -> str:
    return p.get("account") or p.get("name") or "Unknown"

def _get_from_maybe_list(obj: Any, *keys, default=0):
    """
    Read a numeric value from either:
      - a dict (checking multiple key aliases), or
      - a list of dicts (return the first non-missing value).
    Returns default if missing / unparsable.
    """
    if isinstance(obj, dict):
        for k in keys:
            if k in obj and obj[k] is not None:
                try:
                    return int(obj[k])
                except Exception:
                    try:
                        return float(obj[k])
                    except Exception:
                        return default
        return default
    if isinstance(obj, list):
        for el in obj:
            if isinstance(el, dict):
                v = _get_from_maybe_list(el, *keys, default=None)
                if v is not None:
                    return v
        return default
    return default

def _player_downs(p: Dict[str, Any]) -> int:
    defenses = p.get("defenses")
    return int(_get_from_maybe_list(defenses, "downCount", "downs", "downed", "downedCount", default=0))

def _player_deaths(p: Dict[str, Any]) -> int:
    defenses = p.get("defenses")
    return int(_get_from_maybe_list(defenses, "deadCount", "deaths", default=0))

def _player_resurrects(p: Dict[str, Any]) -> int:
    support = p.get("support")
    return int(_get_from_maybe_list(support, "resurrects", "resurrectsPerformed", default=0))

def _player_boss_dps(p: Dict[str, Any]) -> float:
    """
    Prefer target DPS (power+condi) for phase 0 of first target.
    Fallback to overall dpsAll[0].dps if needed.
    """
    try:
        dps_targets = p.get("dpsTargets")
        if isinstance(dps_targets, list) and dps_targets:
            t0 = dps_targets[0]
            if isinstance(t0, list) and t0:
                e = t0[0]
                if isinstance(e, dict):
                    if "powerDps" in e or "condiDps" in e:
                        return float(e.get("powerDps", 0)) + float(e.get("condiDps", 0))
                    return float(e.get("dps", 0))
        dps_all = p.get("dpsAll")
        if isinstance(dps_all, list) and dps_all:
            e = dps_all[0]
            if isinstance(e, dict):
                if "powerDps" in e or "condiDps" in e:
                    return float(e.get("powerDps", 0)) + float(e.get("condiDps", 0))
                return float(e.get("dps", 0))
    except Exception:
        pass
    return 0.0

from typing import Optional, Tuple

def _collect_mechanic_counts(
    j: Dict[str, Any],
    substrings: List[str],
    exact_names: Optional[List[str]] = None,
    canonical: Optional[str] = None,
    dedup_ms: Optional[int] = None,
) -> Dict[str, int]:
    """
    Return {account -> count} for a given mechanic spec.

    Matching: exact (preferred) or substring fallback.
    Priority:
      1) mechanics[*].players / playerHits (EI aggregated)
      2) mechanics[*].mechanicsData (per hit, dedup across entries using canonical key; optional time tolerance)
      3) top-level mechanic logs (rare fallback; same dedup)
    Dedup applies ONLY across different mechanic entries (not within the same entry).
    """

    def _texts(m: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
        return (
            str(m.get("name") or ""),
            str(m.get("shortName") or ""),
            str(m.get("fullName") or ""),
            str(m.get("description") or ""),
            str(m.get("tooltip") or ""),
        )

    def _match(m: Dict[str, Any]) -> bool:
        name, short, full, desc, tip = _texts(m)
        if exact_names:
            lowers = {t.strip().lower() for t in (name, short, full) if t}
            exacts = {e.strip().lower() for e in exact_names if e}
            return bool(lowers & exacts)
        lows = [t.lower() for t in (name, short, full, desc, tip) if t]
        needles = [s.lower() for s in (substrings or []) if s]
        return any(any(n in t for t in lows) for n in needles)

    canon_key = (canonical or (",".join((exact_names or substrings or ["mechanic"])))).strip().lower()

    per_actor: DefaultDict[str, int] = defaultdict(int)

    # Map character -> account
    name_to_account: Dict[str, str] = {}
    for pl in (j.get("players") or []):
        if isinstance(pl, dict):
            ch = pl.get("name"); acct = pl.get("account")
            if ch and acct:
                name_to_account[str(ch)] = str(acct)

    mechanics = j.get("mechanics") or []

    # ---- 1) Prefer EI aggregated counts ----
    used_agg = False
    for m in mechanics:
        if not isinstance(m, dict) or not _match(m):
            continue
        players_list = m.get("players") or m.get("playerHits")
        if isinstance(players_list, list) and players_list:
            used_agg = True
            for rec in players_list:
                if not isinstance(rec, dict):
                    continue
                account = rec.get("account")
                if not account:
                    account = name_to_account.get(str(rec.get("name") or rec.get("actor") or ""))
                if not account:
                    continue
                c = rec.get("c", rec.get("count", 1))
                try: c = int(c)
                except Exception: c = 1
                if c > 0:
                    per_actor[str(account)] += c
    if used_agg:
        return dict(per_actor)

    # Helper: cross-entry dedup
    # For each (account, canon_key) keep a list of (ms, entry_index) we've already counted.
    seen_by_actor: Dict[Tuple[str, str], List[Tuple[int, int]]] = defaultdict(list)

    def _already_seen(account: str, ms: Optional[int], entry_idx: int) -> bool:
        if ms is None:
            return False
        lst = seen_by_actor[(account, canon_key)]
        # cross-entry dedup: allow same-entry duplicates, suppress different-entry duplicates
        for prev_ms, prev_idx in lst:
            if prev_idx == entry_idx:
                continue  # same entry → keep (don’t dedup)
            if dedup_ms is None:
                if prev_ms == ms:
                    return True
            else:
                if abs(prev_ms - ms) <= dedup_ms:
                    return True
        return False

    def _mark_seen(account: str, ms: Optional[int], entry_idx: int):
        if ms is None:
            return
        seen_by_actor[(account, canon_key)].append((ms, entry_idx))

    # ---- 2) mechanics[*].mechanicsData ----
    for idx, m in enumerate(mechanics):
        if not isinstance(m, dict) or not _match(m):
            continue
        md = m.get("mechanicsData")
        if not (isinstance(md, list) and md):
            continue
        for rec in md:
            if not isinstance(rec, dict):
                continue
            actor_char = rec.get("actor") or rec.get("name")
            account = name_to_account.get(str(actor_char)) if actor_char else None
            if not account:
                continue
            t = rec.get("time")
            try:
                ms = int(t) if t is not None else None
            except Exception:
                ms = None
            if _already_seen(account, ms, idx):
                continue
            _mark_seen(account, ms, idx)
            per_actor[str(account)] += 1

    if per_actor:
        return dict(per_actor)

    # ---- 3) Fallback: top-level logs ----
    log_candidates: List[List[Dict[str, Any]]] = []
    for k in ("mechanicLogs", "mechanicsLogs", "mechanicsLog", "mechanicsEvents", "mechLogs"):
        v = j.get(k)
        if isinstance(v, list) and v:
            log_candidates.append(v)
    for k in ("mechanicLogsById", "mechanicsById", "mechData"):
        v = j.get(k)
        if isinstance(v, dict) and v:
            for vv in v.values():
                if isinstance(vv, list) and vv:
                    log_candidates.append(vv)

    for idx, logs in enumerate(log_candidates):
        for rec in logs:
            if not isinstance(rec, dict):
                continue
            pseudo = {
                "name": rec.get("mechanic") or rec.get("name"),
                "shortName": rec.get("shortName"),
                "fullName": rec.get("fullName"),
                "description": rec.get("description"),
                "tooltip": rec.get("tooltip"),
            }
            if not _match(pseudo):
                continue
            actor_char = rec.get("actor") or rec.get("name") or rec.get("source")
            account = name_to_account.get(str(actor_char)) if actor_char else None
            if not account:
                continue
            t = rec.get("time")
            try:
                ms = int(t) if t is not None else None
            except Exception:
                ms = None
            if _already_seen(account, ms, idx):
                continue
            _mark_seen(account, ms, idx)
            per_actor[str(account)] += 1

    return dict(per_actor)

# ---------- Payload normalization (permalink/file -> EI dict) ----------
async def _coerce_payload_to_json(payload: Union[dict, str]) -> Dict[str, Any]:
    """
    Accepts:
      - dict: EI JSON (if it lacks 'players' but has a permalink, we fetch that)
      - str: local JSON path OR dps.report permalink (we try several endpoints)
    Returns a parsed EI JSON dict.
    """
    if isinstance(payload, dict):
        if isinstance(payload.get("players"), list):
            return payload
        link = payload.get("permalink") or payload.get("permaLink") or payload.get("id")
        if isinstance(link, str) and link:
            return await _coerce_payload_to_json(link)
        return payload

    if not isinstance(payload, str):
        raise TypeError("payload must be dict or str (url or file path)")

    s = payload.strip()

    if os.path.isfile(s):
        with open(s, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError(f"Local JSON was {type(data).__name__}, expected object")
            return data

    if s.startswith("http://") or s.startswith("https://"):
        base = s.rstrip("/")

        # extract permalink id if present
        m = re.search(r"dps\.report/([^/?#]+)", base)
        permalink_id = m.group(1) if m else None

        # build endpoint candidates
        if base.lower().endswith(".json"):
            base_candidates = [base]
        else:
            base_candidates = []
            if permalink_id:
                base_candidates.append(f"https://dps.report/getJson?permalink={permalink_id}")  # try this first
            base_candidates += [
                base + "/ei.json",
                base + "/json",
                base + "/report.json",
                base + "/index.json",
            ]

        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; gw2-raidbot/1.0)"}

        # retry a few times because EI JSON may not be ready right after upload
        attempts = 5
        backoff = 1.5  # seconds, exponential

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as sess:
            last_err: Exception | None = None
            for attempt in range(1, attempts + 1):
                for url in base_candidates:
                    try:
                        async with sess.get(url, allow_redirects=True) as resp:
                            if resp.status != 200:
                                last_err = RuntimeError(f"{url} => HTTP {resp.status}")
                                continue
                            text = await resp.text()
                            data = json.loads(text)
                            if isinstance(data, dict) and isinstance(data.get("players"), list):
                                return data
                            # some getJson responses wrap in a list; accept first dict-like
                            if isinstance(data, list):
                                for el in data:
                                    if isinstance(el, dict) and isinstance(el.get("players"), list):
                                        return el
                            last_err = ValueError(f"{url} returned unusable JSON shape")
                    except Exception as e:
                        last_err = e
                        continue

                if attempt < attempts:
                    await asyncio.sleep(backoff)
                    backoff *= 2
            raise last_err or ValueError("No valid EI JSON endpoint returned an object")

    raise ValueError("payload string was neither a readable file nor a valid URL")

# ---------- Public API ----------
async def enrich_upload(upload_id: int, payload: Union[dict, str]):
    """
    Normalize payload to EI JSON and write per-player & encounter metrics for upload_id.
    """
    upload_json = await _coerce_payload_to_json(payload)
    if not isinstance(upload_json, dict):
        prev = (str(upload_json)[:120] + "...") if not isinstance(upload_json, dict) else ""
        raise ValueError(f"EI JSON was not an object (got {type(upload_json).__name__}): {prev}")

    await _ensure_tables()

    players = _players(upload_json)
    if not isinstance(players, list):
        raise ValueError(f"Unexpected 'players' type: {type(players).__name__}")

    boss = _boss_name(upload_json)

    rows: List[Tuple[int, str, str, float]] = []

    # Global per-player metrics
    for p in players:
        if not isinstance(p, dict):
            continue
        actor = _actor_name(p)
        rows.append((upload_id, actor, "downs",      float(_player_downs(p))))
        rows.append((upload_id, actor, "deaths",     float(_player_deaths(p))))
        rows.append((upload_id, actor, "resurrects", float(_player_resurrects(p))))
        rows.append((upload_id, actor, "boss_dps",   float(_player_boss_dps(p))))

    # Encounter-specific mechanics via registry
    b_lower = boss.lower()
    for key, specs in ENCOUNTER_MECHANICS.items():
        if key in b_lower:
            for spec in specs:
                counts = _collect_mechanic_counts(
                    upload_json,
                    substrings=spec.get("match", []),
                    exact_names=spec.get("exact"),
                    canonical=spec.get("canonical"),
                    dedup_ms=spec.get("dedup_ms"),
                )
                for actor, cnt in counts.items():
                    rows.append((upload_id, actor, spec["key"], float(cnt)))

    # Write to DB
    async with aiosqlite.connect("events.db") as db:
        await db.execute("DELETE FROM metrics WHERE upload_id = ?", (upload_id,))
        if rows:
            await db.executemany(
                "INSERT INTO metrics (upload_id, boss_name, actor, metric_key, value) VALUES (?, ?, ?, ?, ?)",
                [(upload_id, boss, a, k, v) for (_, a, k, v) in rows],
            )
        await db.commit()

# ---------- Aggregation ----------
async def build_event_metrics(event_name: str) -> Dict[str, Any]:
    await _ensure_tables()
    async with aiosqlite.connect("events.db") as db:
        cur = await db.execute(
            """
            SELECT id, boss_name
            FROM uploads
            WHERE event_name = ?
            ORDER BY time_utc ASC
            """,
            (event_name,),
        )
        uploads = await cur.fetchall()
        if not uploads:
            return {}

        overall_downs: DefaultDict[str, float] = defaultdict(float)
        overall_deaths: DefaultDict[str, float] = defaultdict(float)
        overall_res: DefaultDict[str, float] = defaultdict(float)
        top_dps_per_encounter: List[Tuple[str, str, float]] = []
        enc_specific: DefaultDict[str, List[Tuple[str, str, float]]] = defaultdict(list)

        for upload_id, boss_name in uploads:
            # Global aggregates
            for key, agg in [("downs", overall_downs), ("deaths", overall_deaths), ("resurrects", overall_res)]:
                cur = await db.execute(
                    """
                    SELECT actor, SUM(value) FROM metrics
                    WHERE upload_id = ? AND metric_key = ?
                    GROUP BY actor
                    """,
                    (upload_id, key),
                )
                for actor, v in await cur.fetchall():
                    agg[actor] += float(v or 0)

            # Top DPS per encounter
            cur = await db.execute(
                """
                SELECT actor, value FROM metrics
                WHERE upload_id = ? AND metric_key = 'boss_dps'
                ORDER BY value DESC
                LIMIT 1
                """,
                (upload_id,),
            )
            top = await cur.fetchone()
            if top:
                top_dps_per_encounter.append((boss_name, top[0], float(top[1])))

            # Encounter-specific metrics
            b_lower = (boss_name or "").lower()
            wanted_keys: List[str] = []
            for k, specs in ENCOUNTER_MECHANICS.items():
                if k in b_lower:
                    wanted_keys.extend([s["key"] for s in specs])
            if wanted_keys:
                marks = ",".join("?" for _ in wanted_keys)
                cur = await db.execute(
                    f"""
                    SELECT metric_key, actor, SUM(value)
                    FROM metrics
                    WHERE upload_id = ? AND metric_key IN ({marks})
                    GROUP BY metric_key, actor
                    ORDER BY metric_key
                    """,
                    (upload_id, *wanted_keys),
                )
                rows = await cur.fetchall()
                label_map = {s["key"]: s["label"] for specs in ENCOUNTER_MECHANICS.values() for s in specs}
                for k, actor, v in rows:
                    enc_specific[boss_name].append((label_map.get(k, k), actor, float(v or 0)))

    def sort_desc(m: Dict[str, float]) -> List[Tuple[str, int]]:
        return sorted(((a, int(v)) for a, v in m.items()), key=lambda x: (-x[1], x[0].lower()))

    return {
        "overall_downs":      sort_desc(overall_downs),
        "overall_deaths":     sort_desc(overall_deaths),
        "overall_resurrects": sort_desc(overall_res),
        "top_boss_dps":       top_dps_per_encounter,
        "encounter_specific": dict(enc_specific),
    }

# ---------- Embeds ----------
def _fmt_table(rows: List[Tuple[str, float]], limit: int = 10) -> str:
    if not rows:
        return "_No data_"
    return "\n".join(f"• **{a}** — {int(v)}" for a, v in rows[:limit])

def _fmt_top_dps(rows: List[Tuple[str, str, float]], limit: int = 10) -> str:
    if not rows:
        return "_No data_"
    return "\n".join(f"• **{boss}** — **{actor}** ({int(dps)} DPS)" for boss, actor, dps in rows[:limit])

def _add_multicol_fields(
    em: discord.Embed,
    lines: List[str],
    max_cols: int = 3,
    field_name: str = "\u200b",
) -> None:
    """
    Add the given list of lines to the embed as up to `max_cols` inline fields,
    balancing items per column.
    """
    if not lines:
        em.add_field(name=field_name, value="_No data_", inline=True)
        return

    n = len(lines)
    cols = max_cols if n >= 9 else (2 if n >= 4 else 1)
    per_col = (n + cols - 1) // cols  # ceiling
    for i in range(cols):
        chunk = lines[i * per_col : (i + 1) * per_col]
        if not chunk:
            continue
        em.add_field(name=field_name, value="\n".join(chunk), inline=True)

# put near the bottom of service.py
async def ensure_enriched_for_event(event_name: str) -> int:
    """
    Re-enrich any uploads for this event that have no metrics yet.
    Returns the number of uploads that were (re)processed.
    """
    await _ensure_tables()
    repaired = 0
    async with aiosqlite.connect("events.db") as db:
        # find uploads with zero metrics
        cur = await db.execute(
            """
            SELECT u.id, COALESCE(NULLIF(u.permalink, ''), u.file_path) AS src
            FROM uploads u
            LEFT JOIN (
              SELECT upload_id, COUNT(*) AS cnt
              FROM metrics
              GROUP BY upload_id
            ) m ON m.upload_id = u.id
            WHERE u.event_name = ?
              AND (m.cnt IS NULL OR m.cnt = 0)
            ORDER BY datetime(u.time_utc) ASC
            """,
            (event_name,),
        )
        rows = await cur.fetchall()

    for upload_id, src in rows:
        try:
            # src can be a permalink or a path (our coerce handles both)
            await enrich_upload(upload_id, src)
            repaired += 1
        except Exception:
            # don't explode the whole pass; just skip this one
            pass

    return repaired

async def build_event_analytics_embeds(event_name: str) -> List[discord.Embed]:
    data = await build_event_metrics(event_name)
    if not data:
        return []

    embeds: List[discord.Embed] = []

    em = discord.Embed(title=f"📈 Analytics — {event_name}", color=discord.Color.purple())

    # Three compact columns: downs / deaths / res
    em.add_field(
        name="Times downed (Top 10)",
        value=_fmt_table(data.get("overall_downs", [])),
        inline=True,  # 👈 column
    )
    em.add_field(
        name="Times died (Top 10)",
        value=_fmt_table(data.get("overall_deaths", [])),
        inline=True,  # 👈 column
    )
    em.add_field(
        name="Resurrects (Top 10)",
        value=_fmt_table(data.get("overall_resurrects", [])),
        inline=True,  # 👈 column
    )

    # Top DPS per encounter — split across columns (2–3) if long
    top_dps_rows = data.get("top_boss_dps", [])
    if top_dps_rows:
        dps_lines = [f"• **{boss}** — **{actor}** ({int(dps)} DPS)" for boss, actor, dps in top_dps_rows[:25]]
        em.add_field(name="\u200b", value="\u200b", inline=False)  # spacer
        em.add_field(name="Top DPS per encounter", value="\u200b", inline=False)
        _add_multicol_fields(em, dps_lines, max_cols=3)

    em.set_footer(text="Metrics from dps.report / Elite Insights JSON")
    embeds.append(em)

    enc_spec = data.get("encounter_specific", {})
    for enc_name, rows in enc_spec.items():
        by_label: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)
        totals: DefaultDict[str, int] = defaultdict(int)
        for label, actor, v in rows:
            by_label[label].append((actor, v))
            totals[label] += int(v)

        em2 = discord.Embed(title=f"🧩 Encounter Mechanics — {enc_name}", color=discord.Color.blurple())

        # Sort labels by total desc
        labels_sorted = sorted(by_label.keys(), key=lambda k: -totals[k])

        # Render each mechanic label as an inline field (Discord will flow them into up to 3 columns)
        for label in labels_sorted:
            arr_sorted = sorted(by_label[label], key=lambda x: (-x[1], x[0].lower()))
            em2.add_field(
                name=label,
                value=_fmt_table(arr_sorted, limit=10),
                inline=True,  # 👈 columnized per label
            )

        embeds.append(em2)

    return embeds
