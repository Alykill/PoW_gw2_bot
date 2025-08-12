# analytics/service.py
from __future__ import annotations

import os
import json
import aiohttp
import aiosqlite
from typing import Dict, List, Tuple, Any, DefaultDict, Union
from collections import defaultdict
import discord

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
    # dict case
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
    # list[dict] case
    if isinstance(obj, list):
        for el in obj:
            if isinstance(el, dict):
                v = _get_from_maybe_list(el, *keys, default=None)
                if v is not None:
                    return v
        return default
    return default

def _player_downs(p: Dict[str, Any]) -> int:
    # EI: defenses can be dict OR list[dict]; keys vary by EI version
    defenses = p.get("defenses")
    return int(_get_from_maybe_list(defenses, "downCount", "downs", "downed", "downedCount", default=0))

def _player_deaths(p: Dict[str, Any]) -> int:
    defenses = p.get("defenses")
    return int(_get_from_maybe_list(defenses, "deadCount", "deaths", default=0))

def _player_resurrects(p: Dict[str, Any]) -> int:
    # EI: support can be dict OR list[dict]
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

def _match_text(s: str, needles: List[str]) -> bool:
    ls = (s or "").lower()
    return any(n in ls for n in needles)

def _collect_mechanic_counts(j: Dict[str, Any], matchers: List[str]) -> Dict[str, int]:
    """
    Returns {actor -> count} for mechanics whose name/desc matches any matcher.
    EI shapes vary; be defensive about non-dict / non-list entries.
    """
    per_actor: DefaultDict[str, int] = defaultdict(int)

    mechs = j.get("mechanics") or []
    if not isinstance(mechs, list):
        return {}

    for m in mechs:
        if not isinstance(m, dict):
            continue
        texts = [
            str(m.get("name") or ""),
            str(m.get("fullName") or ""),
            str(m.get("description") or ""),
            str(m.get("tooltip") or ""),
        ]
        if not any(_match_text(t, matchers) for t in texts):
            continue

        players_list = m.get("players") or []
        if not isinstance(players_list, list):
            continue
        for rec in players_list:
            if not isinstance(rec, dict):
                continue
            actor = rec.get("account") or rec.get("name") or "Unknown"
            c = int(rec.get("c", rec.get("count", 0)) or 0)
            if c > 0:
                per_actor[actor] += c

    return dict(per_actor)

# ---------- Payload normalization (permali﻿nk/file -> EI dict) ----------
async def _coerce_payload_to_json(payload: Union[dict, str]) -> Dict[str, Any]:
    """
    Accepts:
      - dict: EI JSON (if it lacks 'players' but has a permalink, we fetch that)
      - str: local JSON path OR dps.report permalink (we try several endpoints)
    Returns a parsed EI JSON dict.
    """
    # Already a dict?
    if isinstance(payload, dict):
        # If this already looks like EI, use it:
        if isinstance(payload.get("players"), list):
            return payload
        # If it looks like an upload response with a permalink, follow it:
        link = payload.get("permalink") or payload.get("permaLink") or payload.get("id")
        if isinstance(link, str) and link:
            return await _coerce_payload_to_json(link)
        # Otherwise return as-is; caller will validate/raise if needed.
        return payload

    # Must be a string now
    if not isinstance(payload, str):
        raise TypeError("payload must be dict or str (url or file path)")

    s = payload.strip()

    # Local JSON file?
    if os.path.isfile(s):
        with open(s, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError(f"Local JSON was {type(data).__name__}, expected object")
            return data

    # URL → try known EI JSON endpoints
    if s.startswith("http://") or s.startswith("https://"):
        base = s.rstrip("/")
        if base.lower().endswith(".json"):
            candidates = [base]
        else:
            candidates = [
                base + "/ei.json",
                base + "/json",
                base + "/report.json",
                base + "/index.json",
            ]

        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; gw2-raidbot/1.0)"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as sess:
            last_err: Exception | None = None
            for url in candidates:
                try:
                    async with sess.get(url, allow_redirects=True) as resp:
                        if resp.status != 200:
                            last_err = RuntimeError(f"{url} => HTTP {resp.status}")
                            continue
                        # EI sometimes serves text/plain
                        text = await resp.text()
                        data = json.loads(text)
                        if isinstance(data, dict) and isinstance(data.get("players"), list):
                            return data
                        last_err = ValueError(f"{url} returned {type(data).__name__}, not EI object")
                except Exception as e:
                    last_err = e
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
                counts = _collect_mechanic_counts(upload_json, spec["match"])
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
                    overall = float(v or 0)
                    agg[actor] += overall

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

async def build_event_analytics_embeds(event_name: str) -> List[discord.Embed]:
    data = await build_event_metrics(event_name)
    if not data:
        return []

    embeds: List[discord.Embed] = []

    em = discord.Embed(title=f"📈 Analytics — {event_name}", color=discord.Color.purple())
    em.add_field(name="Times downed (Top 10)", value=_fmt_table(data.get("overall_downs", [])), inline=False)
    em.add_field(name="Times died (Top 10)", value=_fmt_table(data.get("overall_deaths", [])), inline=False)
    em.add_field(name="Resurrects (Top 10)", value=_fmt_table(data.get("overall_resurrects", [])), inline=False)
    em.add_field(name="Top DPS per encounter", value=_fmt_top_dps(data.get("top_boss_dps", []), limit=25), inline=False)
    em.set_footer(text="Metrics from dps.report / Elite Insights JSON")
    embeds.append(em)

    enc_spec = data.get("encounter_specific", {})
    for enc_name, rows in enc_spec.items():
        by_label: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)
        for label, actor, v in rows:
            by_label[label].append((actor, v))
        em2 = discord.Embed(title=f"🧩 Encounter Mechanics — {enc_name}", color=discord.Color.blurple())
        for label, arr in by_label.items():
            arr_sorted = sorted(arr, key=lambda x: (-x[1], x[0].lower()))
            em2.add_field(name=label, value=_fmt_table(arr_sorted, limit=10), inline=False)
        embeds.append(em2)

    return embeds
