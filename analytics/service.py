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

def _norm_boss(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _boss_key_matches(boss_name: str, key: str) -> bool:
    nb = _norm_boss(boss_name)
    nk = _norm_boss(key)
    # match either way: "gorseval" vs "gorseval the multifarious"
    return nk in nb or nb in nk

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

# ---------- NEW: default coalescing window (used by your helpers below) ----------
COALESCE_DEFAULT_MS = int(os.getenv("MECH_COALESCE_MS", "1000"))

def _coalesce_ms_for(mech_cfg: Dict[str, Any]) -> int:
    """
    Registry may include 'coalesce_ms'. If missing, fall back to COALESCE_DEFAULT_MS.
    """
    try:
        return int(mech_cfg.get("coalesce_ms", COALESCE_DEFAULT_MS))
    except Exception:
        return COALESCE_DEFAULT_MS

def _collect_mechanic_actor_times(ei_json: Dict[str, Any],
                                  mech_idx: int,
                                  mech_def: Dict[str, Any]) -> Dict[str, List[int]]:
    """
    Returns {actor -> sorted [times_ms, ...]} for the mechanic.
    Supports both EI JSON layouts:
      A) mechanic definition contains mechanicsData: [{time, actor}, ...]
      B) global mechanicsData with per-actor blocks referencing 'mechanic' = index
    """
    out: Dict[str, List[int]] = {}

    md = mech_def.get("mechanicsData")
    if isinstance(md, list):  # Layout A
        for e in md:
            actor = e.get("actor")
            t = e.get("time")
            if actor and isinstance(t, (int, float)):
                out.setdefault(actor, []).append(int(t))
        for a in out:
            out[a].sort()
        return out

    # Layout B
    for entry in ei_json.get("mechanicsData", []):
        actor = entry.get("actor")
        if not actor:
            continue
        for ev in entry.get("mechanics", []):
            if ev.get("mechanic") == mech_idx:
                t = ev.get("time")
                if isinstance(t, (int, float)):
                    out.setdefault(actor, []).append(int(t))
    for a in out:
        out[a].sort()
    return out

def _collapse_occurrences(times_ms: List[int], gap_ms: int) -> int:
    """
    Collapse raw hit timestamps into EI-style occurrences by merging consecutive hits
    whose gap <= gap_ms. Returns the number of collapsed occurrences.
    """
    if not times_ms:
        return 0
    occ = 1
    prev = times_ms[0]
    for t in times_ms[1:]:
        if t - prev > gap_ms:
            occ += 1
        prev = t
    return occ

# ---------- (existing) generic collector ----------
def _collect_mechanic_counts(
    j: Dict[str, Any],
    substrings: List[str],
    exact_names: Optional[List[str]] = None,
    canonical: Optional[str] = None,
    dedup_ms: Optional[int] = None,
    COALESCE_DEFAULT_MS = int(os.getenv("MECH_COALESCE_MS", "1000"))
) -> Dict[str, int]:
    """
    Return {account -> count} for a given mechanic spec.

    Matching:
      - If exact_names are provided -> STRICT equality on normalized name/shortName/fullName.
      - Else fall back to normalized substrings (name/shortName/fullName only).

    Priority:
      1) mechanics[*].mechanicsData (per hit, dedup across *different* mechanic entries)
      2) mechanics[*].players / playerHits (EI aggregated)  — only if (1) yields nothing
      3) top-level mechanic logs                           — only if (1) & (2) yield nothing
    """

    def _norm(s: Optional[str]) -> str:
        s = (s or "").lower().strip()
        return re.sub(r"[^a-z0-9]+", "", s)

    # Normalize match inputs
    exact_norm: List[str] = [_norm(x) for x in (exact_names or []) if x]
    subs_norm:  List[str] = [_norm(x) for x in (substrings or []) if x]

    def _match_alias_from_mech_entry(m: Dict[str, Any]) -> Optional[str]:
        """Return the matched token (normalized) or None."""
        fields = [
            _norm(m.get("name")),
            _norm(m.get("shortName")),
            _norm(m.get("fullName")),
        ]
        # 1) strict equality for exact_names
        if exact_norm:
            for ex in exact_norm:
                if ex and any(f == ex for f in fields if f):
                    return ex
        # 2) substring fallback against name-ish fields only
        for sub in subs_norm:
            if sub and any(sub in f for f in fields if f):
                return sub
        return None

    # Build char->account map
    name_to_account: Dict[str, str] = {}
    for pl in (j.get("players") or []):
        if isinstance(pl, dict):
            ch = pl.get("name"); acct = pl.get("account")
            if ch and acct:
                name_to_account[str(ch)] = str(acct)

    canon_key = (canonical or ",".join((exact_names or substrings or ["mechanic"])) ).strip().lower()
    per_actor: DefaultDict[str, int] = defaultdict(int)

    # Dedup per account + canonical across ALL entries (including same entry)
    seen_by_actor: Dict[Tuple[str, str], List[int]] = defaultdict(list)

    def _already_seen(account: str, ms: Optional[int]) -> bool:
        # no dedup if time or window missing
        if ms is None or dedup_ms is None:
            return False
        times = seen_by_actor[(account, canon_key)]
        for prev_ms in times:
            if abs(prev_ms - int(dedup_ms)) <= int(dedup_ms):  # placeholder kept for compatibility
                pass
        for prev_ms in times:
            if abs(prev_ms - ms) <= int(dedup_ms):
                return True
        return False

    def _mark_seen(account: str, ms: Optional[int]):
        if ms is None:
            return
        seen_by_actor[(account, canon_key)].append(ms)

    mechanics = j.get("mechanics") or []

    # ---- (1) Prefer per-hit mechanicsData ----
    per_hit_found = False
    for idx, m in enumerate(mechanics):
        if not isinstance(m, dict):
            continue
        matched = _match_alias_from_mech_entry(m)
        if not matched:
            continue
        md = m.get("mechanicsData")
        if not (isinstance(md, list) and md):
            continue
        per_hit_found = True
        for rec in md:
            if not isinstance(rec, dict):
                continue
            actor_char = rec.get("actor") or rec.get("name")
            account = name_to_account.get(str(actor_char)) if actor_char else None
            if not account:
                continue
            # EI times can be seconds(float) or ms(int)
            t = rec.get("time")
            try:
                # if it's small -> seconds to ms
                x = float(t)
                ms = int(round(x * 1000.0)) if x < 1e5 else int(round(x))
            except Exception:
                ms = None
            if _already_seen(account, ms):
                continue
            _mark_seen(account, ms)
            per_actor[account] += 1

    if per_hit_found and per_actor:
        return dict(per_actor)

    # ---- (2) Fall back to EI aggregated counts (players/playerHits) ----
    used_agg = False
    for m in mechanics:
        if not isinstance(m, dict):
            continue
        if not _match_alias_from_mech_entry(m):
            continue
        players_list = m.get("players") or m.get("playerHits")
        if isinstance(players_list, list) and players_list:
            used_agg = True
            for rec in players_list:
                if not isinstance(rec, dict):
                    continue
                account = rec.get("account") or name_to_account.get(str(rec.get("name") or rec.get("actor") or ""))
                if not account:
                    continue
                try:
                    c = int(rec.get("c", rec.get("count", 1)))
                except Exception:
                    c = 1
                if c > 0:
                    per_actor[account] += c
    if used_agg and per_actor:
        return dict(per_actor)

    # ---- (3) Fallback: top-level logs variants (rare) ----
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
            # Build a pseudo-mechanic name triple to match strictly
            pseudo = {
                "name": rec.get("mechanic") or rec.get("name"),
                "shortName": rec.get("shortName"),
                "fullName": rec.get("fullName"),
            }
            # reuse the matcher from above
            def _norm(s: Optional[str]) -> str:
                s = (s or "").lower().strip()
                return re.sub(r"[^a-z0-9]+", "", s)
            fields = [_norm(pseudo.get("name")), _norm(pseudo.get("shortName")), _norm(pseudo.get("fullName"))]
            hit = False
            exact_norm: List[str] = [_norm(x) for x in (exact_names or []) if x]
            subs_norm:  List[str] = [_norm(x) for x in (substrings or []) if x]
            if exact_norm:
                hit = any(ex == f for ex in exact_norm for f in fields if f)
            if not hit:
                hit = any(sub and any(sub in f for f in fields if f) for sub in subs_norm)
            if not hit:
                continue

            actor_char = rec.get("actor") or rec.get("name") or rec.get("source")
            account = name_to_account.get(str(actor_char)) if actor_char else None
            if not account:
                continue
            t = rec.get("time")
            try:
                x = float(t)
                ms = int(round(x * 1000.0)) if x < 1e5 else int(round(x))
            except Exception:
                ms = None
            if _already_seen(account, ms):
                continue
            _mark_seen(account, ms)
            per_actor[account] += 1

    return dict(per_actor)

# ---------- NEW: name matcher + collapsed collector ----------
def _match_mech_names(mech_def: Dict[str, Any], exact_names: Optional[List[str]], substrings: Optional[List[str]]) -> bool:
    """True if mech_def matches by exact name/shortName/fullName or (fallback) substring."""
    def _norm(s: Optional[str]) -> str:
        s = (s or "").lower().strip()
        return re.sub(r"[^a-z0-9]+", "", s)
    fields = {_norm(mech_def.get("name")), _norm(mech_def.get("shortName")), _norm(mech_def.get("fullName"))}
    if exact_names:
        exact_norm = {_norm(x) for x in exact_names if x}
        if any(f in exact_norm for f in fields if f):
            return True
    if substrings:
        subs_norm = [_norm(x) for x in substrings if x]
        if any(sub and any(sub in f for f in fields if f) for sub in subs_norm):
            return True
    return False

def _collect_mechanic_counts_collapsed(ei_json: Dict[str, Any],
                                       exact_names: Optional[List[str]],
                                       substrings: Optional[List[str]],
                                       gap_ms: int) -> Dict[str, int]:
    """
    EI-style occurrences: find matching mechanics, collect per-actor times, collapse by gap_ms.
    If multiple mechanic entries match, sums occurrences per actor across them.
    """
    mechanics = ei_json.get("mechanics") or []
    per_actor_total: DefaultDict[str, int] = defaultdict(int)

    for mech_idx, mech_def in enumerate(mechanics):
        if not isinstance(mech_def, dict):
            continue
        if not _match_mech_names(mech_def, exact_names, substrings):
            continue
        per_actor_times = _collect_mechanic_actor_times(ei_json, mech_idx, mech_def)
        for actor, times in per_actor_times.items():
            per_actor_total[actor] += _collapse_occurrences(times, gap_ms)

    return dict(per_actor_total)

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
    b_name = boss  # keep original
    for key, specs in ENCOUNTER_MECHANICS.items():
        if _boss_key_matches(b_name, key):
            for spec in specs:
                # NEW: if spec specifies coalesced occurrences, use EI-style collapsed logic
                if "coalesce_ms" in spec:
                    counts = _collect_mechanic_counts_collapsed(
                        upload_json,
                        exact_names=spec.get("exact"),
                        substrings=spec.get("match", []),
                        gap_ms=_coalesce_ms_for(spec),
                    )
                else:
                    # Legacy path (unchanged)
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
def _truthy_success(val: Any) -> bool:
    """Normalize success value from uploads table (int/bool/str) to True/False."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return int(val) != 0
    s = str(val).strip().lower()
    return s in ("1", "true", "t", "yes", "y")

async def build_event_metrics(event_name: str) -> Dict[str, Any]:
    await _ensure_tables()
    async with aiosqlite.connect("events.db") as db:
        # Pull uploads with success + boss_id so we can pick the FIRST successful attempt per boss
        cur = await db.execute(
            """
            SELECT id, boss_name, success, boss_id, time_utc
            FROM uploads
            WHERE event_name = ?
            ORDER BY datetime(time_utc) ASC
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

        # Pick the first SUCCESSFUL upload per boss (boss_id, boss_name)
        first_success_by_boss: Dict[Tuple[int, str], int] = {}
        for upload_id, boss_name, success, boss_id, _t in uploads:
            if _truthy_success(success):
                key = (int(boss_id or -1), str(boss_name or ""))
                if key not in first_success_by_boss:
                    first_success_by_boss[key] = int(upload_id)

        # Global aggregates & encounter-specific (keep current behavior = include all attempts)
        for upload_id, boss_name, success, boss_id, _t in uploads:
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

            # Encounter-specific mechanics (unchanged: all attempts)
            wanted_keys: List[str] = []
            for k, specs in ENCOUNTER_MECHANICS.items():
                if _boss_key_matches(boss_name or "", k):
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

        # Compute Top DPS only for the FIRST SUCCESS per boss
        for (boss_id, boss_name), succ_upload_id in first_success_by_boss.items():
            cur = await db.execute(
                """
                SELECT actor, value FROM metrics
                WHERE upload_id = ? AND metric_key = 'boss_dps'
                ORDER BY value DESC
                LIMIT 1
                """,
                (succ_upload_id,),
            )
            top = await cur.fetchone()
            if top:
                top_dps_per_encounter.append((boss_name, top[0], float(top[1])))

    def sort_desc(m: Dict[str, float]) -> List[Tuple[str, int]]:
        return sorted(((a, int(v)) for a, v in m.items()), key=lambda x: (-x[1], x[0].lower()))

    return {
        "overall_downs":      sort_desc(overall_downs),
        "overall_deaths":     sort_desc(overall_deaths),
        "overall_resurrects": sort_desc(overall_res),
        "top_boss_dps":       top_dps_per_encounter,
        "encounter_specific": dict(enc_specific),
    }

# shorten "Name.1234" -> "Name"
_NAME_TAG_RE = re.compile(r"\.\d{3,5}$")

def _short_actor(name: str | None) -> str:
    s = str(name or "")
    return _NAME_TAG_RE.sub("", s)

# ---------- Embeds ----------
def _fmt_table(rows: List[Tuple[str, float]], limit: int = 10) -> str:
    if not rows:
        return "_No data_"
    return "\n".join(f"• **{_short_actor(a)}** — {int(v)}" for a, v in rows[:limit])

def _fmt_top_dps(rows: List[Tuple[str, str, float]], limit: int = 10) -> str:
    if not rows:
        return "_No data_"
    return "\n".join(
        f"• **{boss}** — **{_short_actor(actor)}** ({int(dps)} DPS)"
        for boss, actor, dps in rows[:limit]
    )

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

def _sum_mech(values):
    return sum(int(v) for _a, v in values)

def _format_mechanics_two_cols(by_label: DefaultDict[str, List[Tuple[str, float]]],
                               per_label_limit: int = 10) -> Tuple[str, str]:
    """
    Collapse per-label mechanics into two text columns.
    Each label block shows the label header and top actors with counts.
    """
    # Sort labels by total desc
    labels_sorted = sorted(by_label.keys(), key=lambda k: -_sum_mech(by_label[k]))

    left_blocks, right_blocks = [], []
    for i, label in enumerate(labels_sorted):
        arr_sorted = sorted(by_label[label], key=lambda x: (-x[1], x[0].lower()))
        lines = [f"**{label}**"] + [f"• **{actor}** — {int(v)}" for actor, v in arr_sorted[:per_label_limit]]
        block = "\n".join(lines)
        (left_blocks if i % 2 == 0 else right_blocks).append(block)

    col1 = "\n\n".join(left_blocks) if left_blocks else "_No data_"
    col2 = "\n\n".join(right_blocks) if right_blocks else "_No data_"
    return col1, col2


async def build_event_analytics_embeds(event_name: str) -> List[discord.Embed]:
    data = await build_event_metrics(event_name)
    if not data:
        return []

    embeds: List[discord.Embed] = []

    # Primary analytics embed
    em = discord.Embed(title=f"📈 Analytics — {event_name}", color=discord.Color.purple())
    em.add_field(name="Times downed ♿", value=_fmt_table(data.get("overall_downs", [])), inline=True)
    em.add_field(name="Times died 💀",  value=_fmt_table(data.get("overall_deaths", [])), inline=True)
    em.add_field(name="Ressed others 🕊️", value=_fmt_table(data.get("overall_resurrects", [])), inline=True)
    em.set_footer(text="Metrics from dps.report / Elite Insights JSON")
    embeds.append(em)

    # Append encounter mechanics INSIDE the analytics embed (after metrics).
    enc_spec = data.get("encounter_specific", {})
    current = em

    for enc_name, rows in enc_spec.items():
        # Aggregate rows across all attempts:
        # label -> (actor -> summed value)
        per_label_sums: DefaultDict[str, DefaultDict[str, float]] = defaultdict(lambda: defaultdict(float))
        for label, actor, v in rows:
            per_label_sums[label][actor] += float(v or 0)

        # Convert to label -> [(actor_display, total)] with shortened names,
        # sorted desc by total then by name. We keep aggregation on full accounts,
        # and only shorten at display time.
        by_label_display: DefaultDict[str, List[Tuple[str, float]]] = defaultdict(list)
        for label, actor_map in per_label_sums.items():
            aggregated_short = [(_short_actor(actor), total) for actor, total in actor_map.items()]
            aggregated_short.sort(key=lambda x: (-x[1], x[0].lower()))
            by_label_display[label] = aggregated_short

        # Format into two columns of text
        col1, col2 = _format_mechanics_two_cols(by_label_display, per_label_limit=10)

        # Ensure we don't exceed Discord's 25 fields per embed
        # We will add 3 fields per encounter section: spacer + 2 columns
        if len(current.fields) + 3 > 25:
            current = discord.Embed(title=f"📈 Analytics — {event_name} (cont.)", color=discord.Color.purple())
            embeds.append(current)

        # Spacer to break to a new row inside the same embed
        current.add_field(name="\u200b", value="\u200b", inline=False)
        # First column carries the section header
        current.add_field(name=f"Failed Mechanics — {enc_name}", value=col1, inline=True)
        # Only add second column if it actually has content
        if col2.strip() and col2.strip() != "_No data_":
            current.add_field(name="\u200b", value=col2, inline=True)

    return embeds
