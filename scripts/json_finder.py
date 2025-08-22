# scripts/json_finder.py
import json, urllib.request, time
from typing import List, Dict, Any, Tuple

URL = "https://dps.report/getJson?permalink=szj7-20250818-094517_qpeer"

TARGET_KEY   = "S.Magma.F"           # shortName / name / fullName
TARGET_ACTOR = "Lights And Shadows"  # exact actor string as in JSON
COALESCE_MS  = 1000                  # collapse hits within this gap into one "occurrence"

TIMEOUT_S    = 10
RETRIES      = 3

def fetch_json(url: str) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=TIMEOUT_S) as r:
                return json.load(r)
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 8))
    raise RuntimeError(f"Failed to fetch JSON after {RETRIES} attempts: {last_err}")

def find_mechanic(data: Dict[str, Any], key: str) -> Tuple[int, Dict[str, Any]]:
    for i, m in enumerate(data.get("mechanics", [])):
        if key in (m.get("shortName"), m.get("name"), m.get("fullName")):
            return i, m
    return -1, {}

def collect_hit_times_all_actors(data: Dict[str, Any], mech_idx: int, mech_def: Dict[str, Any]) -> Dict[str, List[int]]:
    """Return {actor: [times_ms...]} for the mechanic, handling both EI layouts."""
    out: Dict[str, List[int]] = {}

    # Layout A: mechanic def contains mechanicsData
    md = mech_def.get("mechanicsData")
    if isinstance(md, list):
        for e in md:
            actor = e.get("actor")
            t = e.get("time")
            if actor and isinstance(t, (int, float)):
                out.setdefault(actor, []).append(int(t))
        for a in out:
            out[a].sort()
        return out

    # Layout B: global mechanicsData with per-actor blocks
    for entry in data.get("mechanicsData", []):
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

def collapse_into_occurrences(times_ms: List[int], gap_ms: int):
    """Collapse consecutive hits whose gap <= gap_ms."""
    if not times_ms:
        return []
    occs = []
    start = prev = times_ms[0]
    hits = 1
    for t in times_ms[1:]:
        if t - prev <= gap_ms:
            hits += 1
        else:
            occs.append({'start': start, 'end': prev, 'hits': hits})
            start = t
            hits = 1
        prev = t
    occs.append({'start': start, 'end': prev, 'hits': hits})
    return occs

def split_by_phases(times_ms: List[int], phases: List[Dict[str, Any]]) -> List[List[int]]:
    """Split timestamps into per-phase buckets based on phases[].index time windows (ms)."""
    if not phases:
        return [times_ms]
    # EI phases are indexed with start/end times in ms (relative to log)
    buckets: List[List[int]] = [[] for _ in phases]
    for t in times_ms:
        for i, ph in enumerate(phases):
            # Some JSONs have only 'start' and 'end', others have 'start'/'end' inside a 'times' array
            start = ph.get("start", ph.get("startMS", 0))
            end   = ph.get("end", ph.get("endMS", 10**12))
            if start <= t < end:
                buckets[i].append(t)
                break
    return buckets

def main():
    data = fetch_json(URL)
    mech_idx, mech_def = find_mechanic(data, TARGET_KEY)
    if mech_idx < 0:
        print(f"Mechanic '{TARGET_KEY}' not found.")
        return

    mname = mech_def.get('fullName') or mech_def.get('name') or mech_def.get('shortName')
    phases = data.get("phases", [])  # if present

    # Gather for all actors so we can compare
    per_actor_times = collect_hit_times_all_actors(data, mech_idx, mech_def)

    # Focus actor (what you asked about)
    times = per_actor_times.get(TARGET_ACTOR, [])
    occs  = collapse_into_occurrences(times, COALESCE_MS)

    print(f"Mechanic: {mname}  (key='{TARGET_KEY}')")
    print(f"Actor: {TARGET_ACTOR}")
    print(f"Raw hits: {len(times)}")
    print(f"Collapsed occurrences (<= {COALESCE_MS} ms gap): {len(occs)}")
    for i, o in enumerate(occs, 1):
        print(f"{i:02d}) start={o['start']} ms, end={o['end']} ms, dur={o['end']-o['start']} ms, hits={o['hits']}")

    # Per-phase view for this actor (helps explain “2” when spread across phases)
    if phases:
        print("\nPer-phase collapsed counts for this actor:")
        actor_phase_times = split_by_phases(times, phases)
        for i, pt in enumerate(actor_phase_times):
            pdef = phases[i]
            pname = pdef.get("name", f"Phase {i}")
            cnt = len(collapse_into_occurrences(pt, COALESCE_MS))
            print(f" - {i:02d} {pname}: {cnt}")

    # Party aggregate (sum of per-actor collapsed counts — this is what EI tables often show)
    party_occ_total = 0
    for a, ts in per_actor_times.items():
        party_occ_total += len(collapse_into_occurrences(ts, COALESCE_MS))
    print(f"\nParty aggregate (sum of per-actor collapsed occurrences): {party_occ_total}")

    # Show which actors have nonzero occurrences (to spot who shows “2”)
    print("\nActors with occurrences:")
    for a, ts in per_actor_times.items():
        occ_n = len(collapse_into_occurrences(ts, COALESCE_MS))
        if occ_n:
            print(f" - {a}: raw={len(ts)}, occ={occ_n}")

if __name__ == "__main__":
    main()
