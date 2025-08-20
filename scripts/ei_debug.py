# scripts/ei_debug.py
# To run - type this in console:
# python -m scripts.ei_debug https://dps.report/ABCD-1234 --alias "Scythe" --mode occurrences --dedup-ms 500 --show-logs
from __future__ import annotations
import asyncio, argparse, json, re
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional, DefaultDict

# We reuse your network/file coercion so this works for both local JSON and dps.report links.
from analytics.service import _coerce_payload_to_json  # noqa: E402

def _norm_key(s: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _to_ms(t) -> Optional[int]:
    if t is None:
        return None
    try:
        x = float(t)
    except Exception:
        return None
    return int(round(x * 1000.0)) if x < 1e5 else int(round(x))

def _name_to_account(j: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for pl in (j.get("players") or []):
        if isinstance(pl, dict):
            ch = pl.get("name"); acct = pl.get("account")
            if ch and acct:
                out[str(ch)] = str(acct)
    return out

def _agg_counts(recs: List[Dict[str, Any]], n2a: Dict[str, str]) -> Dict[str, int]:
    out: DefaultDict[str, int] = defaultdict(int)
    for r in recs or []:
        if not isinstance(r, dict):
            continue
        acct = r.get("account") or n2a.get(str(r.get("name") or r.get("actor") or r.get("destination") or r.get("dst") or ""))
        if not acct:
            continue
        try:
            c = int(r.get("c", r.get("count", 0)))
        except Exception:
            c = 0
        if c > 0:
            out[acct] += c
    return dict(out)

def _perhit_counts(md: List[Dict[str, Any]], n2a: Dict[str, str], mode: str, dedup_ms: int) -> Dict[str, int]:
    """
    mode='hits' -> count every record; mode='occurrences' -> dedup per (actor) within dedup_ms.
    """
    out: DefaultDict[str, int] = defaultdict(int)
    last_by_actor: Dict[str, int] = {}
    for r in md or []:
        if not isinstance(r, dict):
            continue
        actor_char = r.get("actor") or r.get("name") or r.get("destination") or r.get("dst")
        acct = n2a.get(str(actor_char or ""))
        if not acct:
            continue
        t_ms = _to_ms(r.get("time"))
        if mode == "hits":
            out[acct] += 1
        else:
            if t_ms is None:
                out[acct] += 1
            else:
                prev = last_by_actor.get(acct)
                if prev is None or abs(t_ms - prev) > dedup_ms:
                    out[acct] += 1
                    last_by_actor[acct] = t_ms
    return dict(out)

def _sum(d: Dict[str, int]) -> int:
    return sum(d.values())

def _fmt_counts(title: str, d: Dict[str, int]) -> str:
    if not d:
        return f"{title}: (no data)\n"
    rows = "\n".join(f"  - {acct}: {cnt}" for acct, cnt in sorted(d.items(), key=lambda x: (-x[1], x[0].lower())))
    return f"{title}: total={_sum(d)}\n{rows}\n"

def _iter_mechanics(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [m for m in (j.get("mechanics") or []) if isinstance(m, dict)]

def _iter_mech_logs(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for k in ("mechanicLogs", "mechanicsLogs", "mechanicsLog", "mechanicsEvents", "mechLogs"):
        v = j.get(k)
        if isinstance(v, list):
            out.extend([r for r in v if isinstance(r, dict)])
    for k in ("mechanicLogsById", "mechanicsById", "mechData"):
        v = j.get(k)
        if isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, list):
                    out.extend([r for r in vv if isinstance(r, dict)])
    return out

def _title(m: Dict[str, Any]) -> str:
    parts = [m.get("name"), m.get("shortName"), m.get("fullName")]
    return " / ".join([str(p) for p in parts if p])

async def main():
    ap = argparse.ArgumentParser(description="EI mechanics debug (per-hit vs aggregates).")
    ap.add_argument("src", help="Local EI JSON path or dps.report permalink")
    ap.add_argument("--alias", help="Focus on a mechanic alias (e.g. 'Scythe'). If omitted, prints catalog.", default=None)
    ap.add_argument("--mode", choices=["occurrences", "hits"], default="occurrences", help="How to treat per-hit data.")
    ap.add_argument("--dedup-ms", type=int, default=1500, help="Dedup window for occurrences (ms).")
    ap.add_argument("--show-logs", action="store_true", help="Also inspect top-level mechanicLogs fallback.")
    args = ap.parse_args()

    j = await _coerce_payload_to_json(args.src)
    n2a = _name_to_account(j)
    mech = _iter_mechanics(j)

    enc = j.get("encounter") or {}
    boss = enc.get("boss") or j.get("boss") or j.get("fightName") or "Unknown Encounter"
    print(f"\n=== Encounter: {boss} ===")
    print(f"Players: {len(j.get('players') or [])}, Mechanics: {len(mech)}\n")

    if not args.alias:
        # catalog view
        print("Available mechanics (per-entry signal):")
        for m in mech:
            md = m.get("mechanicsData")
            players_list = m.get("players")
            hits_list = m.get("playerHits")
            flags = []
            if isinstance(md, list) and md:
                flags.append(f"mechanicsData={len(md)}")
            if isinstance(players_list, list) and players_list:
                flags.append(f"players={len(players_list)}")
            if isinstance(hits_list, list) and hits_list:
                flags.append(f"playerHits={len(hits_list)}")
            name = _title(m) or "<unnamed>"
            print(f" • {name}  [{', '.join(flags) if flags else 'no signals'}]")
        if args.show_logs:
            logs = _iter_mech_logs(j)
            got = defaultdict(int)
            for r in logs:
                nm = r.get("mechanic") or r.get("name") or r.get("shortName") or "?"
                got[nm] += 1
            if got:
                print("\nTop-level mechanicLogs present (counts of rows by name):")
                for nm, cnt in sorted(got.items(), key=lambda x: (-x[1], x[0].lower())):
                    print(f" • {nm}: {cnt}")
        return

    # focused view on an alias
    alias_norm = _norm_key(args.alias)
    matches = [m for m in mech if alias_norm in {_norm_key(m.get("name")), _norm_key(m.get("shortName")), _norm_key(m.get("fullName"))}]
    # also accept substring in full mechanic name text to be safe
    if not matches:
        matches = [m for m in mech if alias_norm and alias_norm in _norm_key(_title(m))]

    if not matches:
        print(f"No mechanics matched alias '{args.alias}'. Try --show-logs.")
        if args.show_logs:
            logs = _iter_mech_logs(j)
            lines = []
            for r in logs:
                nm = (r.get("mechanic") or r.get("name") or r.get("shortName") or "").strip()
                if alias_norm and alias_norm in _norm_key(nm):
                    lines.append(nm)
            if lines:
                print("Found in top-level mechanicLogs names:")
                for s in sorted(set(lines)):
                    print(f" • {s}")
        return

    print(f"Matched {len(matches)} mechanic entr{ 'y' if len(matches)==1 else 'ies' } for alias '{args.alias}':\n")
    total_final: DefaultDict[str, int] = defaultdict(int)

    for idx, m in enumerate(matches, 1):
        title = _title(m) or "<unnamed>"
        print(f"--- [{idx}] {title} ---")
        md = m.get("mechanicsData") if isinstance(m.get("mechanicsData"), list) else []
        players_list = m.get("players") if isinstance(m.get("players"), list) else []
        hits_list = m.get("playerHits") if isinstance(m.get("playerHits"), list) else []

        if md:
            counts_md = _perhit_counts(md, n2a, args.mode, args.dedup_ms)
            print(_fmt_counts(f"per-hit mechanicsData ({args.mode}, dedup={args.dedup_ms}ms)", counts_md))
            for a, v in counts_md.items():
                total_final[a] += v
        else:
            print("per-hit mechanicsData: (none)\n")

        if players_list:
            counts_players = _agg_counts(players_list, n2a)
            print(_fmt_counts("aggregated 'players' (occurrences)", counts_players))
        else:
            print("aggregated 'players': (none)\n")

        if hits_list:
            counts_hits = _agg_counts(hits_list, n2a)
            print(_fmt_counts("aggregated 'playerHits' (hits)", counts_hits))
        else:
            print("aggregated 'playerHits': (none)\n")

    # Optional: top-level mech logs as fallback
    if args.show_logs:
        logs = _iter_mech_logs(j)
        by_actor: DefaultDict[str, int] = defaultdict(int)
        for r in logs:
            nm = (r.get("mechanic") or r.get("name") or r.get("shortName") or "")
            if alias_norm and alias_norm not in _norm_key(nm):
                continue
            acct = n2a.get(str(r.get("actor") or r.get("name") or r.get("source") or ""))
            if not acct:
                continue
            # treat as per-hit with same dedup rules
            t_ms = _to_ms(r.get("time"))
            # simple occurrences dedup per actor
            by_actor[acct] += 1 if args.mode == "hits" else 0  # we’ll do fancy dedup only in mechanicsData section
        if by_actor:
            print(_fmt_counts("top-level mechanicLogs (rough hits count)", by_actor))

    if total_final:
        print(_fmt_counts("== combined per-hit (final, across matching entries) ==", total_final))

if __name__ == "__main__":
    asyncio.run(main())
