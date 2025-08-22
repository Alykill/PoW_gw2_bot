# analytics/registry.py
from typing import Dict, List

# ─────────────────────────────────────────────────────────────────────────────
# NOTES:
# - For EI-style “collapsed occurrences” (matching the permalink UI), add
#   `"coalesce_ms": <int>` to a mechanic entry. service.py will use the new
#   collapsed counter.
# - For legacy per-hit counting with a small merge window, keep `"dedup_ms"`.
# - Fields kept for backward-compatibility: "modes", "force_per_hit".
#   (They aren’t required by service.py, but left intact if you use them elsewhere.)
# ─────────────────────────────────────────────────────────────────────────────

GLOBAL_PLAYER_METRICS = [
    ("downs",       "Times downed"),
    ("deaths",      "Times died"),
    ("resurrects",  "Resurrects"),
    ("boss_dps",    "Full-fight target DPS (Power+Condi)"),
]

# Minimal mapping; extend as you like
ENCOUNTER_WINGS_BY_NAME = {
    # Wing 1 — Spirit Vale
    "vale guardian": "W1",
    "spirit race": "W1",
    "gorseval": "W1",
    "gorseval the multifarious": "W1",   # alias seen in some exports
    "sabetha the saboteur": "W1",

    # Wing 2 — Salvation Pass
    "slothasor": "W2",
    "bandit trio": "W2",
    "matthias gabrel": "W2",

    # Wing 3 — Stronghold of the Faithful
    "escort": "W3",
    "keep construct": "W3",
    "twisted castle": "W3",  # event
    "xera": "W3",

    # Wing 4 — Bastion of the Penitent
    "cairn the indomitable": "W4",
    "mursaat overseer": "W4",
    "samarog": "W4",
    "deimos": "W4",

    # Wing 5 — Hall of Chains
    "soulless horror": "W5",           # (Desmina)
    "river of souls": "W5",            # event
    "statues of grenth": "W5",         # event chain
    "dhuum": "W5",

    # Wing 6 — Mythwright Gambit
    "conjured amalgamate": "W6",
    "twin largos": "W6",
    "nikare and kenut": "W6",
    "qadim": "W6",

    # Wing 7 — The Key of Ahdashim
    "cardinal adina": "W7",
    "cardinal sabir": "W7",
    "qadim the peerless": "W7",

    # Wing 8 — Mount Balrior
    "greer": "W8",
    "decima": "W8",
    "ura": "W8",
}

# IMPORTANT: enrichment matches mechanics by exact (normalized) alias, not substring.
ENCOUNTER_MECHANICS: Dict[str, List[Dict]] = {
    # ── Wing 1
    "gorseval the multifarious": [
        {
            "key": "Egg",
            "label": "Eggs",
            "exact": ["Egg", "Egged"],
            "modes": ["occurrences"],
            "dedup_ms": 500,   # merge rapid multi-pulses
        },
    ],

    "slothasor": [
        {
            "key": "Tantrum",
            "label": "Circle KD",
            "exact": ["Tantrum", "Tripple Circles"],
            "modes": ["occurrences", "hits"],
        },
    ],

    "matthias gabrel": [
        {
            "key": "Spirit",
            "label": "Touched Spirit",
            "exact": ["Spirit", "Spirit hit"],
            "modes": ["occurrences"],
        },
    ],

    "keep construct": [
        {
            "key": "bad_white_orb",
            "label": "Wrong orb collected - white",
            "exact": ["BW.Orb", "Bad White Orb"],
            "modes": ["occurrences"],
        },
        {
            "key": "bad_red_orb",
            "label": "Wrong orb collected - red",
            "exact": ["BR.Orb", "Bad Red Orb"],
            "modes": ["occurrences"],
        },
    ],

    "samarog": [
        {
            "key": "sweep",
            "label": "Swept",
            "exact": ["Swp", "Prisoner Sweep"],
            "modes": ["occurrences"],
        },
        {
            "key": "spear return",
            "label": "Returning Spears",
            "exact": ["S.Rt", "Spear Return"],
            "modes": ["occurrences"],
        },
        {
            "key": "shockwave_from_spears",
            "label": "Shockwaved",
            "exact": ["Schk.Wv", "Shockwave from Spears"],
            "modes": ["occurrences"],
        },
    ],

    # ── Wing 4
    "deimos": [
        {
            "key": "Oil T.",
            "label": "Oil Trigger",
            "exact": ["Oil T.", "Oil Trigger"],
            "force_per_hit": True,
            "dedup_ms": 500,   # collapse multi-pulse records into one occurrence
        },
        {
            "key": "Pizza",
            "label": "Pizza",
            "exact": ["Pizza", "Cascading Pizza attack"],
            "force_per_hit": True,
            "dedup_ms": 500,   # collapse the pizza’s rapid ticks
        },
    ],

    # (Key name is case-insensitive in service; leave as-is if you prefer)
    "Soulless Horror": [
        {
            "key":   "sh_scythe_hits",
            "label": "Scythed",
            "exact": ["Scythe"],      # strict match on EI's mechanic name
            "canonical": "scythe",
            "dedup_ms": 500,
        },
    ],

    "statue of darkness": [
        {
            "key": "hard_cc_judge",
            "label": "CC'ed",
            "exact": ["Hard CC Judge", "CC Judge"],
            "modes": ["occurrences"],
        },
    ],

    "dhuum": [
        {
            "key": "dhuum_dip",
            "label": "Dip AoE",
            "exact": ["Dip", "Dip AoE"],
            "force_per_hit": True,
            "dedup_ms": 10000,   # collapse multi-pulse records into one occurrence
        },
        {
            "key": "crack",
            "label": "Stood in Cracks",
            "exact": ["Crack", "Cull (Fearing Fissure)"],
            "modes": ["occurrences"],
        },
        {
            "key": "rending_swipe_hit",
            "label": "Swiped",
            "exact": ["Enf.Swipe", "Rending Swipe Hit"],
            "modes": ["occurrences"],
        },
    ],

    # ── Wing 6
    "conjured amalgamate": [
        {
            "key": "arm_slam",
            "label": "Arm Slammed",  # do NOT match 'Tremor'
            "exact": ["Arm Slam", "Arm Slam: Pulverize"],
            "modes": ["occurrences"],
        },
    ],

    "twin largos": [
        {
            "key": "float",
            "label": "Bubbled",
            "exact": ["Flt"],
            "force_per_hit": True,
            "dedup_ms": 1000,   # collapse multi-pulse records into one occurrence
        },
        {
            "key": "steal",
            "label": "Boon Steal",
            "exact": ["Steal", "Boon Steal"],
            "force_per_hit": False,
            "dedup_ms": 1000,
        },
    ],

    "qadim": [
        {
            "key": "qadim_hitbox_aoe",
            "label": "Stood in Qadim hitbox",
            "exact": ["Q.Hitbox", "Qadim Hitbox AoE"],
            "modes": ["occurrences"],
        },
    ],

    # ── Wing 7
    "cardinal adina": [
        {
            "key": "radiant_blindness",
            "label": "Blinded",
            "exact": ["R.Blind", "Radiant Blindness"],
            "modes": ["occurrences"],
        },
    ],

    "cardinal sabir": [
        {
            "key": "shockwave",
            "label": "Shockwaved",
            "exact": ["Shockwave", "Shockwave Hit"],
            "modes": ["occurrences"],
        },
        {
            "key": "pushed",
            "label": "Pushed during CC",
            "exact": ["Pushed", "Pushed by rotating breakbar"],
            "modes": ["occurrences"],
        },
    ],

    "qadim the peerless": [
        {
            "key": "qtp_lightning",
            "label": "Hit by Expanding Lightning",
            "exact": ["Lght.H", "Lightning Hit"],
            "modes": ["occurrences"],
        },
        {
            "key": "Magma.F",
            "label": "Stood in magma field",
            "exact": ["Magma.F"],
            "modes": ["occurrences"],
            "coalesce_ms": 1000,  # NEW: true EI-style collapsed occurrences
        },
        {
            "key": "S.Magma.F",
            "label": "Stood in Lightning fire puddle",
            "exact": ["S.Magma.F"],
            "modes": ["occurrences"],
            "coalesce_ms": 1000,  # NEW: true EI-style collapsed occurrences
        },
        {
            "key": "pylon_magma",
            "label": "Stood in Pylon fire",
            "exact": ["P.Magma", "Pylon Magma"],
            "modes": ["occurrences"],
        },
        {
            "key": "dome_knockback",
            "label": "Pylon knockback",
            "exact": ["Dome.KB", "Dome Knockback"],
            "modes": ["occurrences"],
        },
    ],
}
