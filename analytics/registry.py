# analytics/registry.py
from typing import Dict, List

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

# IMPORTANT: enrichment should match mechanics by exact (normalized) alias, not substring.
ENCOUNTER_MECHANICS: Dict[str, List[Dict]] = {
    # "vale guardian": [
    #     {"key": "Boss TP", "label": "Blue ports",
    #      "exact": ["Boss TP", "Boss Teleport"], "modes": ["occurrences", "occurrences"]},
    # ],
    "gorseval the multifarious": [
        {"key": "Egg", "label": "Eggs",
         "exact": ["Egg", "Egged"], "modes": ["occurrences", "occurrences"]},
    ],
    "slothasor": [
        {"key": "Tantrum", "label": "Circle KD",
         "exact": ["Tantrum", "Tripple Circles"], "modes": ["occurrences", "hits"]},
    ],
    "matthias gabrel": [
        {"key": "Spirit", "label": "Touched Spirit",
         "exact": ["Spirit", "Spirit hit"], "modes": ["occurrences", "occurrences"]},
    ],
    "keep construct": [
        {"key": "bad_white_orb", "label": "Wrong orb collected - white",
         "exact": ["BW.Orb", "Bad White Orb"], "modes": ["occurrences", "occurrences"]},
        {"key": "bad_red_orb", "label": "Wrong orb collected - red",
         "exact": ["BR.Orb", "Bad Red Orb"], "modes": ["occurrences", "occurrences"]},
    ],
    "samarog": [
        {"key": "sweep", "label": "Swept",
         "exact": ["Swp", "Prisoner Sweep"], "modes": ["occurrences", "occurrences"]},
        {"key": "spear return", "label": "Returning Spears",
         "exact": ["S.Rt", "Spear Return"], "modes": ["occurrences", "occurrences"]},
        {"key": "shockwave_from_spears", "label": "Shockwaved",
         "exact": ["Schk.Wv", "Shockwave from Spears"], "modes": ["occurrences", "occurrences"]},
    ],
    "deimos": [
        {"key": "Oil T.", "label": "Oil Trigger",                # ONLY the Oil Trigger
         "exact": ["Oil T."], "modes": ["occurrences"]},
        {"key": "black_oil_trigger", "label": "Black Oil Trigger",  # separate from Oil T.
         "exact": ["Black Oil Trigger"], "modes": ["occurrences"]},
        {"key": "Pizza", "label": "Pizza",
         "exact": ["Pizza", "Cascading Pizza attack"], "modes": ["occurrences", "occurrences"]},
    ],
    "soulless horror": [
        {
            "key": "scythe",
            "label": "Scythed",
            "exact": ["Scythe", "Spinning Slash"],
            "modes": ["hits", "hits"],  # count every record
            "force_per_hit": True  # never fall back to 'players'
        },
    ],
    "statue of darkness": [
        {"key": "hard_cc_judge", "label": "CC'ed",
         "exact": ["Hard CC Judge", "CC Judge"], "modes": ["occurrences", "occurrences"]},
    ],
    "dhuum": [
        {"key": "dhuum_dip", "label": "Dip AoE",
         "exact": ["Dip", "Dip AoE"], "canonical": "dhuum_dip", "dedup_ms": 5000},
        {"key": "crack", "label": "Stood in Cracks",
         "exact": ["Crack", "Cull (Fearing Fissure)"], "modes": ["occurrences", "occurrences"]},
        {"key": "rending_swipe_hit", "label": "Swiped",
         "exact": ["Enf.Swipe", "Rending Swipe Hit"], "modes": ["occurrences", "occurrences"]},
    ],
    "conjured amalgamate": [
        {"key": "arm_slam", "label": "Arm Slammed",  # do NOT match 'Tremor'
         "exact": ["Arm Slam", "Arm Slam: Pulverize"], "modes": ["occurrences", "occurrences"]},
    ],
    "twin largos": [
        {"key": "float", "label": "Bubbled",
        "exact": ["Float", "Float Bubble"], "modes": ["occurrences", "occurrences"], "dedup_ms": 2500},
        {"key": "steal", "label": "Boon Steal",
        "exact": ["Steal", "Boon Steal"], "modes": ["occurrences"]},
    ],
    "qadim": [
        {"key": "qadim_hitbox_aoe", "label": "Stood in Qadim hitbox",
         "exact": ["Q.Hitbox", "Qadim Hitbox AoE"], "modes": ["occurrences", "occurrences"]},
    ],
    "cardinal adina": [
        {"key": "radiant_blindness", "label": "Blinded",
         "exact": ["R.Blind", "Radiant Blindness"], "modes": ["occurrences", "occurrences"]},
    ],
    "cardinal sabir": [
        {"key": "shockwave", "label": "Shockwaved",
         "exact": ["Shockwave", "Shockwave Hit"], "modes": ["occurrences", "occurrences"]},
        {"key": "pushed", "label": "Pushed during CC",
         "exact": ["Pushed", "Pushed by rotating breakbar"], "modes": ["occurrences", "occurrences"]},
    ],
    "qadim the peerless": [
        {"key": "qtp_knckpll", "label": "Knocked Back/Pulled",
         "exact": ["Knck.Pll", "Knocked Back/Pulled"], "modes": ["occurrences", "occurrences"]},
        {"key": "qtp_lightning", "label": "Hit by Expanding Lightning",
         "exact": ["Lght.H", "Lightning Hit"], "modes": ["occurrences", "occurrences"]},
        {"key": "qtp_small_lightning", "label": "Hit by Small Lightning",
         "exact": ["S.Lght.H", "Small Lightning Hit"], "modes": ["occurrences", "occurrences"]},
        {"key": "qtp_magma", "label": "Stood in Magma Field",
         "exact": ["Magma.F", "Magma Field"], "modes": ["occurrences", "occurrences"]},
        {"key": "qtp_small_magma", "label": "Stood in Lightning fire puddle",
         "exact": ["S.Magma.F", "Small Magma Field"], "modes": ["occurrences", "occurrences"]},
        {"key": "pylon_magma", "label": "Stood in Pylon fire",
         "exact": ["P.Magma", "Pylon Magma"], "modes": ["occurrences", "occurrences"]},
        {"key": "dome_knockback", "label": "Pylon knockback",
         "exact": ["Dome.KB", "Dome Knockback"], "modes": ["occurrences", "occurrences"]},
    ],
}
