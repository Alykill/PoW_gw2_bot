# analytics/registry.py
from typing import Dict, List

GLOBAL_PLAYER_METRICS = [
    ("downs",       "Times downed"),
    ("deaths",      "Times died"),
    ("resurrects",  "Resurrects"),
    ("boss_dps",    "Full-fight target DPS (Power+Condi)"),
]

ENCOUNTER_MECHANICS: Dict[str, List[Dict]] = {
    "vale guardian": [
        {"key": "Boss TP", "label": "Blue ports",
         "exact": ["Boss TP", "Boss Teleport"], "modes": ["occurrences"]},
    ],
    "gorseval the multifarious": [
        {"key": "Egg", "label": "Eggs",
         "exact": ["Egg", "Egged"], "modes": ["occurrences", "occurrences"]},
    ],
    "slothasor": [
        {"key": "Tantrum", "label": "Circle KD",
         "exact": ["Tantrum", "Tripple Circles"], "modes": ["occurrences", "hits"]},
    ],
    "matthias gaberl": [
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
        {"key": "Oil T.", "label": "Oil Trigger",
         "exact": ["Oil T.", "Black Oil Trigger"], "modes": ["occurrences"]},
        {"key": "Pizza", "label": "Pizza",
         "exact": ["Pizza", "Cascading Pizza attack"], "modes": ["occurrences"]},
    ],
    "soulless horror": [
        {"key": "scythe", "label": "Scythed",
         "exact": ["Scythe", "Spinning Slash"], "modes": ["occurrences"]},
    ],
    "statue of darkness": [
        {"key": "hard_cc_judge", "label": "CC'ed",
         "exact": ["Hard CC Judge", "CC Judge"], "modes": ["occurrences"]},
    ],
    "dhuum": [
        {"key": "dhuum_dip", "label": "Dip AoE",
         "exact": ["Dip", "Dip AoE"], "modes": ["occurrences"]},
        {"key": "crack", "label": "Stood in Cracks",
         "exact": ["Crack", "Cull (Fearing Fissure)"], "modes": ["occurrences"]},
        {"key": "rending_swipe_hit", "label": "Swiped",
         "exact": ["Enf.Swipe", "Rending Swipe Hit"], "modes": ["occurrences"]},
    ],
    "conjured amalgamate": [
        {"key": "arm_slam", "label": "Arm Slammed",
         "exact": ["Arm Slam", "Arm Slam: Pulverize"], "modes": ["occurrences"]},
    ],
    "twin largos": [
        {"key": "float", "label": "Bubbled",
         "exact": ["Float", "Float Bubble"], "modes": ["hits"]},
        {"key": "steal", "label": "Boon Steal",
         "exact": ["Steal", "Boon Steal"], "modes": ["occurrences"]},
    ],
    "qadim": [
        {"key": "qadim_hitbox_aoe", "label": "Stood in Qadim hitbox",
         "exact": ["Q.Hitbox", "Qadim Hitbox AoE"], "modes": ["occurrences"]},
    ],
    "cardinal adina": [
        {"key": "radiant_blindness", "label": "Blinded",
         "exact": ["R.Blind", "Radiant Blindness"], "modes": ["occurrences"]},
    ],
    "cardinal sabir": [
        {"key": "shockwave", "label": "Shockwaved",
         "exact": ["Shockwave", "Shockwave Hit"], "modes": ["occurrences"]},
        {"key": "pushed", "label": "Pushed during CC",
         "exact": ["Pushed", "Pushed by rotating breakbar"], "modes": ["occurrences"]},
    ],
    "qadim the peerless": [
        {"key": "qtp_knckpll", "label": "Knocked Back/Pulled",
         "exact": ["Knck.Pll", "Knocked Back/Pulled"], "modes": ["occurrences"]},
        {"key": "qtp_lightning", "label": "Hit by Expanding Lightning",
         "exact": ["Lght.H", "Lightning Hit"], "modes": ["occurrences"]},
        {"key": "qtp_small_lightning", "label": "Hit by Small Lightning",
         "exact": ["S.Lght.H", "Small Lightning Hit"], "modes": ["occurrences"]},
        {"key": "qtp_magma", "label": "Stood in Magma Field",
         "exact": ["Magma.F", "Magma Field"], "modes": ["occurrences"]},
        {"key": "qtp_small_magma", "label": "Stood in Lightning fire puddle",
         "exact": ["S.Magma.F", "Small Magma Field"], "modes": ["occurrences", "hits"]},
        {"key": "pylon_magma", "label": "Stood in Pylon fire",
         "exact": ["P.Magma", "Pylon Magma"], "modes": ["occurrences"]},
        {"key": "dome_knockback", "label": "Pylon knockback",
         "exact": ["Dome.KB", "Dome Knockback"], "modes": ["occurrences"]},
    ],
}

