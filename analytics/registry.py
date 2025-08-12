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
    "dhuum": [
        {"key": "dhuum_dip", "label": "Dip AoE (Lesser Death Mark hits)",
         "exact": ["Dip", "Dip AoE"], "modes": ["occurrences"]},
    ],
    "qadim the peerless": [
        {"key": "qtp_knckpll", "label": "Knck.Pll (Knocked Back/Pulled)",
         "exact": ["Knck.Pll", "Knocked Back/Pulled"], "modes": ["occurrences"]},
        {"key": "qtp_lightning", "label": "Lght.H (Expanding Lightning)",
         "exact": ["Lght.H", "Lightning Hit"], "modes": ["occurrences"]},
        {"key": "qtp_small_lightning", "label": "S.Lght.H (Small Lightning Hit)",
         "exact": ["S.Lght.H", "Small Lightning Hit"], "modes": ["occurrences"]},
        {"key": "qtp_magma", "label": "Magma.F (Magma Field)",
         "exact": ["Magma.F", "Magma Field"], "modes": ["occurrences"]},
        {"key": "qtp_small_magma", "label": "S.Magma.F (Small Magma Field)",
         "exact": ["S.Magma.F", "Small Magma Field"], "modes": ["occurrences", "hits"]},
    ],
    "deimos": [
        {"key": "Oil T.", "label": "Oil Trigger",
         "exact": ["Oil T.", "Black Oil Trigger"], "modes": ["occurrences"]},
        {"key": "Pizza", "label": "Pizza",
         "exact": ["Pizza", "Cascading Pizza attack"], "modes": ["occurrences"]},
    ],
}
