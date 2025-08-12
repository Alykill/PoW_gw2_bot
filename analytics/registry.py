# analytics/registry.py
from typing import Dict, List

# Global (applies to every encounter)
GLOBAL_PLAYER_METRICS = [
    # key, label, source, path(s) in EI JSON
    # We’ll compute these in code, this is just for names/labels.
    ("downs",       "Times downed"),
    ("deaths",      "Times died"),
    ("resurrects",  "Resurrects"),
    ("boss_dps",    "Full-fight target DPS (Power+Condi)"),
]

# Encounter-specific metric registry.
# Match by boss name substring (lower-cased) → list of mechanics to count.
# Each mechanic entry: key, label, list of substrings to match against mechanic name/description.
ENCOUNTER_MECHANICS: Dict[str, List[Dict]] = {
    # Dhuum
    "dhuum": [
        {"key": "dhuum_dip", "label": "Dip AoE (Lesser Death Mark hits)",
         "match": ["dip", "lesser death mark"]},
    ],
    # Qadim the Peerless
    "qadim the peerless": [
        {"key": "qtp_knckpll", "label": "Knck.Pll (Knocked Back/Pulled)",
         "match": ["knock", "pull"]},
        {"key": "qtp_lightning", "label": "Lght.H (Expanding Lightning)",
         "match": ["lightning"]},
        {"key": "qtp_magma", "label": "Magma.F (Magma Field)",
         "match": ["magma field"]},
        {"key": "qtp_small_magma", "label": "S.Magma.F (Small Magma Field)",
         "match": ["small magma"]},
    ],
}
