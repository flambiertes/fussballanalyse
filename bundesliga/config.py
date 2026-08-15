from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
DATA_DIR = PACKAGE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "bundesliga.sqlite"


LEAGUES = {
    "D1": {
        "name": "Bundesliga",
        "football_data_code": "D1",
        "openligadb_code": "bl1",
    },
    "D2": {
        "name": "2. Bundesliga",
        "football_data_code": "D2",
        "openligadb_code": "bl2",
    },
}


# Football-Data verwendet kurze englische Namen. Die kanonischen Namen sind
# bewusst deutsch und passen weitgehend zu den vorhandenen Zweitliga-CSVs.
TEAM_ALIASES = {
    "Aachen": "Alemannia Aachen",
    "Bayern Munich": "Bayern München",
    "FC Bayern München": "Bayern München",
    "Dortmund": "Borussia Dortmund",
    "Leverkusen": "Bayer 04 Leverkusen",
    "Bayer Leverkusen": "Bayer 04 Leverkusen",
    "M'gladbach": "Borussia Mönchengladbach",
    "Borussia Monchengladbach": "Borussia Mönchengladbach",
    "Ein Frankfurt": "Eintracht Frankfurt",
    "FC Koln": "1. FC Köln",
    "FC Cologne": "1. FC Köln",
    "1. FC Cologne": "1. FC Köln",
    "FC Köln": "1. FC Köln",
    "Kaiserslautern": "1. FC Kaiserslautern",
    "Nurnberg": "1. FC Nürnberg",
    "Greuther Furth": "SpVgg Greuther Fürth",
    "Fortuna Dusseldorf": "Fortuna Düsseldorf",
    "Hertha": "Hertha BSC",
    "Hamburg": "Hamburger SV",
    "Hannover": "Hannover 96",
    "Paderborn": "SC Paderborn 07",
    "SC Paderborn": "SC Paderborn 07",
    "St Pauli": "FC St. Pauli",
    "Schalke 04": "FC Schalke 04",
    "Duisburg": "MSV Duisburg",
    "Braunschweig": "Eintracht Braunschweig",
    "Union Berlin": "1. FC Union Berlin",
    "FC Union Berlin": "1. FC Union Berlin",
    "1.FC Union Berlin": "1. FC Union Berlin",
    "Mainz": "1. FSV Mainz 05",
    "Mainz 05": "1. FSV Mainz 05",
    "FSV Mainz 05": "1. FSV Mainz 05",
    "1.FSV Mainz 05": "1. FSV Mainz 05",
    "Werder Bremen": "SV Werder Bremen",
    "Wolfsburg": "VfL Wolfsburg",
    "Augsburg": "FC Augsburg",
    "Freiburg": "SC Freiburg",
    "Hoffenheim": "TSG 1899 Hoffenheim",
    "TSG Hoffenheim": "TSG 1899 Hoffenheim",
    "Bochum": "VfL Bochum",
    "Heidenheim": "1. FC Heidenheim 1846",
    "VfB Stuttgart": "Stuttgart",
    "1.FC Köln": "1. FC Köln",
    "1.FC Kaiserslautern": "1. FC Kaiserslautern",
    "1.FC Nürnberg": "1. FC Nürnberg",
    "1.FC Heidenheim 1846": "1. FC Heidenheim 1846",
    "Bielefeld": "Arminia Bielefeld",
    "Munich 1860": "TSV 1860 München",
    "Uerdingen": "KFC Uerdingen 05",
    "Unterhaching": "SpVgg Unterhaching",
    "Cottbus": "Energie Cottbus",
    "Erzgebirge Aue": "FC Erzgebirge Aue",
    "Ingolstadt": "FC Ingolstadt 04",
    "Darmstadt": "SV Darmstadt 98",
    "Regensburg": "Jahn Regensburg",
    "Sandhausen": "SV Sandhausen",
    "Osnabruck": "VfL Osnabrück",
    "Elversberg": "SV 07 Elversberg",
    "Mannheim": "Waldhof Mannheim",
    "RW Essen": "Rot-Weiss Essen",
    "TB Berlin": "Tennis Borussia Berlin",
}


@dataclass(frozen=True)
class ModelConfig:
    """Parameter, die in historischen Backtests veraendert werden duerfen."""

    lookback_years: float = 6.0
    half_life_days: float = 420.0
    ridge: float = 0.08
    min_training_matches: int = 250
    max_goals: int = 10
    form_matches: int = 8
    form_half_life_matches: float = 3.5
    form_weight: float = 0.0
    venue_form_weight: float = 0.0
    h2h_matches: int = 6
    h2h_half_life_days: float = 730.0
    h2h_weight: float = 0.0
    market_value_weight: float = 0.0
    market_decay_matches: int = 10
    bookmaker_weight: float = 0.0
    bookmaker_snapshot_type: str = "opening"
    use_lower_league_priors: bool = False
    lower_league_scale: float = 0.85
    promotion_attack_penalty: float = -0.20
    promotion_defense_penalty: float = 0.20
    promotion_prior_matches: int = 10
