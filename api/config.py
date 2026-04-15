from pathlib import Path

SQLITE_PATH = Path("data/features/player_features.sqlite")
MODEL_ARTIFACT_PATH = Path("data/models/match_outcome_tree.pkl")
DEFAULT_FEATURES = [
    "career_win_pct",
    "service_points_won_pct",
    "return_points_won_pct",
    "aces_per_service_game",
    "double_faults_per_service_game",
    "break_points_saved_pct",
    "elo_pre",
    "win_pct_last_20_matches",
    "surface_win_pct_last_20_matches",
]

METRIC_COLUMN_MAP = {
    "elo": "elo_pre",
    "ace_pct": "ace_pct",
    "aces_per_service_game": "aces_per_service_game",
    "break_points_won_pct": "break_points_saved_pct",
    "win_pct": "career_win_pct",
}
