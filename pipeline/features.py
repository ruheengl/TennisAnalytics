#!/usr/bin/env python3
"""Generate player-level feature vectors from cleaned ATP match CSV files.

The pipeline consumes `data/processed/atp_YYYY_clean.csv` files, creates one row per
player-match observation, and emits:

1. Columnar Parquet features (`player_features.parquet`) for analytics.
2. An indexed SQLite serving table (`player_features.sqlite`) for online retrieval.
3. A JSON state file used for incremental refreshes so only affected players are
   recomputed when match files change.

Incremental mode fingerprints each cleaned input file. If only a subset of files
changed, the script extracts player IDs from those files and recomputes feature rows
for just those players while preserving all untouched rows.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

try:
    import duckdb  # type: ignore
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "duckdb is required for Parquet persistence. Install with: pip install duckdb"
    ) from exc


ELO_K_FACTOR = 32.0
DEFAULT_MATCH_WINDOWS = (5, 10, 20)
DEFAULT_DAY_WINDOWS = (30, 90, 365)
DEFAULT_WORKERS = min(8, os.cpu_count() or 1)

FEATURE_COLUMNS: Sequence[Tuple[str, str]] = (
    ("match_id", "Unique match identifier from source cleaned file."),
    ("match_date", "Match start date in ISO-8601 format (YYYY-MM-DD)."),
    ("event_year", "Tournament event year for partitioning/backfills."),
    ("surface", "Court surface label from Court column (e.g., Hard, Clay, Grass)."),
    ("player_id", "Player identifier for the focal player row."),
    ("opponent_id", "Opponent player identifier for the focal player row."),
    ("is_winner", "1 if focal player won the match, else 0."),
    ("elo_pre", "Player Elo rating immediately before this match."),
    ("opponent_elo_pre", "Opponent Elo rating immediately before this match."),
    ("elo_pre_diff", "Player pre-match Elo minus opponent pre-match Elo."),
    ("career_matches", "Count of matches played by player prior to this match."),
    ("career_win_pct", "Career win percentage prior to this match."),
    ("opponent_career_matches", "Count of matches played by opponent prior to this match."),
    ("opponent_career_win_pct", "Career win percentage for opponent prior to this match."),
    ("career_matches_diff", "Player prior career matches minus opponent prior career matches."),
    ("career_win_pct_diff", "Player prior career win percentage minus opponent prior career win percentage."),
    ("service_points_won_pct", "Match-level total service points won percentage."),
    ("opponent_service_points_won_pct", "Opponent match-level total service points won percentage."),
    ("service_points_won_pct_diff", "Player service points won percentage minus opponent percentage."),
    ("return_points_won_pct", "Match-level total return points won percentage."),
    ("opponent_return_points_won_pct", "Opponent match-level total return points won percentage."),
    ("return_points_won_pct_diff", "Player return points won percentage minus opponent percentage."),
    ("aces_per_service_game", "Aces divided by service games played in this match."),
    ("opponent_aces_per_service_game", "Opponent aces divided by service games played in this match."),
    ("aces_per_service_game_diff", "Player aces/service-game minus opponent aces/service-game."),
    (
        "double_faults_per_service_game",
        "Double faults divided by service games played in this match.",
    ),
    (
        "opponent_double_faults_per_service_game",
        "Opponent double faults divided by service games played in this match.",
    ),
    (
        "double_faults_per_service_game_diff",
        "Player double-faults/service-game minus opponent double-faults/service-game.",
    ),
    (
        "break_points_saved_pct",
        "Break points saved percentage in this match (Set[0] aggregate).",
    ),
    ("opponent_break_points_saved_pct", "Opponent break points saved percentage in this match."),
    ("break_points_saved_pct_diff", "Player break points saved percentage minus opponent percentage."),
)


@dataclass
class PlayerMatchObservation:
    match_id: str
    match_date: date
    event_year: int
    surface: str
    player_id: str
    opponent_id: str
    is_winner: int
    service_points_won_pct: Optional[float]
    return_points_won_pct: Optional[float]
    aces_per_service_game: Optional[float]
    double_faults_per_service_game: Optional[float]
    break_points_saved_pct: Optional[float]


@dataclass
class PlayerHistoryState:
    matches_played: int = 0
    wins: int = 0


@dataclass
class HistoryEntry:
    match_date: date
    surface: str
    is_win: int
    elo_pre: float
    service_points_won_pct: Optional[float]
    return_points_won_pct: Optional[float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory with cleaned match files (atp_YYYY_clean.csv).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/features"),
        help="Directory for feature artifacts (parquet/sqlite/state).",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("data/features/feature_state.json"),
        help="Incremental state JSON path.",
    )
    parser.add_argument(
        "--parquet-file",
        type=Path,
        default=Path("data/features/player_features.parquet"),
        help="Parquet output path.",
    )
    parser.add_argument(
        "--sqlite-file",
        type=Path,
        default=Path("data/features/player_features.sqlite"),
        help="SQLite serving table path.",
    )
    parser.add_argument(
        "--feature-doc",
        type=Path,
        default=Path("docs/player_feature_columns.md"),
        help="Markdown output documenting feature columns.",
    )
    parser.add_argument(
        "--match-windows",
        type=int,
        nargs="+",
        default=list(DEFAULT_MATCH_WINDOWS),
        help="Rolling windows over the last N matches.",
    )
    parser.add_argument(
        "--day-windows",
        type=int,
        nargs="+",
        default=list(DEFAULT_DAY_WINDOWS),
        help="Rolling windows over trailing day ranges.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Worker processes for file-level parallelism (default targets up to 8 physical cores).",
    )
    return parser.parse_args()


def file_metadata(path: Path) -> Dict[str, object]:
    stat = path.stat()
    return {
        "size": stat.st_size,
        "mtime": int(stat.st_mtime),
    }


def file_sha256(path: Path) -> str:
    hash_obj = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def file_fingerprint(path: Path) -> Dict[str, object]:
    payload = file_metadata(path)
    payload["sha256"] = file_sha256(path)
    return payload


def load_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"file_fingerprints": {}, "updated_at": None}
    with path.open() as fh:
        return json.load(fh)


def save_state(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def parse_date(raw: str) -> Optional[date]:
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_float(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def infer_surface(row: Dict[str, str]) -> str:
    for key in ("Court", "Surface"):
        value = (row.get(key) or "").strip()
        if value:
            return value
    return "Unknown"


def iter_clean_files(input_dir: Path) -> List[Path]:
    return sorted(input_dir.glob("atp_*_clean.csv"))


def changed_files(
    files: Sequence[Path], state: Dict[str, object], workers: int
) -> Tuple[List[Path], Dict[str, Dict[str, object]]]:
    prior = state.get("file_fingerprints", {}) if isinstance(state, dict) else {}
    if not isinstance(prior, dict):
        prior = {}

    changed: List[Path] = []
    fingerprints: Dict[str, Dict[str, object]] = {}
    files_requiring_hash: List[Path] = []
    for path in files:
        path_key = str(path)
        current_meta = file_metadata(path)
        previous = prior.get(path_key)
        if (
            isinstance(previous, dict)
            and previous.get("size") == current_meta["size"]
            and previous.get("mtime") == current_meta["mtime"]
        ):
            fingerprints[path_key] = {
                "size": current_meta["size"],
                "mtime": current_meta["mtime"],
                "sha256": previous.get("sha256"),
            }
            continue
        files_requiring_hash.append(path)
        current_fp = {
            "size": current_meta["size"],
            "mtime": current_meta["mtime"],
        }
        fingerprints[path_key] = current_fp
    if files_requiring_hash:
        if workers > 1:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                sha_values = list(pool.map(file_sha256, files_requiring_hash))
        else:
            sha_values = [file_sha256(path) for path in files_requiring_hash]
        for path, sha in zip(files_requiring_hash, sha_values):
            path_key = str(path)
            fingerprints[path_key]["sha256"] = sha
            if prior.get(path_key) != fingerprints[path_key]:
                changed.append(path)
    return changed, fingerprints


def affected_players_from_file(file_path: Path) -> Set[str]:
    affected: Set[str] = set()
    with file_path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            for key in ("PlayerTeam1.PlayerId", "PlayerTeam2.PlayerId"):
                player_id = (row.get(key) or "").strip()
                if player_id:
                    affected.add(player_id)
    return affected


def collect_affected_players(files: Sequence[Path], workers: int) -> Set[str]:
    affected: Set[str] = set()
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for player_set in pool.map(affected_players_from_file, files):
                affected.update(player_set)
    else:
        for file_path in files:
            affected.update(affected_players_from_file(file_path))
    return affected


def extract_observations_from_file(
    file_path: Path, players: Optional[Set[str]]
) -> List[PlayerMatchObservation]:
    observations: List[PlayerMatchObservation] = []
    with file_path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            match_id = (row.get("MatchId") or "").strip()
            match_date = parse_date(row.get("StartDate") or "")
            if not match_id or match_date is None:
                continue

            event_year = int(parse_float(row.get("EventYear")) or match_date.year)
            surface = infer_surface(row)
            winner_id = (row.get("WinningPlayerId") or "").strip()

            entries = [
                ("PlayerTeam1", "PlayerTeam2"),
                ("PlayerTeam2", "PlayerTeam1"),
            ]

            for side, opponent_side in entries:
                player_id = (row.get(f"{side}.PlayerId") or "").strip()
                opponent_id = (row.get(f"{opponent_side}.PlayerId") or "").strip()
                if not player_id or not opponent_id:
                    continue
                if players is not None and player_id not in players:
                    continue

                service_games = parse_float(
                    row.get(f"{side}.Sets[0].Stats.ServiceStats.ServiceGamesPlayed.Number")
                )
                aces = parse_float(row.get(f"{side}.Sets[0].Stats.ServiceStats.Aces.Number"))
                dfs = parse_float(row.get(f"{side}.Sets[0].Stats.ServiceStats.DoubleFaults.Number"))

                observations.append(
                    PlayerMatchObservation(
                        match_id=match_id,
                        match_date=match_date,
                        event_year=event_year,
                        surface=surface,
                        player_id=player_id,
                        opponent_id=opponent_id,
                        is_winner=1 if winner_id and winner_id == player_id else 0,
                        service_points_won_pct=parse_float(
                            row.get(f"{side}.Sets[0].Stats.PointStats.TotalServicePointsWon.Percent")
                        ),
                        return_points_won_pct=parse_float(
                            row.get(f"{side}.Sets[0].Stats.PointStats.TotalReturnPointsWon.Percent")
                        ),
                        aces_per_service_game=safe_ratio(aces, service_games),
                        double_faults_per_service_game=safe_ratio(dfs, service_games),
                        break_points_saved_pct=parse_float(
                            row.get(f"{side}.Sets[0].Stats.ServiceStats.BreakPointsSaved.Percent")
                        ),
                    )
                )
    return observations


def extract_observations(
    input_files: Sequence[Path], players: Optional[Set[str]], workers: int
) -> List[PlayerMatchObservation]:
    observations: List[PlayerMatchObservation] = []
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for file_observations in pool.map(
                extract_observations_from_file,
                input_files,
                [players] * len(input_files),
            ):
                observations.extend(file_observations)
    else:
        for file_path in input_files:
            observations.extend(extract_observations_from_file(file_path, players))

    observations.sort(key=lambda x: (x.match_date, x.match_id, x.player_id))
    return observations


def trailing_window(entries: Sequence[HistoryEntry], current_date: date, max_days: int) -> List[HistoryEntry]:
    cutoff = current_date.toordinal() - max_days
    return [e for e in entries if e.match_date.toordinal() >= cutoff]


def linear_slope(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    n = len(values)
    x_sum = (n - 1) * n / 2.0
    xx_sum = (n - 1) * n * (2 * n - 1) / 6.0
    y_sum = float(sum(values))
    xy_sum = float(sum(i * y for i, y in enumerate(values)))
    denom = n * xx_sum - x_sum * x_sum
    if abs(denom) < 1e-12:
        return None
    return (n * xy_sum - x_sum * y_sum) / denom


def win_pct(entries: Sequence[HistoryEntry]) -> Optional[float]:
    if not entries:
        return None
    return sum(e.is_win for e in entries) / float(len(entries))


def average_metric(entries: Sequence[HistoryEntry], attr: str) -> Optional[float]:
    values = [getattr(e, attr) for e in entries if getattr(e, attr) is not None]
    if not values:
        return None
    return float(sum(values)) / float(len(values))


def compute_features(
    observations: Sequence[PlayerMatchObservation],
    match_windows: Sequence[int],
    day_windows: Sequence[int],
) -> List[Dict[str, object]]:
    player_elo: Dict[str, float] = defaultdict(lambda: 1500.0)
    player_state: Dict[str, PlayerHistoryState] = defaultdict(PlayerHistoryState)
    player_history: Dict[str, List[HistoryEntry]] = defaultdict(list)
    player_surface_history: Dict[str, Dict[str, List[HistoryEntry]]] = defaultdict(
        lambda: defaultdict(list)
    )

    rows: List[Dict[str, object]] = []

    def numeric_difference(left: object, right: object) -> Optional[float]:
        if left is None or right is None:
            return None
        if isinstance(left, bool) or isinstance(right, bool):
            return None
        if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
            return None
        return float(left) - float(right)

    def add_difference_features(row: Dict[str, object]) -> None:
        for key in list(row.keys()):
            if not key.startswith("opponent_"):
                continue
            base_key = key[len("opponent_") :]
            if base_key not in row:
                continue
            row[f"{base_key}_diff"] = numeric_difference(row[base_key], row[key])

    all_matches = sorted(observations, key=lambda x: (x.match_date, x.match_id, x.player_id))
    grouped_match: Dict[Tuple[str, date], List[PlayerMatchObservation]] = defaultdict(list)
    for obs in all_matches:
        grouped_match[(obs.match_id, obs.match_date)].append(obs)

    def add_rolling_features(
        row: Dict[str, object],
        history_entries: Sequence[HistoryEntry],
        surface_entries: Sequence[HistoryEntry],
        current_date: date,
        current_surface: str,
        *,
        prefix: str = "",
    ) -> None:
        for n in match_windows:
            recent = history_entries[-n:]
            recent_surface = surface_entries[-n:]
            row[f"{prefix}win_pct_last_{n}_matches"] = win_pct(recent)
            row[f"{prefix}elo_slope_last_{n}_matches"] = linear_slope([e.elo_pre for e in recent])
            row[f"{prefix}win_pct_slope_last_{n}_matches"] = linear_slope([float(e.is_win) for e in recent])
            row[f"{prefix}srv_points_won_last_{n}_matches"] = average_metric(
                recent, "service_points_won_pct"
            )
            row[f"{prefix}ret_points_won_last_{n}_matches"] = average_metric(
                recent, "return_points_won_pct"
            )
            row[f"{prefix}surface_win_pct_last_{n}_matches"] = win_pct(recent_surface)

        for days in day_windows:
            day_recent = trailing_window(history_entries, current_date, days)
            day_surface_recent = [e for e in day_recent if e.surface == current_surface]
            row[f"{prefix}matches_last_{days}_days"] = len(day_recent)
            row[f"{prefix}win_pct_last_{days}_days"] = win_pct(day_recent)
            row[f"{prefix}elo_slope_last_{days}_days"] = linear_slope([e.elo_pre for e in day_recent])
            row[f"{prefix}surface_win_pct_last_{days}_days"] = win_pct(day_surface_recent)

    for (_, _), pair in sorted(grouped_match.items(), key=lambda item: (item[0][1], item[0][0])):
        if len(pair) != 2:
            continue
        a, b = pair[0], pair[1]
        a_elo = player_elo[a.player_id]
        b_elo = player_elo[b.player_id]

        expected_a = 1.0 / (1.0 + math.pow(10.0, (b_elo - a_elo) / 400.0))
        expected_b = 1.0 - expected_a

        pre_elos = {
            a.player_id: a_elo,
            b.player_id: b_elo,
        }

        for obs, opponent_obs, opponent_elo in (
            (a, b, b_elo),
            (b, a, a_elo),
        ):
            history_entries = player_history[obs.player_id]
            surface_entries = player_surface_history[obs.player_id][obs.surface]
            opponent_history_entries = player_history[obs.opponent_id]
            opponent_surface_entries = player_surface_history[obs.opponent_id][obs.surface]
            state = player_state[obs.player_id]
            opponent_state = player_state[obs.opponent_id]

            row: Dict[str, object] = {
                "match_id": obs.match_id,
                "match_date": obs.match_date.isoformat(),
                "event_year": obs.event_year,
                "surface": obs.surface,
                "player_id": obs.player_id,
                "opponent_id": obs.opponent_id,
                "is_winner": obs.is_winner,
                "elo_pre": player_elo[obs.player_id],
                "opponent_elo_pre": opponent_elo,
                "career_matches": state.matches_played,
                "career_win_pct": (state.wins / state.matches_played) if state.matches_played else None,
                "opponent_career_matches": opponent_state.matches_played,
                "opponent_career_win_pct": (
                    opponent_state.wins / opponent_state.matches_played
                )
                if opponent_state.matches_played
                else None,
                "service_points_won_pct": obs.service_points_won_pct,
                "opponent_service_points_won_pct": opponent_obs.service_points_won_pct,
                "return_points_won_pct": obs.return_points_won_pct,
                "opponent_return_points_won_pct": opponent_obs.return_points_won_pct,
                "aces_per_service_game": obs.aces_per_service_game,
                "opponent_aces_per_service_game": opponent_obs.aces_per_service_game,
                "double_faults_per_service_game": obs.double_faults_per_service_game,
                "opponent_double_faults_per_service_game": opponent_obs.double_faults_per_service_game,
                "break_points_saved_pct": obs.break_points_saved_pct,
                "opponent_break_points_saved_pct": opponent_obs.break_points_saved_pct,
            }

            add_rolling_features(
                row,
                history_entries,
                surface_entries,
                obs.match_date,
                obs.surface,
            )
            add_rolling_features(
                row,
                opponent_history_entries,
                opponent_surface_entries,
                obs.match_date,
                obs.surface,
                prefix="opponent_",
            )
            add_difference_features(row)

            rows.append(row)

        # Update post-match states.
        player_elo[a.player_id] = a_elo + ELO_K_FACTOR * (a.is_winner - expected_a)
        player_elo[b.player_id] = b_elo + ELO_K_FACTOR * (b.is_winner - expected_b)

        for obs in (a, b):
            player_state[obs.player_id].matches_played += 1
            player_state[obs.player_id].wins += obs.is_winner
            entry = HistoryEntry(
                match_date=obs.match_date,
                surface=obs.surface,
                is_win=obs.is_winner,
                elo_pre=pre_elos[obs.player_id],
                service_points_won_pct=obs.service_points_won_pct,
                return_points_won_pct=obs.return_points_won_pct,
            )
            player_history[obs.player_id].append(entry)
            player_surface_history[obs.player_id][obs.surface].append(entry)

    return rows


def generate_column_docs(rows: Sequence[Dict[str, object]], path: Path) -> None:
    if not rows:
        return

    dynamic_columns = sorted([c for c in rows[0].keys() if c not in {name for name, _ in FEATURE_COLUMNS}])
    docs = ["# Player feature columns", "", "| Column | Description |", "|---|---|"]
    for name, desc in FEATURE_COLUMNS:
        docs.append(f"| `{name}` | {desc} |")

    dynamic_descriptions = {
        "win_pct_last_": "Win percentage over trailing N matches before the current match.",
        "opponent_win_pct_last_": "Opponent win percentage over trailing N matches before the current match.",
        "elo_slope_last_": "Linear slope of pre-match Elo over the specified trailing window.",
        "opponent_elo_slope_last_": "Opponent linear slope of pre-match Elo over the specified trailing window.",
        "win_pct_slope_last_": "Linear slope of binary win indicator (1/0) over trailing matches.",
        "opponent_win_pct_slope_last_": "Opponent linear slope of binary win indicator (1/0) over trailing matches.",
        "srv_points_won_last_": "Average service points won percent over trailing N matches.",
        "opponent_srv_points_won_last_": "Opponent average service points won percent over trailing N matches.",
        "ret_points_won_last_": "Average return points won percent over trailing N matches.",
        "opponent_ret_points_won_last_": "Opponent average return points won percent over trailing N matches.",
        "surface_win_pct_last_": "Win percentage for same-surface matches in trailing window.",
        "opponent_surface_win_pct_last_": "Opponent win percentage for same-surface matches in trailing window.",
        "matches_last_": "Number of matches played during trailing day window.",
        "opponent_matches_last_": "Number of matches opponent played during trailing day window.",
    }
    for col in dynamic_columns:
        desc = "Derived rolling/trend feature generated by window configuration."
        for prefix, prefix_desc in dynamic_descriptions.items():
            if col.startswith(prefix):
                desc = prefix_desc
                break
        if col.endswith("_diff"):
            desc = "Player value minus corresponding opponent value for the same feature."
        docs.append(f"| `{col}` | {desc} |")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(docs) + "\n")


def init_sqlite(sqlite_path: Path, columns: Sequence[str]) -> sqlite3.Connection:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path)

    def sqlite_column_type(col: str) -> str:
        if col in {"event_year", "is_winner", "career_matches", "opponent_career_matches"} or col.startswith("matches_last_") or col.startswith("opponent_matches_last_"):
            return "INTEGER"
        if col in {"match_id", "match_date", "surface", "player_id", "opponent_id"}:
            return "TEXT"
        return "REAL"

    col_defs = []
    for col in columns:
        col_defs.append(f'"{col}" {sqlite_column_type(col)}')

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS player_features (
            {', '.join(col_defs)},
            PRIMARY KEY (player_id, match_id)
        )
        """
    )
    existing_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(player_features)")
    }
    missing_columns = [col for col in columns if col not in existing_columns]
    for col in missing_columns:
        conn.execute(
            f'ALTER TABLE player_features ADD COLUMN "{col}" {sqlite_column_type(col)}'
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_player_features_match_date ON player_features (match_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_player_features_surface ON player_features (surface, player_id, match_date)"
    )
    conn.commit()
    return conn


def upsert_sqlite_rows(conn: sqlite3.Connection, rows: Sequence[Dict[str, object]], affected_players: Set[str]) -> None:
    if not rows:
        return

    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    quoted_columns = ", ".join(f'"{c}"' for c in columns)
    update_expr = ", ".join([f'"{c}"=excluded."{c}"' for c in columns if c not in {"player_id", "match_id"}])

    if affected_players:
        delete_placeholders = ",".join(["?"] * len(affected_players))
        conn.execute(
            f"DELETE FROM player_features WHERE player_id IN ({delete_placeholders})",
            tuple(sorted(affected_players)),
        )

    conn.executemany(
        f"""
        INSERT INTO player_features ({quoted_columns}) VALUES ({placeholders})
        ON CONFLICT(player_id, match_id) DO UPDATE SET {update_expr}
        """,
        [tuple(row[c] for c in columns) for row in rows],
    )
    conn.commit()


def upsert_parquet(
    parquet_path: Path,
    rows: Sequence[Dict[str, object]],
    affected_players: Set[str],
    columns: Sequence[str],
) -> None:
    if not rows:
        return

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    col_defs = []
    for col in columns:
        if col in {"event_year", "is_winner", "career_matches", "opponent_career_matches"} or col.startswith("matches_last_") or col.startswith("opponent_matches_last_"):
            col_type = "BIGINT"
        elif col in {"match_id", "match_date", "surface", "player_id", "opponent_id"}:
            col_type = "VARCHAR"
        else:
            col_type = "DOUBLE"
        col_defs.append(f'"{col}" {col_type}')

    con.execute(f"CREATE TEMP TABLE updates ({', '.join(col_defs)})")
    cols = ', '.join(f'"{c}"' for c in columns)
    placeholders = ', '.join(['?'] * len(columns))

    con.executemany(
        f"INSERT INTO updates ({cols}) VALUES ({placeholders})",
        [tuple(row[c] for c in columns) for row in rows],
    )

    if parquet_path.exists():
        con.execute("CREATE TEMP TABLE existing AS SELECT * FROM read_parquet(?)", [str(parquet_path)])
        if affected_players:
            placeholders = ", ".join(["?"] * len(affected_players))
            con.execute(
                f"""
                CREATE TEMP TABLE merged AS
                SELECT * FROM existing WHERE player_id NOT IN ({placeholders})
                UNION ALL
                SELECT * FROM updates
                """,
                list(sorted(affected_players)),
            )
        else:
            con.execute(
                "CREATE TEMP TABLE merged AS SELECT * FROM existing UNION ALL SELECT * FROM updates"
            )
    else:
        con.execute("CREATE TEMP TABLE merged AS SELECT * FROM updates")

    con.execute("COPY merged TO ? (FORMAT PARQUET)", [str(parquet_path)])
    con.close()


def build_feature_rows(
    all_files: Sequence[Path],
    changed: Sequence[Path],
    match_windows: Sequence[int],
    day_windows: Sequence[int],
    workers: int,
) -> Tuple[List[Dict[str, object]], Set[str]]:
    if not changed:
        return [], set()
    affected_players = collect_affected_players(changed, workers)
    observations = extract_observations(all_files, affected_players, workers)
    rows = compute_features(observations, match_windows, day_windows)
    return rows, affected_players


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    files = iter_clean_files(args.input_dir)
    if not files:
        raise SystemExit(f"No cleaned files found under {args.input_dir}")

    state = load_state(args.state_file)
    workers = max(1, args.workers)
    changed, fingerprints = changed_files(files, state, workers)

    if not changed:
        print("No changed input files detected; feature artifacts are already up to date.")
        return 0

    rows, affected_players = build_feature_rows(
        files, changed, args.match_windows, args.day_windows, workers
    )
    if not rows:
        print("Changed files detected but no feature rows produced; nothing to persist.")
        return 0

    columns = list(rows[0].keys())
    conn = init_sqlite(args.sqlite_file, columns)
    upsert_sqlite_rows(conn, rows, affected_players)
    conn.close()

    upsert_parquet(args.parquet_file, rows, affected_players, columns)
    generate_column_docs(rows, args.feature_doc)

    state_payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "file_fingerprints": fingerprints,
        "match_windows": list(args.match_windows),
        "day_windows": list(args.day_windows),
        "rows_last_refresh": len(rows),
        "affected_players_last_refresh": len(affected_players),
    }
    save_state(args.state_file, state_payload)

    print(
        f"Wrote {len(rows)} rows for {len(affected_players)} players to "
        f"{args.parquet_file} and {args.sqlite_file}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
