from __future__ import annotations

import csv
import hashlib
import json
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple


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
    ace_pct: Optional[float]
    double_faults_per_service_game: Optional[float]
    break_points_saved_pct: Optional[float]


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
        fingerprints[path_key] = {
            "size": current_meta["size"],
            "mtime": current_meta["mtime"],
        }

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

            for side, opponent_side in (("PlayerTeam1", "PlayerTeam2"), ("PlayerTeam2", "PlayerTeam1")):
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
                total_service_points = parse_float(
                    row.get(f"{side}.Sets[0].Stats.PointStats.TotalServicePointsWon.Divisor")
                )
                ace_pct = safe_ratio(aces, total_service_points)

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
                        ace_pct=(ace_pct * 100.0 if ace_pct is not None else None),
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
