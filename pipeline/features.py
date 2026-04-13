#!/usr/bin/env python3
"""Generate player-level feature vectors from cleaned ATP match CSV files."""

from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple

try:
    import duckdb  # type: ignore
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "duckdb is required for Parquet persistence. Install with: pip install duckdb"
    ) from exc

from pipeline.feature_compute import compute_features
from pipeline.feature_data import (
    changed_files,
    collect_affected_players,
    extract_observations,
    iter_clean_files,
    load_state,
    save_state,
)

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
    ("double_faults_per_service_game", "Double faults divided by service games played in this match."),
    ("opponent_double_faults_per_service_game", "Opponent double faults divided by service games played in this match."),
    ("double_faults_per_service_game_diff", "Player double-faults/service-game minus opponent double-faults/service-game."),
    ("break_points_saved_pct", "Break points saved percentage in this match (Set[0] aggregate)."),
    ("opponent_break_points_saved_pct", "Opponent break points saved percentage in this match."),
    ("break_points_saved_pct_diff", "Player break points saved percentage minus opponent percentage."),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/features"))
    parser.add_argument("--state-file", type=Path, default=Path("data/features/feature_state.json"))
    parser.add_argument("--parquet-file", type=Path, default=Path("data/features/player_features.parquet"))
    parser.add_argument("--sqlite-file", type=Path, default=Path("data/features/player_features.sqlite"))
    parser.add_argument("--feature-doc", type=Path, default=Path("docs/player_feature_columns.md"))
    parser.add_argument("--match-windows", type=int, nargs="+", default=list(DEFAULT_MATCH_WINDOWS))
    parser.add_argument("--day-windows", type=int, nargs="+", default=list(DEFAULT_DAY_WINDOWS))
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    return parser.parse_args()


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

    col_defs = [f'"{col}" {sqlite_column_type(col)}' for col in columns]
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS player_features (
            {', '.join(col_defs)},
            PRIMARY KEY (player_id, match_id)
        )
        """
    )
    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(player_features)")}
    for col in [col for col in columns if col not in existing_columns]:
        conn.execute(f'ALTER TABLE player_features ADD COLUMN "{col}" {sqlite_column_type(col)}')

    conn.execute("CREATE INDEX IF NOT EXISTS idx_player_features_match_date ON player_features (match_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_player_features_surface ON player_features (surface, player_id, match_date)")
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
    col_types: Dict[str, str] = {}
    for col in columns:
        if col in {"event_year", "is_winner", "career_matches", "opponent_career_matches"} or col.startswith("matches_last_") or col.startswith("opponent_matches_last_"):
            col_type = "BIGINT"
        elif col in {"match_id", "match_date", "surface", "player_id", "opponent_id"}:
            col_type = "VARCHAR"
        else:
            col_type = "DOUBLE"
        col_types[col] = col_type
        col_defs.append(f'"{col}" {col_type}')

    con.execute(f"CREATE TEMP TABLE updates ({', '.join(col_defs)})")
    cols = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    con.executemany(
        f"INSERT INTO updates ({cols}) VALUES ({placeholders})",
        [tuple(row[c] for c in columns) for row in rows],
    )

    if parquet_path.exists():
        existing_cols = {
            row[0]
            for row in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(parquet_path)],
            ).fetchall()
        }
        existing_projection = ", ".join(
            [
                f'"{col}"'
                if col in existing_cols
                else f'CAST(NULL AS {col_types[col]}) AS "{col}"'
                for col in columns
            ]
        )
        con.execute(
            f"CREATE TEMP TABLE existing AS SELECT {existing_projection} FROM read_parquet(?)",
            [str(parquet_path)],
        )
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
            con.execute("CREATE TEMP TABLE merged AS SELECT * FROM existing UNION ALL SELECT * FROM updates")
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
    return compute_features(observations, match_windows, day_windows), affected_players


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

    rows, affected_players = build_feature_rows(files, changed, args.match_windows, args.day_windows, workers)
    if not rows:
        print("Changed files detected but no feature rows produced; nothing to persist.")
        return 0

    columns = list(rows[0].keys())
    conn = init_sqlite(args.sqlite_file, columns)
    upsert_sqlite_rows(conn, rows, affected_players)
    conn.close()

    upsert_parquet(args.parquet_file, rows, affected_players, columns)
    generate_column_docs(rows, args.feature_doc)

    save_state(
        args.state_file,
        {
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "file_fingerprints": fingerprints,
            "match_windows": list(args.match_windows),
            "day_windows": list(args.day_windows),
            "rows_last_refresh": len(rows),
            "affected_players_last_refresh": len(affected_players),
        },
    )

    print(f"Wrote {len(rows)} rows for {len(affected_players)} players to {args.parquet_file} and {args.sqlite_file}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
