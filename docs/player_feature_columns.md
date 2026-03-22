# Player feature columns

`pipeline/features.py` emits one row per `(player_id, match_id)` with baseline,
rolling-window, trend, and surface-aware features.

## Core columns

| Column | Description |
|---|---|
| `match_id` | Unique match identifier from cleaned source files. |
| `match_date` | Match start date (`YYYY-MM-DD`). |
| `event_year` | Event year from source file. |
| `surface` | Surface label (from `Court`/`Surface`). |
| `player_id` | Focal player ID. |
| `opponent_id` | Opponent player ID. |
| `is_winner` | `1` if focal player won, otherwise `0`. |
| `elo_pre` | Pre-match Elo rating for the focal player. |
| `opponent_elo_pre` | Pre-match Elo rating for the opponent. |
| `elo_delta_expected` | Elo delta implied by result and expected score. |
| `career_matches` | Matches played by focal player before current match. |
| `career_win_pct` | Win rate before current match. |
| `service_points_won_pct` | Match service points won percentage. |
| `return_points_won_pct` | Match return points won percentage. |
| `aces_per_service_game` | Aces divided by service games played. |
| `double_faults_per_service_game` | Double faults divided by service games played. |
| `break_points_saved_pct` | Break points saved percentage. |

## Generated rolling/trend columns

These are generated dynamically from `--match-windows` and `--day-windows`.

| Pattern | Description |
|---|---|
| `win_pct_last_{N}_matches` | Win percentage over previous `N` matches. |
| `elo_slope_last_{N}_matches` | Linear slope of pre-match Elo over previous `N` matches. |
| `win_pct_slope_last_{N}_matches` | Linear slope of win indicator over previous `N` matches. |
| `srv_points_won_last_{N}_matches` | Mean service points won over previous `N` matches. |
| `ret_points_won_last_{N}_matches` | Mean return points won over previous `N` matches. |
| `surface_win_pct_last_{N}_matches` | Same-surface win rate over previous `N` matches. |
| `matches_last_{D}_days` | Number of matches in trailing `D`-day window. |
| `win_pct_last_{D}_days` | Win rate in trailing `D`-day window. |
| `elo_slope_last_{D}_days` | Elo slope in trailing `D`-day window. |
| `surface_win_pct_last_{D}_days` | Same-surface win rate in trailing `D`-day window. |

> Runtime note: each run also rewrites this documentation file with the exact column
> names present for the configured windows.
