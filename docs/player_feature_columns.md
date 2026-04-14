# Player feature columns

| Column | Description |
|---|---|
| `match_id` | Unique match identifier from source cleaned file. |
| `match_date` | Match start date in ISO-8601 format (YYYY-MM-DD). |
| `event_year` | Tournament event year for partitioning/backfills. |
| `surface` | Court surface label from Court column (e.g., Hard, Clay, Grass). |
| `player_id` | Player identifier for the focal player row. |
| `opponent_id` | Opponent player identifier for the focal player row. |
| `is_winner` | 1 if focal player won the match, else 0. |
| `elo_pre` | Player Elo rating immediately before this match. |
| `opponent_elo_pre` | Opponent Elo rating immediately before this match. |
| `elo_pre_diff` | Player pre-match Elo minus opponent pre-match Elo. |
| `career_matches` | Count of matches played by player prior to this match. |
| `career_win_pct` | Career win percentage prior to this match. |
| `opponent_career_matches` | Count of matches played by opponent prior to this match. |
| `opponent_career_win_pct` | Career win percentage for opponent prior to this match. |
| `career_matches_diff` | Player prior career matches minus opponent prior career matches. |
| `career_win_pct_diff` | Player prior career win percentage minus opponent prior career win percentage. |
| `elo_slope_last_10_matches` | Linear slope of pre-match Elo over the specified trailing window. |
| `elo_slope_last_20_matches` | Linear slope of pre-match Elo over the specified trailing window. |
| `elo_slope_last_30_days` | Linear slope of pre-match Elo over the specified trailing window. |
| `elo_slope_last_365_days` | Linear slope of pre-match Elo over the specified trailing window. |
| `elo_slope_last_5_matches` | Linear slope of pre-match Elo over the specified trailing window. |
| `elo_slope_last_90_days` | Linear slope of pre-match Elo over the specified trailing window. |
| `matches_last_30_days` | Number of matches played during trailing day window. |
| `matches_last_365_days` | Number of matches played during trailing day window. |
| `matches_last_90_days` | Number of matches played during trailing day window. |
| `ret_points_won_last_10_matches` | Average return points won percent over trailing N matches. |
| `ret_points_won_last_20_matches` | Average return points won percent over trailing N matches. |
| `ret_points_won_last_5_matches` | Average return points won percent over trailing N matches. |
| `srv_points_won_last_10_matches` | Average service points won percent over trailing N matches. |
| `srv_points_won_last_20_matches` | Average service points won percent over trailing N matches. |
| `srv_points_won_last_5_matches` | Average service points won percent over trailing N matches. |
| `surface_win_pct_last_10_matches` | Win percentage for same-surface matches in trailing window. |
| `surface_win_pct_last_20_matches` | Win percentage for same-surface matches in trailing window. |
| `surface_win_pct_last_30_days` | Win percentage for same-surface matches in trailing window. |
| `surface_win_pct_last_365_days` | Win percentage for same-surface matches in trailing window. |
| `surface_win_pct_last_5_matches` | Win percentage for same-surface matches in trailing window. |
| `surface_win_pct_last_90_days` | Win percentage for same-surface matches in trailing window. |
| `win_pct_last_10_matches` | Win percentage over trailing N matches before the current match. |
| `win_pct_last_20_matches` | Win percentage over trailing N matches before the current match. |
| `win_pct_last_30_days` | Win percentage over trailing N matches before the current match. |
| `win_pct_last_365_days` | Win percentage over trailing N matches before the current match. |
| `win_pct_last_5_matches` | Win percentage over trailing N matches before the current match. |
| `win_pct_last_90_days` | Win percentage over trailing N matches before the current match. |
| `win_pct_slope_last_10_matches` | Linear slope of binary win indicator (1/0) over trailing matches. |
| `win_pct_slope_last_20_matches` | Linear slope of binary win indicator (1/0) over trailing matches. |
| `win_pct_slope_last_5_matches` | Linear slope of binary win indicator (1/0) over trailing matches. |
