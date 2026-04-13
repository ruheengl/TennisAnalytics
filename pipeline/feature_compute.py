from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date
from typing import Deque, Dict, List, Optional, Sequence, Tuple

from pipeline.feature_data import PlayerMatchObservation

ELO_K_FACTOR = 32.0


@dataclass
class HistoryEntry:
    match_date: date
    surface: str
    is_win: int
    elo_pre: float
    service_points_won_pct: Optional[float]
    return_points_won_pct: Optional[float]


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


@dataclass
class MatchWindowState:
    maxlen: int
    entries: Deque[HistoryEntry] = field(default_factory=deque)
    wins_sum: float = 0.0
    service_sum: float = 0.0
    service_count: int = 0
    return_sum: float = 0.0
    return_count: int = 0
    elo_sum: float = 0.0
    elo_xy_sum: float = 0.0
    win_indicator_sum: float = 0.0
    win_indicator_xy_sum: float = 0.0

    def push(self, entry: HistoryEntry) -> None:
        prev_len = len(self.entries)
        if prev_len == self.maxlen:
            self._evict_oldest()
            prev_len -= 1
        self.entries.append(entry)
        self.wins_sum += entry.is_win
        self.elo_sum += entry.elo_pre
        self.elo_xy_sum += prev_len * entry.elo_pre
        win_val = float(entry.is_win)
        self.win_indicator_sum += win_val
        self.win_indicator_xy_sum += prev_len * win_val
        if entry.service_points_won_pct is not None:
            self.service_sum += entry.service_points_won_pct
            self.service_count += 1
        if entry.return_points_won_pct is not None:
            self.return_sum += entry.return_points_won_pct
            self.return_count += 1

    def _evict_oldest(self) -> None:
        if not self.entries:
            return
        oldest = self.entries.popleft()
        old_elo_sum = self.elo_sum
        old_win_sum = self.win_indicator_sum
        self.wins_sum -= oldest.is_win
        self.elo_sum -= oldest.elo_pre
        self.win_indicator_sum -= float(oldest.is_win)
        self.elo_xy_sum = self.elo_xy_sum - (old_elo_sum - oldest.elo_pre)
        self.win_indicator_xy_sum = self.win_indicator_xy_sum - (old_win_sum - float(oldest.is_win))
        if oldest.service_points_won_pct is not None:
            self.service_sum -= oldest.service_points_won_pct
            self.service_count -= 1
        if oldest.return_points_won_pct is not None:
            self.return_sum -= oldest.return_points_won_pct
            self.return_count -= 1

    def win_pct(self) -> Optional[float]:
        if not self.entries:
            return None
        return self.wins_sum / len(self.entries)

    def elo_slope(self) -> Optional[float]:
        return _slope_from_sums(len(self.entries), self.elo_sum, self.elo_xy_sum)

    def win_pct_slope(self) -> Optional[float]:
        return _slope_from_sums(len(self.entries), self.win_indicator_sum, self.win_indicator_xy_sum)

    def average_metric(self, attr: str) -> Optional[float]:
        if attr == "service_points_won_pct":
            if self.service_count == 0:
                return None
            return self.service_sum / float(self.service_count)
        if attr == "return_points_won_pct":
            if self.return_count == 0:
                return None
            return self.return_sum / float(self.return_count)
        values = [getattr(e, attr) for e in self.entries if getattr(e, attr) is not None]
        if not values:
            return None
        return float(sum(values)) / float(len(values))


@dataclass
class DayWindowState:
    max_days: int
    entries: Deque[HistoryEntry] = field(default_factory=deque)
    wins_sum: float = 0.0
    elo_sum: float = 0.0
    elo_xy_sum: float = 0.0

    def evict_old(self, current_date: date) -> None:
        cutoff = current_date.toordinal() - self.max_days
        while self.entries and self.entries[0].match_date.toordinal() < cutoff:
            oldest = self.entries.popleft()
            old_elo_sum = self.elo_sum
            self.wins_sum -= oldest.is_win
            self.elo_sum -= oldest.elo_pre
            self.elo_xy_sum = self.elo_xy_sum - (old_elo_sum - oldest.elo_pre)

    def push(self, entry: HistoryEntry) -> None:
        idx = len(self.entries)
        self.entries.append(entry)
        self.wins_sum += entry.is_win
        self.elo_sum += entry.elo_pre
        self.elo_xy_sum += idx * entry.elo_pre

    def matches(self) -> int:
        return len(self.entries)

    def win_pct(self) -> Optional[float]:
        if not self.entries:
            return None
        return self.wins_sum / len(self.entries)

    def elo_slope(self) -> Optional[float]:
        return _slope_from_sums(len(self.entries), self.elo_sum, self.elo_xy_sum)


def _slope_from_sums(n: int, y_sum: float, xy_sum: float) -> Optional[float]:
    if n < 2:
        return None
    x_sum = (n - 1) * n / 2.0
    xx_sum = (n - 1) * n * (2 * n - 1) / 6.0
    denom = n * xx_sum - x_sum * x_sum
    if abs(denom) < 1e-12:
        return None
    return (n * xy_sum - x_sum * y_sum) / denom


@dataclass
class PlayerRollingState:
    career_matches: int = 0
    career_wins: int = 0
    match_windows: Dict[int, MatchWindowState] = field(default_factory=dict)
    surface_match_windows: Dict[str, Dict[int, MatchWindowState]] = field(default_factory=dict)
    day_windows: Dict[int, DayWindowState] = field(default_factory=dict)
    surface_day_windows: Dict[str, Dict[int, DayWindowState]] = field(default_factory=dict)

    def ensure_surface(self, surface: str, match_windows: Sequence[int], day_windows: Sequence[int]) -> None:
        if surface not in self.surface_match_windows:
            self.surface_match_windows[surface] = {n: MatchWindowState(maxlen=n) for n in match_windows}
        if surface not in self.surface_day_windows:
            self.surface_day_windows[surface] = {d: DayWindowState(max_days=d) for d in day_windows}


def compute_features(
    observations: Sequence[PlayerMatchObservation],
    match_windows: Sequence[int],
    day_windows: Sequence[int],
) -> List[Dict[str, object]]:
    player_elo: Dict[str, float] = defaultdict(lambda: 1500.0)
    player_state: Dict[str, PlayerRollingState] = defaultdict(
        lambda: PlayerRollingState(
            match_windows={n: MatchWindowState(maxlen=n) for n in match_windows},
            day_windows={d: DayWindowState(max_days=d) for d in day_windows},
        )
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
            if key.startswith("opponent_"):
                base_key = key[len("opponent_") :]
                if base_key in row:
                    row[f"{base_key}_diff"] = numeric_difference(row[base_key], row[key])

    def prepare_day_windows(state: PlayerRollingState, current_date: date, surface: str) -> None:
        state.ensure_surface(surface, match_windows, day_windows)
        for w in state.day_windows.values():
            w.evict_old(current_date)
        for w in state.surface_day_windows[surface].values():
            w.evict_old(current_date)

    def add_rolling_features(
        row: Dict[str, object],
        state: PlayerRollingState,
        current_date: date,
        current_surface: str,
        *,
        prefix: str = "",
    ) -> None:
        prepare_day_windows(state, current_date, current_surface)
        state.ensure_surface(current_surface, match_windows, day_windows)
        surface_match = state.surface_match_windows[current_surface]
        surface_day = state.surface_day_windows[current_surface]

        for n in match_windows:
            recent = state.match_windows[n]
            recent_surface = surface_match[n]
            row[f"{prefix}win_pct_last_{n}_matches"] = recent.win_pct()
            row[f"{prefix}elo_slope_last_{n}_matches"] = recent.elo_slope()
            row[f"{prefix}win_pct_slope_last_{n}_matches"] = recent.win_pct_slope()
            row[f"{prefix}srv_points_won_last_{n}_matches"] = recent.average_metric("service_points_won_pct")
            row[f"{prefix}ret_points_won_last_{n}_matches"] = recent.average_metric("return_points_won_pct")
            row[f"{prefix}surface_win_pct_last_{n}_matches"] = recent_surface.win_pct()

        for d in day_windows:
            dw = state.day_windows[d]
            sdw = surface_day[d]
            row[f"{prefix}matches_last_{d}_days"] = dw.matches()
            row[f"{prefix}win_pct_last_{d}_days"] = dw.win_pct()
            row[f"{prefix}elo_slope_last_{d}_days"] = dw.elo_slope()
            row[f"{prefix}surface_win_pct_last_{d}_days"] = sdw.win_pct()

    def push_history(state: PlayerRollingState, entry: HistoryEntry) -> None:
        state.ensure_surface(entry.surface, match_windows, day_windows)
        for w in state.match_windows.values():
            w.push(entry)
        for w in state.surface_match_windows[entry.surface].values():
            w.push(entry)
        for w in state.day_windows.values():
            w.push(entry)
        for w in state.surface_day_windows[entry.surface].values():
            w.push(entry)

    grouped_match: Dict[Tuple[str, date], List[PlayerMatchObservation]] = defaultdict(list)
    for obs in sorted(observations, key=lambda x: (x.match_date, x.match_id, x.player_id)):
        grouped_match[(obs.match_id, obs.match_date)].append(obs)

    for (_, _), pair in sorted(grouped_match.items(), key=lambda item: (item[0][1], item[0][0])):
        if len(pair) != 2:
            continue

        a, b = pair[0], pair[1]
        a_elo = player_elo[a.player_id]
        b_elo = player_elo[b.player_id]

        expected_a = 1.0 / (1.0 + math.pow(10.0, (b_elo - a_elo) / 400.0))
        expected_b = 1.0 - expected_a
        pre_elos = {a.player_id: a_elo, b.player_id: b_elo}

        a_state = player_state[a.player_id]
        b_state = player_state[b.player_id]

        for obs, opponent_obs, state, opponent_state, opponent_elo in (
            (a, b, a_state, b_state, b_elo),
            (b, a, b_state, a_state, a_elo),
        ):
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
                "career_matches": state.career_matches,
                "career_win_pct": (state.career_wins / state.career_matches if state.career_matches else None),
                "opponent_career_matches": opponent_state.career_matches,
                "opponent_career_win_pct": (
                    opponent_state.career_wins / opponent_state.career_matches if opponent_state.career_matches else None
                ),
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
            add_rolling_features(row, state, obs.match_date, obs.surface)
            add_rolling_features(row, opponent_state, obs.match_date, obs.surface, prefix="opponent_")
            add_difference_features(row)
            rows.append(row)

        player_elo[a.player_id] = a_elo + ELO_K_FACTOR * (a.is_winner - expected_a)
        player_elo[b.player_id] = b_elo + ELO_K_FACTOR * (b.is_winner - expected_b)

        for obs in (a, b):
            state = player_state[obs.player_id]
            state.career_matches += 1
            state.career_wins += obs.is_winner
            push_history(
                state,
                HistoryEntry(
                    match_date=obs.match_date,
                    surface=obs.surface,
                    is_win=obs.is_winner,
                    elo_pre=pre_elos[obs.player_id],
                    service_points_won_pct=obs.service_points_won_pct,
                    return_points_won_pct=obs.return_points_won_pct,
                ),
            )

    return rows
