#!/usr/bin/env python3
"""Aggregate per-set ATP stats into Set[0] match-level totals.

This script reads raw yearly ATP CSV files from ./data, aggregates per-set
statistics for each player into `Sets[0]`, clears `Sets[1..]` stat columns to
avoid double counting, and writes versioned cleaned outputs to
`data/processed/atp_YYYY_clean.csv`.

A data-quality report is also written with row counts and corrected match
counts per input file.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

STATS_COL_RE = re.compile(r"^(PlayerTeam[12])\.Sets\[(\d+)\]\.Stats\.(.+)$")
YEAR_RE = re.compile(r"atp_(\d{4})\.csv$")


@dataclass(frozen=True)
class MetricColumn:
    team: str
    set_index: int
    suffix: str
    column_name: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing source atp_YYYY.csv files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory where cleaned files are written.",
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        default=Path("data/processed/cleaning_report.json"),
        help="Path for the data-quality report JSON.",
    )
    parser.add_argument(
        "--years",
        nargs="*",
        help="Optional explicit list of years to process (e.g. 2024 2025).",
    )
    return parser.parse_args()


def parse_float(raw: str) -> Optional[float]:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def format_number(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def parse_time_to_seconds(raw: str) -> Optional[int]:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    parts = text.split(":")
    if not all(part.isdigit() for part in parts):
        return None

    if len(parts) == 3:
        hh, mm, ss = map(int, parts)
        return hh * 3600 + mm * 60 + ss
    if len(parts) == 2:
        mm, ss = map(int, parts)
        return mm * 60 + ss
    if len(parts) == 1:
        return int(parts[0])
    return None


def format_seconds_to_time(total_seconds: int) -> str:
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def detect_metric_columns(fieldnames: Sequence[str]) -> Dict[Tuple[str, str], Dict[int, str]]:
    mapping: Dict[Tuple[str, str], Dict[int, str]] = {}
    for col in fieldnames:
        m = STATS_COL_RE.match(col)
        if not m:
            continue
        team, set_idx_raw, suffix = m.groups()
        set_idx = int(set_idx_raw)
        key = (team, suffix)
        mapping.setdefault(key, {})[set_idx] = col
    return mapping


def ratio_bases(metric_map: Dict[Tuple[str, str], Dict[int, str]], team: str) -> Set[str]:
    bases: Set[str] = set()
    suffixes = {suffix for t, suffix in metric_map.keys() if t == team}
    for suffix in suffixes:
        if suffix.endswith(".Percent"):
            base = suffix[: -len(".Percent")]
            if f"{base}.Dividend" in suffixes and f"{base}.Divisor" in suffixes:
                bases.add(base)
    return bases


def get_non_empty_sets(
    row: Dict[str, str],
    metric_map: Dict[Tuple[str, str], Dict[int, str]],
    team: str,
) -> Set[int]:
    non_empty: Set[int] = set()
    team_entries = [v for (t, _), v in metric_map.items() if t == team]
    for set_map in team_entries:
        for set_idx, col in set_map.items():
            if row.get(col, "").strip() != "":
                non_empty.add(set_idx)
    return non_empty


def aggregate_row(row: Dict[str, str], metric_map: Dict[Tuple[str, str], Dict[int, str]]) -> bool:
    row_changed = False

    for team in ("PlayerTeam1", "PlayerTeam2"):
        non_empty_sets = get_non_empty_sets(row, metric_map, team)
        if not non_empty_sets:
            continue

        ratio_base_set = ratio_bases(metric_map, team)
        ratio_related_suffixes = {
            f"{base}.Percent" for base in ratio_base_set
        } | {f"{base}.Dividend" for base in ratio_base_set} | {
            f"{base}.Divisor" for base in ratio_base_set
        }

        team_suffixes = [suffix for (t, suffix) in metric_map.keys() if t == team]

        # Aggregate additive metrics (excluding ratio-related fields and time).
        for suffix in team_suffixes:
            if suffix in ratio_related_suffixes or suffix == "Time":
                continue

            set_cols = metric_map[(team, suffix)]
            set0_col = set_cols.get(0)
            if not set0_col:
                continue

            total = 0.0
            found_any = False
            for set_idx in sorted(non_empty_sets):
                col = set_cols.get(set_idx)
                if not col:
                    continue
                parsed = parse_float(row.get(col, ""))
                if parsed is None:
                    continue
                total += parsed
                found_any = True

            if found_any:
                new_value = format_number(total)
                if row.get(set0_col, "") != new_value:
                    row[set0_col] = new_value
                    row_changed = True

        # Aggregate time metric.
        time_cols = metric_map.get((team, "Time"), {})
        if time_cols and 0 in time_cols:
            total_seconds = 0
            found_time = False
            for set_idx in sorted(non_empty_sets):
                col = time_cols.get(set_idx)
                if not col:
                    continue
                parsed_sec = parse_time_to_seconds(row.get(col, ""))
                if parsed_sec is None:
                    continue
                total_seconds += parsed_sec
                found_time = True

            if found_time:
                new_time = format_seconds_to_time(total_seconds)
                if row.get(time_cols[0], "") != new_time:
                    row[time_cols[0]] = new_time
                    row_changed = True

        # Recompute ratio metrics from aggregated numerator/denominator.
        for base in ratio_base_set:
            dividend_suffix = f"{base}.Dividend"
            divisor_suffix = f"{base}.Divisor"
            percent_suffix = f"{base}.Percent"

            dividend_cols = metric_map[(team, dividend_suffix)]
            divisor_cols = metric_map[(team, divisor_suffix)]
            percent_cols = metric_map[(team, percent_suffix)]

            if 0 not in dividend_cols or 0 not in divisor_cols or 0 not in percent_cols:
                continue

            total_dividend = 0.0
            total_divisor = 0.0
            has_dividend = False
            has_divisor = False

            for set_idx in sorted(non_empty_sets):
                d_col = dividend_cols.get(set_idx)
                n_col = divisor_cols.get(set_idx)

                if d_col:
                    d_val = parse_float(row.get(d_col, ""))
                    if d_val is not None:
                        total_dividend += d_val
                        has_dividend = True

                if n_col:
                    n_val = parse_float(row.get(n_col, ""))
                    if n_val is not None:
                        total_divisor += n_val
                        has_divisor = True

            if has_dividend:
                new_dividend = format_number(total_dividend)
                if row.get(dividend_cols[0], "") != new_dividend:
                    row[dividend_cols[0]] = new_dividend
                    row_changed = True

            if has_divisor:
                new_divisor = format_number(total_divisor)
                if row.get(divisor_cols[0], "") != new_divisor:
                    row[divisor_cols[0]] = new_divisor
                    row_changed = True

            if has_dividend and has_divisor and total_divisor > 0:
                percent = (total_dividend / total_divisor) * 100.0
                new_percent = format_number(round(percent, 6))
            elif has_dividend and has_divisor:
                new_percent = ""
            else:
                new_percent = row.get(percent_cols[0], "")

            if row.get(percent_cols[0], "") != new_percent:
                row[percent_cols[0]] = new_percent
                row_changed = True

        # Clear per-set stats after set 0 to avoid double counting.
        for suffix in team_suffixes:
            set_cols = metric_map[(team, suffix)]
            for set_idx, col in set_cols.items():
                if set_idx == 0:
                    continue
                if row.get(col, "") != "":
                    row[col] = ""
                    row_changed = True

    return row_changed


def collect_input_files(input_dir: Path, years: Optional[Sequence[str]]) -> List[Path]:
    if years:
        files = [input_dir / f"atp_{year}.csv" for year in years]
        return [f for f in files if f.exists()]

    return sorted(input_dir.glob("atp_*.csv"))


def output_path_for(input_file: Path, output_dir: Path) -> Path:
    m = YEAR_RE.search(input_file.name)
    year = m.group(1) if m else datetime.now(timezone.utc).strftime("%Y")
    return output_dir / f"atp_{year}_clean.csv"


def process_file(input_file: Path, output_file: Path) -> Dict[str, object]:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with input_file.open("r", newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        if reader.fieldnames is None:
            raise ValueError(f"File has no header: {input_file}")
        fieldnames = reader.fieldnames
        metric_map = detect_metric_columns(fieldnames)

        row_count = 0
        corrected_matches = 0

        with output_file.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                row_count += 1
                changed = aggregate_row(row, metric_map)
                if changed:
                    corrected_matches += 1
                writer.writerow(row)

    return {
        "source_file": str(input_file),
        "output_file": str(output_file),
        "row_count": row_count,
        "corrected_matches": corrected_matches,
    }


def main() -> int:
    args = parse_args()
    files = collect_input_files(args.input_dir, args.years)
    if not files:
        raise SystemExit("No input files found to process.")

    report_entries: List[Dict[str, object]] = []

    for input_file in files:
        out_file = output_path_for(input_file, args.output_dir)
        entry = process_file(input_file, out_file)
        report_entries.append(entry)
        print(
            f"Processed {input_file.name}: rows={entry['row_count']}, "
            f"corrected={entry['corrected_matches']} -> {out_file}"
        )

    total_rows = sum(int(e["row_count"]) for e in report_entries)
    total_corrected = sum(int(e["corrected_matches"]) for e in report_entries)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_dir": str(args.input_dir),
        "output_dir": str(args.output_dir),
        "totals": {
            "files_processed": len(report_entries),
            "row_count": total_rows,
            "corrected_matches": total_corrected,
        },
        "files": report_entries,
    }

    args.report_file.parent.mkdir(parents=True, exist_ok=True)
    with args.report_file.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote data-quality report: {args.report_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
