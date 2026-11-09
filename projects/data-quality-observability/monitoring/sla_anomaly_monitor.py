"""Simulate data-freshness SLA alerting and daily-volume anomaly detection."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from statistics import median
from typing import Any


def read_order_count(path: str) -> int:
    with Path(path).open(newline="", encoding="utf-8") as source:
        return sum(1 for _ in csv.DictReader(source))


def read_volume_history(path: str) -> list[int]:
    with Path(path).open(newline="", encoding="utf-8") as source:
        counts = [int(row["order_count"]) for row in csv.DictReader(source)]
    if not counts:
        raise ValueError("Volume history must contain at least one order_count")
    return counts


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=str(project_root / "dbt" / "seeds" / "orders.csv"),
        help="Current orders CSV used to measure volume",
    )
    parser.add_argument(
        "--history",
        default=str(project_root / "data" / "historical_volume.csv"),
        help="CSV with historical order_count values",
    )
    parser.add_argument("--simulate-lag-minutes", type=float, default=5.0)
    parser.add_argument("--sla-minutes", type=float, default=30.0)
    parser.add_argument(
        "--volume-override",
        type=int,
        help="Override the current row count to simulate volume anomalies",
    )
    parser.add_argument("--fail-on-alert", action="store_true")
    args = parser.parse_args()
    if args.simulate_lag_minutes < 0 or args.sla_minutes < 0:
        parser.error("lag and SLA values cannot be negative")
    if args.volume_override is not None and args.volume_override < 0:
        parser.error("--volume-override cannot be negative")
    return args


def evaluate(current_volume: int, history: list[int], lag: float, sla: float) -> dict[str, Any]:
    baseline = median(history)
    deviations = [abs(count - baseline) for count in history]
    mad = median(deviations)
    alerts: list[dict[str, Any]] = []

    if lag > sla:
        alerts.append(
            {
                "type": "freshness_sla_breach",
                "lag_minutes": lag,
                "sla_minutes": sla,
            }
        )

    if mad == 0:
        volume_anomaly = current_volume != baseline
        robust_z_score = None
    else:
        robust_z_score = round(0.6745 * (current_volume - baseline) / mad, 3)
        volume_anomaly = abs(robust_z_score) > 3.5

    if volume_anomaly:
        alerts.append(
            {
                "type": "volume_anomaly",
                "current_volume": current_volume,
                "baseline_median": baseline,
                "baseline_mad": mad,
                "robust_z_score": robust_z_score,
            }
        )

    return {
        "status": "alert" if alerts else "healthy",
        "current_volume": current_volume,
        "baseline_median": baseline,
        "baseline_mad": mad,
        "simulated_lag_minutes": lag,
        "sla_minutes": sla,
        "alerts": alerts,
    }


def main() -> None:
    args = parse_args()
    volume = (
        args.volume_override
        if args.volume_override is not None
        else read_order_count(args.input)
    )
    result = evaluate(
        current_volume=volume,
        history=read_volume_history(args.history),
        lag=args.simulate_lag_minutes,
        sla=args.sla_minutes,
    )
    print(json.dumps(result, indent=2))
    if args.fail_on_alert and result["alerts"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
