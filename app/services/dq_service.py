from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


@dataclass
class DQRule:
    name: str
    layer: str
    threshold_label: str
    evaluator: Callable[[list[dict]], tuple[bool, float]]


def _validity_rate(records: list[dict]) -> tuple[bool, float]:
    total = len(records)
    valid = sum(1 for r in records if r.get("is_valid"))
    return (total > 0, valid / total if total else 0.0)


def _null_close_rate(records: list[dict]) -> tuple[bool, float]:
    total = len(records)
    null_close = sum(1 for r in records if r.get("close_price") is None)
    return (total > 0, null_close / total if total else 0.0)


def evaluate_rules(
    records: list[dict],
    table: str,
    rules: list[DQRule],
    predicates: dict[str, Callable[[float], bool]],
) -> list[dict]:
    if not records:
        return []

    now = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for rule in rules:
        has_data, value = rule.evaluator(records)
        checker = predicates[rule.name]
        passed = checker(value) if has_data else False

        results.append(
            {
                "check_name": rule.name,
                "layer": rule.layer,
                "table": table,
                "passed": passed,
                "actual": round(value, 4),
                "threshold": rule.threshold_label,
                "checked_at": now,
            }
        )

    return results


def bronze_default_rules() -> list[DQRule]:
    return [
        DQRule(
            name="validity_rate",
            layer="bronze",
            threshold_label="configurable",
            evaluator=_validity_rate,
        ),
        DQRule(
            name="null_close_price_rate",
            layer="bronze",
            threshold_label="configurable",
            evaluator=_null_close_rate,
        ),
    ]
