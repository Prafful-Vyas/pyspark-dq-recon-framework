"""Apply a contract's rules to a batch, splitting it into valid and quarantined rows."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .contracts import Contract, Rule
from .rules import DATASET_RULES, ROW_RULES, attach_reference_flags, dataset_checks, failure_expr, implicit_rules


@dataclass
class DQResult:
    valid: DataFrame
    quarantine: DataFrame
    total: int
    rule_metrics: list[dict]
    dataset_metrics: list[dict]

    @property
    def breaches(self) -> list[dict]:
        """Error-severity rules whose failure rate exceeded their threshold, plus
        failed error-severity dataset checks: any breach fails the batch."""
        rows = [m for m in self.rule_metrics if m["severity"] == "error" and m["breached"]]
        return rows + [m for m in self.dataset_metrics if m["severity"] == "error" and not m["passed"]]

    @property
    def passed(self) -> bool:
        return not self.breaches


def apply(df: DataFrame, c: Contract, refs: dict[str, DataFrame] | None = None) -> DQResult:
    rules = implicit_rules(c) + list(c.rules)
    row_rules = [r for r in rules if r.type in ROW_RULES]
    unknown = [r.name for r in rules if r.type not in ROW_RULES | DATASET_RULES]
    if unknown:
        raise ValueError(f"unknown rule types in {c.name}: {unknown}")

    df = attach_reference_flags(df, row_rules, refs or {}, c)
    for r in row_rules:
        df = df.withColumn(_flag(r), F.coalesce(failure_expr(r, df), F.lit(False)))

    def names(severity: str):
        hits = [F.when(F.col(_flag(r)), F.lit(r.name)) for r in row_rules if r.severity == severity]
        return F.filter(F.array(*hits), lambda x: x.isNotNull()) if hits else F.array().cast("array<string>")

    df = df.withColumn("_dq_errors", names("error")).withColumn("_dq_warnings", names("warn")).cache()

    # One aggregation for the row count and every rule's failure count.
    agg = df.agg(F.count(F.lit(1)).alias("_total"),
                 *[F.sum(F.col(_flag(r)).cast("long")).alias(r.name) for r in row_rules]).first()
    total = agg["_total"]
    metrics = []
    for r in row_rules:
        failed = agg[r.name] or 0
        pct = round(100.0 * failed / total, 4) if total else 0.0
        metrics.append({"rule": r.name, "type": r.type, "column": r.column, "severity": r.severity,
                        "failed": failed, "failed_pct": pct, "max_failure_pct": r.max_failure_pct,
                        "breached": pct > r.max_failure_pct})

    flag_cols = [_flag(r) for r in row_rules] + [col for col in df.columns if col.startswith("_ref_missing_")]
    valid = df.where(F.size("_dq_errors") == 0).drop(*flag_cols, "_dq_errors", "_corrupt_record")
    quarantine = df.where(F.size("_dq_errors") > 0).drop(*flag_cols)
    ds = dataset_checks([r for r in rules if r.type in DATASET_RULES], valid)
    return DQResult(valid, quarantine, total, metrics, ds)


def _flag(r: Rule) -> str:
    return f"_dqf_{r.name}"
