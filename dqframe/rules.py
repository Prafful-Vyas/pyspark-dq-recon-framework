"""Data-quality rules.

Row-level rules become boolean "this row fails" columns, all evaluated in a single
pass over the data. Dataset-level rules (volume, freshness) are evaluated over
aggregates afterwards.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .contracts import Contract, Rule
from .io import CORRUPT

ROW_RULES = {"not_null", "unique", "in_set", "range", "regex", "expression", "referential", "parse", "not_future"}
DATASET_RULES = {"min_rows", "freshness"}


def implicit_rules(c: Contract) -> list[Rule]:
    """Rules every contract gets: unparseable rows, non-nullable columns and the
    primary key are always enforced, so a contract can't forget them."""
    rules = [Rule("parse_error", "parse")]
    rules += [Rule(f"{col.name}_not_null", "not_null", col.name) for col in c.columns if not col.nullable]
    rules.append(Rule("primary_key_unique", "unique", params={"columns": list(c.primary_key)}))
    return rules


def failure_expr(rule: Rule, df: DataFrame) -> SparkColumn:
    col = F.col(rule.column) if rule.column else None
    p = rule.params
    t = rule.type
    if t == "parse":
        return F.col(CORRUPT).isNotNull()
    if t == "not_null":
        dtype = dict(df.dtypes)[rule.column]
        return col.isNull() | (F.trim(col) == "") if dtype == "string" else col.isNull()
    if t == "unique":
        cols = p.get("columns") or [rule.column]
        # Null keys are a not_null problem, not a duplicate.
        all_present = reduce(lambda a, b: a & b, [F.col(k).isNotNull() for k in cols])
        return (F.count(F.lit(1)).over(Window.partitionBy(*cols)) > 1) & all_present
    if t == "in_set":
        return col.isNotNull() & ~col.isin(p["values"])
    if t == "range":
        cond = F.lit(False)
        if "min" in p:
            cond = cond | (col < F.lit(p["min"]))
        if "max" in p:
            cond = cond | (col > F.lit(p["max"]))
        return col.isNotNull() & cond
    if t == "regex":
        return col.isNotNull() & ~col.rlike(p["pattern"])
    if t == "expression":
        return ~F.coalesce(F.expr(p["expr"]), F.lit(False))
    if t == "not_future":
        slack = int(p.get("tolerance_minutes", 0))
        return col.isNotNull() & (col > F.current_timestamp() + F.expr(f"INTERVAL {slack} MINUTES"))
    if t == "referential":
        return F.col(f"_ref_missing_{rule.name}")
    raise ValueError(f"unknown rule type {t!r}")


def attach_reference_flags(df: DataFrame, rules: list[Rule], refs: dict[str, DataFrame], c: Contract) -> DataFrame:
    """Referential rules need a join; do it once per rule and leave a flag column."""
    for r in rules:
        if r.type != "referential":
            continue
        ref_name = r.params["reference"]
        ref_col = c.references[ref_name]["key"]
        keys = (refs[ref_name].select(F.col(ref_col).alias("_ref_key")).where(F.col("_ref_key").isNotNull())
                .distinct().withColumn("_ref_hit", F.lit(True)))
        df = (df.join(F.broadcast(keys), F.col(r.column) == F.col("_ref_key"), "left")
                .withColumn(f"_ref_missing_{r.name}", F.col(r.column).isNotNull() & F.col("_ref_hit").isNull())
                .drop("_ref_key", "_ref_hit"))
    return df


def dataset_checks(rules: list[Rule], df: DataFrame) -> list[dict]:
    out = []
    for r in rules:
        if r.type == "min_rows":
            n = df.count()
            out.append({"rule": r.name, "severity": r.severity, "passed": n >= r.params["min"],
                        "observed": n, "expected": f">= {r.params['min']}"})
        elif r.type == "freshness":
            max_age = int(r.params["max_age_hours"])
            age_hours = df.agg(
                ((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max(r.column))) / 3600).alias("age")
            ).first()["age"]
            out.append({"rule": r.name, "severity": r.severity,
                        "passed": age_hours is not None and age_hours <= max_age,
                        "observed": None if age_hours is None else round(age_hours, 2),
                        "expected": f"<= {max_age}h"})
    return out
