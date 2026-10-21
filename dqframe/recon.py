"""Source-to-target reconciliation: prove that what landed in the target is exactly
the valid source rows — same count, same totals, same values, key by key."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _row_hash(df: DataFrame, cols: list[str]) -> DataFrame:
    # Null-safe and type-stable: every value is cast to string with an explicit null
    # marker, so (NULL, "a") and ("", "a") hash differently.
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in cols]
    return df.withColumn("_h", F.sha2(F.concat_ws("␟", *parts), 256))


def reconcile(source: DataFrame, target: DataFrame, keys: list[str], compare: list[str],
              sums: list[str] | None = None, sample: int = 20) -> dict:
    sums = sums or []
    s = _row_hash(source.select(*keys, *compare), compare).select(*keys, F.col("_h").alias("_hs"))
    t = _row_hash(target.select(*keys, *compare), compare).select(*keys, F.col("_h").alias("_ht"))
    joined = s.join(t, keys, "full_outer").withColumn(
        "_status",
        F.when(F.col("_ht").isNull(), "missing_in_target")
         .when(F.col("_hs").isNull(), "unexpected_in_target")
         .when(F.col("_hs") != F.col("_ht"), "value_mismatch")
         .otherwise("match")).cache()
    counts = {r["_status"]: r["n"] for r in joined.groupBy("_status").agg(F.count(F.lit(1)).alias("n")).collect()}
    diffs = joined.where("_status != 'match'").select(*keys, "_status").limit(sample).collect()

    src_tot = source.agg(F.count(F.lit(1)).alias("rows"), *[F.sum(c).alias(c) for c in sums]).first().asDict()
    tgt_tot = target.agg(F.count(F.lit(1)).alias("rows"), *[F.sum(c).alias(c) for c in sums]).first().asDict()
    totals = {k: {"source": _num(src_tot[k]), "target": _num(tgt_tot[k]),
                  "match": src_tot[k] == tgt_tot[k]} for k in ["rows", *sums]}
    joined.unpersist()
    mismatches = sum(v for k, v in counts.items() if k != "match")
    return {
        "matched": counts.get("match", 0),
        "missing_in_target": counts.get("missing_in_target", 0),
        "unexpected_in_target": counts.get("unexpected_in_target", 0),
        "value_mismatch": counts.get("value_mismatch", 0),
        "totals": totals,
        "passed": mismatches == 0 and all(t["match"] for t in totals.values()),
        "sample": [r.asDict() for r in diffs],
    }


def _num(v):
    return str(v) if isinstance(v, Decimal) else v
