"""Slowly Changing Dimension type 2 merge in plain Spark (no Delta/Iceberg required).

`incoming` is a delta: keys absent from it are left unchanged (no deletes). Running
the same batch twice is a no-op the second time, because the incoming hash then
equals the current version's hash.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HIGH_DATE = "9999-12-31"
SCD_COLS = ["dim_sk", "valid_from", "valid_to", "is_current", "_row_hash"]


def _hash(df: DataFrame, tracked: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in tracked]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("␟", *parts), 256))


def merge(current: DataFrame | None, incoming: DataFrame, business_key: list[str], tracked: list[str],
          effective_date: str) -> DataFrame:
    eff = F.lit(effective_date).cast("date")
    attrs = [c for c in incoming.columns if c not in SCD_COLS]
    new = _hash(incoming, tracked)

    def open_version(df: DataFrame) -> DataFrame:
        return (df.withColumn("valid_from", eff)
                  .withColumn("valid_to", F.lit(HIGH_DATE).cast("date"))
                  .withColumn("is_current", F.lit(True))
                  .withColumn("dim_sk", F.sha2(F.concat_ws("|", *[F.col(k).cast("string") for k in business_key],
                                                           F.lit(effective_date)), 256))
                  .select(*attrs, *SCD_COLS))

    if current is None:
        return open_version(new)

    history = current.where(~F.col("is_current"))
    live = current.where(F.col("is_current"))
    cmp = (live.select(*business_key, F.col("_row_hash").alias("_old_hash"))
               .join(new.select(*business_key, F.col("_row_hash").alias("_new_hash")), business_key, "full_outer"))
    changed_keys = cmp.where(F.col("_old_hash").isNotNull() & F.col("_new_hash").isNotNull()
                             & (F.col("_old_hash") != F.col("_new_hash"))).select(*business_key)
    new_keys = cmp.where(F.col("_old_hash").isNull()).select(*business_key)

    closed = (live.join(changed_keys, business_key, "left_semi")
                  .withColumn("valid_to", F.date_sub(eff, 1))
                  .withColumn("is_current", F.lit(False)))
    untouched = live.join(changed_keys, business_key, "left_anti")
    inserts = open_version(new.join(changed_keys.unionByName(new_keys), business_key, "left_semi"))
    return (history.unionByName(closed).unionByName(untouched).unionByName(inserts)
                   .select(*attrs, *SCD_COLS))
