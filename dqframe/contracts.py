"""Dataset contracts: one YAML file per dataset describes how to read it, what "good"
looks like, how to load it and how to reconcile it. Onboarding a new feed is a new
contract file, not new pipeline code."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pyspark.sql import types as T

TYPES = {
    "string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(),
    "double": T.DoubleType(), "boolean": T.BooleanType(), "date": T.DateType(),
    "timestamp": T.TimestampType(),
}
LOAD_MODES = {"append", "scd2"}
# Metadata columns added by the reader; contracts may partition or key on them.
META_COLUMNS = {"_batch_id", "_run_date", "_ingested_at", "_source_file"}
SEVERITIES = {"error", "warn"}


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True

    def spark_type(self) -> T.DataType:
        if self.type.startswith("decimal("):
            p, s = self.type[8:-1].split(",")
            return T.DecimalType(int(p), int(s))
        if self.type not in TYPES:
            raise ContractError(f"column {self.name}: unsupported type {self.type!r}")
        return TYPES[self.type]


@dataclass(frozen=True)
class Rule:
    name: str
    type: str
    column: str | None = None
    severity: str = "error"
    params: dict = field(default_factory=dict)
    max_failure_pct: float = 100.0  # batch fails if more than this % of rows fail the rule


@dataclass(frozen=True)
class Contract:
    name: str
    source_format: str
    source_path: str
    source_options: dict
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    target_path: str
    partition_by: tuple[str, ...]
    load_mode: str
    rules: tuple[Rule, ...]
    business_key: tuple[str, ...] = ()
    tracked_columns: tuple[str, ...] = ()
    recon_sums: tuple[str, ...] = ()
    references: dict = field(default_factory=dict)

    def schema(self) -> T.StructType:
        return T.StructType([T.StructField(c.name, c.spark_type(), True) for c in self.columns])


def load(path: str | Path, **substitutions: str) -> Contract:
    """Load a contract, substituting ${var} placeholders (e.g. ${landing}, ${lake}) so
    one contract serves local runs, CI and every AWS environment."""
    text = Path(path).read_text(encoding="utf-8")
    for k, v in substitutions.items():
        text = text.replace("${" + k + "}", v)
    raw = yaml.safe_load(text)
    try:
        c = Contract(
            name=raw["name"],
            source_format=raw["source"]["format"],
            source_path=raw["source"]["path"],
            source_options=raw["source"].get("options", {}),
            columns=tuple(Column(**col) for col in raw["schema"]),
            primary_key=tuple(raw["primary_key"]),
            target_path=raw["target"]["path"],
            partition_by=tuple(raw["target"].get("partition_by", [])),
            load_mode=raw["target"].get("mode", "append"),
            rules=tuple(Rule(**r) for r in raw.get("rules", [])),
            business_key=tuple(raw["target"].get("business_key", [])),
            tracked_columns=tuple(raw["target"].get("tracked_columns", [])),
            recon_sums=tuple(raw.get("reconciliation", {}).get("sums", [])),
            references=raw.get("references", {}),
        )
    except KeyError as exc:
        raise ContractError(f"{path}: missing required key {exc}") from exc
    _validate(c)
    return c


def _validate(c: Contract) -> None:
    names = {col.name for col in c.columns} | META_COLUMNS
    for col in c.columns:
        col.spark_type()
    if c.load_mode not in LOAD_MODES:
        raise ContractError(f"{c.name}: target.mode must be one of {sorted(LOAD_MODES)}")
    for k in (*c.primary_key, *c.partition_by, *c.business_key, *c.tracked_columns, *c.recon_sums):
        if k not in names:
            raise ContractError(f"{c.name}: {k!r} is not a column in the schema")
    if c.load_mode == "scd2" and not (c.business_key and c.tracked_columns):
        raise ContractError(f"{c.name}: scd2 needs target.business_key and target.tracked_columns")
    for r in c.rules:
        if r.severity not in SEVERITIES:
            raise ContractError(f"{c.name}/{r.name}: severity must be error or warn")
        if r.column and r.column not in names:
            raise ContractError(f"{c.name}/{r.name}: unknown column {r.column!r}")
        if r.type == "referential" and r.params.get("reference") not in c.references:
            raise ContractError(f"{c.name}/{r.name}: reference {r.params.get('reference')!r} not declared")
