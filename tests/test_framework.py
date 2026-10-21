import csv
import json
import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dqframe import contracts, engine, io, pipeline, recon, scd2
from dqframe.contracts import Column, Contract, ContractError, Rule

ROOT = Path(__file__).resolve().parent.parent


def contract(**over) -> Contract:
    base = dict(
        name="t", source_format="csv", source_path="x", source_options={},
        columns=(Column("id", "string", False), Column("amt", "decimal(10,2)"), Column("ccy", "string")),
        primary_key=("id",), target_path="y", partition_by=(), load_mode="append", rules=(),
    )
    return Contract(**{**base, **over})


def frame(spark, rows):
    df = spark.createDataFrame(rows, "id string, amt decimal(10,2), ccy string")
    return df.withColumn("_corrupt_record", F.lit(None).cast("string"))


# -- contracts -------------------------------------------------------------------

def test_shipped_contracts_load():
    for f in (ROOT / "contracts").glob("*.yml"):
        c = contracts.load(f, landing="/l", lake="/k", run_date="2026-10-20")
        assert c.source_path.startswith("/l/") and c.target_path.startswith("/k/")


@pytest.mark.parametrize("over,msg", [
    ({"primary_key": ("nope",)}, "not a column"),
    ({"load_mode": "upsert"}, "target.mode"),
    ({"load_mode": "scd2"}, "business_key"),
    ({"rules": (Rule("r", "in_set", "ccy", severity="fatal"),)}, "severity"),
    ({"rules": (Rule("r", "referential", "ccy", params={"reference": "fx"}),)}, "not declared"),
    ({"columns": (Column("id", "uuid"),)}, "unsupported type"),
])
def test_contract_validation(over, msg):
    with pytest.raises(ContractError, match=msg):
        contracts._validate(contract(**over))


# -- rules & engine --------------------------------------------------------------

def test_implicit_rules_quarantine_nulls_and_duplicate_keys(spark):
    df = frame(spark, [("a", Decimal("1.00"), "GBP"), ("a", Decimal("2.00"), "GBP"),
                       (None, Decimal("3.00"), "GBP"), ("  ", Decimal("4.00"), "GBP"), ("b", None, "GBP")])
    res = engine.apply(df, contract())
    m = {r["rule"]: r["failed"] for r in res.rule_metrics}
    assert m["id_not_null"] == 2 and m["primary_key_unique"] == 2
    assert res.valid.count() == 1 and res.quarantine.count() == 4  # nullable amt is fine


def test_error_vs_warn_and_thresholds(spark):
    rules = (Rule("ccy_ok", "in_set", "ccy", params={"values": ["GBP"]}, max_failure_pct=30),
             Rule("small", "range", "amt", severity="warn", params={"max": 100}))
    df = frame(spark, [("1", Decimal("5"), "GBP"), ("2", Decimal("500"), "GBP"), ("3", Decimal("5"), "XXX")])
    res = engine.apply(df, contract(rules=rules))
    assert res.valid.count() == 2  # warn-only row is kept
    assert res.passed is False  # 33% > 30% threshold
    assert [b["rule"] for b in res.breaches] == ["ccy_ok"]
    q = res.quarantine.select("id", "_dq_errors").first()
    assert q["id"] == "3" and q["_dq_errors"] == ["ccy_ok"]


@pytest.mark.parametrize("rule,bad_ids", [
    (Rule("re", "regex", "ccy", params={"pattern": "^[A-Z]{3}$"}), {"3"}),
    (Rule("ex", "expression", params={"expr": "amt >= 0"}), {"2"}),
    (Rule("rg", "range", "amt", params={"min": 0, "max": 10}), {"2"}),
])
def test_row_rules(spark, rule, bad_ids):
    df = frame(spark, [("1", Decimal("5"), "GBP"), ("2", Decimal("-5"), "GBP"), ("3", Decimal("5"), "gb")])
    res = engine.apply(df, contract(rules=(rule,)))
    assert {r["id"] for r in res.quarantine.collect()} == bad_ids


def test_referential_rule(spark):
    ref = spark.createDataFrame([("GBP",), ("USD",)], "code string")
    c = contract(references={"fx": {"key": "code"}},
                 rules=(Rule("ccy_known", "referential", "ccy", params={"reference": "fx"}),))
    df = frame(spark, [("1", Decimal("1"), "GBP"), ("2", Decimal("1"), "JPY"), ("3", Decimal("1"), None)])
    res = engine.apply(df, c, {"fx": ref})
    assert {r["id"] for r in res.quarantine.collect()} == {"2"}  # null is not a broken reference


def test_dataset_rules(spark):
    rules = (Rule("vol", "min_rows", params={"min": 5}),)
    res = engine.apply(frame(spark, [("1", Decimal("1"), "GBP")]), contract(rules=rules))
    assert not res.passed and res.dataset_metrics[0]["observed"] == 1


# -- reconciliation --------------------------------------------------------------

def test_recon_classifies_differences(spark):
    src = spark.createDataFrame([("1", 10), ("2", 20), ("3", 30)], "id string, v int")
    tgt = spark.createDataFrame([("1", 10), ("2", 21), ("4", 40)], "id string, v int")
    r = recon.reconcile(src, tgt, ["id"], ["v"], ["v"])
    assert (r["matched"], r["value_mismatch"], r["missing_in_target"], r["unexpected_in_target"]) == (1, 1, 1, 1)
    assert not r["passed"] and r["totals"]["v"] == {"source": 60, "target": 71, "match": False}


def test_recon_is_null_safe(spark):
    a = spark.createDataFrame([("1", None)], "id string, v string")
    b = spark.createDataFrame([("1", "")], "id string, v string")
    assert recon.reconcile(a, b, ["id"], ["v"])["value_mismatch"] == 1
    assert recon.reconcile(a, a, ["id"], ["v"])["passed"]


# -- scd2 ------------------------------------------------------------------------

def test_scd2_lifecycle(spark):
    schema = "k string, name string, tier string"
    d1 = scd2.merge(None, spark.createDataFrame([("a", "Acme", "1"), ("b", "Bolt", "2")], schema), ["k"], ["name", "tier"], "2026-10-01")
    assert d1.where("is_current").count() == 2

    d2 = scd2.merge(d1, spark.createDataFrame([("a", "Acme", "3"), ("c", "Cog", "1")], schema), ["k"], ["name", "tier"], "2026-10-02")
    rows = {(r.k, r.tier): r for r in d2.collect()}
    assert rows[("a", "1")].is_current is False and str(rows[("a", "1")].valid_to) == "2026-10-01"
    assert rows[("a", "3")].is_current and str(rows[("a", "3")].valid_from) == "2026-10-02"
    assert rows[("b", "2")].is_current  # absent from the delta: untouched
    assert rows[("c", "1")].is_current
    assert d2.count() == 4 and d2.select("dim_sk").distinct().count() == 4

    again = scd2.merge(d2, spark.createDataFrame([("a", "Acme", "3")], schema), ["k"], ["name", "tier"], "2026-10-02")
    assert sorted(map(tuple, again.collect())) == sorted(map(tuple, d2.collect()))  # idempotent


def test_snapshot_pointer(spark, tmp_path):
    base = str(tmp_path / "dim")
    assert io.read_snapshot(spark, base) is None
    io.write_snapshot(spark, spark.createDataFrame([(1,)], "x int"), base, "v1")
    io.write_snapshot(spark, spark.createDataFrame([(1,), (2,)], "x int"), base, "v2")
    assert io.read_snapshot(spark, base).count() == 2


# -- end to end ------------------------------------------------------------------

@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    out = tmp_path_factory.mktemp("landing")
    d1, d2 = (date.today() - timedelta(days=2)).isoformat(), (date.today() - timedelta(days=1)).isoformat()
    subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_data.py"), "--out", str(out), "--dates", d1, d2,
                    "--merchants", "200", "--txns", "3000"], check=True, capture_output=True)
    return out, d1, d2


def run_day(spark, lake, landing_dir, day, txn_contract=None):
    subs = {"landing": str(landing_dir), "lake": str(lake), "run_date": day}
    m = contracts.load(ROOT / "contracts" / "merchants.yml", **subs)
    t = txn_contract or contracts.load(ROOT / "contracts" / "card_transactions.yml", **subs)
    pipeline.run(spark, m, day, f"m-{day}", audit_path=f"{lake}/_audit")
    refs = {"merchants": io.read_snapshot(spark, m.target_path).where("is_current")}
    return pipeline.run(spark, t, day, f"t-{day}", refs, audit_path=f"{lake}/_audit")


def merchant_file(landing_dir, day):
    with open(landing_dir / "merchants" / day / "part-0000.csv", newline="", encoding="utf-8") as fh:
        return {r["merchant_id"]: r for r in csv.DictReader(fh)}


def expected_quarantine(landing_dir, day):
    d = json.loads((landing_dir / "card_transactions" / day / "_defects.json").read_text(encoding="utf-8"))
    # odd_channel is warn-only; each duplicate quarantines both copies.
    return sum(v for k, v in d.items() if k not in ("odd_channel", "duplicate_txn")) + 2 * d["duplicate_txn"]


def test_end_to_end_two_days_and_rerun(spark, landing, tmp_path):
    landing_dir, d1, d2 = landing
    lake = tmp_path / "lake"
    r1 = run_day(spark, lake, landing_dir, d1)
    r2 = run_day(spark, lake, landing_dir, d2)
    assert r1.rows_quarantined == expected_quarantine(landing_dir, d1)
    assert r2.rows_quarantined == expected_quarantine(landing_dir, d2)
    for r in (r1, r2):
        assert r.status == "SUCCEEDED" and r.audit["reconciliation"]["passed"]

    run_day(spark, lake, landing_dir, d2)  # re-run of the same business date
    txns = spark.read.parquet(str(lake / "card_transactions"))
    assert txns.count() == r1.rows_valid + r2.rows_valid
    assert txns.select("txn_id").distinct().count() == txns.count()
    dim = io.read_snapshot(spark, str(lake / "merchants"))
    day1, day2 = merchant_file(landing_dir, d1), merchant_file(landing_dir, d2)
    really_changed = [k for k, row in day2.items() if k in day1 and row != day1[k]]
    assert dim.where("is_current").count() == len(day1.keys() | day2.keys())
    assert dim.where("not is_current").count() == len(really_changed) > 0


def test_breached_threshold_fails_batch_but_keeps_evidence(spark, landing, tmp_path):
    landing_dir, d1, _ = landing
    lake = tmp_path / "lake"
    subs = {"landing": str(landing_dir), "lake": str(lake), "run_date": d1}
    strict = contracts.load(ROOT / "contracts" / "card_transactions.yml", **subs)
    strict = Contract(**{**strict.__dict__, "rules": tuple(
        Rule(**{**r.__dict__, "max_failure_pct": 0.1}) if r.name == "merchant_known" else r for r in strict.rules)})
    with pytest.raises(pipeline.DQFailure, match="merchant_known"):
        run_day(spark, lake, landing_dir, d1, strict)
    assert not (lake / "card_transactions").exists()  # nothing loaded
    assert spark.read.parquet(str(lake / "card_transactions_quarantine")).count() > 0
    audit = [json.loads(r.record) for r in spark.read.json(str(lake / "_audit")).collect()]
    assert {a["contract"]: a["status"] for a in audit}["card_transactions"] == "FAILED"


def test_snapshot_rewrite_with_same_version_and_retention(spark, tmp_path):
    base = str(tmp_path / "dim")
    io.write_snapshot(spark, spark.createDataFrame([(1,)], "x int"), base, "same", keep=2)
    for n in range(2, 5):  # read current and write a new snapshot under the same batch id
        cur = io.read_snapshot(spark, base)
        io.write_snapshot(spark, cur.unionByName(spark.createDataFrame([(n,)], "x int")), base, "same", keep=2)
    assert sorted(r.x for r in io.read_snapshot(spark, base).collect()) == [1, 2, 3, 4]
    assert len([p for p in (tmp_path / "dim").iterdir() if p.name.startswith("_v=")]) == 2
