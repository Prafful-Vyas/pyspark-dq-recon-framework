"""Generate landing files for two business dates, with realistic defects.

    python scripts/gen_data.py --out data/landing --dates 2026-10-19 2026-10-20

Merchants: day 1 is a full load. Later days are deltas that rename some merchants,
re-rate their risk, and add new merchants (this exercises SCD2).
Transactions: ~`--txns` rows per day. Defects are injected at fixed rates and the
counts are written to `_defects.json`, so tests can assert exact quarantine numbers.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

MCCS = ["5411", "5812", "5999", "4111", "5732", "7011", "5541", "6011", "5311", "4812"]
COUNTRIES = ["GB", "US", "IN", "DE", "FR", "AE", "SG"]
WORDS = ["Corner", "Metro", "Green", "Royal", "Prime", "City", "Blue", "North", "Star", "Urban"]
KINDS = ["Grocers", "Cafe", "Fuel", "Electronics", "Hotel", "Pharmacy", "Books", "Travel"]


def merchants(rng: random.Random, n: int, start: int = 1) -> list[dict]:
    return [{"merchant_id": f"M{start + i:06d}",
             "merchant_name": f"{rng.choice(WORDS)} {rng.choice(KINDS)} {start + i}",
             "mcc": rng.choice(MCCS), "country": rng.choice(COUNTRIES),
             "risk_rating": rng.choice(["LOW", "LOW", "LOW", "MEDIUM", "HIGH"])} for i in range(n)]


def card(rng: random.Random) -> str:
    return f"{rng.choice(['453201', '520082', '601100', '371449'])}******{rng.randint(0, 9999):04d}"


def write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--dates", nargs="+", required=True)
    ap.add_argument("--merchants", type=int, default=500)
    ap.add_argument("--txns", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    out = Path(args.out)

    all_merchants = merchants(rng, args.merchants)
    next_id = args.merchants + 1
    txn_seq = 0
    for day_no, date in enumerate(args.dates):
        if day_no == 0:
            delta = all_merchants
        else:
            changed = rng.sample(all_merchants, max(1, len(all_merchants) // 20))
            for m in changed:
                m["risk_rating"] = rng.choice(["MEDIUM", "HIGH"])
                if rng.random() < 0.5:
                    m["merchant_name"] += " Ltd"
            added = merchants(rng, max(1, len(all_merchants) // 50), next_id)
            next_id += len(added)
            all_merchants += added
            delta = [dict(m) for m in changed] + added
        write(out / "merchants" / date / "part-0000.csv", delta)

        base = datetime.fromisoformat(date)
        defects = {"negative_amount": 0, "bad_currency": 0, "unmasked_card": 0, "unknown_merchant": 0,
                   "null_card": 0, "duplicate_txn": 0, "malformed_amount": 0, "odd_channel": 0}
        rows: list[dict] = []
        for _ in range(args.txns):
            txn_seq += 1
            row = {"txn_id": f"T{txn_seq:010d}", "card_id": card(rng),
                   "merchant_id": rng.choice(all_merchants)["merchant_id"],
                   "txn_ts": (base + timedelta(seconds=rng.randint(0, 86399))).strftime("%Y-%m-%dT%H:%M:%S"),
                   "amount": f"{rng.lognormvariate(3.5, 1.1):.2f}", "currency": rng.choice(["GBP", "GBP", "USD", "EUR", "INR"]),
                   "channel": rng.choice(["POS", "ECOM", "CONTACTLESS", "ATM"]),
                   "auth_status": rng.choice(["APPROVED"] * 8 + ["DECLINED", "REVERSED"])}
            r = rng.random()
            # Each defect alone makes the row fail exactly one error rule (except odd_channel: warn only).
            if r < 0.004:
                row["amount"] = f"-{row['amount']}"
                defects["negative_amount"] += 1
            elif r < 0.007:
                row["currency"] = "XYZ"
                defects["bad_currency"] += 1
            elif r < 0.009:
                row["card_id"] = f"4532019{rng.randint(10**8, 10**9 - 1)}"
                defects["unmasked_card"] += 1
            elif r < 0.019:
                row["merchant_id"] = f"M9{rng.randint(10**5, 10**6 - 1)}"
                defects["unknown_merchant"] += 1
            elif r < 0.021:
                row["card_id"] = ""
                defects["null_card"] += 1
            elif r < 0.023:
                row["amount"] = "12,3x"
                defects["malformed_amount"] += 1
            elif r < 0.03:
                row["channel"] = "MOTO"
                defects["odd_channel"] += 1
            rows.append(row)
        # Resent records from the switch: exact duplicates of existing txn_ids (both copies fail uniqueness).
        for dup in rng.sample(rows[: len(rows) // 2], 15):
            rows.append(dict(dup))
            defects["duplicate_txn"] += 1
        rng.shuffle(rows)
        write(out / "card_transactions" / date / "part-0000.csv", rows)
        (out / "card_transactions" / date / "_defects.json").write_text(json.dumps(defects, indent=2), encoding="utf-8")
        print(date, len(delta), "merchant rows", len(rows), "txns", defects)


if __name__ == "__main__":
    main()
