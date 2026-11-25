#!/usr/bin/env python3
"""
generate_data.py - simulates the Sentinal Motor source systems.

Writes files in the layout the Snowflake stage expects:
    sentinal_motor/policies/YYYY/MM/DD/policies_YYYYMMDD_HHMMSS.csv
    sentinal_motor/claims/YYYY/MM/DD/claims_YYYYMMDD_HHMMSS.json
    sentinal_motor/telematics/YYYY/MM/DD/telematics_YYYYMMDD_HHMMSS.json

Files are always written to ./output/ first. Add --bucket to also upload to S3.

Examples
    # First full load (local only)
    python generate_data.py init --policies 200 --claims 300 --trips 2000

    # Same, uploaded to S3 using the "sentinal" AWS CLI profile
    python generate_data.py init --policies 200 --claims 300 --trips 2000 \
        --bucket my-sentinal-bucket --profile sentinal

    # Later: exactly 5 new, clean claims (for the incremental test in step 5.9)
    python generate_data.py incremental --claims 5 --clean \
        --bucket my-sentinal-bucket --profile sentinal

All names and SSNs are fake. SSNs use the 900-999 area range, which is never
issued as a real Social Security Number.

To match your existing tables, edit the make_policy / make_claim / make_trip
functions: each one returns one record, and its keys become the columns.
"""

import argparse
import csv
import datetime as dt
import io
import json
import pathlib
import random
import sys

PREFIX = "sentinal_motor"
STATE_FILE = pathlib.Path("generator_state.json")
OUTPUT_DIR = pathlib.Path("output")

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
               "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
               "Joseph", "Jessica", "Thomas", "Sarah", "Daniel", "Karen", "Priya",
               "Arjun", "Wei", "Mei", "Carlos", "Sofia", "Omar", "Aisha"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson",
              "Anderson", "Thomas", "Taylor", "Moore", "Patel", "Shah", "Chen",
              "Nguyen", "Kim", "Khan", "Ali"]
STATES = ["CA", "TX", "FL", "NY", "IL", "PA", "OH", "GA", "NC", "MI"]
CLAIM_STATUSES = ["OPEN", "IN_REVIEW", "SETTLED", "DENIED"]
LOSS_TYPES = ["Rear-end collision", "Side impact at junction", "Parking damage",
              "Windscreen damage", "Theft of vehicle", "Single-vehicle skid",
              "Hit and run", "Flood damage"]
EVENTS = ["NORMAL", "NORMAL", "NORMAL", "HARD_BRAKE", "SPEEDING", "HARSH_ACCELERATION"]


# ---------------------------------------------------------------- helpers

def fake_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def fake_ssn():
    return f"{random.randint(900, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"


def rand_date(start, end):
    days = (end - start).days
    return start + dt.timedelta(days=random.randint(0, max(days, 0)))


def iso(d):
    return d.isoformat() if d else None


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"next_policy": 1, "next_claim": 1, "next_trip": 1, "policies": []}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------- records

def make_policy(state, today):
    num = state["next_policy"]
    state["next_policy"] += 1
    start = rand_date(today - dt.timedelta(days=700), today)
    record = {
        "policy_id": f"POL-{num:06d}",
        "holder_name": fake_name(),
        "ssn": fake_ssn(),
        "state": random.choice(STATES),
        "vehicle_id": f"VEH-{num:06d}",
        "premium_amount": round(random.uniform(400, 3000), 2),
        "policy_start_date": iso(start),
        "policy_end_date": iso(start + dt.timedelta(days=365)),
        "updated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }
    # remember what claims and trips need later
    state["policies"].append({k: record[k] for k in
                              ("policy_id", "holder_name", "ssn",
                               "policy_start_date", "policy_end_date")})
    return record


def make_claim(state, today):
    num = state["next_claim"]
    state["next_claim"] += 1
    pol = random.choice(state["policies"])
    p_start = dt.date.fromisoformat(pol["policy_start_date"])
    p_end = min(dt.date.fromisoformat(pol["policy_end_date"]), today)

    loss_date = rand_date(p_start, p_end)
    filed_date = min(loss_date + dt.timedelta(days=random.randint(0, 14)), today)
    status = random.choice(CLAIM_STATUSES)
    settlement_date = None
    if status in ("SETTLED", "DENIED"):
        settlement_date = filed_date + dt.timedelta(days=random.randint(5, 120))
        if settlement_date > today:          # can't settle in the future
            status, settlement_date = "IN_REVIEW", None

    third_party = random.random() < 0.2      # sometimes the claimant isn't the holder
    return {
        "claim_id": f"CLM-{num:07d}",
        "policy_id": pol["policy_id"],
        "claimant": {
            "name": fake_name() if third_party else pol["holder_name"],
            "ssn": fake_ssn() if third_party else pol["ssn"],
            "is_policy_holder": not third_party,
        },
        "loss": {
            "loss_date": iso(loss_date),
            "description": random.choice(LOSS_TYPES),
        },
        "filed_date": iso(filed_date),
        "settlement_date": iso(settlement_date),
        "claim_amount": round(random.lognormvariate(8.3, 0.9), 2),
        "claim_status": status,
        "updated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }


def make_trip(state, today):
    num = state["next_trip"]
    state["next_trip"] += 1
    pol = random.choice(state["policies"])
    p_start = dt.date.fromisoformat(pol["policy_start_date"])
    p_end = min(dt.date.fromisoformat(pol["policy_end_date"]), today)
    return {
        "trip_id": f"TRP-{num:08d}",
        "policy_id": pol["policy_id"],
        "trip_date": iso(rand_date(p_start, p_end)),
        "trip_miles": round(random.uniform(1, 120), 1),
        "risk_score": None if random.random() < 0.05 else round(random.uniform(5, 95), 1),
        "event": random.choice(EVENTS),
        "recorded_at": dt.datetime.now().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------- dirty data
# A few bad records, so the staging validation and quarantine have work to do.

def dirty_policies(rows, rate):
    for r in rows:
        if random.random() < rate:
            choice = random.choice(["premium_text", "no_state", "bad_date"])
            if choice == "premium_text":
                r["premium_amount"] = "N/A"
            elif choice == "no_state":
                r["state"] = ""
            else:
                r["policy_start_date"] = "31/02/2026"
    dupes = [dict(r) for r in rows if random.random() < rate / 2]
    return rows + dupes


def dirty_claims(rows, rate):
    for r in rows:
        if random.random() < rate:
            choice = random.choice(["orphan", "no_id", "settle_before_filed", "negative"])
            if choice == "orphan":
                r["policy_id"] = "POL-999999"
            elif choice == "no_id":
                r["claim_id"] = None
            elif choice == "settle_before_filed" and r["filed_date"]:
                r["settlement_date"] = iso(dt.date.fromisoformat(r["filed_date"])
                                           - dt.timedelta(days=3))
                r["claim_status"] = "SETTLED"
            else:
                r["claim_amount"] = -abs(r["claim_amount"])
    dupes = [json.loads(json.dumps(r)) for r in rows if random.random() < rate / 2]
    return rows + dupes


def dirty_trips(rows, rate):
    for r in rows:
        if random.random() < rate:
            r["policy_id"] = "POL-999999"
    dupes = [dict(r) for r in rows if random.random() < rate / 2]
    return rows + dupes


# ---------------------------------------------------------------- output

def to_csv(rows):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def to_json(rows):
    return json.dumps(rows, indent=1)


def write(entity, ext, body, now, s3):
    key = (f"{PREFIX}/{entity}/{now:%Y/%m/%d}/"
           f"{entity}_{now:%Y%m%d_%H%M%S}.{ext}")
    local = OUTPUT_DIR / key
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(body, encoding="utf-8")
    where = str(local)
    if s3:
        client, bucket = s3
        client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
        where += f"  ->  s3://{bucket}/{key}"
    print(f"  {entity:<11} {where}")


def get_s3(bucket, profile):
    if not bucket:
        return None
    try:
        import boto3
    except ImportError:
        sys.exit("boto3 is not installed: run  pip install boto3")
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("s3"), bucket


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Generate Sentinal Motor source files")
    ap.add_argument("mode", choices=["init", "incremental"],
                    help="init = first full load, incremental = new records only")
    ap.add_argument("--policies", type=int, default=None)
    ap.add_argument("--claims", type=int, default=None)
    ap.add_argument("--trips", type=int, default=None)
    ap.add_argument("--dirty-rate", type=float, default=0.05,
                    help="share of records made invalid on purpose (default 0.05)")
    ap.add_argument("--clean", action="store_true", help="no invalid records, no duplicates")
    ap.add_argument("--bucket", help="S3 bucket to upload to (omit for local only)")
    ap.add_argument("--profile", help="AWS CLI profile name")
    ap.add_argument("--seed", type=int, help="random seed for repeatable output")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    defaults = {"init": (200, 300, 2000), "incremental": (0, 0, 0)}[args.mode]
    n_pol = args.policies if args.policies is not None else defaults[0]
    n_clm = args.claims if args.claims is not None else defaults[1]
    n_trp = args.trips if args.trips is not None else defaults[2]
    rate = 0.0 if args.clean else args.dirty_rate

    if args.mode == "init" and STATE_FILE.exists():
        sys.exit(f"{STATE_FILE} already exists. Delete it to start over, "
                 "or use 'incremental' to add new records.")

    state = load_state()
    if args.mode == "incremental" and not state["policies"] and n_pol == 0:
        sys.exit("No policies yet. Run 'init' first, or pass --policies.")

    today = dt.date.today()
    now = dt.datetime.now()
    s3 = get_s3(args.bucket, args.profile)

    print(f"Generating ({args.mode}, dirty rate {rate}):")

    if n_pol:
        rows = dirty_policies([make_policy(state, today) for _ in range(n_pol)], rate)
        write("policies", "csv", to_csv(rows), now, s3)
    if n_clm:
        rows = dirty_claims([make_claim(state, today) for _ in range(n_clm)], rate)
        write("claims", "json", to_json(rows), now, s3)
    if n_trp:
        rows = dirty_trips([make_trip(state, today) for _ in range(n_trp)], rate)
        write("telematics", "json", to_json(rows), now, s3)

    save_state(state)
    print(f"Done. Next IDs: policy {state['next_policy']}, "
          f"claim {state['next_claim']}, trip {state['next_trip']}")


if __name__ == "__main__":
    main()
