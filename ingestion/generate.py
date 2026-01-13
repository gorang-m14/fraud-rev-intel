import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from psycopg2.extras import Json

from faker import Faker

from ingestion.db import pg_conn, insert_many

fake = Faker()

COUNTRIES = ["US","IN","GB","DE","SG","AE"]
MCCS = ["5411","5812","5732","5999","5311","4111"]  # grocery, restaurant, electronics, misc, dept store, travel
RISK = ["low","medium","high"]
KYC = ["none","basic","full"]
DEVICE_TYPES = ["android","ios","web"]
CHANNELS = ["web","mobile"]
STATUSES = ["authorized","captured","failed","refunded","chargeback"]
EVENT_TYPES = ["signup","login","password_reset","kyc_started","kyc_verified","pm_added","address_change"]

def utcnow():
    return datetime.now(timezone.utc)

def random_time_within(days=30):
    return utcnow() - timedelta(seconds=random.randint(0, days*24*3600))

def gen_seed_entities(n_customers=800, n_merchants=120):
    customers = []
    for _ in range(n_customers):
        email = fake.unique.email()
        customers.append((email, fake.phone_number(), random.choice(COUNTRIES), random.choices(RISK, weights=[0.7,0.25,0.05])[0], random.choices(KYC, weights=[0.25,0.5,0.25])[0]))
    merchants = []
    for _ in range(n_merchants):
        merchants.append((fake.company(), random.choice(MCCS), random.choice(COUNTRIES), random.choices(RISK, weights=[0.75,0.2,0.05])[0]))
    return customers, merchants

def main(rows: int, seed_customers: int, seed_merchants: int):
    conn = pg_conn()
    conn.autocommit = False
    cur = conn.cursor()

    # Seed customers and merchants
    customers, merchants = gen_seed_entities(seed_customers, seed_merchants)
    insert_many(cur,
        "INSERT INTO customers (email, phone, country, risk_tier, kyc_tier) VALUES %s ON CONFLICT (email) DO NOTHING",
        customers
    )
    insert_many(cur,
        "INSERT INTO merchants (merchant_name, mcc, country, risk_tier) VALUES %s",
        merchants
    )

    # Fetch ids
    cur.execute("SELECT customer_id, risk_tier, country FROM customers")
    customer_rows = cur.fetchall()
    cur.execute("SELECT merchant_id, risk_tier, country FROM merchants")
    merchant_rows = cur.fetchall()

    # Seed devices
    devices = []
    for _ in range(max(400, seed_customers//2)):
        devices.append((fake.sha1(), random.choice(DEVICE_TYPES), fake.numerify("##.##"), fake.user_agent()))
    insert_many(cur,
        "INSERT INTO devices (device_fingerprint, device_type, os_version, browser) VALUES %s ON CONFLICT (device_fingerprint) DO NOTHING",
        devices
    )
    cur.execute("SELECT device_id FROM devices")
    device_ids = [r[0] for r in cur.fetchall()]

    # Seed sessions
    sessions = []
    for _ in range(max(1200, rows//2)):
        cust_id, _, ctry = random.choice(customer_rows)
        sessions.append((cust_id, random.choice(device_ids), fake.ipv4_public(), str(random.randint(1000,99999)), ctry, random_time_within(14)))
    insert_many(cur,
        "INSERT INTO sessions (customer_id, device_id, ip, asn, country, started_at) VALUES %s",
        sessions
    )
    cur.execute("SELECT session_id, customer_id FROM sessions")
    session_rows = cur.fetchall()
    sessions_by_customer = {}
    for sid, cid in session_rows:
        sessions_by_customer.setdefault(cid, []).append(sid)

    # Seed payment methods
    pms = []
    for _ in range(seed_customers * 2):
        cust_id, _, _ = random.choice(customer_rows)
        pms.append((cust_id, random.choice(["card","bank","wallet"]), fake.sha1(), True))
    insert_many(cur,
        "INSERT INTO payment_methods (customer_id, method_type, fingerprint, is_active) VALUES %s",
        pms
    )
    cur.execute("SELECT payment_method_id, customer_id FROM payment_methods WHERE is_active")
    pm_rows = cur.fetchall()
    pms_by_customer = {}
    for pmid, cid in pm_rows:
        pms_by_customer.setdefault(cid, []).append(pmid)

    # Generate events + transactions
    events = []
    txns = []
    disputes = []
    alerts = []
    cases = []

    for _ in range(rows):
        cust_id, cust_risk, cust_country = random.choice(customer_rows)
        merch_id, merch_risk, merch_country = random.choice(merchant_rows)

        session_id = random.choice(sessions_by_customer.get(cust_id, [None]))
        pm_list = pms_by_customer.get(cust_id, [])
        pm_id = random.choice(pm_list) if pm_list else None

        # Amount distribution: most small, some big outliers
        base = random.randint(200, 20000)
        if random.random() < 0.03:
            base *= random.randint(20, 80)
        amount = base

        channel = random.choice(CHANNELS)
        t = random_time_within(30)

        # Status influenced by risk
        fail_p = 0.03 + (0.05 if cust_risk == "high" else 0) + (0.03 if merch_risk == "high" else 0)
        cb_p = 0.004 + (0.02 if cust_risk == "high" else 0)
        refund_p = 0.01

        status = "captured"
        r = random.random()
        if r < fail_p:
            status = "failed"
        elif r < fail_p + cb_p:
            status = "chargeback"
        elif r < fail_p + cb_p + refund_p:
            status = "refunded"
        else:
            status = random.choices(["authorized","captured"], weights=[0.2,0.8])[0]

        txn_id = uuid.uuid4()
        idem = f"idem_{txn_id.hex}"

        auth_code = fake.bothify(text="??#####") if status in ("authorized","captured") else None
        failure_reason = random.choice(["insufficient_funds","stolen_card","3ds_failed","suspected_fraud"]) if status == "failed" else None

        txns.append((
            str(txn_id), idem, t, str(cust_id), str(merch_id), str(pm_id) if pm_id else None, str(session_id) if session_id else None,
            amount, "USD", channel, status, auth_code, failure_reason
        ))

        # Emit a few events per txn
        if random.random() < 0.25:
            events.append((random_time_within(30), str(cust_id), str(session_id) if session_id else None, "login", json.dumps({"ip": fake.ipv4_public()})))
        if random.random() < 0.06:
            events.append((random_time_within(30), str(cust_id), str(session_id) if session_id else None, "password_reset", json.dumps({"method":"email"})))

        # Disputes for chargebacks
        if status == "chargeback":
            disp_id = uuid.uuid4()
            disputes.append((str(disp_id), t + timedelta(days=random.randint(1,10)), str(txn_id), random.choice(["fraud","service_not_received","duplicate"]), random.choice(["open","lost","won"]), amount))

        # Fraud alerts (simple rules)
        score = 0.0
        rules_hit = []
        if cust_risk == "high":
            score += 0.35; rules_hit.append("high_risk_customer")
        if merch_risk == "high":
            score += 0.25; rules_hit.append("high_risk_merchant")
        if amount > 150000:
            score += 0.30; rules_hit.append("large_amount")
        if status in ("failed","chargeback"):
            score += 0.20; rules_hit.append("bad_outcome")

        if score >= 0.45:
            alert_id = uuid.uuid4()
            sev = "high" if score >= 0.75 else ("medium" if score >= 0.55 else "low")
            alerts.append((
                str(alert_id), t, str(cust_id), str(txn_id), " | ".join(rules_hit), sev, round(score,3),
                json.dumps({"rules": rules_hit, "cust_risk": cust_risk, "merch_risk": merch_risk})
            ))
            # Create case for higher severity
            if sev in ("medium","high") and random.random() < 0.7:
                case_id = uuid.uuid4()
                cases.append((str(case_id), t, str(alert_id), "open", None, None))

    # Bulk insert transactions (as text → cast in SQL)
    insert_many(cur, """        INSERT INTO transactions
        (txn_id, idempotency_key, created_at, customer_id, merchant_id, payment_method_id, session_id,
         amount_cents, currency, channel, status, auth_code, failure_reason)
        VALUES %s
        ON CONFLICT (idempotency_key) DO NOTHING
    """, txns)

    insert_many(cur, """\
        INSERT INTO events (event_time, customer_id, session_id, event_type, metadata)
        VALUES %s
    """, [(e[0], e[1], e[2], e[3], Json(json.loads(e[4]))) for e in events])


    insert_many(cur, """        INSERT INTO disputes (dispute_id, created_at, txn_id, dispute_reason, outcome, amount_cents)
        VALUES %s
    """, disputes)

    insert_many(cur, """\
        INSERT INTO alerts (alert_id, created_at, customer_id, txn_id, rule_name, severity, score, details)
        VALUES %s
    """, [(a[0], a[1], a[2], a[3], a[4], a[5], a[6], Json(json.loads(a[7]))) for a in alerts])


    insert_many(cur, """        INSERT INTO cases (case_id, created_at, alert_id, status, investigator, closed_at)
        VALUES %s
    """, cases)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted: txns={len(txns)}, events={len(events)}, disputes={len(disputes)}, alerts={len(alerts)}, cases={len(cases)}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--seed-customers", type=int, default=800)
    ap.add_argument("--seed-merchants", type=int, default=120)
    args = ap.parse_args()
    main(args.rows, args.seed_customers, args.seed_merchants)
