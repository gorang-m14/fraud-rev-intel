-- OLTP schema (Postgres)
-- Note: keep it normalized; analytics is built via dbt + ClickHouse.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS customers (
  customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  email TEXT UNIQUE NOT NULL,
  phone TEXT,
  country TEXT NOT NULL,
  risk_tier TEXT NOT NULL CHECK (risk_tier IN ('low','medium','high')),
  kyc_tier TEXT NOT NULL CHECK (kyc_tier IN ('none','basic','full'))
);

CREATE TABLE IF NOT EXISTS merchants (
  merchant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  merchant_name TEXT NOT NULL,
  mcc TEXT NOT NULL,
  country TEXT NOT NULL,
  risk_tier TEXT NOT NULL CHECK (risk_tier IN ('low','medium','high'))
);

CREATE TABLE IF NOT EXISTS payment_methods (
  payment_method_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL REFERENCES customers(customer_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  method_type TEXT NOT NULL CHECK (method_type IN ('card','bank','wallet')),
  fingerprint TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS devices (
  device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  device_fingerprint TEXT NOT NULL UNIQUE,
  device_type TEXT NOT NULL CHECK (device_type IN ('android','ios','web')),
  os_version TEXT,
  browser TEXT
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL REFERENCES customers(customer_id),
  device_id UUID REFERENCES devices(device_id),
  ip TEXT,
  asn TEXT,
  country TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
  txn_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  customer_id UUID NOT NULL REFERENCES customers(customer_id),
  merchant_id UUID NOT NULL REFERENCES merchants(merchant_id),
  payment_method_id UUID REFERENCES payment_methods(payment_method_id),
  session_id UUID REFERENCES sessions(session_id),
  amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
  currency TEXT NOT NULL DEFAULT 'USD',
  channel TEXT NOT NULL CHECK (channel IN ('web','mobile')),
  status TEXT NOT NULL CHECK (status IN ('authorized','captured','failed','refunded','chargeback')),
  auth_code TEXT,
  failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS events (
  event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  customer_id UUID REFERENCES customers(customer_id),
  session_id UUID REFERENCES sessions(session_id),
  event_type TEXT NOT NULL CHECK (event_type IN ('signup','login','password_reset','kyc_started','kyc_verified','pm_added','address_change')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS disputes (
  dispute_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  txn_id UUID NOT NULL REFERENCES transactions(txn_id),
  dispute_reason TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('open','won','lost')),
  amount_cents BIGINT NOT NULL CHECK (amount_cents > 0)
);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  customer_id UUID REFERENCES customers(customer_id),
  txn_id UUID REFERENCES transactions(txn_id),
  rule_name TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('low','medium','high')),
  score NUMERIC(6,3) NOT NULL,
  details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS cases (
  case_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  alert_id UUID REFERENCES alerts(alert_id),
  status TEXT NOT NULL CHECK (status IN ('open','in_review','closed_legit','closed_fraud')),
  investigator TEXT,
  closed_at TIMESTAMPTZ
);

-- Helpful indexes (baseline)
CREATE INDEX IF NOT EXISTS idx_txn_customer_time ON transactions(customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_txn_merchant_time ON transactions(merchant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_customer_time ON events(customer_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(created_at DESC);
