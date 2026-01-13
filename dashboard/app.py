import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import clickhouse_connect

# -----------------------
# Config
# -----------------------
st.set_page_config(page_title="Fraud + Revenue Intelligence", layout="wide")

CH_HOST = os.environ.get("FRI_CH_HOST", "localhost")
CH_PORT = int(os.environ.get("FRI_CH_HTTP_PORT", "8123"))
CH_USER = os.environ.get("FRI_CH_USER", "fri")
CH_PASSWORD = os.environ.get("FRI_CH_PASSWORD", "fri")
CH_DB = os.environ.get("FRI_CH_DB", "analytics")

@st.cache_resource
def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB
    )

def qdf(sql: str, params=None) -> pd.DataFrame:
    client = ch_client()
    params = params or {}
    return client.query_df(sql, parameters=params)

def money(x_cents: float) -> str:
    if x_cents is None:
        return "$0"
    return f"${(x_cents/100.0):,.0f}"

# -----------------------
# Sidebar filters
# -----------------------
st.title("Real-Time Fraud + Revenue Intelligence (Streamlit)")

today = date.today()
default_start = today - timedelta(days=14)

with st.sidebar:
    st.header("Filters")

    d1, d2 = st.date_input(
        "Date range",
        value=(default_start, today),
        max_value=today
    )

    # Populate filter dropdowns from ClickHouse
    # Keep them light (LowCardinality columns -> fast)
    countries = qdf("SELECT DISTINCT country FROM fct_transactions ORDER BY country")["country"].tolist()
    risks = qdf("SELECT DISTINCT risk_tier FROM fct_transactions ORDER BY risk_tier")["risk_tier"].tolist()

    country = st.selectbox("Customer country", ["All"] + countries, index=0)
    risk_tier = st.selectbox("Customer risk tier", ["All"] + risks, index=0)

    # Merchants can be large; show top merchants by txn volume for selection
    top_merchants = qdf("""
        SELECT merchant_id, count() AS txn_count
        FROM fct_transactions
        WHERE event_time >= {start:Date} AND event_time <= {end:Date}
        GROUP BY merchant_id
        ORDER BY txn_count DESC
        LIMIT 200
    """, {"start": d1, "end": d2})
    merchant_options = ["All"] + top_merchants["merchant_id"].astype(str).tolist()
    merchant_id = st.selectbox("Merchant (top 200 by volume)", merchant_options, index=0)

# Build WHERE clause
where = ["event_time >= {start:Date}", "event_time <= {end:Date}"]
params = {"start": d1, "end": d2}

if country != "All":
    where.append("country = {country:String}")
    params["country"] = country

if risk_tier != "All":
    where.append("risk_tier = {risk:String}")
    params["risk"] = risk_tier

if merchant_id != "All":
    where.append("merchant_id = {merchant:String}")
    params["merchant"] = merchant_id

where_sql = " AND ".join(where)

# -----------------------
# KPI row
# -----------------------
kpi = qdf(f"""
SELECT
  sumIf(amount_cents, status IN ('authorized','captured')) AS gmv_cents,
  sumIf(amount_cents, status = 'refunded') AS refund_cents,
  sumIf(amount_cents, status = 'chargeback') AS chargeback_cents,
  count() AS txn_count,
  countIf(status = 'failed') AS failed_count,
  countIf(risk_tier = 'high') AS high_risk_count
FROM fct_transactions
WHERE {where_sql}
""", params)

gmv = float(kpi.loc[0, "gmv_cents"] or 0)
refund = float(kpi.loc[0, "refund_cents"] or 0)
chargeback = float(kpi.loc[0, "chargeback_cents"] or 0)
net = gmv - refund - chargeback
txn_count = int(kpi.loc[0, "txn_count"] or 0)
failed = int(kpi.loc[0, "failed_count"] or 0)
high_risk = int(kpi.loc[0, "high_risk_count"] or 0)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("GMV", money(gmv))
c2.metric("Net revenue (proxy)", money(net))
c3.metric("Chargeback loss", money(chargeback))
c4.metric("Failed rate", f"{(failed / max(txn_count,1))*100:.2f}%")
c5.metric("High-risk share", f"{(high_risk / max(txn_count,1))*100:.2f}%")

st.divider()

# -----------------------
# Tabs
# -----------------------
tab1, tab2, tab3 = st.tabs(["Executive", "Fraud Monitoring", "Merchant Deep Dive"])

with tab1:
    st.subheader("Daily trends")

    daily = qdf(f"""
    SELECT
      toDate(event_time) AS day,
      sumIf(amount_cents, status IN ('authorized','captured')) AS gmv_cents,
      sumIf(amount_cents, status='refunded') AS refund_cents,
      sumIf(amount_cents, status='chargeback') AS chargeback_cents,
      count() AS txn_count
    FROM fct_transactions
    WHERE {where_sql}
    GROUP BY day
    ORDER BY day
    """, params)

    if daily.empty:
        st.info("No data for selected filters.")
    else:
        daily["net_cents"] = daily["gmv_cents"] - daily["refund_cents"] - daily["chargeback_cents"]
        colA, colB = st.columns(2)
        with colA:
            st.caption("GMV ($)")
            st.line_chart(daily.set_index("day")["gmv_cents"] / 100.0)
        with colB:
            st.caption("Net revenue proxy ($)")
            st.line_chart(daily.set_index("day")["net_cents"] / 100.0)

        st.caption("Chargeback loss ($)")
        st.line_chart(daily.set_index("day")["chargeback_cents"] / 100.0)

    st.subheader("Top merchants (by GMV)")
    top = qdf(f"""
    SELECT
      merchant_id,
      sumIf(amount_cents, status IN ('authorized','captured')) AS gmv_cents,
      sumIf(amount_cents, status='chargeback') AS chargeback_cents,
      count() AS txn_count
    FROM fct_transactions
    WHERE {where_sql}
    GROUP BY merchant_id
    ORDER BY gmv_cents DESC
    LIMIT 15
    """, params)

    if not top.empty:
        top_show = top.copy()
        top_show["gmv_usd"] = (top_show["gmv_cents"] / 100.0).round(0)
        top_show["chargeback_usd"] = (top_show["chargeback_cents"] / 100.0).round(0)
        st.dataframe(top_show[["merchant_id","txn_count","gmv_usd","chargeback_usd"]], use_container_width=True)

with tab2:
    st.subheader("Risk & outcomes")

    mix = qdf(f"""
    SELECT
      status,
      count() AS txn_count
    FROM fct_transactions
    WHERE {where_sql}
    GROUP BY status
    ORDER BY txn_count DESC
    """, params)

    col1, col2 = st.columns(2)
    with col1:
        st.caption("Transaction status distribution")
        st.bar_chart(mix.set_index("status")["txn_count"])

    risk_dist = qdf(f"""
    SELECT
      risk_tier,
      count() AS txn_count,
      countIf(status='chargeback') AS chargeback_count,
      countIf(status='failed') AS failed_count
    FROM fct_transactions
    WHERE {where_sql}
    GROUP BY risk_tier
    ORDER BY txn_count DESC
    """, params)

    with col2:
        st.caption("Risk tier distribution")
        st.bar_chart(risk_dist.set_index("risk_tier")["txn_count"])

    st.subheader("Chargeback rate trend")
    cb_trend = qdf(f"""
    SELECT
      toDate(event_time) AS day,
      countIf(status='chargeback') AS cb,
      count() AS total,
      if(total=0, 0, cb/total) AS cb_rate
    FROM fct_transactions
    WHERE {where_sql}
    GROUP BY day
    ORDER BY day
    """, params)

    if not cb_trend.empty:
        st.line_chart(cb_trend.set_index("day")["cb_rate"])

with tab3:
    st.subheader("Merchant drill-down (requires Merchant filter)")

    if merchant_id == "All":
        st.warning("Select a specific Merchant in the sidebar to see drill-down.")
    else:
        m_daily = qdf(f"""
        SELECT
          toDate(event_time) AS day,
          sumIf(amount_cents, status IN ('authorized','captured')) AS gmv_cents,
          sumIf(amount_cents, status='chargeback') AS chargeback_cents,
          count() AS txn_count,
          countIf(risk_tier='high') AS high_risk_txn
        FROM fct_transactions
        WHERE {where_sql}
        GROUP BY day
        ORDER BY day
        """, params)

        col1, col2 = st.columns(2)
        with col1:
            st.caption("Merchant GMV ($)")
            st.line_chart(m_daily.set_index("day")["gmv_cents"] / 100.0)
        with col2:
            st.caption("Merchant chargeback loss ($)")
            st.line_chart(m_daily.set_index("day")["chargeback_cents"] / 100.0)

        st.caption("High-risk share (merchant)")
        m_daily["high_risk_share"] = m_daily["high_risk_txn"] / m_daily["txn_count"].clip(lower=1)
        st.line_chart(m_daily.set_index("day")["high_risk_share"])

        st.subheader("Recent transactions (sample)")
        recent = qdf(f"""
        SELECT
          event_time,
          txn_id,
          customer_id,
          amount_cents,
          status,
          country,
          risk_tier
        FROM fct_transactions
        WHERE {where_sql}
        ORDER BY event_time DESC
        LIMIT 50
        """, params)

        if not recent.empty:
            recent["amount_usd"] = (recent["amount_cents"] / 100.0).round(2)
            st.dataframe(
                recent[["event_time","txn_id","customer_id","amount_usd","status","country","risk_tier"]],
                use_container_width=True
            )

st.caption("Data source: ClickHouse analytics.fct_transactions / analytics.agg_daily_merchant_kpis (serving layer)")
