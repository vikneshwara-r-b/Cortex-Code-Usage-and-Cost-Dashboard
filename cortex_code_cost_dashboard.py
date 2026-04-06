"""
Cortex Code Platform Owner Dashboard

Cost governance dashboard for Snowflake platform owners managing Cortex Code rollouts.
Queries SNOWFLAKE.ACCOUNT_USAGE views for spend, usage, and efficiency metrics.
Covers both Cortex Code CLI and Cortex Code Snowsight (UI) usage.
Supports synthetic demo data when no real usage data is available.

What's covered
--------------
- Executive KPIs: credits, projected spend, active users, query volume
- CLI vs Snowsight spend split
- Per-user cost breakdown with USD estimates
- Model-level token & credit breakdown (authoritative per-token-type pricing)
- Cache efficiency and output/input ratio analysis
- Cost Controls: rolling 24h spend vs daily credit limits (Apr 2, 2026 feature)
- Trend analysis: daily/hourly patterns, new user onboarding
- Governance reference: managed settings, model allowlist, tiered access

Data sources
------------
  SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
  SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
  SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY

Pricing: Cortex Code moved to AI credits billing on Apr 1, 2026.
  1 AI credit ≈ $2.00 USD.
"""

from datetime import date, timedelta
import random

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Cortex Code Usage and Cost Dashboard",
    page_icon=":material/analytics:",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Global Altair dark theme — transparent background, muted grid, light labels
# ---------------------------------------------------------------------------
def _altair_dark_theme() -> dict:
    _label  = "#8b949e"
    _grid   = "#21262d"
    _domain = "#30363d"
    return {
        "config": {
            "background": "transparent",
            "view": {"strokeWidth": 0},
            "axis": {
                "labelColor": _label,  "titleColor": _label,
                "gridColor":  _grid,   "domainColor": _domain,
                "tickColor":  _domain, "labelFontSize": 11,
            },
            "legend": {"labelColor": _label, "titleColor": _label, "labelFontSize": 11},
            "title":  {"color": "#e6edf3", "fontSize": 13, "fontWeight": "normal"},
            "mark":   {"color": "#29B5E8"},
        }
    }

alt.themes.register("dark_coco", _altair_dark_theme)
alt.themes.enable("dark_coco")

# ---------------------------------------------------------------------------
CHART_HEIGHT = 300

# Primary chart palette
CHART_TEAL   = "#29B5E8"   # main bars / areas / lines
CHART_AMBER  = "#f59e0b"   # secondary series
CHART_GREEN  = "#22c55e"   # positive / good
CHART_RED    = "#ef4444"   # negative / alert

SRC_CLI = "CLI"
SRC_UI = "Snowsight"
SRC_ALL = "All"

# 1 AI credit = $2.00 USD (effective Apr 1, 2026)
CREDITS_PER_USD = 2.00

# Authoritative model credit rates (credits per million tokens)
# Source: cost-queries.sql
COCO_MODEL_COSTS_SQL = """
    SELECT model_name, input_cpmt, output_cpmt, cache_writ_input_cpmt, cache_read_input_cpmt
    FROM VALUES
        ('claude-4-sonnet',   1.50,  7.50, 1.88, 0.15),
        ('claude-opus-4-5',   2.75, 13.75, 3.44, 0.28),
        ('claude-opus-4-6',   2.75, 13.75, 3.44, 0.28),
        ('claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17),
        ('claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17),
        ('openai-gpt-5.2',    0.97,  7.70, NULL, 0.10)
        AS t(model_name, input_cpmt, output_cpmt, cache_writ_input_cpmt, cache_read_input_cpmt)
"""

TIME_RANGE_OPTIONS = {
    "Last 12 hours": "DATEADD('hour', -12, CURRENT_TIMESTAMP())",
    "Last 24 hours": "DATEADD('hour', -24, CURRENT_TIMESTAMP())",
    "Last 7 days":   "DATEADD('day',  -7,  CURRENT_TIMESTAMP())",
    "Last 30 days":  "DATEADD('day',  -30, CURRENT_DATE())",
    "Last 90 days":  "DATEADD('day',  -90, CURRENT_DATE())",
}


# =============================================================================
# Snowflake connection
# =============================================================================

def get_connection():
    # Inside Snowflake (Streamlit in Snowflake), the built-in connection is
    # always named "snowflake". Locally it falls back to "snowhouse".
    for name in ("snowflake", "snowhouse"):
        try:
            return st.connection(name)
        except Exception:
            pass
    st.error("Could not connect to Snowflake. Configure a connection named 'snowhouse' in `.streamlit/secrets.toml`.")
    st.stop()


def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    df = conn.query(sql)
    df.columns = df.columns.str.lower()
    # Snowflake returns NUMBER/DECIMAL columns as decimal.Decimal objects.
    # Coerce object-typed columns that contain numerics to float so that
    # standard Python arithmetic (e.g. col * 2.00) works without TypeError.
    df = df.apply(
        lambda col: pd.to_numeric(col, errors="ignore") if col.dtype == object else col
    )
    return df


# =============================================================================
# SQL helpers
# =============================================================================

def _source_cte(source_filter: str, time_filter: str) -> str:
    """
    Returns a CTE fragment named cortex_code_usage, filtered by surface and time.
    Usage: WITH {_source_cte(...)} SELECT ... FROM cortex_code_usage
    """
    cli_block = f"""
        SELECT *, '{SRC_CLI}' AS SOURCE
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
        WHERE USAGE_TIME >= {time_filter}"""

    ui_block = f"""
        SELECT *, '{SRC_UI}' AS SOURCE
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
        WHERE USAGE_TIME >= {time_filter}"""

    if source_filter == SRC_CLI:
        body = cli_block
    elif source_filter == SRC_UI:
        body = ui_block
    else:
        body = f"{cli_block}\n        UNION ALL{ui_block}"

    return f"cortex_code_usage AS ({body}\n    )"


def _trend_granularity(time_filter: str) -> tuple[str, str]:
    """Return (DATE_TRUNC level, tooltip format) appropriate for the window."""
    if "'hour'" in time_filter:
        return "hour", "%Y-%m-%d %H:%M"
    return "day", "%Y-%m-%d"


# =============================================================================
# Synthetic demo data
# =============================================================================

def _demo_users():
    return [
        "Alice Chen", "Bob Martinez", "Carol Singh", "David Kim",
        "Eva Patel", "Frank Liu", "Grace Taylor", "Hiro Nakamura",
        "Iris Johansson", "Jake O'Brien", "Kira Novak", "Leo Rossi",
    ]


def _demo_models():
    return ["claude-sonnet-4-6", "claude-4-sonnet", "claude-opus-4-6", "openai-gpt-5.2"]


@st.cache_data(ttl=900)
def demo_executive_summary() -> pd.DataFrame:
    np.random.seed(42)
    active = 8
    queries = 1247
    mtd = round(np.random.uniform(35, 55), 2)
    days_used = 18
    avg_daily = round(mtd / days_used, 2)
    projected = round(avg_daily * 30, 2)
    return pd.DataFrame([{
        "active_users": active,
        "total_queries": queries,
        "mtd_credits": mtd,
        "est_cost_usd": round(mtd * CREDITS_PER_USD, 2),
        "avg_daily_credits": avg_daily,
        "projected_monthly": projected,
        "projected_monthly_usd": round(projected * CREDITS_PER_USD, 2),
        "credits_per_user": round(mtd / active, 2),
        "queries_per_user": round(queries / active, 1),
        "overall_cache_hit_pct": round(np.random.uniform(55, 75), 1),
        "overall_output_ratio": round(np.random.uniform(1.2, 2.0), 2),
    }])


@st.cache_data(ttl=900)
def demo_daily_spend_trend() -> pd.DataFrame:
    np.random.seed(42)
    days = 30
    dates = [date.today() - timedelta(days=days - i) for i in range(days)]
    base = np.random.uniform(0.3, 0.8, days)
    for i in range(days):
        if dates[i].weekday() >= 5:
            base[i] *= 0.3
        base[i] *= 1 + (i / days) * 0.5
    return pd.DataFrame({
        "usage_date": dates,
        "active_users": np.random.randint(3, 10, days),
        "total_queries": np.random.randint(8, 45, days),
        "daily_credits": np.round(base, 4),
        "est_cost_usd": np.round(base * CREDITS_PER_USD, 2),
        "total_tokens": np.random.randint(50000, 300000, days),
    })


@st.cache_data(ttl=900)
def demo_user_breakdown() -> pd.DataFrame:
    np.random.seed(42)
    users = _demo_users()[:8]
    credits = sorted(np.random.uniform(2, 12, len(users)), reverse=True)
    credits_arr = np.round(credits, 4)
    queries = np.random.randint(40, 300, len(users))
    return pd.DataFrame({
        "user_name": users,
        "total_queries": queries,
        "total_credits": credits_arr,
        "est_cost_usd": np.round(credits_arr * CREDITS_PER_USD, 2),
        "avg_credit_per_query": np.round(credits_arr / queries, 6),
        "total_tokens": np.random.randint(100000, 900000, len(users)),
        "first_query": [date.today() - timedelta(days=random.randint(20, 60)) for _ in users],
        "last_query": [date.today() - timedelta(days=random.randint(0, 3)) for _ in users],
    })


@st.cache_data(ttl=900)
def demo_model_distribution() -> pd.DataFrame:
    models = _demo_models()
    tokens = [4500000, 2100000, 850000, 320000]
    total = sum(tokens)
    return pd.DataFrame({
        "model_name": models,
        "query_count": [620, 410, 217, 88],
        "unique_users": [8, 7, 4, 3],
        "total_tokens": tokens,
        "pct_of_total": [round(t / total * 100, 1) for t in tokens],
    })


@st.cache_data(ttl=900)
def demo_model_cost_breakdown() -> pd.DataFrame:
    """Per-model credit breakdown using authoritative token-type pricing."""
    model_data = [
        ("claude-sonnet-4-6", 2800000, 850000, 1500000, 300000, 1.65, 8.25, 2.07, 0.17),
        ("claude-4-sonnet",   1200000, 320000,  600000, 100000, 1.50, 7.50, 1.88, 0.15),
        ("claude-opus-4-6",    450000, 180000,  200000,  50000, 2.75, 13.75, 3.44, 0.28),
        ("openai-gpt-5.2",     180000,  90000,   80000,      0, 0.97,  7.70, 0.00, 0.10),
    ]
    rows = []
    for m, it, ot, cr, cw, ic_r, oc_r, cw_r, cr_r in model_data:
        ic = it / 1e6 * ic_r
        oc = ot / 1e6 * oc_r
        crc = cr / 1e6 * cr_r
        cwc = cw / 1e6 * cw_r
        total = ic + oc + crc + cwc
        rows.append({
            "model_name": m,
            "query_count": [620, 410, 217, 88][len(rows)],
            "total_input_tokens": it,
            "total_output_tokens": ot,
            "total_cache_read_tokens": cr,
            "total_cache_write_tokens": cw,
            "input_credits": round(ic, 4),
            "output_credits": round(oc, 4),
            "cache_read_credits": round(crc, 4),
            "cache_write_credits": round(cwc, 4),
            "total_credits_calc": round(total, 4),
            "est_cost_usd": round(total * CREDITS_PER_USD, 2),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=900)
def demo_cache_efficiency() -> pd.DataFrame:
    np.random.seed(42)
    users = _demo_users()[:8]
    cache_pct = np.random.uniform(30, 85, len(users))
    health = ["GOOD" if p >= 70 else "FAIR" if p >= 50 else "LOW" for p in cache_pct]
    cache_read = np.random.randint(50000, 500000, len(users))
    input_tokens = np.round(cache_read * (100 / cache_pct - 1)).astype(int)
    return pd.DataFrame({
        "user_name": users,
        "cache_read_tokens": cache_read,
        "input_tokens": input_tokens,
        "cache_hit_pct": np.round(cache_pct, 1),
        "cache_health": health,
    })


@st.cache_data(ttl=900)
def demo_output_ratio() -> pd.DataFrame:
    np.random.seed(42)
    users = _demo_users()[:8]
    ratio = np.random.uniform(0.8, 3.5, len(users))
    flags = ["HIGH" if r > 3.0 else "ELEVATED" if r > 2.0 else "NORMAL" for r in ratio]
    queries = np.random.randint(15, 200, len(users))
    return pd.DataFrame({
        "user_name": users,
        "total_queries": queries,
        "avg_output_tokens": np.random.randint(800, 5000, len(users)),
        "avg_input_tokens": np.random.randint(2000, 8000, len(users)),
        "output_to_input_ratio": np.round(ratio, 2),
        "cost_flag": flags,
    })


@st.cache_data(ttl=900)
def demo_daily_trends() -> pd.DataFrame:
    np.random.seed(42)
    days = 30
    dates = [date.today() - timedelta(days=days - i) for i in range(days)]
    base = np.random.uniform(0.3, 1.2, days)
    for i in range(days):
        if dates[i].weekday() >= 5:
            base[i] *= 0.3
    credits = np.round(base, 4)
    users = np.random.randint(4, 10, days)
    prev = np.concatenate([[np.nan], credits[:-1]])
    wow = np.round(
        (credits - prev) / np.where(np.isnan(prev) | (prev == 0), np.nan, prev) * 100, 1
    )
    return pd.DataFrame({
        "bucket": dates,
        "active_users": users,
        "total_queries": np.random.randint(20, 80, days),
        "bucket_credits": credits,
        "prev_credits": prev,
        "dod_change_pct": wow,
        "net_new_users": np.random.randint(-1, 3, days),
    })


@st.cache_data(ttl=900)
def demo_ai_services_breakdown() -> pd.DataFrame:
    return pd.DataFrame({
        "service": ["Cortex Code CLI", "Cortex Code Snowsight", "Total AI Services (billing)"],
        "mtd_credits": [42.15, 8.73, 128.73],
    })


@st.cache_data(ttl=900)
def demo_surface_breakdown() -> pd.DataFrame:
    return pd.DataFrame({
        "source": [SRC_CLI, SRC_UI],
        "total_credits": [42.15, 8.73],
        "total_queries": [980, 267],
        "active_users": [8, 5],
    })


@st.cache_data(ttl=900)
def demo_user_model_heatmap() -> pd.DataFrame:
    users = _demo_users()[:8]
    models = _demo_models()
    rows = []
    np.random.seed(42)
    for u in users:
        for m in models:
            if np.random.random() > 0.2:
                rows.append({
                    "user_name": u,
                    "model_name": m,
                    "query_count": np.random.randint(5, 120),
                    "total_tokens": np.random.randint(10000, 500000),
                })
    return pd.DataFrame(rows)


@st.cache_data(ttl=900)
def demo_new_user_onboarding() -> pd.DataFrame:
    np.random.seed(42)
    days = 30
    dates = [date.today() - timedelta(days=days - i) for i in range(days)]
    new_users = np.random.choice([0, 0, 0, 0, 1, 1, 2], days)
    cumulative = np.cumsum(new_users)
    mask = new_users > 0
    return pd.DataFrame({
        "first_use_date": np.array(dates)[mask],
        "new_users": new_users[mask],
        "cumulative_users": cumulative[mask],
    })


@st.cache_data(ttl=900)
def demo_rolling_24h_spend() -> pd.DataFrame:
    np.random.seed(42)
    users = _demo_users()[:8]
    cli_credits = np.round(np.random.uniform(0.5, 4.5, len(users)), 4)
    ss_credits = np.round(np.random.uniform(0.05, 1.2, len(users)), 4)
    total = np.round(cli_credits + ss_credits, 4)
    return pd.DataFrame({
        "user_name": users,
        "cli_24h_credits": cli_credits,
        "snowsight_24h_credits": ss_credits,
        "total_24h_credits": total,
        "total_24h_requests": np.random.randint(15, 120, len(users)),
    })


# =============================================================================
# Real data queries
# =============================================================================

@st.cache_data(ttl=900, show_spinner="Loading executive summary...")
def load_executive_summary(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        mtd AS (
            SELECT
                COUNT(DISTINCT c.USER_ID) AS ACTIVE_USERS,
                COUNT(*)                  AS TOTAL_QUERIES,
                SUM(c.TOKEN_CREDITS)      AS MTD_CREDITS,
                SUM(c.TOKENS)             AS TOTAL_TOKENS
            FROM cortex_code_usage c
        ),
        days_info AS (
            SELECT COUNT(DISTINCT DATE_TRUNC('day', c.USAGE_TIME)::DATE) AS DAYS_WITH_USAGE
            FROM cortex_code_usage c
        ),
        granular_totals AS (
            SELECT
                SUM(CASE WHEN mdl.KEY = 'input'            THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_INPUT,
                SUM(CASE WHEN mdl.KEY = 'output'           THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_OUTPUT,
                SUM(CASE WHEN mdl.KEY = 'cache_read_input' THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_CACHE_READ
            FROM cortex_code_usage c,
                LATERAL FLATTEN(INPUT => c.TOKENS_GRANULAR) svc,
                LATERAL FLATTEN(INPUT => svc.VALUE) mdl
        )
        SELECT
            m.ACTIVE_USERS,
            m.TOTAL_QUERIES,
            ROUND(m.MTD_CREDITS, 2)                                                          AS MTD_CREDITS,
            ROUND(m.MTD_CREDITS * {CREDITS_PER_USD}, 2)                                      AS EST_COST_USD,
            ROUND(m.MTD_CREDITS / NULLIF(d.DAYS_WITH_USAGE, 0), 2)                          AS AVG_DAILY_CREDITS,
            ROUND(m.MTD_CREDITS / NULLIF(d.DAYS_WITH_USAGE, 0) * 30, 2)                     AS PROJECTED_MONTHLY,
            ROUND(m.MTD_CREDITS / NULLIF(d.DAYS_WITH_USAGE, 0) * 30 * {CREDITS_PER_USD}, 2) AS PROJECTED_MONTHLY_USD,
            ROUND(m.MTD_CREDITS / NULLIF(m.ACTIVE_USERS, 0), 2)                             AS CREDITS_PER_USER,
            ROUND(m.TOTAL_QUERIES::FLOAT / NULLIF(m.ACTIVE_USERS, 0), 1)                    AS QUERIES_PER_USER,
            ROUND(g.TOTAL_CACHE_READ::FLOAT
                  / NULLIF(g.TOTAL_CACHE_READ + g.TOTAL_INPUT, 0) * 100, 1)                 AS OVERALL_CACHE_HIT_PCT,
            ROUND(g.TOTAL_OUTPUT::FLOAT / NULLIF(g.TOTAL_INPUT, 0), 2)                      AS OVERALL_OUTPUT_RATIO
        FROM mtd m
        CROSS JOIN days_info d
        CROSS JOIN granular_totals g
    """)


@st.cache_data(ttl=900, show_spinner="Loading spend trend...")
def load_daily_spend_trend(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte}
        SELECT
            DATE_TRUNC('day', c.USAGE_TIME)::DATE             AS USAGE_DATE,
            COUNT(DISTINCT c.USER_ID)                          AS ACTIVE_USERS,
            COUNT(*)                                           AS TOTAL_QUERIES,
            ROUND(SUM(c.TOKEN_CREDITS), 4)                     AS DAILY_CREDITS,
            ROUND(SUM(c.TOKEN_CREDITS) * {CREDITS_PER_USD}, 2) AS EST_COST_USD,
            ROUND(SUM(c.TOKENS), 0)                            AS TOTAL_TOKENS
        FROM cortex_code_usage c
        GROUP BY 1
        ORDER BY 1
    """)


@st.cache_data(ttl=900, show_spinner="Loading user breakdown...")
def load_user_breakdown(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte}
        SELECT
            u.NAME                                             AS USER_NAME,
            COUNT(*)                                           AS TOTAL_QUERIES,
            ROUND(SUM(c.TOKEN_CREDITS), 4)                     AS TOTAL_CREDITS,
            ROUND(SUM(c.TOKEN_CREDITS) * {CREDITS_PER_USD}, 2) AS EST_COST_USD,
            ROUND(AVG(c.TOKEN_CREDITS), 6)                     AS AVG_CREDIT_PER_QUERY,
            ROUND(SUM(c.TOKENS), 0)                            AS TOTAL_TOKENS,
            MIN(c.USAGE_TIME)                                  AS FIRST_QUERY,
            MAX(c.USAGE_TIME)                                  AS LAST_QUERY
        FROM cortex_code_usage c
        JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON c.USER_ID = u.USER_ID
        GROUP BY 1
        ORDER BY TOTAL_CREDITS DESC
    """)


@st.cache_data(ttl=900, show_spinner="Loading model distribution...")
def load_model_distribution(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        flattened AS (
            SELECT
                c.REQUEST_ID, c.USER_ID,
                svc.KEY AS MODEL_NAME,
                mdl.KEY AS TOKEN_TYPE,
                mdl.VALUE::NUMBER AS TOKEN_COUNT
            FROM cortex_code_usage c,
                LATERAL FLATTEN(INPUT => c.TOKENS_GRANULAR) svc,
                LATERAL FLATTEN(INPUT => svc.VALUE) mdl
        )
        SELECT
            MODEL_NAME,
            COUNT(DISTINCT REQUEST_ID)                                                    AS QUERY_COUNT,
            COUNT(DISTINCT USER_ID)                                                       AS UNIQUE_USERS,
            ROUND(SUM(TOKEN_COUNT), 0)                                                    AS TOTAL_TOKENS,
            ROUND(SUM(TOKEN_COUNT) * 100.0 / NULLIF(SUM(SUM(TOKEN_COUNT)) OVER (), 0), 1) AS PCT_OF_TOTAL
        FROM flattened
        GROUP BY 1
        ORDER BY TOTAL_TOKENS DESC
    """)


@st.cache_data(ttl=900, show_spinner="Loading model cost breakdown...")
def load_model_cost_breakdown(source_filter: str, time_filter: str) -> pd.DataFrame:
    """
    Per-model credit breakdown using authoritative per-token-type rates.
    Pattern from cost-queries.sql (C4) — uses TOKENS_GRANULAR for accurate attribution.
    """
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        coco_model_costs AS ({COCO_MODEL_COSTS_SQL}),
        flattened AS (
            SELECT
                svc.KEY                                                                      AS MODEL_NAME,
                COUNT(DISTINCT c.REQUEST_ID)                                                 AS QUERY_COUNT,
                SUM(CASE WHEN mdl.KEY = 'input'             THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_INPUT,
                SUM(CASE WHEN mdl.KEY = 'output'            THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_OUTPUT,
                SUM(CASE WHEN mdl.KEY = 'cache_read_input'  THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_CACHE_READ,
                SUM(CASE WHEN mdl.KEY = 'cache_write_input' THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_CACHE_WRITE
            FROM cortex_code_usage c,
                LATERAL FLATTEN(INPUT => c.TOKENS_GRANULAR) svc,
                LATERAL FLATTEN(INPUT => svc.VALUE) mdl
            GROUP BY 1
        )
        SELECT
            f.MODEL_NAME,
            f.QUERY_COUNT,
            f.TOTAL_INPUT,
            f.TOTAL_OUTPUT,
            f.TOTAL_CACHE_READ,
            f.TOTAL_CACHE_WRITE,
            ROUND(f.TOTAL_INPUT       / 1000000 * COALESCE(p.INPUT_CPMT, 0),               4) AS INPUT_CREDITS,
            ROUND(f.TOTAL_OUTPUT      / 1000000 * COALESCE(p.OUTPUT_CPMT, 0),              4) AS OUTPUT_CREDITS,
            ROUND(f.TOTAL_CACHE_READ  / 1000000 * COALESCE(p.CACHE_READ_INPUT_CPMT, 0),   4) AS CACHE_READ_CREDITS,
            ROUND(f.TOTAL_CACHE_WRITE / 1000000 * COALESCE(p.CACHE_WRIT_INPUT_CPMT, 0),   4) AS CACHE_WRITE_CREDITS,
            ROUND(
                  f.TOTAL_INPUT       / 1000000 * COALESCE(p.INPUT_CPMT, 0)
                + f.TOTAL_OUTPUT      / 1000000 * COALESCE(p.OUTPUT_CPMT, 0)
                + f.TOTAL_CACHE_READ  / 1000000 * COALESCE(p.CACHE_READ_INPUT_CPMT, 0)
                + f.TOTAL_CACHE_WRITE / 1000000 * COALESCE(p.CACHE_WRIT_INPUT_CPMT, 0)
            , 4) AS TOTAL_CREDITS_CALC,
            ROUND((
                  f.TOTAL_INPUT       / 1000000 * COALESCE(p.INPUT_CPMT, 0)
                + f.TOTAL_OUTPUT      / 1000000 * COALESCE(p.OUTPUT_CPMT, 0)
                + f.TOTAL_CACHE_READ  / 1000000 * COALESCE(p.CACHE_READ_INPUT_CPMT, 0)
                + f.TOTAL_CACHE_WRITE / 1000000 * COALESCE(p.CACHE_WRIT_INPUT_CPMT, 0)
            ) * {CREDITS_PER_USD}, 2) AS EST_COST_USD
        FROM flattened f
        LEFT JOIN coco_model_costs p ON LOWER(f.MODEL_NAME) = p.MODEL_NAME
        ORDER BY TOTAL_CREDITS_CALC DESC NULLS LAST
    """)


@st.cache_data(ttl=900, show_spinner="Loading cache efficiency...")
def load_cache_efficiency(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        user_agg AS (
            SELECT
                c.USER_ID,
                SUM(CASE WHEN mdl.KEY = 'cache_read_input' THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_CACHE_READ,
                SUM(CASE WHEN mdl.KEY = 'input'            THEN mdl.VALUE::NUMBER ELSE 0 END) AS TOTAL_INPUT
            FROM cortex_code_usage c,
                LATERAL FLATTEN(INPUT => c.TOKENS_GRANULAR) svc,
                LATERAL FLATTEN(INPUT => svc.VALUE) mdl
            GROUP BY 1
        )
        SELECT
            u.NAME AS USER_NAME,
            a.TOTAL_CACHE_READ AS CACHE_READ_TOKENS,
            a.TOTAL_INPUT      AS INPUT_TOKENS,
            ROUND(a.TOTAL_CACHE_READ::FLOAT
                  / NULLIF(a.TOTAL_CACHE_READ + a.TOTAL_INPUT, 0) * 100, 1) AS CACHE_HIT_PCT,
            CASE
                WHEN a.TOTAL_CACHE_READ::FLOAT
                     / NULLIF(a.TOTAL_CACHE_READ + a.TOTAL_INPUT, 0) * 100 >= 70 THEN 'GOOD'
                WHEN a.TOTAL_CACHE_READ::FLOAT
                     / NULLIF(a.TOTAL_CACHE_READ + a.TOTAL_INPUT, 0) * 100 >= 50 THEN 'FAIR'
                ELSE 'LOW'
            END AS CACHE_HEALTH
        FROM user_agg a
        JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON a.USER_ID = u.USER_ID
        WHERE (a.TOTAL_CACHE_READ + a.TOTAL_INPUT) > 0
        ORDER BY CACHE_HIT_PCT ASC
    """)


@st.cache_data(ttl=900, show_spinner="Loading output ratios...")
def load_output_ratio(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        user_agg AS (
            SELECT
                c.USER_ID,
                COUNT(DISTINCT c.REQUEST_ID)                                               AS TOTAL_QUERIES,
                SUM(CASE WHEN mdl.KEY = 'input'  THEN mdl.VALUE::NUMBER ELSE 0 END)       AS TOTAL_INPUT,
                SUM(CASE WHEN mdl.KEY = 'output' THEN mdl.VALUE::NUMBER ELSE 0 END)       AS TOTAL_OUTPUT
            FROM cortex_code_usage c,
                LATERAL FLATTEN(INPUT => c.TOKENS_GRANULAR) svc,
                LATERAL FLATTEN(INPUT => svc.VALUE) mdl
            GROUP BY 1
        )
        SELECT
            u.NAME AS USER_NAME,
            a.TOTAL_QUERIES,
            ROUND(a.TOTAL_OUTPUT::FLOAT / NULLIF(a.TOTAL_QUERIES, 0), 0) AS AVG_OUTPUT_TOKENS,
            ROUND(a.TOTAL_INPUT::FLOAT  / NULLIF(a.TOTAL_QUERIES, 0), 0) AS AVG_INPUT_TOKENS,
            ROUND(a.TOTAL_OUTPUT::FLOAT / NULLIF(a.TOTAL_INPUT, 0), 2)   AS OUTPUT_TO_INPUT_RATIO,
            CASE
                WHEN a.TOTAL_OUTPUT::FLOAT / NULLIF(a.TOTAL_INPUT, 0) > 3.0 THEN 'HIGH'
                WHEN a.TOTAL_OUTPUT::FLOAT / NULLIF(a.TOTAL_INPUT, 0) > 2.0 THEN 'ELEVATED'
                ELSE 'NORMAL'
            END AS COST_FLAG
        FROM user_agg a
        JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON a.USER_ID = u.USER_ID
        WHERE a.TOTAL_QUERIES >= 5
        ORDER BY OUTPUT_TO_INPUT_RATIO DESC
    """)


@st.cache_data(ttl=900, show_spinner="Loading trends...")
def load_daily_trends(source_filter: str, time_filter: str) -> pd.DataFrame:
    trunc, _ = _trend_granularity(time_filter)
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        bucketed AS (
            SELECT
                DATE_TRUNC('{trunc}', c.USAGE_TIME)::TIMESTAMP_NTZ AS BUCKET,
                COUNT(DISTINCT c.USER_ID)                           AS ACTIVE_USERS,
                COUNT(*)                                            AS TOTAL_QUERIES,
                ROUND(SUM(c.TOKEN_CREDITS), 4)                      AS BUCKET_CREDITS
            FROM cortex_code_usage c
            GROUP BY 1
        )
        SELECT
            BUCKET,
            ACTIVE_USERS,
            TOTAL_QUERIES,
            BUCKET_CREDITS,
            LAG(BUCKET_CREDITS) OVER (ORDER BY BUCKET)     AS PREV_CREDITS,
            ROUND((BUCKET_CREDITS - LAG(BUCKET_CREDITS) OVER (ORDER BY BUCKET))
                / NULLIF(LAG(BUCKET_CREDITS) OVER (ORDER BY BUCKET), 0) * 100, 1) AS DOD_CHANGE_PCT,
            ACTIVE_USERS - COALESCE(LAG(ACTIVE_USERS) OVER (ORDER BY BUCKET), 0) AS NET_NEW_USERS
        FROM bucketed
        ORDER BY BUCKET
    """)


@st.cache_data(ttl=900, show_spinner="Loading AI services breakdown...")
def load_ai_services_breakdown(time_filter: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT 'Cortex Code CLI' AS SERVICE,
               ROUND(SUM(TOKEN_CREDITS), 2) AS MTD_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
        WHERE USAGE_TIME >= {time_filter}
        UNION ALL
        SELECT 'Cortex Code Snowsight',
               ROUND(SUM(TOKEN_CREDITS), 2)
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
        WHERE USAGE_TIME >= {time_filter}
        UNION ALL
        SELECT 'Total AI Services (billing)', ROUND(SUM(CREDITS_USED), 2)
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE SERVICE_TYPE = 'AI_SERVICES'
          AND USAGE_DATE >= CURRENT_DATE()
    """)


@st.cache_data(ttl=900, show_spinner="Loading surface breakdown...")
def load_surface_breakdown(time_filter: str) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            '{SRC_CLI}' AS SOURCE,
            COUNT(DISTINCT USER_ID) AS ACTIVE_USERS,
            COUNT(*) AS TOTAL_QUERIES,
            ROUND(SUM(TOKEN_CREDITS), 4) AS TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
        WHERE USAGE_TIME >= {time_filter}
        UNION ALL
        SELECT
            '{SRC_UI}' AS SOURCE,
            COUNT(DISTINCT USER_ID) AS ACTIVE_USERS,
            COUNT(*) AS TOTAL_QUERIES,
            ROUND(SUM(TOKEN_CREDITS), 4) AS TOTAL_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
        WHERE USAGE_TIME >= {time_filter}
    """)


@st.cache_data(ttl=900, show_spinner="Loading user-model heatmap...")
def load_user_model_heatmap(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        flattened AS (
            SELECT
                c.USER_ID, c.REQUEST_ID,
                svc.KEY AS MODEL_NAME,
                mdl.VALUE::NUMBER AS TOKEN_COUNT
            FROM cortex_code_usage c,
                LATERAL FLATTEN(INPUT => c.TOKENS_GRANULAR) svc,
                LATERAL FLATTEN(INPUT => svc.VALUE) mdl
        )
        SELECT
            u.NAME AS USER_NAME,
            f.MODEL_NAME,
            COUNT(DISTINCT f.REQUEST_ID) AS QUERY_COUNT,
            ROUND(SUM(f.TOKEN_COUNT), 0) AS TOTAL_TOKENS
        FROM flattened f
        JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
        GROUP BY 1, 2
        ORDER BY 1, TOTAL_TOKENS DESC
    """)


@st.cache_data(ttl=900, show_spinner="Loading new user onboarding...")
def load_new_user_onboarding(source_filter: str, time_filter: str) -> pd.DataFrame:
    cte = _source_cte(source_filter, time_filter)
    return run_query(f"""
        WITH {cte},
        first_use AS (
            SELECT c.USER_ID,
                MIN(DATE_TRUNC('day', c.USAGE_TIME)::DATE) AS FIRST_USE_DATE
            FROM cortex_code_usage c
            GROUP BY 1
        )
        SELECT
            FIRST_USE_DATE,
            COUNT(*) AS NEW_USERS,
            SUM(COUNT(*)) OVER (ORDER BY FIRST_USE_DATE
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CUMULATIVE_USERS
        FROM first_use
        GROUP BY 1
        ORDER BY 1
    """)


@st.cache_data(ttl=900, show_spinner="Loading rolling 24h spend...")
def load_rolling_24h_spend() -> pd.DataFrame:
    """Always queries the fixed rolling 24h window — used for daily limit monitoring."""
    ts24 = "DATEADD('hour', -24, CURRENT_TIMESTAMP())"
    return run_query(f"""
        SELECT
            u.NAME                                                                          AS USER_NAME,
            ROUND(SUM(CASE WHEN ch.src = 'CLI'       THEN ch.TOKEN_CREDITS ELSE 0 END), 4) AS CLI_24H_CREDITS,
            ROUND(SUM(CASE WHEN ch.src = 'Snowsight' THEN ch.TOKEN_CREDITS ELSE 0 END), 4) AS SNOWSIGHT_24H_CREDITS,
            ROUND(SUM(ch.TOKEN_CREDITS), 4)                                                 AS TOTAL_24H_CREDITS,
            COUNT(*)                                                                        AS TOTAL_24H_REQUESTS
        FROM (
            SELECT USER_ID, TOKEN_CREDITS, 'CLI' AS src
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
            WHERE USAGE_TIME >= {ts24}
            UNION ALL
            SELECT USER_ID, TOKEN_CREDITS, 'Snowsight' AS src
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
            WHERE USAGE_TIME >= {ts24}
        ) ch
        JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON ch.USER_ID = u.USER_ID
        GROUP BY 1
        ORDER BY TOTAL_24H_CREDITS DESC
    """)


# =============================================================================
# Data availability check
# =============================================================================

def _has_real_data(time_filter: str) -> bool:
    try:
        df = run_query(f"""
            SELECT COUNT(*) AS CNT FROM (
                SELECT REQUEST_ID FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
                WHERE USAGE_TIME >= {time_filter}
                UNION ALL
                SELECT REQUEST_ID FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
                WHERE USAGE_TIME >= {time_filter}
            )
        """)
        return int(df.iloc[0]["cnt"]) > 0
    except Exception:
        return False


def get_data(real_loader, demo_loader, use_demo: bool, *args) -> pd.DataFrame:
    if use_demo:
        return demo_loader()
    return real_loader(*args)


# =============================================================================
# Chart utilities
# =============================================================================

def _health_color_style(val: str) -> str:
    colors = {
        "GOOD": CHART_GREEN, "FAIR": CHART_AMBER, "LOW": CHART_RED,
        "NORMAL": CHART_GREEN, "ELEVATED": CHART_AMBER, "HIGH": CHART_RED,
    }
    return f"color: {colors.get(val, '#6b7280')}; font-weight: 600"


def _threshold_rule(x_val: float, color: str = CHART_RED) -> alt.Chart:
    return (
        alt.Chart(pd.DataFrame({"x": [x_val]}))
        .mark_rule(color=color, strokeDash=[4, 4], strokeWidth=1.5)
        .encode(x="x:Q")
    )


# =============================================================================
# Sidebar
# =============================================================================

get_connection()

with st.sidebar:
    st.markdown("### Time range")
    time_range_label = st.selectbox(
        "Show data from:",
        list(TIME_RANGE_OPTIONS.keys()),
        index=0,  # default: Last 12 hours
    )
    TIME_FILTER = TIME_RANGE_OPTIONS[time_range_label]

    st.markdown("---")

    st.markdown("### Data source")
    has_data = _has_real_data(TIME_FILTER)
    if has_data:
        use_demo = st.toggle("Show demo data", value=False)
        if use_demo:
            st.caption("Showing synthetic demo data")
        else:
            st.caption(f"Live Snowflake data — {time_range_label}")
    else:
        use_demo = True
        st.warning(
            f"No Cortex Code usage found in the {time_range_label.lower()}. "
            "Showing synthetic demo data.",
            icon=":material/info:",
        )

    st.markdown("---")

    st.markdown("### Surface filter")
    surface_filter = st.radio(
        "Show usage from:",
        options=[SRC_ALL, SRC_CLI, SRC_UI],
        index=0,
        help=(
            "**All** — combine CLI and Snowsight\n\n"
            "**CLI** — terminal / IDE usage only\n\n"
            "**Snowsight** — browser UI usage only"
        ),
        disabled=use_demo,
    )
    if use_demo:
        st.caption("Disabled in demo mode.")

    st.markdown("---")

    st.markdown("### Cost alert threshold")
    daily_budget = st.number_input(
        "Daily credits per user",
        min_value=0.1,
        value=5.0,
        step=0.5,
        help=(
            "Visual alert threshold on the **Cost Controls** tab. "
            "Users exceeding this in the rolling 24h window are flagged.\n\n"
            "To enforce a hard limit at the account level:\n\n"
            "`ALTER ACCOUNT SET`\n"
            "`CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = N`"
        ),
    )

    st.markdown("---")
    st.caption(
        "Sources: `SNOWFLAKE.ACCOUNT_USAGE`\n\n"
        "`CORTEX_CODE_CLI_USAGE_HISTORY`\n"
        "`CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY`\n\n"
        f"Window: {time_range_label}\n\n"
        "Latency: 45 min – 2 hr\n\n"
        "Pricing live Apr 1 2026  ·  1 credit ≈ $2 USD"
    )


# =============================================================================
# Page header
# =============================================================================

hdr_left, hdr_right = st.columns([4, 1])
with hdr_left:
    st.markdown("# Cortex Code cost dashboard")
with hdr_right:
    if st.button("Refresh", type="secondary", icon=":material/refresh:"):
        st.cache_data.clear()
        st.rerun()

if use_demo:
    st.caption(
        ":material/science: **Demo mode** — synthetic data  |  "
        "Toggle in sidebar to switch to live data"
    )
else:
    surface_label = {
        SRC_ALL: "CLI + Snowsight", SRC_CLI: "CLI only", SRC_UI: "Snowsight only",
    }[surface_filter]
    st.caption(
        f":material/cloud: Connected  |  Surface: **{surface_label}**  |  "
        f"Window: **{time_range_label}**  |  "
        "`SNOWFLAKE.ACCOUNT_USAGE` (45 min – 2 hr latency)"
    )


# =============================================================================
# Tabs
# =============================================================================

tab_overview, tab_users, tab_models, tab_efficiency, tab_controls, tab_trends = st.tabs([
    ":material/dashboard: Overview",
    ":material/group: Users",
    ":material/smart_toy: Models",
    ":material/bolt: Efficiency",
    ":material/shield: Cost controls",
    ":material/trending_up: Trends",
])


# -----------------------------------------------------------------------------
# TAB 1 — Overview
# -----------------------------------------------------------------------------
with tab_overview:
    summary = get_data(
        load_executive_summary, demo_executive_summary, use_demo,
        surface_filter, TIME_FILTER,
    )

    if summary.empty or summary.iloc[0]["mtd_credits"] is None:
        st.info("No Cortex Code usage data found for the selected time range.")
    else:
        row = summary.iloc[0]

        # KPI row 1 — spend
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Credits", f"{row['mtd_credits']:,.2f}",
                  help=f"Total TOKEN_CREDITS in the {time_range_label.lower()}")
        k2.metric("Est. cost (USD)", f"${row['est_cost_usd']:,.2f}",
                  help="Credits × $2.00 (Apr 1 2026 pricing)")
        k3.metric("Projected monthly credits", f"{row['projected_monthly']:,.2f}",
                  help="Daily average × 30 days")
        k4.metric("Projected monthly (USD)", f"${row['projected_monthly_usd']:,.2f}")

        # KPI row 2 — activity
        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Active users", f"{int(row['active_users'])}")
        k6.metric("Total queries", f"{int(row['total_queries']):,}")
        k7.metric("Credits / user", f"{row['credits_per_user']:,.2f}")
        k8.metric("Queries / user", f"{row['queries_per_user']:,.1f}")

        # KPI row 3 — efficiency signals
        cache_pct = row["overall_cache_hit_pct"]
        output_ratio = row["overall_output_ratio"]
        k9, k10, _ = st.columns(3)
        k9.metric(
            "Cache hit rate",
            f"{cache_pct:.1f}%" if cache_pct is not None else "N/A",
            help="cache_read_input / (cache_read_input + input). Higher = cheaper. Target ≥70%.",
        )
        k10.metric(
            "Output / input ratio",
            f"{output_ratio:.2f}x" if output_ratio is not None else "N/A",
            help="Output tokens cost ~5× input. Ratios >3× indicate verbose generation.",
        )

    st.markdown("---")

    # CLI vs Snowsight split
    st.subheader(f"CLI vs Snowsight — {time_range_label}")
    surface_df = get_data(load_surface_breakdown, demo_surface_breakdown, use_demo, TIME_FILTER)
    if not surface_df.empty:
        scol1, scol2, scol3 = st.columns(3)
        for _, r in surface_df.iterrows():
            target = scol1 if r["source"] == SRC_CLI else scol2
            with target:
                with st.container(border=True):
                    st.markdown(f"**{r['source']}**")
                    st.metric("Credits", f"{r['total_credits']:,.4f}")
                    st.metric("Est. cost (USD)", f"${r['total_credits'] * CREDITS_PER_USD:,.2f}")
                    st.metric("Queries", f"{int(r['total_queries']):,}")
                    st.metric("Active users", f"{int(r['active_users'])}")

        with scol3:
            donut = (
                alt.Chart(surface_df)
                .mark_arc(innerRadius=50)
                .encode(
                    theta=alt.Theta("total_credits:Q"),
                    color=alt.Color(
                        "source:N",
                        scale=alt.Scale(domain=[SRC_CLI, SRC_UI], range=[CHART_TEAL, CHART_AMBER]),
                        legend=alt.Legend(orient="bottom", title=None),
                    ),
                    tooltip=[
                        alt.Tooltip("source:N", title="Surface"),
                        alt.Tooltip("total_credits:Q", title="Credits", format=",.4f"),
                        alt.Tooltip("total_queries:Q", title="Queries"),
                    ],
                )
                .properties(height=240, title="Credit share by surface")
            )
            st.altair_chart(donut, use_container_width=True)

    st.markdown("---")

    # Spend trend
    st.subheader(f"Daily spend trend — {time_range_label}")
    daily = get_data(
        load_daily_spend_trend, demo_daily_spend_trend, use_demo, surface_filter, TIME_FILTER,
    )
    if not daily.empty:
        daily["usage_date"] = pd.to_datetime(daily["usage_date"])
        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown("**Credits per day**")
                chart = (
                    alt.Chart(daily)
                    .mark_area(opacity=0.25, line={"color": CHART_TEAL}, color=CHART_TEAL)
                    .encode(
                        x=alt.X("usage_date:T", title=None),
                        y=alt.Y("daily_credits:Q", title="Credits"),
                        tooltip=[
                            alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                            alt.Tooltip("daily_credits:Q", title="Credits", format=",.4f"),
                            alt.Tooltip("active_users:Q", title="Active users"),
                            alt.Tooltip("total_queries:Q", title="Queries"),
                        ],
                    )
                    .properties(height=CHART_HEIGHT)
                    .interactive()
                )
                st.altair_chart(chart, use_container_width=True)
        with col2:
            with st.container(border=True):
                st.markdown("**Est. cost (USD) per day**")
                chart2 = (
                    alt.Chart(daily)
                    .mark_bar(color=CHART_AMBER, opacity=0.85)
                    .encode(
                        x=alt.X("usage_date:T", title=None),
                        y=alt.Y("est_cost_usd:Q", title="Est. cost USD"),
                        tooltip=[
                            alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                            alt.Tooltip("est_cost_usd:Q", title="Est. USD", format="$,.2f"),
                            alt.Tooltip("daily_credits:Q", title="Credits", format=",.4f"),
                        ],
                    )
                    .properties(height=CHART_HEIGHT)
                    .interactive()
                )
                st.altair_chart(chart2, use_container_width=True)
    else:
        st.info("No spend data available for the selected time range.")

    st.markdown("---")

    # AI services billing context
    st.subheader("AI Services billing context")
    st.caption("Cortex Code credits vs. total AI Services consumption for today.")
    breakdown = get_data(load_ai_services_breakdown, demo_ai_services_breakdown, use_demo, TIME_FILTER)
    if not breakdown.empty:
        svc_cols = st.columns(len(breakdown))
        for i, (_, r) in enumerate(breakdown.iterrows()):
            label = r["service"]
            val = r["mtd_credits"]
            help_txt = (
                "From METERING_DAILY_HISTORY (SERVICE_TYPE = 'AI_SERVICES') — today's total. "
                "Includes Cortex Code, Cortex Agents, Snowflake Intelligence, and other AI services."
                if "Total" in label else None
            )
            svc_cols[i].metric(label, f"{val:,.2f} credits", help=help_txt)


# -----------------------------------------------------------------------------
# TAB 2 — Users
# -----------------------------------------------------------------------------
with tab_users:
    st.subheader(f"Per-user breakdown — {time_range_label}")
    users_df = get_data(
        load_user_breakdown, demo_user_breakdown, use_demo, surface_filter, TIME_FILTER,
    )

    if not users_df.empty:
        top_n = min(15, len(users_df))
        top_users = users_df.head(top_n)

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown("**Credits by user**")
                chart = (
                    alt.Chart(top_users)
                    .mark_bar(color=CHART_TEAL)
                    .encode(
                        x=alt.X("total_credits:Q", title="Credits"),
                        y=alt.Y("user_name:N", title=None, sort="-x"),
                        tooltip=[
                            alt.Tooltip("user_name:N", title="User"),
                            alt.Tooltip("total_credits:Q", title="Credits", format=",.4f"),
                            alt.Tooltip("est_cost_usd:Q", title="Est. USD", format="$,.2f"),
                            alt.Tooltip("total_queries:Q", title="Queries"),
                        ],
                    )
                    .properties(height=max(CHART_HEIGHT, top_n * 28))
                )
                st.altair_chart(chart, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown("**Est. cost (USD) by user**")
                chart2 = (
                    alt.Chart(top_users)
                    .mark_bar(color=CHART_AMBER)
                    .encode(
                        x=alt.X("est_cost_usd:Q", title="Est. cost USD"),
                        y=alt.Y("user_name:N", title=None, sort="-x"),
                        tooltip=[
                            alt.Tooltip("user_name:N", title="User"),
                            alt.Tooltip("est_cost_usd:Q", title="Est. USD", format="$,.2f"),
                            alt.Tooltip("total_credits:Q", title="Credits", format=",.4f"),
                            alt.Tooltip("total_queries:Q", title="Queries"),
                        ],
                    )
                    .properties(height=max(CHART_HEIGHT, top_n * 28))
                )
                st.altair_chart(chart2, use_container_width=True)

        with st.expander("Full user table", icon=":material/table:"):
            st.dataframe(
                users_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "total_credits": st.column_config.NumberColumn("Credits", format="%.4f"),
                    "est_cost_usd": st.column_config.NumberColumn("Est. cost (USD)", format="$%.2f"),
                    "avg_credit_per_query": st.column_config.NumberColumn("Credits/query", format="%.6f"),
                },
            )
    else:
        st.info("No user data available for the selected time range.")

    st.markdown("---")

    st.subheader(f"User × model heatmap — {time_range_label}")
    heatmap_df = get_data(
        load_user_model_heatmap, demo_user_model_heatmap, use_demo, surface_filter, TIME_FILTER,
    )
    if not heatmap_df.empty:
        n_users = heatmap_df["user_name"].nunique()
        chart = (
            alt.Chart(heatmap_df)
            .mark_rect()
            .encode(
                x=alt.X("model_name:N", title="Model"),
                y=alt.Y("user_name:N", title=None),
                color=alt.Color("query_count:Q", title="Queries", scale=alt.Scale(scheme="blues")),
                tooltip=[
                    alt.Tooltip("user_name:N", title="User"),
                    alt.Tooltip("model_name:N", title="Model"),
                    alt.Tooltip("query_count:Q", title="Queries"),
                    alt.Tooltip("total_tokens:Q", title="Tokens", format=","),
                ],
            )
            .properties(height=max(CHART_HEIGHT, n_users * 30))
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No granular model data available.")


# -----------------------------------------------------------------------------
# TAB 3 — Models
# -----------------------------------------------------------------------------
with tab_models:
    # Token distribution
    st.subheader(f"Model token distribution — {time_range_label}")
    models_df = get_data(
        load_model_distribution, demo_model_distribution, use_demo, surface_filter, TIME_FILTER,
    )

    if not models_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown("**Token share by model**")
                chart = (
                    alt.Chart(models_df)
                    .mark_arc(innerRadius=60)
                    .encode(
                        theta=alt.Theta("total_tokens:Q"),
                        color=alt.Color("model_name:N", legend=alt.Legend(orient="bottom", title=None)),
                        tooltip=[
                            alt.Tooltip("model_name:N", title="Model"),
                            alt.Tooltip("total_tokens:Q", title="Tokens", format=","),
                            alt.Tooltip("pct_of_total:Q", title="% of total", format=".1f"),
                        ],
                    )
                    .properties(height=CHART_HEIGHT)
                )
                st.altair_chart(chart, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown("**Query count by model**")
                chart = (
                    alt.Chart(models_df)
                    .mark_bar(color=CHART_TEAL)
                    .encode(
                        x=alt.X("query_count:Q", title="Queries"),
                        y=alt.Y("model_name:N", title=None, sort="-x"),
                        tooltip=[
                            alt.Tooltip("model_name:N", title="Model"),
                            alt.Tooltip("query_count:Q", title="Queries"),
                            alt.Tooltip("unique_users:Q", title="Unique users"),
                        ],
                    )
                    .properties(height=CHART_HEIGHT)
                )
                st.altair_chart(chart, use_container_width=True)

    st.markdown("---")

    # Model-level credit cost breakdown (C4 pattern from cost-queries.sql)
    st.subheader(f"Model-level credit cost breakdown — {time_range_label}")
    st.caption(
        "Credits calculated per token type using authoritative per-model rates. "
        "Output tokens cost ~5× input. Cache reads cost ~10% of input tokens."
    )

    cost_df = get_data(
        load_model_cost_breakdown, demo_model_cost_breakdown, use_demo, surface_filter, TIME_FILTER,
    )
    if not cost_df.empty:
        # Stacked bar: credit composition per model
        credit_cols = ["input_credits", "output_credits", "cache_read_credits", "cache_write_credits"]
        cost_melt = cost_df.melt(
            id_vars=["model_name"],
            value_vars=credit_cols,
            var_name="token_type",
            value_name="credits",
        )
        cost_melt["token_type"] = (
            cost_melt["token_type"]
            .str.replace("_credits", "", regex=False)
            .str.replace("_", " ", regex=False)
            .str.title()
        )

        stacked = (
            alt.Chart(cost_melt)
            .mark_bar()
            .encode(
                x=alt.X("credits:Q", title="Credits"),
                y=alt.Y("model_name:N", title=None, sort="-x"),
                color=alt.Color(
                    "token_type:N",
                    scale=alt.Scale(
                        domain=["Input", "Output", "Cache Read", "Cache Write"],
                        range=[CHART_TEAL, CHART_RED, CHART_GREEN, CHART_AMBER],
                    ),
                    legend=alt.Legend(orient="right", title="Token type"),
                ),
                tooltip=[
                    alt.Tooltip("model_name:N", title="Model"),
                    alt.Tooltip("token_type:N", title="Token type"),
                    alt.Tooltip("credits:Q", title="Credits", format=",.4f"),
                ],
            )
            .properties(height=CHART_HEIGHT, title="Credit cost by token type per model")
        )
        st.altair_chart(stacked, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            with st.expander("Model rates reference (credits per million tokens)", icon=":material/info:"):
                rates_df = pd.DataFrame([
                    {"Model": "claude-4-sonnet",   "Input": 1.50, "Output": 7.50,  "Cache write": 1.88, "Cache read": 0.15},
                    {"Model": "claude-sonnet-4-5", "Input": 1.65, "Output": 8.25,  "Cache write": 2.06, "Cache read": 0.17},
                    {"Model": "claude-sonnet-4-6", "Input": 1.65, "Output": 8.25,  "Cache write": 2.07, "Cache read": 0.17},
                    {"Model": "claude-opus-4-5",   "Input": 2.75, "Output": 13.75, "Cache write": 3.44, "Cache read": 0.28},
                    {"Model": "claude-opus-4-6",   "Input": 2.75, "Output": 13.75, "Cache write": 3.44, "Cache read": 0.28},
                    {"Model": "openai-gpt-5.2",    "Input": 0.97, "Output": 7.70,  "Cache write": None, "Cache read": 0.10},
                ])
                st.dataframe(rates_df, use_container_width=True, hide_index=True)

        with col2:
            with st.expander("Full model cost table", icon=":material/table:"):
                st.dataframe(
                    cost_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "total_credits_calc": st.column_config.NumberColumn("Total credits", format="%.4f"),
                        "est_cost_usd": st.column_config.NumberColumn("Est. cost (USD)", format="$%.2f"),
                        "input_credits": st.column_config.NumberColumn("Input credits", format="%.4f"),
                        "output_credits": st.column_config.NumberColumn("Output credits", format="%.4f"),
                        "cache_read_credits": st.column_config.NumberColumn("Cache read credits", format="%.4f"),
                        "cache_write_credits": st.column_config.NumberColumn("Cache write credits", format="%.4f"),
                    },
                )
    else:
        st.info("No model cost data available. TOKENS_GRANULAR must be populated.")


# -----------------------------------------------------------------------------
# TAB 4 — Efficiency
# -----------------------------------------------------------------------------
with tab_efficiency:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Cache efficiency — {time_range_label}")
        cache_df = get_data(
            load_cache_efficiency, demo_cache_efficiency, use_demo, surface_filter, TIME_FILTER,
        )
        if not cache_df.empty:
            chart = (
                alt.Chart(cache_df)
                .mark_bar()
                .encode(
                    x=alt.X("cache_hit_pct:Q", title="Cache hit %", scale=alt.Scale(domain=[0, 100])),
                    y=alt.Y("user_name:N", title=None, sort="x"),
                    color=alt.Color(
                        "cache_health:N",
                        scale=alt.Scale(
                            domain=["GOOD", "FAIR", "LOW"],
                            range=[CHART_GREEN, CHART_AMBER, CHART_RED],
                        ),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("user_name:N", title="User"),
                        alt.Tooltip("cache_hit_pct:Q", title="Cache hit %", format=".1f"),
                        alt.Tooltip("cache_health:N", title="Status"),
                        alt.Tooltip("cache_read_tokens:Q", title="Cache read tokens", format=","),
                        alt.Tooltip("input_tokens:Q", title="Input tokens", format=","),
                    ],
                )
                .properties(height=max(CHART_HEIGHT, len(cache_df) * 28))
            )
            st.altair_chart(
                chart + _threshold_rule(70, CHART_GREEN),
                use_container_width=True,
            )
            st.caption("Green dashed line = 70% target. Cache reads cost ~10% of normal input tokens.")
        else:
            st.info("No cache efficiency data available.")

    with col2:
        st.subheader(f"Output / input ratio — {time_range_label}")
        ratio_df = get_data(
            load_output_ratio, demo_output_ratio, use_demo, surface_filter, TIME_FILTER,
        )
        if not ratio_df.empty:
            chart = (
                alt.Chart(ratio_df)
                .mark_bar()
                .encode(
                    x=alt.X("output_to_input_ratio:Q", title="Output / input ratio"),
                    y=alt.Y("user_name:N", title=None, sort="-x"),
                    color=alt.Color(
                        "cost_flag:N",
                        scale=alt.Scale(
                            domain=["HIGH", "ELEVATED", "NORMAL"],
                            range=[CHART_RED, CHART_AMBER, CHART_GREEN],
                        ),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("user_name:N", title="User"),
                        alt.Tooltip("output_to_input_ratio:Q", title="Ratio", format=".2f"),
                        alt.Tooltip("avg_output_tokens:Q", title="Avg output tokens", format=","),
                        alt.Tooltip("avg_input_tokens:Q", title="Avg input tokens", format=","),
                        alt.Tooltip("total_queries:Q", title="Queries"),
                        alt.Tooltip("cost_flag:N", title="Flag"),
                    ],
                )
                .properties(height=max(CHART_HEIGHT, len(ratio_df) * 28))
            )
            st.altair_chart(
                chart + _threshold_rule(3.0, CHART_RED),
                use_container_width=True,
            )
            st.caption("Red dashed line = 3.0× flag threshold. Output tokens cost ~5× input.")
        else:
            st.info("No output ratio data available.")


# -----------------------------------------------------------------------------
# TAB 5 — Cost Controls  (NEW — Apr 2, 2026 features)
# -----------------------------------------------------------------------------
with tab_controls:
    st.info(
        "**New (Apr 2, 2026):** Snowflake introduced daily estimated credit limits per user "
        "for both CLI and Snowsight. Limits operate on a rolling 24-hour window.\n\n"
        "- `-1` = no limit &nbsp;&nbsp; `0` = block access &nbsp;&nbsp; positive = rolling 24h cap\n"
        "- Set at account level, override per user\n"
        "- Temporary permissions note: accountadmin-only enforcement tightening in next GS release",
        icon=":material/new_releases:",
    )

    gcol1, gcol2 = st.columns(2)
    with gcol1:
        with st.container(border=True):
            st.markdown("**Set account-level limits**")
            st.code(
                "-- CLI daily limit (all users)\n"
                "ALTER ACCOUNT SET\n"
                "  CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = 5;\n\n"
                "-- Snowsight daily limit (all users)\n"
                "ALTER ACCOUNT SET\n"
                "  CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = 5;",
                language="sql",
            )
    with gcol2:
        with st.container(border=True):
            st.markdown("**Override per user**")
            st.code(
                "-- Override for a specific user\n"
                "ALTER USER alice\n"
                "  SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = 10;\n\n"
                "-- Check current settings\n"
                "SHOW PARAMETERS LIKE 'CORTEX_CODE%' IN ACCOUNT;\n"
                "SHOW PARAMETERS LIKE 'CORTEX_CODE%' IN USER alice;",
                language="sql",
            )

    st.markdown("---")
    st.subheader("Rolling 24h spend per user")
    st.caption(
        f"Always shows the last 24 hours regardless of the time range filter above. "
        f"Alert threshold from sidebar: **{daily_budget:.1f} credits** "
        f"(≈ ${daily_budget * CREDITS_PER_USD:.2f} USD)."
    )

    spend_24h = get_data(load_rolling_24h_spend, demo_rolling_24h_spend, use_demo)

    if not spend_24h.empty:
        spend_24h = spend_24h.copy()
        spend_24h["pct_of_limit"] = (spend_24h["total_24h_credits"] / daily_budget * 100).round(1)
        spend_24h["alert"] = spend_24h["pct_of_limit"].apply(
            lambda p: "Over limit" if p >= 100 else "Near limit" if p >= 80 else "OK"
        )
        spend_24h["est_cost_usd"] = (spend_24h["total_24h_credits"] * CREDITS_PER_USD).round(2)

        over = (spend_24h["alert"] == "Over limit").sum()
        near = (spend_24h["alert"] == "Near limit").sum()
        ok   = (spend_24h["alert"] == "OK").sum()

        al1, al2, al3 = st.columns(3)
        al1.metric(
            "Over limit", over,
            help=f"Users at or above {daily_budget:.1f} credits in the last 24h",
        )
        al2.metric(
            "Near limit (≥80%)", near,
            help=f"Users between {daily_budget * 0.8:.1f} and {daily_budget:.1f} credits",
        )
        al3.metric("Within limit", ok)

        alert_chart = (
            alt.Chart(spend_24h)
            .mark_bar()
            .encode(
                x=alt.X("total_24h_credits:Q", title="Credits (rolling 24h)"),
                y=alt.Y("user_name:N", title=None, sort="-x"),
                color=alt.Color(
                    "alert:N",
                    scale=alt.Scale(
                        domain=["Over limit", "Near limit", "OK"],
                        range=[CHART_RED, CHART_AMBER, CHART_GREEN],
                    ),
                    legend=alt.Legend(title="Status"),
                ),
                tooltip=[
                    alt.Tooltip("user_name:N", title="User"),
                    alt.Tooltip("total_24h_credits:Q", title="24h credits", format=",.4f"),
                    alt.Tooltip("est_cost_usd:Q", title="Est. USD", format="$,.2f"),
                    alt.Tooltip("cli_24h_credits:Q", title="CLI credits", format=",.4f"),
                    alt.Tooltip("snowsight_24h_credits:Q", title="Snowsight credits", format=",.4f"),
                    alt.Tooltip("pct_of_limit:Q", title="% of threshold", format=".1f"),
                    alt.Tooltip("alert:N", title="Status"),
                    alt.Tooltip("total_24h_requests:Q", title="Requests"),
                ],
            )
            .properties(height=max(CHART_HEIGHT, len(spend_24h) * 28))
        )
        st.altair_chart(
            alert_chart + _threshold_rule(daily_budget),
            use_container_width=True,
        )

        with st.expander("Full 24h spend table", icon=":material/table:"):
            st.dataframe(
                spend_24h[[
                    "user_name", "cli_24h_credits", "snowsight_24h_credits",
                    "total_24h_credits", "est_cost_usd", "pct_of_limit", "alert", "total_24h_requests",
                ]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "user_name": "User",
                    "cli_24h_credits": st.column_config.NumberColumn("CLI credits (24h)", format="%.4f"),
                    "snowsight_24h_credits": st.column_config.NumberColumn("Snowsight credits (24h)", format="%.4f"),
                    "total_24h_credits": st.column_config.NumberColumn("Total credits (24h)", format="%.4f"),
                    "est_cost_usd": st.column_config.NumberColumn("Est. cost (USD)", format="$%.2f"),
                    "pct_of_limit": st.column_config.NumberColumn("% of threshold", format="%.1f%%"),
                    "alert": "Status",
                    "total_24h_requests": st.column_config.NumberColumn("Requests", format="%d"),
                },
            )
    else:
        st.info("No usage in the last 24 hours.")

    st.markdown("---")
    st.subheader("Governance & model access controls")

    gcol1, gcol2 = st.columns(2)
    with gcol1:
        with st.container(border=True):
            st.markdown("**Model governance — dual requirement**")
            st.markdown(
                "A model must be **both on the allowlist AND granted** to the user's role. "
                "Missing either blocks access.\n\n"
                "**Recommended tiered access by model family:**\n\n"
                "| Role tier | Models |  \n"
                "|---|---|  \n"
                "| `ANALYST` | claude-4-sonnet (cost-optimized) |  \n"
                "| `DEVELOPER` | claude-sonnet-4-6, claude-4-sonnet |  \n"
                "| `POWER_USER` | all models incl. opus |  \n"
            )

    with gcol2:
        with st.container(border=True):
            st.markdown("**Managed settings reference**")
            st.markdown(
                "| Setting | Purpose |\n"
                "|---|---|\n"
                "| `confirm` mode | Require approval before executing changes |\n"
                "| `plan` mode | Preview changes before applying — use in prod |\n"
                "| `bypass` mode | Skip confirmations for automation |\n"
                "| `private` sessions | Disable session telemetry |\n"
                "| `daily_est_credit_limit` | Rolling 24h credit cap per user |\n\n"
                "Docs: [Managed settings]"
                "(https://docs.snowflake.com/en/user-guide/cortex-code/settings"
                "#label-cortex-code-managed-settings)"
            )

    st.markdown("---")
    st.subheader("Pricing reference")
    with st.container(border=True):
        st.markdown(
            "Cortex Code (CLI **and** Snowsight) moved to **AI credits billing on Apr 1, 2026**. "
            "Snowsight was previously in free preview.\n\n"
            "- **1 AI credit ≈ $2.00 USD**\n"
            "- Deducted from your AI Services budget\n"
            "- Top-line monitoring: `SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY` "
            "where `SERVICE_TYPE = 'AI_SERVICES'`\n"
            "- Per-user attribution & chargeback: use `CORTEX_CODE_CLI_USAGE_HISTORY` "
            "and `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` separately for accurate channel isolation"
        )


# -----------------------------------------------------------------------------
# TAB 6 — Trends
# -----------------------------------------------------------------------------
with tab_trends:
    trunc_level, time_fmt_str = _trend_granularity(TIME_FILTER)
    bucket_label = "Hourly" if trunc_level == "hour" else "Daily"

    st.subheader(f"{bucket_label} trends — {time_range_label}")

    trends_df = get_data(
        load_daily_trends, demo_daily_trends, use_demo, surface_filter, TIME_FILTER,
    )

    if not trends_df.empty:
        trends_df["bucket"] = pd.to_datetime(trends_df["bucket"])

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown(f"**Credits per {bucket_label.lower()}**")
                chart = (
                    alt.Chart(trends_df)
                    .mark_bar(color=CHART_TEAL)
                    .encode(
                        x=alt.X("bucket:T", title=None),
                        y=alt.Y("bucket_credits:Q", title="Credits"),
                        tooltip=[
                            alt.Tooltip("bucket:T", title=bucket_label, format=time_fmt_str),
                            alt.Tooltip("bucket_credits:Q", title="Credits", format=",.4f"),
                            alt.Tooltip("dod_change_pct:Q", title="Change %", format=".1f"),
                        ],
                    )
                    .properties(height=CHART_HEIGHT)
                )
                st.altair_chart(chart, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown(f"**Active users per {bucket_label.lower()}**")
                chart = (
                    alt.Chart(trends_df)
                    .mark_line(point=True, color=CHART_AMBER)
                    .encode(
                        x=alt.X("bucket:T", title=None),
                        y=alt.Y("active_users:Q", title="Users"),
                        tooltip=[
                            alt.Tooltip("bucket:T", title=bucket_label, format=time_fmt_str),
                            alt.Tooltip("active_users:Q", title="Active users"),
                            alt.Tooltip("net_new_users:Q", title="Net new users"),
                            alt.Tooltip("total_queries:Q", title="Queries"),
                        ],
                    )
                    .properties(height=CHART_HEIGHT)
                )
                st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No trend data available for the selected time range.")

    st.markdown("---")

    st.subheader(f"New user onboarding — {time_range_label}")
    onboard_df = get_data(
        load_new_user_onboarding, demo_new_user_onboarding, use_demo, surface_filter, TIME_FILTER,
    )
    if not onboard_df.empty:
        onboard_df["first_use_date"] = pd.to_datetime(onboard_df["first_use_date"])
        base = alt.Chart(onboard_df).encode(x=alt.X("first_use_date:T", title=None))
        bars = base.mark_bar(color=CHART_TEAL, opacity=0.6).encode(
            y=alt.Y("new_users:Q", title="New users"),
            tooltip=[
                alt.Tooltip("first_use_date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("new_users:Q", title="New users"),
                alt.Tooltip("cumulative_users:Q", title="Cumulative"),
            ],
        )
        line = base.mark_line(color=CHART_RED, strokeWidth=2).encode(
            y=alt.Y("cumulative_users:Q", title="Cumulative users"),
        )
        chart = alt.layer(bars, line).resolve_scale(y="independent").properties(height=CHART_HEIGHT)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No new user data available for the selected time range.")
