# Cortex Code Cost Dashboard

A Streamlit app for Snowflake platform owners to monitor **Cortex Code** usage, credit consumption, and cost efficiency across their organization.

![Dashboard UI](app-1.png)

## What's covered

| Tab | Contents |
|---|---|
| **Overview** | Executive KPIs, CLI vs Snowsight spend split, daily spend trend, AI Services billing context |
| **Users** | Per-user credit/cost breakdown, user × model heatmap |
| **Models** | Token distribution by model, credit cost breakdown by token type |
| **Efficiency** | Cache hit rate per user, output/input ratio per user |
| **Cost Controls** | Rolling 24h spend vs alert threshold, daily limit SQL, governance reference |
| **Trends** | Hourly/daily credit and user trends, new user onboarding growth |

Supports **synthetic demo data** when no real Cortex Code usage is available in the selected time window.

---

## Before you start

### 1. Edit `snowflake.yml`

Open `snowflake.yml` and replace the three placeholder values with your own:

```yaml
identifier:
  database: "<YOUR_DATABASE>"   # e.g. MY_DB
  schema: "<YOUR_SCHEMA>"       # e.g. PUBLIC
query_warehouse: "<YOUR_WAREHOUSE>"  # e.g. COMPUTE_WH
```

Any existing database and schema in your account will work. The warehouse just needs to be able to run queries.

### 2. Required privilege

The Snowflake role used to run the app must have `ACCOUNTADMIN` or `MONITOR USAGE` — both are needed to read `SNOWFLAKE.ACCOUNT_USAGE` views.

That's it. No other account-specific changes are needed. All queries target `SNOWFLAKE.ACCOUNT_USAGE`, which is standard in every Snowflake account.

---

## Data sources

All data is read from `SNOWFLAKE.ACCOUNT_USAGE` — requires `ACCOUNTADMIN` or `MONITOR USAGE` privilege.

| View | Available since |
|---|---|
| `CORTEX_CODE_CLI_USAGE_HISTORY` | 2026-02-15 |
| `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` | 2026-03-11 |
| `METERING_DAILY_HISTORY` | — |

> Views have a latency of **45 minutes to 2 hours**.

---

## Repository structure

```
cortex-code-cost-dashboard/
├── cortex_code_cost_dashboard.py   # Streamlit app (main file)
├── cost-queries.sql                # Reference SQL queries for manual analysis
├── snowflake.yml                   # Snowflake CLI deployment config
├── pyproject.toml                  # Python dependencies
├── .streamlit/
│   └── config.toml                 # Dark theme configuration
└── README.md
```

---

## Option 1 — Deploy with Snowflake CLI (recommended)

### Prerequisites

- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) installed
- A configured Snowflake connection

### Deploy

```bash
snow streamlit deploy --replace
```

This reads `snowflake.yml` and deploys to the database/schema defined there. The app will be available in Snowsight under **Streamlit**.

---

## Option 2 — Deploy with Cortex Code (AI-assisted)

Use [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) to deploy with a natural language prompt — no need to remember CLI flags.

### Prerequisites

- Cortex Code CLI installed
- A configured Snowflake connection

### One-shot (non-interactive)

```bash
cortex -p "Deploy the Streamlit app defined in snowflake.yml to Snowflake" --connection <your-connection>
```

Cortex Code reads `snowflake.yml`, runs `snow streamlit deploy --replace`, and reports the result.

### Interactive

```bash
cortex --connection <your-connection>
```

Then type at the prompt:

```
Deploy the Streamlit app to Snowflake using snowflake.yml
```

Cortex Code will show you the deployment plan and ask for confirmation before executing.

### Tip — plan before deploying

Prefix your prompt with `/plan` to review exactly what will run before anything is executed:

```
/plan Deploy the Streamlit app to Snowflake using snowflake.yml
```

---

## Option 3 — Run locally

### Prerequisites

Python 3.11+ and [uv](https://github.com/astral-sh/uv) (or pip).

### Install dependencies

```bash
uv sync
# or: pip install -e .
```

### Configure a Snowflake connection

Create `.streamlit/secrets.toml` (this file is git-ignored):

```toml
[connections.snowhouse]
account   = "your-account-identifier"
user      = "your-user"
password  = "your-password"
warehouse = "your-warehouse"
role      = "ACCOUNTADMIN"
```

### Run

```bash
streamlit run cortex_code_cost_dashboard.py
```

---

## Option 4 — Upload via Snowsight

1. Open Snowsight → **Streamlit** → **+ Streamlit App**
2. Upload `cortex_code_cost_dashboard.py` as the main file
3. Upload `pyproject.toml` and `.streamlit/config.toml` as additional files
4. Set the warehouse and click **Run**

---

## Sidebar controls

| Control | Description |
|---|---|
| **Time range** | Last 12h / 24h / 7d / 30d / 90d |
| **Demo mode** | Toggle synthetic data on/off (auto-enabled when no real data found) |
| **Surface filter** | All / CLI only / Snowsight only |
| **Daily credit threshold** | Alert threshold for the Cost Controls tab |

---

## Cost controls (April 2, 2026)

Snowflake introduced per-user daily credit limits on a rolling 24-hour window.

```sql
-- Account-level limit for all users
ALTER ACCOUNT SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = 5;
ALTER ACCOUNT SET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = 5;

-- Override for a specific user
ALTER USER alice SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = 10;

-- Inspect current settings
SHOW PARAMETERS LIKE 'CORTEX_CODE%' IN ACCOUNT;
SHOW PARAMETERS LIKE 'CORTEX_CODE%' IN USER alice;
```

`-1` = no limit · `0` = block access · positive value = rolling 24h cap

---

## Reference SQL

`cost-queries.sql` contains standalone queries for manual analysis outside the dashboard:

- **C1** — CLI daily credits & tokens by user
- **C2** — Snowsight daily credits & tokens by user
- **C3** — Combined CLI + Snowsight daily summary by channel
- **C4** — Model-level token & credit breakdown using `TOKENS_GRANULAR`
