


-- ############################################################
-- SECTION C: CUSTOMER-FACING USAGE VIEWS
-- Available in every customer account via SNOWFLAKE.ACCOUNT_USAGE
-- Requires ACCOUNTADMIN or MONITOR USAGE privilege
-- Isolates Cortex Code specifically (no other Cortex AI mixed in)
-- Views available as of: CLI=2026-02-15, Snowsight=2026-03-11
-- ############################################################

-- ============================================================
-- C1. CLI Usage — Daily Credits & Tokens by User
--     Source: SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
-- ============================================================
SELECT
    DATE(USAGE_TIME)                                     AS usage_date,
    USER_ID,
    COUNT(*)                                             AS request_count,
    SUM(TOKENS)                                          AS total_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4)                         AS total_credits,
    ROUND(SUM(TOKEN_CREDITS) * 2.00, 2)                  AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
WHERE USAGE_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, total_credits DESC;

-- ============================================================
-- C2. Snowsight Usage — Daily Credits & Tokens by User
--     Source: SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
--     Note: view created 2026-03-11, no data before that date
-- ============================================================
SELECT
    DATE(USAGE_TIME)                                     AS usage_date,
    USER_ID,
    COUNT(*)                                             AS request_count,
    SUM(TOKENS)                                          AS total_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4)                         AS total_credits,
    ROUND(SUM(TOKEN_CREDITS) * 2.00, 2)                  AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
WHERE USAGE_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, total_credits DESC;

-- ============================================================
-- C3. Combined CLI + Snowsight — Daily Summary by Channel
-- ============================================================
SELECT
    DATE(USAGE_TIME)                                     AS usage_date,
    'CLI'                                                AS channel,
    COUNT(DISTINCT USER_ID)                              AS active_users,
    COUNT(*)                                             AS request_count,
    SUM(TOKENS)                                          AS total_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4)                         AS total_credits,
    ROUND(SUM(TOKEN_CREDITS) * 2.00, 2)                  AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
WHERE USAGE_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2

UNION ALL

SELECT
    DATE(USAGE_TIME),
    'Snowsight',
    COUNT(DISTINCT USER_ID),
    COUNT(*),
    SUM(TOKENS),
    ROUND(SUM(TOKEN_CREDITS), 4),
    ROUND(SUM(TOKEN_CREDITS) * 2.00, 2)
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
WHERE USAGE_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2

ORDER BY 1 DESC, 2;

-- ============================================================
-- C4. Snowsight — Model-Level Token & Credit Breakdown (Customer-Facing)
--     Source: SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
--     Uses TOKENS_GRANULAR to reconstruct credits per model.
--     Required until TOKEN_CREDITS is fully populated in the view
--     (Snowsight billing was not active before ~2026-03-31).
--     Credit source: Eric Gertonson (authoritative model rates)
-- ============================================================
WITH coco_model_costs AS (
    SELECT model_name, input_cpmt, output_cpmt, cache_writ_input_cpmt, cache_read_input_cpmt
    FROM VALUES
        ('claude-4-sonnet',   1.50,  7.50, 1.88, 0.15),
        ('claude-opus-4-5',   2.75, 13.75, 3.44, 0.28),
        ('claude-opus-4-6',   2.75, 13.75, 3.44, 0.28),
        ('claude-sonnet-4-5', 1.65,  8.25, 2.06, 0.17),
        ('claude-sonnet-4-6', 1.65,  8.25, 2.07, 0.17),
        ('openai-gpt-5.2',    0.97,  7.70, NULL, 0.10)
        AS t(model_name, input_cpmt, output_cpmt, cache_writ_input_cpmt, cache_read_input_cpmt)
),
ss_usage AS (
    SELECT
        u.user_id,
        u.name                                                         AS user_name,
        uh.request_id,
        uh.parent_request_id,
        uh.usage_time,
        OBJECT_KEYS(uh.tokens_granular)[0]                             AS model_name,
        uh.tokens_granular[OBJECT_KEYS(uh.tokens_granular)[0]]:cache_read_input  AS cache_read_tokens,
        uh.tokens_granular[OBJECT_KEYS(uh.tokens_granular)[0]]:cache_write_input AS cache_write_tokens,
        uh.tokens_granular[OBJECT_KEYS(uh.tokens_granular)[0]]:input             AS input_tokens,
        uh.tokens_granular[OBJECT_KEYS(uh.tokens_granular)[0]]:output            AS output_tokens
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY uh
    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON uh.user_id = u.user_id
),
ss_credits AS (
    SELECT
        u.user_id,
        u.user_name,
        DATE(u.usage_time)                                             AS usage_date,
        u.model_name,
        SUM(u.cache_read_tokens)                                       AS cache_read_tokens,
        SUM(u.cache_write_tokens)                                      AS cache_write_tokens,
        SUM(u.input_tokens)                                            AS input_tokens,
        SUM(u.output_tokens)                                           AS output_tokens,
        SUM(u.cache_read_tokens  / 1000000 * p.cache_read_input_cpmt) AS crt_credits,
        SUM(u.cache_write_tokens / 1000000 * p.cache_writ_input_cpmt) AS cwt_credits,
        SUM(u.input_tokens       / 1000000 * p.input_cpmt)            AS it_credits,
        SUM(u.output_tokens      / 1000000 * p.output_cpmt)           AS ot_credits
    FROM ss_usage u
    JOIN coco_model_costs p ON u.model_name = p.model_name
    GROUP BY ALL
)
SELECT
    *,
    ROUND(crt_credits + cwt_credits + it_credits + ot_credits, 6)     AS total_credits
FROM ss_credits
ORDER BY usage_date DESC, model_name;