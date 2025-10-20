-- =============================================
-- PROJECT 2: REVENUE ANALYTICS DASHBOARD
-- Comprehensive SQL Script for Tableau Dashboard
-- =============================================

-- =============================================
-- SECTION 1: DATA EXPLORATION QUERIES
-- =============================================

-- Query 1.1: Database Schema Exploration
-- Purpose: Identify available tables in project schema
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'project'
  AND table_type = 'BASE TABLE';

-- Query 1.2: Table Structure Analysis  
-- Purpose: Examine columns and data types
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'project'
  AND table_name = 'games_payments'
ORDER BY ordinal_position;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'project'
  AND table_name = 'games_paid_users'
ORDER BY ordinal_position;

-- =============================================
-- SECTION 2: BASIC METRICS CALCULATION
-- =============================================

-- Query 2.1: Monthly MRR and Paid Users
-- Purpose: Foundation metrics for dashboard
SELECT
    DATE_TRUNC('month', payment_date) AS month,
    COUNT(DISTINCT user_id) AS paid_users,
    SUM(revenue_amount_usd) AS mrr
FROM project.games_payments
GROUP BY DATE_TRUNC('month', payment_date)
ORDER BY month;

-- Query 2.2: New Paid Users Identification
-- Purpose: Track user acquisition over time
WITH first_payments AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(payment_date)) AS first_payment_month
    FROM project.games_payments
    GROUP BY user_id
)
SELECT
    first_payment_month AS month,
    COUNT(DISTINCT user_id) AS new_paid_users
FROM first_payments
GROUP BY first_payment_month
ORDER BY month;

-- =============================================
-- SECTION 3: CUSTOMER LIFETIME ANALYSIS
-- =============================================

-- Query 3.1: Customer Lifetime and LTV Calculation
-- Purpose: Calculate long-term customer value metrics
WITH user_metrics AS (
    SELECT
        user_id,
        COUNT(DISTINCT DATE_TRUNC('month', payment_date)) AS active_months,
        SUM(revenue_amount_usd) AS total_revenue
    FROM project.games_payments
    GROUP BY user_id
    HAVING COUNT(DISTINCT DATE_TRUNC('month', payment_date)) > 0
)
SELECT
    AVG(active_months) AS avg_customer_lifetime,
    AVG(total_revenue) AS avg_ltv,
    COUNT(DISTINCT user_id) AS total_customers
FROM user_metrics;

-- =============================================
-- SECTION 4: COMPREHENSIVE REVENUE ANALYTICS
-- FINAL QUERY FOR TABLEAU DASHBOARD
-- =============================================

WITH monthly_revenue AS (
    -- User-level monthly revenue with demographic data
    SELECT
        gp.user_id,
        DATE_TRUNC('month', gp.payment_date) AS month,
        SUM(gp.revenue_amount_usd) AS user_mrr,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM project.games_payments gp
    LEFT JOIN project.games_paid_users gpu 
        ON gp.user_id = gpu.user_id 
        AND gp.game_name = gpu.game_name
    GROUP BY gp.user_id, DATE_TRUNC('month', gp.payment_date), gpu.language, gpu.age, gpu.has_older_device_model
),

user_revenue_status AS (
    -- User revenue history with previous/next month comparisons
    SELECT
        *,
        LAG(user_mrr) OVER (PARTITION BY user_id ORDER BY month) AS prev_user_mrr,
        LAG(month) OVER (PARTITION BY user_id ORDER BY month) AS prev_month,
        LEAD(month) OVER (PARTITION BY user_id ORDER BY month) AS next_month
    FROM monthly_revenue
),

user_status AS (
    -- Categorize user status based on revenue changes
    SELECT
        urs.month,
        urs.user_id,
        urs.user_mrr,
        urs.language,
        urs.age,
        urs.has_older_device_model,
        CASE 
            WHEN urs.prev_user_mrr IS NULL THEN 'New'
            WHEN urs.next_month IS NULL OR urs.next_month > urs.month + INTERVAL '1 month' THEN 'Churn'
            WHEN urs.user_mrr > urs.prev_user_mrr THEN 'Expansion' 
            WHEN urs.user_mrr < urs.prev_user_mrr THEN 'Contraction'
            ELSE 'Retained'
        END AS status
    FROM user_revenue_status urs
),

monthly_metrics AS (
    -- Aggregate metrics by month and demographics
    SELECT
        us.month,
        us.language,
        us.age,
        us.has_older_device_model,
        COUNT(DISTINCT us.user_id) AS paid_users,
        SUM(us.user_mrr) AS mrr,
        COUNT(DISTINCT CASE WHEN us.status = 'New' THEN us.user_id END) AS new_paid_users,
        SUM(CASE WHEN us.status = 'New' THEN us.user_mrr ELSE 0 END) AS new_mrr,
        COUNT(DISTINCT CASE WHEN us.status = 'Churn' THEN us.user_id END) AS churned_users,
        SUM(CASE WHEN us.status = 'Churn' THEN us.user_mrr ELSE 0 END) AS churned_revenue,
        SUM(CASE WHEN us.status = 'Expansion' THEN us.user_mrr - urs.prev_user_mrr ELSE 0 END) AS expansion_mrr,
        SUM(CASE WHEN us.status = 'Contraction' THEN urs.prev_user_mrr - us.user_mrr ELSE 0 END) AS contraction_mrr
    FROM user_status us
    LEFT JOIN user_revenue_status urs ON us.user_id = urs.user_id AND us.month = urs.month
    GROUP BY us.month, us.language, us.age, us.has_older_device_model
),

final_metrics AS (
    -- Calculate derived metrics and rates
    SELECT
        m.*,
        -- Average Revenue Per Paid User
        m.mrr / NULLIF(m.paid_users, 0) AS arppu,
        -- User Churn Rate
        m.churned_users / NULLIF(LAG(m.paid_users) OVER (PARTITION BY m.language ORDER BY m.month), 0) AS churn_rate,
        -- Revenue Churn Rate  
        m.churned_revenue / NULLIF(LAG(m.mrr) OVER (PARTITION BY m.language ORDER BY m.month), 0) AS revenue_churn_rate,
        -- Net MRR Change
        m.mrr - LAG(m.mrr) OVER (PARTITION BY m.language ORDER BY m.month) AS net_mrr_change,
        -- Net User Change
        m.paid_users - LAG(m.paid_users) OVER (PARTITION BY m.language ORDER BY m.month) AS net_user_change
    FROM monthly_metrics m
)

-- FINAL OUTPUT: All metrics for Tableau dashboard
SELECT 
    month,
    language,
    age,
    has_older_device_model,
    paid_users,
    mrr,
    new_paid_users,
    new_mrr,
    churned_users,
    churned_revenue,
    expansion_mrr,
    contraction_mrr,
    arppu,
    churn_rate,
    revenue_churn_rate,
    net_mrr_change,
    net_user_change
FROM final_metrics
WHERE month IS NOT NULL
ORDER BY month, language, age;

-- =============================================
-- SECTION 5: DATA VALIDATION QUERIES
-- =============================================

-- Query 5.1: Data Quality Check
-- Purpose: Verify data consistency and ranges
SELECT 
    MIN(payment_date) as data_start_date,
    MAX(payment_date) as data_end_date,
    COUNT(DISTINCT DATE_TRUNC('month', payment_date)) as total_months,
    COUNT(*) as total_payment_records,
    COUNT(DISTINCT user_id) as total_unique_users
FROM project.games_payments;

-- Query 5.2: Metric Sanity Check
-- Purpose: Ensure metrics are within expected ranges

SELECT
    MIN(revenue_amount_usd) as min_payment,
    MAX(revenue_amount_usd) as max_payment,
    AVG(revenue_amount_usd) as avg_payment,
    COUNT(*) as total_payments,
    COUNT(DISTINCT user_id) as paying_users
FROM project.games_payments;


