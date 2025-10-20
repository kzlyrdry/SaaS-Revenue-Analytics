-- 1. Aylık MRR ve Ücretli Kullanıcı Sayısı
SELECT
    DATE_TRUNC('month', payment_date) AS month,
    COUNT(DISTINCT user_id) AS paid_users,
    SUM(revenue_amount_usd) AS mrr
FROM project.games_payments
GROUP BY DATE_TRUNC('month', payment_date)
ORDER BY month;

-- 2. Yeni Ücretli Kullanıcılar (ilk ödemeyi yapan kullanıcılar)
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


-- 3. Kullanıcı Bazlı Aylık MRR ve Durum Analizi
WITH monthly_revenue AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date) AS month,
        SUM(revenue_amount_usd) AS user_mrr
    FROM project.games_payments
    GROUP BY user_id, DATE_TRUNC('month', payment_date)
),
user_status AS (
    SELECT
        *,
        LAG(user_mrr) OVER (PARTITION BY user_id ORDER BY month) AS prev_mrr,
        LAG(month) OVER (PARTITION BY user_id ORDER BY month) AS prev_month,
        LEAD(month) OVER (PARTITION BY user_id ORDER BY month) AS next_month
    FROM monthly_revenue
),
user_status_calculated AS (
    SELECT
        *,
        CASE 
            WHEN prev_mrr IS NULL THEN 'New'
            WHEN next_month IS NULL OR next_month > month + INTERVAL '1 month' THEN 'Churn'
            WHEN user_mrr > prev_mrr THEN 'Expansion'
            WHEN user_mrr < prev_mrr THEN 'Contraction'
            ELSE 'Retained'
        END AS user_status
    FROM user_status
)
SELECT
    month,
    user_status,
    COUNT(DISTINCT user_id) AS user_count,
    SUM(user_mrr) AS mrr_amount
FROM user_status_calculated
GROUP BY month, user_status
ORDER BY month, user_status;

--  Customer Lifetime ve LTV Hesaplama
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



WITH monthly_revenue AS (
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
    SELECT
        *,
        LAG(user_mrr) OVER (PARTITION BY user_id ORDER BY month) AS prev_user_mrr,
        LAG(month) OVER (PARTITION BY user_id ORDER BY month) AS prev_month,
        LEAD(month) OVER (PARTITION BY user_id ORDER BY month) AS next_month
    FROM monthly_revenue
),

user_status AS (
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

-- LTV HESAPLAMA İÇİN YENİ CTE
user_lifetime_calc AS (
    SELECT
        user_id,
        COUNT(DISTINCT month) AS lifetime_months,
        SUM(user_mrr) AS total_ltv
    FROM monthly_revenue
    GROUP BY user_id
    HAVING COUNT(DISTINCT month) > 0
),

lifetime_metrics AS (
    SELECT
        AVG(lifetime_months) AS avg_customer_lifetime,
        AVG(total_ltv) AS avg_ltv,
        COUNT(DISTINCT user_id) AS total_customers
    FROM user_lifetime_calc
),

monthly_metrics AS (
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
    SELECT
        m.*,
        m.mrr / NULLIF(m.paid_users, 0) AS arppu,
        m.churned_users / NULLIF(LAG(m.paid_users) OVER (PARTITION BY m.language ORDER BY m.month), 0) AS churn_rate,
        m.churned_revenue / NULLIF(LAG(m.mrr) OVER (PARTITION BY m.language ORDER BY m.month), 0) AS revenue_churn_rate,
        m.mrr - LAG(m.mrr) OVER (PARTITION BY m.language ORDER BY m.month) AS net_mrr_change,
        m.paid_users - LAG(m.paid_users) OVER (PARTITION BY m.language ORDER BY m.month) AS net_user_change,
        lm.avg_customer_lifetime,
        lm.avg_ltv,
        lm.total_customers
    FROM monthly_metrics m
    CROSS JOIN lifetime_metrics lm  
)

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
    net_user_change,
    avg_customer_lifetime,
    avg_ltv,
    total_customers
FROM final_metrics
WHERE month IS NOT NULL
ORDER BY month, language, age

















