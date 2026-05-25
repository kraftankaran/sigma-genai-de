WITH filtered_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM {{ ref('stg_transactions') }}
    WHERE status IN ('completed', 'failed')
),

merchant_details AS (
    SELECT
        merchant_id,
        merchant_name,
        category,
        city,
        onboarded_date
    FROM {{ source('sigma_analytics', 'dim_merchant') }}
),

aggregated_metrics AS (
    SELECT
        ft.merchant_id,
        SUM(CASE WHEN ft.status = 'completed' THEN ft.amount ELSE 0 END) AS total_revenue,
        COUNT(ft.transaction_id) AS total_transactions,
        COUNT(CASE WHEN ft.status = 'failed' THEN 1 END) AS failed_count,
        (COUNT(CASE WHEN ft.status = 'failed' THEN 1 END) * 100.0 / COUNT(ft.transaction_id)) AS failure_rate_pct,
        AVG(CASE WHEN ft.status = 'completed' THEN ft.amount ELSE NULL END) AS avg_transaction_value,
        COUNT(DISTINCT ft.customer_id) AS unique_customers
    FROM filtered_transactions ft
    GROUP BY ft.merchant_id
)

SELECT
    am.merchant_id,
    md.merchant_name,
    md.category,
    md.city,
    md.onboarded_date,
    am.total_revenue,
    am.total_transactions,
    am.failed_count,
    am.failure_rate_pct,
    am.avg_transaction_value,
    am.unique_customers
FROM aggregated_metrics am
JOIN merchant_details md ON am.merchant_id = md.merchant_id
