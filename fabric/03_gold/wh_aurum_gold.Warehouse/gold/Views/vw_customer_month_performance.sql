CREATE   VIEW gold.vw_customer_month_performance
AS
SELECT
    t.user_id,
    d.[year],
    d.month_num,
    d.month_name,
    t.is_partial_month,
    t.has_orders,
    t.monthly_target,
    COALESCE(r.revenue, 0)                                    AS revenue,
    COALESCE(r.revenue, 0) / NULLIF(t.monthly_target, 0)      AS attainment
FROM gold.fact_targets_monthly AS t
JOIN gold.dim_date AS d
     ON d.date_key = t.month_date_key
LEFT JOIN (
    SELECT o.user_id, dd.month_start, SUM(o.revenue) AS revenue
    FROM gold.fact_orders AS o
    JOIN gold.dim_date    AS dd ON dd.date_key = o.date_key
    GROUP BY o.user_id, dd.month_start
) AS r
     ON r.user_id = t.user_id
    AND r.month_start = d.month_start;

GO