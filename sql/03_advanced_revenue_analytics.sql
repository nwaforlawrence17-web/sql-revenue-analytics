-- =====================================================================
-- 🚀 ADVANCED ANALYTICS: MONTH-OVER-MONTH GROWTH & YTD TRAJECTORY
-- Executed By: Chinua Analytics Engine
-- =====================================================================

WITH MonthlyRevenue AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS reporting_month,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(total) AS current_month_revenue
    FROM 
        sales_data
    GROUP BY 
        1
)
SELECT 
    reporting_month,
    total_orders,
    current_month_revenue,
    LAG(current_month_revenue) OVER (ORDER BY reporting_month) AS previous_month_revenue,
    -- Calculate MoM Growth Percentage
    ROUND(
        ((current_month_revenue - LAG(current_month_revenue) OVER (ORDER BY reporting_month)) / 
        NULLIF(LAG(current_month_revenue) OVER (ORDER BY reporting_month), 0)) * 100, 
    2) AS mom_growth_pct,
    -- Calculate Rolling YTD Total
    SUM(current_month_revenue) OVER (
        ORDER BY reporting_month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ytd_cumulative_revenue
FROM 
    MonthlyRevenue
ORDER BY 
    reporting_month DESC;
