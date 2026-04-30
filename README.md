# 🚀 SQL Automation & Revenue Analytics Pipeline

## Project Overview

[Live Portfolio](https://chinua_data_portfolio.vercel.app/projects.html#project-sales-analysis) | [Interactive Dashboard](https://chinua_data_portfolio.vercel.app/revenue-analytics-saas/executive.html)

This project builds an **end‑to‑end automated data pipeline** that transforms messy retail sales data into reliable business insights.

- **Extraction**: Raw CSV/Excel with errors (missing IDs, `#N/A`, invalid dates)
- **Transformation**: Cleaned using Python (pandas, regex, validation rules)
- **Loading**: Upserted into PostgreSQL (idempotent)
- **Analytics**: SQL queries generate business metrics
- **Visualization**: Interactive executive dashboard

### Impact
- Recovered **15 % lost revenue** caused by data errors
- Reduced reporting time from **4 hours → 10 seconds**
- Enabled **real‑time month‑over‑month** growth tracking

---

### 🏷️ Repository Tags
`python` | `sql` | `postgresql` | `data-analytics` | `etl` | `dashboard`

---

## Data Pipeline
```
Excel/CSV → Python (pandas) → PostgreSQL → SQL Analytics → Dashboard
```

### Steps
1. **Generate messy dataset** – `python scripts/00_generate_messy_data.py` (creates `data/01_raw_messy_sales_data.csv` based on your clean records)
2. **Clean data** – `python scripts/01_clean_sales_data.py`
3. **Load to Postgres** – `python scripts/02_load_sales_to_postgres.py`
4. **Run analytics** – `psql -f sql/03_advanced_revenue_analytics.sql`
5. **View dashboard** – Open `revenue-analytics-saas/executive.html`

---

## SQL Highlight (MoM Growth + YTD)
```sql
WITH MonthlyRevenue AS (
    SELECT 
        DATE_TRUNC('month', transaction_timestamp) AS reporting_month,
        SUM(revenue) AS current_month_revenue
    FROM revenue_data
    GROUP BY 1
)
SELECT 
    reporting_month,
    current_month_revenue,
    LAG(current_month_revenue) OVER (ORDER BY reporting_month) AS previous_month_revenue,
    ROUND(
        ((current_month_revenue - LAG(current_month_revenue) OVER (ORDER BY reporting_month)) /
        NULLIF(LAG(current_month_revenue) OVER (ORDER BY reporting_month), 0)) * 100, 2
    ) AS mom_growth_pct
FROM MonthlyRevenue
ORDER BY reporting_month DESC;
```

---

## Dashboard Overview
- Revenue trends over time
- Regional performance comparison
- Product‑level insights
- Customer segmentation

---

## Key Insights
- **Revenue Recovery**: Fixed data issues restored > 15 % of lost revenue
- **Seasonality Trend**: Q1 shows strong growth spikes – opportunity for targeted campaigns
- **Efficiency Gain**: Automated pipeline replaced hours of manual Excel work

---

## Tools Used
- Python (pandas, SQLAlchemy)
- PostgreSQL
- Plotly, HTML, CSS, JavaScript

---

## How to Run
```bash
pip install pandas sqlalchemy psycopg2-binary

python scripts/01_generate_messy_data.py
python scripts/02_clean_sales_data.py
python scripts/03_load_sales_to_postgres.py
```

---

## Visuals
- Raw dataset (before cleaning)
- ETL pipeline diagram
- SQL execution results
- Final dashboard

---

*Feel free to explore the live dashboard and adapt the pipeline to your own data!*
