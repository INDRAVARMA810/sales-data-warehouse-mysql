-- =============================================================================
-- analytical_queries.sql — Mini Sales Data Warehouse
-- =============================================================================
--
--   Target platform : MySQL 8.0.16+
--   Prerequisites   : schema.sql, sample_data.sql, etl_simulation.sql,
--                     indexes.sql, views.sql
--   Run with:
--       mysql --user=root --password --show-warnings < analytical_queries.sql
--
--   Business questions answered against the mart layer (mart.vw_sales_detail,
--   mart.vw_inventory_detail, mart.mv_sales_monthly, mart.mv_customer_rfm) —
--   that is the entire point of Milestone 6: an analyst writes these without
--   ever seeing a surrogate key. Each query is standalone and runnable on its
--   own; there is no shared verification section because the result sets ARE
--   the deliverable.
--
-- -----------------------------------------------------------------------------
-- PORT NOTE — no GROUPING SETS / CUBE in MySQL
-- -----------------------------------------------------------------------------
-- MySQL 8 supports only `GROUP BY ... WITH ROLLUP` (a fixed hierarchical
-- rollup of the trailing grouping columns) plus the GROUPING() function to
-- distinguish a real NULL from a rollup subtotal row. It has no equivalent of
-- PostgreSQL/SQL Server's arbitrary `GROUPING SETS (...)` or `CUBE`. Section 3
-- uses WITH ROLLUP — the closest available substitute, sufficient for a
-- category -> subcategory hierarchy, but not a stand-in for arbitrary
-- combinations of grouping columns.
-- =============================================================================


-- =============================================================================
-- SECTION 1 — MONTHLY SALES TRENDS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1  Monthly revenue: month-over-month growth and running total.
-- Technique: CTE + LAG() for MoM %, SUM() OVER a frame for the running total.
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT order_year, order_month, order_month_name,
           SUM(net_sales_amount) AS net_sales
    FROM   mart.vw_sales_detail
    GROUP  BY order_year, order_month, order_month_name
)
SELECT
    order_year,
    order_month_name,
    net_sales,
    ROUND(100 * (net_sales - LAG(net_sales) OVER (ORDER BY order_year, order_month))
          / NULLIF(LAG(net_sales) OVER (ORDER BY order_year, order_month), 0), 2)
                                                                    AS mom_growth_pct,
    SUM(net_sales) OVER (ORDER BY order_year, order_month
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                                    AS running_net_sales
FROM   monthly
ORDER  BY order_year, order_month;

-- -----------------------------------------------------------------------------
-- 1.2  Year-over-year: same calendar month vs. the prior year.
-- Technique: LAG() PARTITIONed BY month instead of a self-join on
-- prior_year_date_key — simpler at monthly grain, where "prior year" always
-- means "the previous row for this same month".
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT order_year, order_month, order_month_name,
           SUM(net_sales_amount) AS net_sales
    FROM   mart.vw_sales_detail
    GROUP  BY order_year, order_month, order_month_name
)
SELECT
    order_month_name,
    order_year,
    net_sales,
    LAG(net_sales) OVER (PARTITION BY order_month ORDER BY order_year)
                                                            AS prior_year_net_sales,
    ROUND(100 * (net_sales - LAG(net_sales) OVER (PARTITION BY order_month ORDER BY order_year))
          / NULLIF(LAG(net_sales) OVER (PARTITION BY order_month ORDER BY order_year), 0), 2)
                                                            AS yoy_growth_pct
FROM   monthly
ORDER  BY order_month, order_year;


-- =============================================================================
-- SECTION 2 — TOP-N RANKINGS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1  Top 10 products by net sales.
-- Technique: CTE aggregates first, RANK() ranks the already-aggregated rows.
-- -----------------------------------------------------------------------------
WITH product_sales AS (
    SELECT product_id, product_name, category,
           SUM(quantity)         AS units_sold,
           SUM(net_sales_amount) AS net_sales
    FROM   mart.vw_sales_detail
    GROUP  BY product_id, product_name, category
)
SELECT
    RANK() OVER (ORDER BY net_sales DESC) AS sales_rank,
    product_id, product_name, category, units_sold, net_sales
FROM   product_sales
ORDER  BY net_sales DESC
LIMIT  10;

-- -----------------------------------------------------------------------------
-- 2.2  Best-selling product WITHIN each category.
-- Technique: DENSE_RANK() PARTITION BY category, then filter rank = 1 in an
-- outer query — MySQL has no QUALIFY clause, so filtering on a window
-- function's result always needs this two-step CTE shape.
-- -----------------------------------------------------------------------------
WITH product_sales AS (
    SELECT category, product_id, product_name,
           SUM(net_sales_amount) AS net_sales
    FROM   mart.vw_sales_detail
    GROUP  BY category, product_id, product_name
),
ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY category ORDER BY net_sales DESC) AS category_rank
    FROM   product_sales
)
SELECT category, product_id, product_name, net_sales
FROM   ranked
WHERE  category_rank = 1
ORDER  BY net_sales DESC;


-- =============================================================================
-- SECTION 3 — CATEGORY ANALYSIS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1  Revenue by category / subcategory, with subtotals and a grand total.
-- Technique: GROUP BY ... WITH ROLLUP (see PORT NOTE above) + GROUPING() to
-- label the subtotal/grand-total rows instead of leaving them as bare NULLs.
-- Sorted on the raw GROUPING() flags, not the text labels, so subtotals and
-- the grand total always sort to the bottom of their group as expected.
-- -----------------------------------------------------------------------------
SELECT
    IF(GROUPING(category),    'ALL CATEGORIES',    category)    AS category,
    IF(GROUPING(subcategory), 'ALL SUBCATEGORIES', subcategory) AS subcategory,
    SUM(net_sales_amount)    AS net_sales,
    SUM(gross_profit_amount) AS gross_profit,
    ROUND(SUM(gross_profit_amount) / NULLIF(SUM(net_sales_amount), 0), 4)
                              AS gross_margin_pct  -- ratio of SUMs, never AVG of a ratio
FROM   mart.vw_sales_detail
GROUP  BY category, subcategory WITH ROLLUP
ORDER  BY GROUPING(category), category, GROUPING(subcategory), subcategory;


-- =============================================================================
-- SECTION 4 — STORE PERFORMANCE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1  Store ranking, and each store's variance against the company average.
-- Technique: RANK() and a frameless AVG() OVER () (empty OVER = whole result
-- set) computed side by side, so each row can compare itself to the whole.
-- -----------------------------------------------------------------------------
WITH store_sales AS (
    SELECT store_id, store_name, store_type, store_region,
           SUM(net_sales_amount) AS net_sales
    FROM   mart.vw_sales_detail
    GROUP  BY store_id, store_name, store_type, store_region
)
SELECT
    RANK() OVER (ORDER BY net_sales DESC)  AS store_rank,
    store_id, store_name, store_type, store_region, net_sales,
    ROUND(AVG(net_sales) OVER (), 2)       AS company_avg_net_sales,
    ROUND(100 * (net_sales - AVG(net_sales) OVER ()) / NULLIF(AVG(net_sales) OVER (), 0), 2)
                                            AS pct_vs_company_avg
FROM   store_sales
ORDER  BY net_sales DESC;


-- =============================================================================
-- SECTION 5 — CUSTOMER ANALYSIS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 5.1  Top 10 customers by lifetime value, with a value quartile for everyone.
-- Technique: NTILE(4), read straight from mv_customer_rfm — the RFM rollup is
-- exactly the query ix_fact_sales_customer_date (indexes.sql §3) exists for,
-- already materialized so this costs a scan of a few thousand rows, not 50k.
-- -----------------------------------------------------------------------------
SELECT
    customer_id, customer_name, loyalty_tier, region,
    order_count, net_sales_amount, avg_order_value, recency_days,
    NTILE(4) OVER (ORDER BY net_sales_amount DESC) AS value_quartile
FROM   mart.mv_customer_rfm
ORDER  BY net_sales_amount DESC
LIMIT  10;

-- -----------------------------------------------------------------------------
-- 5.2  Loyalty-tier revenue: "as-was" vs "as-is" (dimensional_modeling.md §5).
-- Both are legitimate and answer different questions — the SCD2 payoff made
-- runnable. as-was needs no extra join: the fact's own customer_key already
-- points at the tier that applied at purchase time.
-- -----------------------------------------------------------------------------
-- As-was: revenue by the tier the customer held AT THE TIME OF PURCHASE.
SELECT customer_loyalty_tier AS loyalty_tier_as_of_purchase,
       SUM(net_sales_amount) AS net_sales
FROM   mart.vw_sales_detail
GROUP  BY customer_loyalty_tier
ORDER  BY net_sales DESC;

-- As-is: the SAME revenue, re-attributed to customers' CURRENT tier.
SELECT cur.loyalty_tier      AS current_loyalty_tier,
       SUM(v.net_sales_amount) AS net_sales
FROM   mart.vw_sales_detail    v
JOIN   mart.vw_customer_current cur ON cur.customer_id = v.customer_id
GROUP  BY cur.loyalty_tier
ORDER  BY net_sales DESC;


-- =============================================================================
-- SECTION 6 — PROMOTION & CHANNEL
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6.1  Promoted vs. non-promoted line items — a coarse read on promotional
-- lift, not a controlled study (no adjustment for product mix). Deliberately
-- uses SUM(net_sales_amount)/SUM(quantity) for average selling price, never
-- AVG(unit_price) — unit_price is a NON-ADDITIVE rate (star_schema.md §5;
-- dimensional_modeling.md §6), and averaging an average would silently
-- misweight a 1-unit line the same as a 100-unit line.
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN promotion_id = 'N/A'     THEN 'No Promotion'
        WHEN promotion_id = 'UNKNOWN' THEN 'Unknown Promotion'
        ELSE 'Promoted'
    END                                                        AS promotion_flag,
    COUNT(*)                                                   AS line_count,
    SUM(quantity)                                              AS units_sold,
    ROUND(SUM(net_sales_amount) / NULLIF(SUM(quantity), 0), 2) AS avg_selling_price_per_unit,
    ROUND(SUM(gross_profit_amount) / NULLIF(SUM(net_sales_amount), 0), 4)
                                                                AS gross_margin_pct
FROM   mart.vw_sales_detail
GROUP  BY promotion_flag
ORDER  BY line_count DESC;

-- -----------------------------------------------------------------------------
-- 6.2  Revenue by channel by month — the headline report
-- ix_fact_sales_channel_date_covering (indexes.sql §3) was built for. Queried
-- here straight from mv_sales_monthly: a few hundred pre-aggregated rows
-- instead of a 50,000-row scan, which is the entire reason that table exists.
-- -----------------------------------------------------------------------------
SELECT
    year_month_number, channel_name, channel_group,
    SUM(net_sales_amount)    AS net_sales,
    SUM(gross_profit_amount) AS gross_profit
FROM   mart.mv_sales_monthly
GROUP  BY year_month_number, channel_name, channel_group
ORDER  BY year_month_number, net_sales DESC;


-- =============================================================================
-- SECTION 7 — INVENTORY INSIGHTS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 7.1  Right-now stock alert: stockouts and below-reorder-point positions as
-- of the latest snapshot date loaded.
-- -----------------------------------------------------------------------------
SELECT
    product_id, product_name, category,
    store_id, store_name,
    quantity_on_hand, reorder_point, quantity_on_order,
    is_stockout, is_below_reorder_point
FROM   mart.vw_inventory_detail
WHERE  snapshot_date = (SELECT MAX(snapshot_date) FROM mart.vw_inventory_detail)
  AND  (is_stockout = 1 OR is_below_reorder_point = 1)
ORDER  BY is_stockout DESC, quantity_on_hand ASC;

-- -----------------------------------------------------------------------------
-- 7.2  Stockout frequency per product, over the whole snapshot history.
-- THE SEMI-ADDITIVE RULE, applied correctly (star_schema.md §5):
-- SUM(is_stockout) across dates is valid — it counts distinct stockout EVENTS
-- (stockout-days). SUM(quantity_on_hand) across dates would NOT be valid — it
-- would double-count the same physical units held on consecutive days. AVG is
-- the correct aggregate for a STATE like stock position.
-- -----------------------------------------------------------------------------
SELECT
    product_id, product_name, category,
    SUM(is_stockout)                  AS stockout_days,
    ROUND(AVG(quantity_on_hand), 1)   AS avg_stock_position
FROM   mart.vw_inventory_detail
GROUP  BY product_id, product_name, category
ORDER  BY stockout_days DESC
LIMIT  15;


-- =============================================================================
-- SECTION 8 — FACT CONSTELLATION DRILL-ACROSS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 8.1  Did stockouts precede the sales decline? — the canonical drill-across
-- query from star_schema.md §2, made runnable. The original joins fact_sales
-- to fact_inventory_snapshot on shared SURROGATE keys; this version joins the
-- two MART views on shared NATURAL keys (product_id, store_id, date) instead
-- — proof that conformance survives even after the warehouse's surrogate keys
-- are hidden from the semantic layer entirely (architecture.md §2.3).
-- -----------------------------------------------------------------------------
SELECT
    v.category,
    v.order_year_month,
    SUM(v.net_sales_amount)          AS net_sales,
    ROUND(AVG(i.quantity_on_hand), 1) AS avg_stock,      -- semi-additive: AVG, never SUM across dates
    SUM(i.is_stockout)                AS stockout_days
FROM        mart.vw_sales_detail     v
LEFT JOIN   mart.vw_inventory_detail i
        ON  i.product_id    = v.product_id
       AND  i.store_id      = v.store_id
       AND  i.snapshot_date = v.order_date
GROUP  BY   v.category, v.order_year_month
ORDER  BY   v.order_year_month, v.category;

-- =============================================================================
-- END OF analytical_queries.sql
--
-- This is the last file in the build pipeline (architecture.md §8). Remaining
-- repository content — interview_questions.md, docs/, diagrams/ — is reference
-- material, not something that runs against the database.
-- =============================================================================
