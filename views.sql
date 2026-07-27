-- =============================================================================
-- views.sql — Mini Sales Data Warehouse
-- =============================================================================
--
--   Target platform : MySQL 8.0.16+
--   Prerequisites   : schema.sql, sample_data.sql, etl_simulation.sql, indexes.sql
--   Run with:
--       mysql --user=root --password --show-warnings < views.sql
--
--   Contains one stored procedure (§3), so this file uses DELIMITER, a `mysql`
--   CLIENT command, not server syntax. Run it through the CLI, not through a
--   driver that submits statements directly.
--
-- -----------------------------------------------------------------------------
-- WHAT THIS FILE IS — architecture.md §2.3
-- -----------------------------------------------------------------------------
-- The `mart` schema, empty since schema.sql, is the semantic layer: it joins the
-- star back together, applies business-friendly names, and pre-aggregates the
-- expensive rollups. THE RULE: no new business logic that is not already in the
-- warehouse. Where a genuinely new derived value is introduced (§2's
-- gross_margin_pct, is_below_reorder_point), it is defined exactly ONCE here —
-- the same discipline schema.sql applied to net_sales_amount via generated
-- columns, applied one layer up.
--
-- -----------------------------------------------------------------------------
-- PORT NOTE — MySQL has no CREATE MATERIALIZED VIEW
-- -----------------------------------------------------------------------------
-- architecture.md §2.3 and §6 call for two kinds of mart object: `vw_*` views
-- and `mv_*` materialized views. PostgreSQL's `CREATE MATERIALIZED VIEW ... WITH
-- DATA` plus `REFRESH MATERIALIZED VIEW` has no MySQL equivalent at all.
--
-- SUBSTITUTE: every `mv_*` object below is an ordinary InnoDB table, with a
-- declared grain and a PRIMARY KEY exactly like a fact table, populated by
-- `mart.sp_refresh_materialized_views()`. Call that procedure wherever
-- PostgreSQL would call REFRESH MATERIALIZED VIEW — after every ETL batch.
--
-- THE COST, stated plainly: on real materialized views, staleness is a property
-- the engine can report (`pg_matviews.ispopulated`) and refresh atomically. Here
-- it is the caller's responsibility — nothing stops someone from querying
-- `mart.mv_sales_monthly` right after a load and before a refresh. `refreshed_at`
-- on every mv table is the only signal a consumer has that the numbers might be
-- behind the warehouse.
-- =============================================================================


-- =============================================================================
-- SECTION 1 — ROLE-PLAYING DATE VIEWS
-- =============================================================================
-- dimensional_modeling.md §7 names this pattern explicitly: dim_date is joined
-- twice to fact_sales (order date, ship date), and the two roles can be
-- disambiguated for BI tools with prefixed views rather than raw aliasing at
-- query time. `vw_sales_detail` (§2) consumes both.
--
-- Reserved members (-1, 0) are NOT filtered out here — the same reason every
-- other join in this warehouse is an INNER JOIN: filtering them here would make
-- an unresolvable date silently disappear instead of being visible as data you
-- can measure. A BI tool building a plain calendar picklist from this view
-- should add `WHERE order_date_key > 0` itself; that is a presentation choice,
-- not a correctness one.
-- =============================================================================

CREATE OR REPLACE VIEW mart.vw_dim_order_date AS
SELECT
    date_key            AS order_date_key,
    full_date            AS order_date,
    year_number          AS order_year,
    quarter_number       AS order_quarter,
    month_number         AS order_month,
    month_name           AS order_month_name,
    day_of_month         AS order_day_of_month,
    week_of_year         AS order_week_of_year,
    day_name             AS order_day_name,
    year_month_number    AS order_year_month,
    is_weekend           AS order_is_weekend,
    is_holiday           AS order_is_holiday,
    fiscal_year          AS order_fiscal_year,
    fiscal_quarter       AS order_fiscal_quarter
FROM warehouse.dim_date;

CREATE OR REPLACE VIEW mart.vw_dim_ship_date AS
SELECT
    date_key            AS ship_date_key,
    full_date            AS ship_date,
    year_number          AS ship_year,
    quarter_number       AS ship_quarter,
    month_number         AS ship_month,
    month_name           AS ship_month_name,
    day_of_month         AS ship_day_of_month,
    week_of_year         AS ship_week_of_year,
    day_name             AS ship_day_name,
    year_month_number    AS ship_year_month,
    is_weekend           AS ship_is_weekend,
    is_holiday           AS ship_is_holiday
FROM warehouse.dim_date;


-- =============================================================================
-- SECTION 2 — SEMANTIC DETAIL VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- mart.vw_customer_current / mart.vw_product_current
-- -----------------------------------------------------------------------------
-- The "as-is" read of each SCD2 dimension: current row only, one per natural
-- key. dimensional_modeling.md §5 is explicit that "as-was" (join fact straight
-- to dim_customer/dim_product, which is what vw_sales_detail below does) and
-- "as-is" (join through to the CURRENT version) are both legitimate and answer
-- different questions. These two views are the building block for "as-is":
-- to restate any as-was report in as-is terms, join its customer_id/product_id
-- to these views instead of to the fact's own dimension key.
--
-- Reserved members (-1, 0) are included, not filtered: they are permanently
-- is_current = 1, and a consumer that lists "all current customers" is entitled
-- to see that an Unknown/Not Applicable bucket exists.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_customer_current AS
SELECT
    customer_id,
    customer_name,
    email,
    customer_segment AS segment,
    loyalty_tier,
    city,
    state_province,
    country,
    region,
    postal_code,
    signup_date
FROM warehouse.dim_customer
WHERE is_current = 1;

CREATE OR REPLACE VIEW mart.vw_product_current AS
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost,
    list_price,
    is_active
FROM warehouse.dim_product
WHERE is_current = 1;


-- -----------------------------------------------------------------------------
-- mart.vw_sales_detail  —  THE headline mart object (named in architecture.md §6)
-- -----------------------------------------------------------------------------
-- GRAIN: unchanged from fact_sales — one row per order line. A view cannot
-- change grain by definition; it can only relabel and join.
--
-- "AS-WAS" BY DEFAULT: joining straight to warehouse.dim_customer /
-- dim_product on the fact's own surrogate key resolves to the SCD2 version that
-- was current AT THE TIME OF SALE — no date predicate needed, because the fact
-- row's key already encodes which version applied (dimensional_modeling.md §5).
-- This is the query mode analysts want by default: "what did Gold customers
-- buy" should mean Gold-at-purchase, not Gold-today.
--
-- ship_date IS TRANSLATED TO NULL, deliberately breaking from the warehouse
-- layer's own "never NULL" rule. That rule exists to keep dimension ATTRIBUTES
-- always groupable ('Unknown' vs 'Not Applicable' must stay visible and
-- distinct, architecture.md §5.2). A ship DATE is different: ship_date_key = 0
-- means "hasn't happened yet", and exposing that as the literal sentinel
-- 1900-01-02 to a business user reading a report is not honest translation,
-- it's confusing. NULL is the standard, unambiguous way to say "not yet" for a
-- date column. `is_shipped` carries the same information as a filterable flag.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_sales_detail AS
SELECT
    f.sales_key,
    f.order_number,
    f.order_line_number,

    od.order_date,
    od.order_year,
    od.order_quarter,
    od.order_month,
    od.order_month_name,
    od.order_year_month,
    od.order_is_weekend,
    od.order_is_holiday,

    CASE WHEN f.ship_date_key = 0 THEN NULL ELSE sd.ship_date END AS ship_date,
    (f.ship_date_key <> 0)                                        AS is_shipped,

    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.loyalty_tier                                                AS customer_loyalty_tier,
    c.city                                                        AS customer_city,
    c.state_province                                              AS customer_state_province,
    c.country                                                     AS customer_country,
    c.region                                                      AS customer_region,

    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,

    s.store_id,
    s.store_name,
    s.store_type,
    s.city                                                        AS store_city,
    s.region                                                      AS store_region,

    pr.promotion_id,
    pr.promotion_name,
    pr.promotion_type,
    pr.discount_pct                                               AS promotion_discount_pct,

    ch.channel_name,
    ch.channel_group,

    oj.order_status,
    oj.payment_method,
    oj.shipping_priority,
    oj.is_gift,

    f.quantity,
    f.unit_price,
    f.unit_cost,
    f.discount_amount,
    f.tax_amount,
    f.gross_sales_amount,
    f.net_sales_amount,
    f.total_cost_amount,
    f.gross_profit_amount,

    -- NEW at this layer, defined once: margin as a ratio. Legitimate at THIS
    -- row's own grain (one line), but NON-ADDITIVE like every other percentage
    -- in this warehouse (star_schema.md §5) — never AVG this column across
    -- rows. At any coarser grain, recompute from
    -- SUM(gross_profit_amount) / SUM(net_sales_amount), the same rule
    -- mv_sales_monthly (§4) follows.
    ROUND(f.gross_profit_amount / NULLIF(f.net_sales_amount, 0), 4)
                                                                   AS gross_margin_pct,

    f.dw_batch_id
FROM        warehouse.fact_sales      f
JOIN        mart.vw_dim_order_date    od ON od.order_date_key = f.order_date_key
JOIN        mart.vw_dim_ship_date     sd ON sd.ship_date_key  = f.ship_date_key
JOIN        warehouse.dim_customer    c  ON c.customer_key    = f.customer_key
JOIN        warehouse.dim_product     p  ON p.product_key     = f.product_key
JOIN        warehouse.dim_store       s  ON s.store_key       = f.store_key
JOIN        warehouse.dim_promotion    pr ON pr.promotion_key  = f.promotion_key
JOIN        warehouse.dim_channel     ch ON ch.channel_key    = f.channel_key
JOIN        warehouse.dim_order_junk  oj ON oj.order_junk_key = f.order_junk_key;


-- -----------------------------------------------------------------------------
-- mart.vw_inventory_detail
-- -----------------------------------------------------------------------------
-- GRAIN: unchanged from fact_inventory_snapshot — one row per product, per
-- store, per day. Only one dim_date role here (no ship date on a stock
-- position — architecture.md §4.1 bus matrix).
--
-- is_below_reorder_point is NEW at this layer, defined once for the same reason
-- gross_margin_pct is above: without a single definition, one dashboard writes
-- `quantity_on_hand < reorder_point` and another writes `<=`, and the two
-- silently disagree about which rows count as low stock. is_stockout itself is
-- NOT redefined here — it already has exactly one definition, the generated
-- column on fact_inventory_snapshot — this view only surfaces it.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vw_inventory_detail AS
SELECT
    i.snapshot_date_key,
    d.full_date                              AS snapshot_date,
    d.year_number                            AS snapshot_year,
    d.month_number                           AS snapshot_month,
    d.year_month_number                      AS snapshot_year_month,

    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,

    s.store_id,
    s.store_name,
    s.store_type,
    s.city                                    AS store_city,
    s.region                                  AS store_region,

    i.quantity_on_hand,
    i.quantity_on_order,
    i.reorder_point,
    i.inventory_value_amount,
    i.is_stockout,
    (i.quantity_on_hand <= i.reorder_point)    AS is_below_reorder_point,

    i.dw_batch_id
FROM warehouse.fact_inventory_snapshot i
JOIN warehouse.dim_date    d ON d.date_key    = i.snapshot_date_key
JOIN warehouse.dim_product p ON p.product_key = i.product_key
JOIN warehouse.dim_store   s ON s.store_key   = i.store_key;


-- =============================================================================
-- SECTION 3 — MATERIALIZED-VIEW SUBSTITUTES (mv_*)
-- =============================================================================
-- Two pre-aggregated rollups, each chosen because it is the exact query an
-- index in indexes.sql was built to serve — this is where those indexes
-- actually get exercised, not just justified in a comment.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- mart.mv_sales_monthly  — named directly in architecture.md §6's example
-- -----------------------------------------------------------------------------
-- GRAIN: one row per calendar month, per product category, per channel.
-- Serves the "revenue by channel by month" headline report that
-- ix_fact_sales_channel_date_covering (indexes.sql §3) exists for, and the
-- product-trend-over-time question ix_fact_sales_date_product covers.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS mart.mv_sales_monthly;

CREATE TABLE mart.mv_sales_monthly (
    year_month_number      INT           NOT NULL COMMENT 'YYYYMM, from dim_date',
    year_number            SMALLINT      NOT NULL,
    month_number           TINYINT       NOT NULL,
    month_name             VARCHAR(20)   NOT NULL,
    category               VARCHAR(100)  NOT NULL,
    channel_name           VARCHAR(100)  NOT NULL,
    channel_group          VARCHAR(50)   NOT NULL,

    order_count            BIGINT        NOT NULL COMMENT 'COUNT(DISTINCT order_number)',
    line_count             BIGINT        NOT NULL,
    total_quantity         BIGINT        NOT NULL,
    gross_sales_amount     DECIMAL(16,2) NOT NULL,
    total_discount_amount  DECIMAL(16,2) NOT NULL,
    net_sales_amount       DECIMAL(16,2) NOT NULL,
    total_cost_amount      DECIMAL(16,2) NOT NULL,
    gross_profit_amount    DECIMAL(16,2) NOT NULL,
    -- DECIMAL(9,4), not the tighter (6,4) a "percentage" suggests: this is a
    -- ratio of two SUMs, and a bucket where discounts/returns push net sales
    -- close to zero can send the ratio far outside +/-99.9999 while cost stays
    -- large. Strict mode would abort the whole refresh on that one row -
    -- headroom here costs nothing and removes that fragility.
    gross_margin_pct       DECIMAL(9,4)  NULL
        COMMENT 'SUM(profit)/SUM(net_sales) AT THIS GRAIN - never rolled up by averaging',

    refreshed_at           DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_mv_sales_monthly PRIMARY KEY (year_month_number, category, channel_name)
) ENGINE = InnoDB
  COMMENT = 'Materialized-view substitute (see PORT NOTE). Refresh via mart.sp_refresh_materialized_views().';


-- -----------------------------------------------------------------------------
-- mart.mv_customer_rfm  — Recency / Frequency / Monetary per customer
-- -----------------------------------------------------------------------------
-- GRAIN: one row per customer_id — NOT per customer_key. A customer with SCD2
-- history owns several customer_keys; this rolls all of them back up to the one
-- natural entity a business question like "who are our best customers" means.
--
-- This is deliberately the exact shape of query ix_fact_sales_customer_date
-- (indexes.sql §3) was added for, and which that file explicitly flags as the
-- most speculative of its five indexes. Materializing this rollup is the real
-- validation that index was asking for: EXPLAIN the query in
-- mart.sp_refresh_materialized_views() below and confirm it is used.
--
-- customer_name / loyalty_tier / region here are the CURRENT (as-is) values —
-- via cur.is_current = 1 — because "who is our best customer today" is the
-- natural phrasing of an RFM report. The underlying net_sales_amount is still
-- summed from whichever historical customer_key each sale actually pointed at,
-- so the money is attributed correctly even though the label shown is current.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS mart.mv_customer_rfm;

CREATE TABLE mart.mv_customer_rfm (
    customer_id          VARCHAR(50)   NOT NULL,
    customer_name        VARCHAR(200)  NOT NULL COMMENT 'CURRENT (as-is) name',
    loyalty_tier         VARCHAR(50)   NOT NULL COMMENT 'CURRENT (as-is) tier',
    region               VARCHAR(100)  NOT NULL COMMENT 'CURRENT (as-is) region',

    first_order_date     DATE          NOT NULL,
    last_order_date      DATE          NOT NULL,
    recency_days         INT           NOT NULL
        COMMENT 'Days since last order, AS OF THE WAREHOUSE MAX ORDER DATE - not wall-clock NOW()',
    order_count          BIGINT        NOT NULL COMMENT 'Frequency: COUNT(DISTINCT order_number)',
    net_sales_amount     DECIMAL(16,2) NOT NULL COMMENT 'Monetary',
    avg_order_value      DECIMAL(14,2) NULL,

    refreshed_at          DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_mv_customer_rfm PRIMARY KEY (customer_id)
) ENGINE = InnoDB
  COMMENT = 'Materialized-view substitute (see PORT NOTE). Refresh via mart.sp_refresh_materialized_views().';


-- =============================================================================
-- SECTION 4 — REFRESH PROCEDURE
-- =============================================================================
-- The stand-in for REFRESH MATERIALIZED VIEW. TRUNCATE + re-INSERT rather than
-- an incremental upsert: at this data volume (~50k fact rows) a full rebuild is
-- simpler, always correct, and fast enough that incremental maintenance would be
-- complexity bought for no real benefit — the same cost/benefit call
-- architecture.md §5.4 makes about partitioning at this scale.
-- =============================================================================

DROP PROCEDURE IF EXISTS mart.sp_refresh_materialized_views;

DELIMITER $$

CREATE PROCEDURE mart.sp_refresh_materialized_views()
BEGIN
    DECLARE v_asof_date DATE;

    -- ---- mv_sales_monthly --------------------------------------------------
    TRUNCATE TABLE mart.mv_sales_monthly;

    INSERT INTO mart.mv_sales_monthly (
        year_month_number, year_number, month_number, month_name,
        category, channel_name, channel_group,
        order_count, line_count, total_quantity,
        gross_sales_amount, total_discount_amount, net_sales_amount,
        total_cost_amount, gross_profit_amount, gross_margin_pct
    )
    SELECT
        d.year_month_number, d.year_number, d.month_number, d.month_name,
        p.category, ch.channel_name, ch.channel_group,
        COUNT(DISTINCT f.order_number),
        COUNT(*),
        SUM(f.quantity),
        SUM(f.gross_sales_amount),
        SUM(f.discount_amount),
        SUM(f.net_sales_amount),
        SUM(f.total_cost_amount),
        SUM(f.gross_profit_amount),
        ROUND(SUM(f.gross_profit_amount) / NULLIF(SUM(f.net_sales_amount), 0), 4)
    FROM   warehouse.fact_sales   f
    JOIN   warehouse.dim_date    d  ON d.date_key     = f.order_date_key
    JOIN   warehouse.dim_product p  ON p.product_key  = f.product_key
    JOIN   warehouse.dim_channel ch ON ch.channel_key = f.channel_key
    GROUP  BY d.year_month_number, d.year_number, d.month_number, d.month_name,
              p.category, ch.channel_name, ch.channel_group;

    -- ---- mv_customer_rfm ----------------------------------------------------
    -- "As of" is the latest order date actually loaded, not wall-clock NOW().
    -- A batch warehouse's notion of "today" is the data, not the calendar:
    -- recency measured against real time would silently drift on every calendar
    -- day even between ETL runs, which is meaningless for a fixed historical
    -- load like this one (orders run through 2025-12-31, per etl_simulation.sql).
    SELECT MAX(d.full_date) INTO v_asof_date
    FROM   warehouse.fact_sales f
    JOIN   warehouse.dim_date  d ON d.date_key = f.order_date_key;

    TRUNCATE TABLE mart.mv_customer_rfm;

    INSERT INTO mart.mv_customer_rfm (
        customer_id, customer_name, loyalty_tier, region,
        first_order_date, last_order_date, recency_days,
        order_count, net_sales_amount, avg_order_value
    )
    SELECT
        cur.customer_id,
        cur.customer_name,
        cur.loyalty_tier,
        cur.region,
        MIN(d.full_date),
        MAX(d.full_date),
        DATEDIFF(v_asof_date, MAX(d.full_date)),
        COUNT(DISTINCT f.order_number),
        SUM(f.net_sales_amount),
        ROUND(SUM(f.net_sales_amount) / NULLIF(COUNT(DISTINCT f.order_number), 0), 2)
    FROM   warehouse.fact_sales   f
    JOIN   warehouse.dim_date     d   ON d.date_key      = f.order_date_key
    JOIN   warehouse.dim_customer c   ON c.customer_key  = f.customer_key
    JOIN   warehouse.dim_customer cur ON cur.customer_id = c.customer_id
                                     AND cur.is_current   = 1
    WHERE  c.customer_key > 0   -- exclude the reserved Unknown/Not Applicable members
    GROUP  BY cur.customer_id, cur.customer_name, cur.loyalty_tier, cur.region;
END$$

DELIMITER ;

-- Populate both mv_* tables now, so views.sql leaves the mart layer usable
-- immediately rather than defined-but-empty.
CALL mart.sp_refresh_materialized_views();


-- =============================================================================
-- SECTION 5 — VERIFICATION
-- =============================================================================

-- 1. Object inventory. Expect 6 VIEWs and 2 BASE TABLEs in `mart`.
SELECT table_type, COUNT(*) AS object_count
FROM   information_schema.tables
WHERE  table_schema = 'mart'
GROUP  BY table_type;

-- 2. mv_* row counts, and how long ago they were refreshed.
SELECT 'mv_sales_monthly' AS mv_name, COUNT(*) AS row_count, MAX(refreshed_at) AS last_refreshed
FROM   mart.mv_sales_monthly
UNION ALL
SELECT 'mv_customer_rfm', COUNT(*), MAX(refreshed_at)
FROM   mart.mv_customer_rfm;

-- 3. vw_sales_detail sanity check — net_sales_amount here must equal the sum
--    straight off the fact table. Any mismatch means a join in the view is
--    fanning out (duplicating rows), almost always a dimension that has more
--    than one current/matching row where the view assumed exactly one.
SELECT
    (SELECT SUM(net_sales_amount) FROM warehouse.fact_sales)     AS fact_total,
    (SELECT SUM(net_sales_amount) FROM mart.vw_sales_detail)     AS view_total;

-- 4. EXPLAIN the customer rollup that mv_customer_rfm materializes — confirm
--    it uses ix_fact_sales_customer_date (indexes.sql §3), the index that file
--    flagged as unproven. `key` should read that index name.
EXPLAIN
SELECT customer_key, COUNT(DISTINCT order_number), SUM(net_sales_amount)
FROM   warehouse.fact_sales
GROUP  BY customer_key;

-- 5. Spot-check: five most recent order lines, business-friendly end to end.
SELECT order_number, order_line_number, order_date, customer_name, product_name,
       store_name, channel_name, net_sales_amount, gross_margin_pct
FROM   mart.vw_sales_detail
ORDER  BY order_date DESC, sales_key DESC
LIMIT  5;

-- =============================================================================
-- END OF views.sql
--
-- Next: analytical_queries.sql — business questions answered against this mart
-- (and, where the point is demonstrating window functions/CTEs/grouping sets
-- directly on the star, against warehouse.fact_sales) with window functions,
-- CTEs, and grouping sets.
-- =============================================================================
