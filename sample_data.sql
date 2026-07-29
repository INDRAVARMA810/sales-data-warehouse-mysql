-- =============================================================================
-- sample_data.sql — Mini Sales Data Warehouse
-- =============================================================================
--
--   Target platform : MySQL 8.0.16+
--   Prerequisite    : schema.sql must have run successfully
--   Run with:
--       mysql --user=root --password --show-warnings < sample_data.sql
--
--   Runtime is well under a minute. Every statement is SET-BASED — there is
--   not a single loop, cursor, or row-by-row INSERT in this file. Generating
--   50,000 rows with a WHILE loop is the classic way to turn a 3-second job
--   into a 3-minute one, and it teaches exactly the wrong instinct.
--
--   Sections 10-12 (the three high-volume generators: orders, order lines,
--   inventory) run as many small INSERT ... SELECT statements instead of one
--   large one each, specifically so no single statement runs long enough to
--   trip a client-side query timeout in MySQL Workbench. See Section 10's
--   header for the full rationale — this is still all set-based; "many small
--   statements" is a different thing from "row-by-row".
--
-- -----------------------------------------------------------------------------
-- WHAT THIS FILE DOES, AND THE ONE THING IT DELIBERATELY DOES NOT
-- -----------------------------------------------------------------------------
-- It plays TWO distinct roles that happen to live in one file. Keeping them
-- straight is the key to reading it:
--
--   ROLE 1 — WAREHOUSE-MANAGED REFERENCE DATA (§4-§7)
--            dim_date, dim_order_junk, dim_channel, dim_promotion are written
--            DIRECTLY into the warehouse, bypassing staging entirely.
--            That is not a shortcut. Each has no source system to extract from:
--              * dim_date       is a calendar. architecture.md §4.2 marks it
--                               Type 0, "pre-generated, never loaded by ETL".
--              * dim_order_junk is a cross-product the warehouse invents; no
--                               source system has a table of flag combinations.
--              * dim_channel    and dim_promotion are near-static reference
--              * dim_promotion  lists. Note architecture.md §2 defines exactly
--                               FIVE staging tables, and there is no
--                               stg_channel or stg_promotion among them — the
--                               approved design already says these are not
--                               extracted.
--
--   ROLE 2 — SOURCE SYSTEM SIMULATION (§8-§12)
--            Everything else pretends to be the OLTP database, the product MDM,
--            and the store CSV feed from er_diagram.md §1. It writes ONLY to
--            staging, in string form, warts and all.
--
-- WHAT IT DOES NOT DO: it never writes dim_customer, dim_product, dim_store,
-- fact_sales or fact_inventory_snapshot. Those are earned by etl_simulation.sql
-- (Milestone 4) transforming staging. If this file populated them, the ETL
-- would have nothing to demonstrate and the SCD2 logic would never run.
--
-- -----------------------------------------------------------------------------
-- DETERMINISM — why CRC32 and not RAND()
-- -----------------------------------------------------------------------------
-- Every "random" value here comes from CRC32(CONCAT('<salt>', <row key>)) % n.
-- CRC32 is a pure function, so this file produces BYTE-IDENTICAL data on every
-- run, on every machine. That matters more than it sounds:
--   * A failing ETL test stays failing until you fix it, instead of vanishing on
--     re-run and coming back next week.
--   * The expected counts in §14 are meaningful rather than approximate.
--   * Two people debugging the same problem are looking at the same rows.
-- RAND() without a seed gives up all three. The salt string is what makes
-- independent draws independent — CRC32('qty7') and CRC32('cust7') are
-- uncorrelated, whereas reusing one hash for quantity and customer would
-- silently couple them and put a visible artifact in the data.
--
-- -----------------------------------------------------------------------------
-- DELIBERATE DATA QUALITY DEFECTS
-- -----------------------------------------------------------------------------
-- This file injects bad data ON PURPOSE. A pipeline demonstrated only against
-- clean input proves nothing — the interesting code paths are the ones that
-- handle the mess, and they are exactly the paths that never execute in a demo
-- built on perfect data.
--
-- Each defect below targets a specific mechanism that etl_simulation.sql must
-- prove it handles. §14 reports the actual count of each.
--
--   #   Defect injected                     Exercises
--   --  ----------------------------------  ------------------------------------
--   1   Empty customer_id                   meta.rejected_record quarantine
--   2   Malformed signup_date               type-conversion rejection
--   3   Duplicate customer_id in one feed   dedupe / "latest wins"
--   4   unit_cost = 'N/A'                   non-numeric in a numeric column
--   5   Negative list_price                 range validation
--   6   Duplicate product_id                dedupe
--   7   Non-numeric square_feet             CSV feed is the least trustworthy
--   8   Empty region on a store             'Unknown' defaulting, not NULL
--   9   Order line -> unknown product_id    LATE-ARRIVING DIMENSION,
--                                           product_key = -1 (Unknown)
--  10   Order line -> unknown customer_id   customer_key = -1
--  11   quantity = 'N/A' / '0'              measure validation, CHECK qty <> 0
--  12   Exact duplicate order lines         IDEMPOTENCY via uq_fact_sales_natural
--  13   Negative quantity_on_hand           CHECK ck_fact_inventory_quantities
--
-- NOT a defect, though it looks like one: ~70% of order lines have an EMPTY
-- promotion_id, and ~12% have an empty shipped_date. Those are the legitimate
-- "Not Applicable" cases that must resolve to key 0, NOT to key -1. Telling the
-- two apart is the entire point of architecture.md §5.2, and the ETL has to get
-- it right in both directions.
--
-- =============================================================================


-- =============================================================================
-- SECTION 0 — SESSION SETUP
-- =============================================================================

-- MONTHNAME() and DAYNAME() return LOCALIZED strings, controlled by
-- lc_time_names. If the server is set to, say, de_DE, dim_date silently fills
-- with 'Januar' and 'Montag' — no error, no warning, and every report grouped by
-- month name is subtly wrong for anyone expecting English. Pin it explicitly.
SET SESSION lc_time_names = 'en_US';

-- The recursive CTE in §3 generates 10,000 rows. MySQL's default recursion cap
-- is 1,000, and exceeding it is a hard ERROR 3636, not a truncation — which is
-- the good outcome, but it stops the script dead if you forget this line.
SET SESSION cte_max_recursion_depth = 20000;

-- Not strictly required here (no surrogate key 0 is inserted by this file — the
-- reserved members already exist from schema.sql §4b), but set for consistency
-- so that copy-pasting a seed statement out of this file into a session that
-- DOES touch reserved members cannot silently misbehave.
SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_AUTO_VALUE_ON_ZERO');

-- The generated calendar and the sales window, defined once so §4, §10 and §12
-- cannot drift apart.
--   Calendar : 2020-01-01 .. 2030-12-31  = exactly 4,018 days
--   Sales    : 2023-01-01 .. 2025-12-31  = 1,096 days (3 full years, so
--              year-over-year analysis has two full comparison pairs)
SET @calendar_start = DATE '2020-01-01';
SET @calendar_days  = 4018;
SET @sales_start    = DATE '2023-01-01';
SET @sales_days     = 1096;


-- =============================================================================
-- SECTION 1 — OPEN AN ETL BATCH
-- =============================================================================
-- Even the seed runs inside a batch. architecture.md §7 requires every warehouse
-- row to carry dw_batch_id, and "0" was reserved in schema.sql for rows the DDL
-- itself created. The reference dimensions written below are created by a
-- process, so they get a real batch id and real lineage.
--
-- This also exercises the meta control plane before etl_simulation.sql depends
-- on it — if the batch pattern is broken, better to find out here.
-- =============================================================================

INSERT INTO meta.etl_batch (batch_name, source_system, batch_status)
VALUES ('sample_data_seed', 'SYSTEM', 'RUNNING');

SET @batch_id = LAST_INSERT_ID();


-- =============================================================================
-- SECTION 2 — RESET (make this file re-runnable)
-- =============================================================================
-- Staging is transient by design (architecture.md §2.1): the tables persist so
-- they document the source contract, the rows do not. TRUNCATE rather than
-- DELETE — it is near-instant and it resets nothing we care about, since staging
-- has no AUTO_INCREMENT.
-- =============================================================================

TRUNCATE TABLE staging.stg_customer;
TRUNCATE TABLE staging.stg_product;
TRUNCATE TABLE staging.stg_store;
TRUNCATE TABLE staging.stg_sales_order_line;
TRUNCATE TABLE staging.stg_inventory;

-- The reference dimensions need care that staging does not.
--
-- WHY `> 0` AND NOT TRUNCATE: the reserved members -1 and 0 seeded by schema.sql
-- must survive. TRUNCATE would delete them, and every fact FK pointing at
-- Unknown or Not-Applicable would then fail with an error blaming the fact load
-- rather than this line.
--
-- WHY DELETE IS THE RIGHT FAILURE MODE HERE: if facts have already been loaded,
-- these DELETEs will FAIL on the RESTRICT foreign keys from fact_sales. That is
-- correct and desirable — silently deleting a dimension row that 50,000 fact
-- rows depend on is precisely the disaster ON DELETE RESTRICT exists to prevent
-- (schema.sql §4). Re-seeding a loaded warehouse means rebuilding it from
-- schema.sql, and the database should say so rather than let you half-do it.
DELETE FROM warehouse.dim_promotion  WHERE promotion_key  > 0;
DELETE FROM warehouse.dim_channel    WHERE channel_key    > 0;
DELETE FROM warehouse.dim_order_junk WHERE order_junk_key > 0;
DELETE FROM warehouse.dim_date       WHERE date_key       > 0;


-- =============================================================================
-- SECTION 3 — THE NUMBERS TABLE
-- =============================================================================
-- A tally table: integers 0..9,999. This single object is what makes every
-- generator below set-based. The pattern is worth internalizing — "I need N
-- rows" becomes "join to the numbers table and filter", which the optimizer
-- handles as one operation instead of N.
--
-- TEMPORARY on purpose: schema.sql's verification query asserts that `staging`
-- contains exactly 5 tables. A permanent helper here would break that check
-- forever. MySQL temporary tables are session-scoped, auto-dropped, and — unlike
-- permanent tables — do not appear in information_schema.tables at all.
--
-- THE TEMPORARY-TABLE TRAP, since every generator below is shaped around it:
-- MySQL cannot reference the same TEMPORARY table twice in one query. No
-- self-joins, no `tmp_number a CROSS JOIN tmp_number b`. Each query below
-- touches each temp table exactly once; where two independent sequences are
-- needed, they come from separate temp tables or from inline derived tables.
--
-- Built with a recursive CTE rather than the classic digit cross-join because it
-- states the intent in four lines instead of thirty. See §0 for the recursion
-- depth this requires.
-- =============================================================================

DROP TEMPORARY TABLE IF EXISTS tmp_number;
CREATE TEMPORARY TABLE tmp_number (
    n INT NOT NULL,
    PRIMARY KEY (n)
) ENGINE = InnoDB;

INSERT INTO tmp_number (n)
WITH RECURSIVE seq (n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 9999
)
SELECT n FROM seq;


-- =============================================================================
-- SECTION 4 — warehouse.dim_date        (SCD TYPE 0, 4,018 rows)
-- =============================================================================
-- One row per calendar day, 2020-01-01 through 2030-12-31.
--
-- The range extends five years past the sales window on purpose. A date
-- dimension that stops at today's data is a bug that surfaces the first time
-- someone loads a future-dated order or a forward-looking forecast: the FK
-- fails, and the fix is a schema change under time pressure. Calendars are
-- cheap — 4,018 rows is under half a megabyte — so generate generously.
--
-- WHY EVERY ATTRIBUTE IS STORED RATHER THAN COMPUTED
-- MONTH(order_date) in a WHERE clause cannot use an index: the column is wrapped
-- in a function, so MySQL must evaluate it for every row. Joining to dim_date
-- and filtering on d.month_number CAN use one. Multiply that across every query
-- an analyst will ever write and the storage is not a close call.
--
-- The fiscal calendar is the sharper argument. "Fiscal year starts 1 July" is a
-- business rule. Encoded here it is defined once and every report agrees;
-- re-derived in each query it is defined N times and eventually one of them is
-- wrong — silently, because the number still looks like a year.
-- =============================================================================

INSERT INTO warehouse.dim_date (
    date_key, full_date, year_number, quarter_number, month_number, month_name,
    day_of_month, week_of_year, day_name, year_month_number,
    is_weekend, is_holiday, fiscal_year, fiscal_quarter, prior_year_date_key
)
SELECT
    CAST(DATE_FORMAT(c.d, '%Y%m%d') AS SIGNED)          AS date_key,
    c.d                                                  AS full_date,
    YEAR(c.d)                                            AS year_number,
    QUARTER(c.d)                                         AS quarter_number,
    MONTH(c.d)                                           AS month_number,
    MONTHNAME(c.d)                                       AS month_name,
    DAYOFMONTH(c.d)                                      AS day_of_month,
    WEEKOFYEAR(c.d)                                      AS week_of_year,
    DAYNAME(c.d)                                         AS day_name,
    YEAR(c.d) * 100 + MONTH(c.d)                         AS year_month_number,

    -- DAYOFWEEK(): 1 = Sunday ... 7 = Saturday. Used instead of DAYNAME()
    -- comparisons throughout, because it is locale-independent — the holiday
    -- rules below must not change meaning when lc_time_names does.
    CASE WHEN DAYOFWEEK(c.d) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,

    -- US holiday calendar. The FLOATING holidays are the interesting part: each
    -- is expressible as "the Nth <weekday> of <month>", which becomes a pure
    -- day-of-month range test once you know the weekday. The 3rd Monday of
    -- January is the only Monday that can fall on the 15th-21st, so that pair of
    -- predicates identifies it exactly, with no iteration and no lookup table.
    CASE
        -- Fixed-date holidays.
        WHEN MONTH(c.d) =  1 AND DAYOFMONTH(c.d) =  1                       THEN 1  -- New Year's Day
        WHEN MONTH(c.d) =  7 AND DAYOFMONTH(c.d) =  4                       THEN 1  -- Independence Day
        WHEN MONTH(c.d) = 11 AND DAYOFMONTH(c.d) = 11                       THEN 1  -- Veterans Day
        WHEN MONTH(c.d) = 12 AND DAYOFMONTH(c.d) = 24                       THEN 1  -- Christmas Eve
        WHEN MONTH(c.d) = 12 AND DAYOFMONTH(c.d) = 25                       THEN 1  -- Christmas Day
        WHEN MONTH(c.d) = 12 AND DAYOFMONTH(c.d) = 31                       THEN 1  -- New Year's Eve
        -- Floating holidays: Nth weekday of the month.
        WHEN MONTH(c.d) =  1 AND DAYOFWEEK(c.d) = 2
                             AND DAYOFMONTH(c.d) BETWEEN 15 AND 21          THEN 1  -- MLK Day, 3rd Mon Jan
        WHEN MONTH(c.d) =  2 AND DAYOFWEEK(c.d) = 2
                             AND DAYOFMONTH(c.d) BETWEEN 15 AND 21          THEN 1  -- Presidents Day, 3rd Mon Feb
        WHEN MONTH(c.d) =  5 AND DAYOFWEEK(c.d) = 2
                             AND DAYOFMONTH(c.d) >= 25                      THEN 1  -- Memorial Day, last Mon May
        WHEN MONTH(c.d) =  9 AND DAYOFWEEK(c.d) = 2
                             AND DAYOFMONTH(c.d) <= 7                       THEN 1  -- Labor Day, 1st Mon Sep
        WHEN MONTH(c.d) = 11 AND DAYOFWEEK(c.d) = 5
                             AND DAYOFMONTH(c.d) BETWEEN 22 AND 28          THEN 1  -- Thanksgiving, 4th Thu Nov
        ELSE 0
    END                                                  AS is_holiday,

    -- Fiscal year runs 1 July - 30 June, so H2 of a calendar year belongs to the
    -- NEXT fiscal year. FY2024 = 2023-07-01 .. 2024-06-30.
    YEAR(c.d) + IF(MONTH(c.d) >= 7, 1, 0)                AS fiscal_year,

    -- Rotates the calendar month so July lands in Q1:
    --   Jul->0 div 3 = 0 -> Q1     Oct->3 div 3 = 1 -> Q2
    --   Jan->6 div 3 = 2 -> Q3     Apr->9 div 3 = 3 -> Q4
    ((MONTH(c.d) + 5) % 12) DIV 3 + 1                    AS fiscal_quarter,

    -- Same date, one year earlier — precomputed so year-over-year comparison is
    -- a self-join on an indexed integer instead of a non-sargable date
    -- expression. Dates in the calendar's first year have no prior-year row, so
    -- they point at the Unknown member (-1) rather than at a key that does not
    -- exist. This column is deliberately NOT a foreign key (schema.sql §4), but
    -- pointing it somewhere real keeps a YoY self-join honest: it returns
    -- 'Unknown' instead of silently dropping 2020 from the result.
    -- MySQL maps 29 Feb -> 28 Feb on the non-leap side, which is the convention
    -- retail finance uses.
    CASE
        WHEN DATE_SUB(c.d, INTERVAL 1 YEAR) >= @calendar_start
        THEN CAST(DATE_FORMAT(DATE_SUB(c.d, INTERVAL 1 YEAR), '%Y%m%d') AS SIGNED)
        ELSE -1
    END                                                  AS prior_year_date_key
FROM (
    SELECT DATE_ADD(@calendar_start, INTERVAL n.n DAY) AS d
    FROM   tmp_number n
    WHERE  n.n < @calendar_days
) c;


-- =============================================================================
-- SECTION 5 — warehouse.dim_order_junk  (SCD TYPE 0, 120 rows)
-- =============================================================================
-- The junk dimension, built as a literal CROSS JOIN — which is the clearest
-- possible statement of what a junk dimension IS. Four flag domains, every
-- combination materialized:
--
--     5 statuses x 4 payment methods x 3 priorities x 2 gift flags = 120 rows
--
-- The alternative designs, and the arithmetic that rejects them
-- (dimensional_modeling.md §7):
--   * Four columns on fact_sales    -> 4 extra columns x 50,000 rows, forever.
--   * Four separate dimensions      -> 4 extra joins to resolve 5, 4, 3 and 2
--                                      rows respectively.
--   * One junk dimension            -> 120 rows, one FK, one join.
--
-- Keys are assigned explicitly via ROW_NUMBER() rather than left to
-- AUTO_INCREMENT, so a re-run of this file reproduces IDENTICAL keys. With
-- AUTO_INCREMENT the counter keeps climbing across the DELETE in §2, and the
-- second run would produce the same 120 rows under different keys — harmless in
-- production, but it would break every hard-coded expectation in a test.
--
-- The labels are verbose ('Awaiting Fulfilment', not 'AWF'). A junk dimension is
-- exactly where verbose values are cheapest — 120 rows — and where they pay off
-- most, because these strings land directly in report output.
-- =============================================================================

INSERT INTO warehouse.dim_order_junk (
    order_junk_key, order_status, payment_method, shipping_priority, is_gift,
    junk_description, dw_batch_id, dw_source_system
)
SELECT
    ROW_NUMBER() OVER (ORDER BY st.ord, pm.ord, sp.ord, gf.is_gift) AS order_junk_key,
    st.order_status,
    pm.payment_method,
    sp.shipping_priority,
    gf.is_gift,
    CONCAT(st.order_status, ' / ', pm.payment_method, ' / ', sp.shipping_priority,
           IF(gf.is_gift = 1, ' / Gift', ''))                       AS junk_description,
    @batch_id,
    'SYSTEM'
FROM      (SELECT 1 AS ord, 'Awaiting Fulfilment' AS order_status
           UNION ALL SELECT 2, 'Shipped'
           UNION ALL SELECT 3, 'Delivered'
           UNION ALL SELECT 4, 'Cancelled'
           UNION ALL SELECT 5, 'Returned')                  st
CROSS JOIN (SELECT 1 AS ord, 'Credit Card' AS payment_method
           UNION ALL SELECT 2, 'Debit Card'
           UNION ALL SELECT 3, 'PayPal'
           UNION ALL SELECT 4, 'Gift Card')                 pm
CROSS JOIN (SELECT 1 AS ord, 'Standard' AS shipping_priority
           UNION ALL SELECT 2, 'Express'
           UNION ALL SELECT 3, 'Overnight')                 sp
CROSS JOIN (SELECT 0 AS is_gift UNION ALL SELECT 1)         gf;


-- =============================================================================
-- SECTION 6 — warehouse.dim_channel     (SCD TYPE 1, 5 rows)
-- =============================================================================
-- Small enough to write out by hand, and it should be: these five values are a
-- business taxonomy, not data. Inventing them procedurally would obscure that.
--
-- Note this dimension is NOT in the junk dimension despite having only five
-- values. Channel is a PRIMARY REPORTING AXIS — "revenue by channel" is a
-- headline number — and it carries its own attribute, channel_group. Junk
-- dimensions are for flags you filter on; this is an axis you group by
-- (schema.sql, dim_channel header).
-- =============================================================================

INSERT INTO warehouse.dim_channel (
    channel_key, channel_id, channel_name, channel_group,
    dw_batch_id, dw_source_system
) VALUES
    (1, 'CH-WEB',     'Web',        'Digital',  @batch_id, 'SYSTEM'),
    (2, 'CH-APP',     'Mobile App', 'Digital',  @batch_id, 'SYSTEM'),
    (3, 'CH-STORE',   'Store',      'Physical', @batch_id, 'SYSTEM'),
    (4, 'CH-PHONE',   'Phone',      'Physical', @batch_id, 'SYSTEM'),
    (5, 'CH-PARTNER', 'Partner',    'Digital',  @batch_id, 'SYSTEM');


-- =============================================================================
-- SECTION 7 — warehouse.dim_promotion   (SCD TYPE 1, 30 rows)
-- =============================================================================
-- Thirty campaigns spread evenly across the three-year sales window — roughly
-- one every 36 days, each running 7 to 45 days.
--
-- The spacing is chosen so that on any given date, zero, one, or occasionally
-- two promotions are active. That variability is what makes promotional-lift
-- analysis possible in Milestone 6: if a promotion were always running there
-- would be no baseline to compare against, and if they never overlapped the
-- attribution question would never come up.
--
-- discount_pct is stored as a FRACTION (0.1500 = 15%), never as 15.00. The
-- schema CHECK enforces 0..1 for exactly this reason — storing 15 to mean 15%
-- would multiply revenue by fifteen wherever the column is used directly, and
-- the CHECK turns that into a load failure rather than a wrong number.
-- =============================================================================

INSERT INTO warehouse.dim_promotion (
    promotion_key, promotion_id, promotion_name, promotion_type,
    discount_pct, start_date, end_date, dw_batch_id, dw_source_system
)
SELECT
    p.n                                        AS promotion_key,
    CONCAT('PR-', LPAD(p.n, 3, '0'))           AS promotion_id,
    CONCAT(
        ELT(1 + (p.n % 4), 'Spring', 'Summer', 'Autumn', 'Winter'), ' ',
        p.promotion_type, ' ',
        YEAR(p.start_date)
    )                                          AS promotion_name,
    p.promotion_type,
    p.discount_pct,
    p.start_date,
    -- 7 to 45 days long.
    DATE_ADD(p.start_date,
             INTERVAL 6 + (CRC32(CONCAT('prlen', p.n)) % 39) DAY) AS end_date,
    @batch_id,
    'SYSTEM'
FROM (
    SELECT
        n.n,
        -- ELT() is MySQL's 1-based array indexing: ELT(2,'a','b','c') -> 'b'.
        -- Cleaner than a five-branch CASE for picking from a fixed list.
        ELT(1 + (n.n % 5),
            'Percent Off', 'BOGO', 'Seasonal Clearance',
            'Flash Sale', 'Loyalty Bonus')                    AS promotion_type,
        -- 5% to 40%, in whole percentage points.
        ROUND(0.05 + (CRC32(CONCAT('prpct', n.n)) % 36) / 100.0, 4) AS discount_pct,
        -- Every ~36 days, jittered by up to 19 days so the campaigns do not sit
        -- on a visibly mechanical grid.
        DATE_ADD(@sales_start,
                 INTERVAL (n.n - 1) * 36 + (CRC32(CONCAT('prstart', n.n)) % 20) DAY)
                                                              AS start_date
    FROM   tmp_number n
    WHERE  n.n BETWEEN 1 AND 30
) p;


-- =============================================================================
-- SECTION 8 — TYPED GENERATORS (temporary)
-- =============================================================================
-- From here the file stops being the warehouse and starts being the SOURCE
-- SYSTEMS. Staging columns are all strings, but generating strings directly
-- would mean casting them back at every use. So each entity is built once as a
-- properly TYPED temporary table, then serialized to staging in §9.
--
-- That mirrors reality — the source system holds typed data; it is the
-- extract/transport that degrades it to text — and it means §10 can do real
-- arithmetic with a product's price instead of parsing it out of a VARCHAR.
-- =============================================================================

-- ---- Geography reference: 25 locations --------------------------------------
-- Shared by stores and customers so that "region" means the same thing on both
-- sides. That is CONFORMANCE in miniature: if stores said 'West' and customers
-- said 'Western US', no report could compare them, and the ETL could not fix it
-- without a mapping table nobody maintains.
DROP TEMPORARY TABLE IF EXISTS tmp_geography;
CREATE TEMPORARY TABLE tmp_geography (
    geo_id         INT          NOT NULL,
    city           VARCHAR(100) NOT NULL,
    state_province VARCHAR(100) NOT NULL,
    country        VARCHAR(100) NOT NULL,
    region         VARCHAR(100) NOT NULL,
    postal_prefix  VARCHAR(10)  NOT NULL,
    PRIMARY KEY (geo_id)
) ENGINE = InnoDB;

INSERT INTO tmp_geography VALUES
    ( 1, 'New York',      'NY', 'United States', 'Northeast', '100'),
    ( 2, 'Boston',        'MA', 'United States', 'Northeast', '021'),
    ( 3, 'Philadelphia',  'PA', 'United States', 'Northeast', '191'),
    ( 4, 'Pittsburgh',    'PA', 'United States', 'Northeast', '152'),
    ( 5, 'Newark',        'NJ', 'United States', 'Northeast', '071'),
    ( 6, 'Atlanta',       'GA', 'United States', 'South',     '303'),
    ( 7, 'Miami',         'FL', 'United States', 'South',     '331'),
    ( 8, 'Dallas',        'TX', 'United States', 'South',     '752'),
    ( 9, 'Austin',        'TX', 'United States', 'South',     '787'),
    (10, 'Houston',       'TX', 'United States', 'South',     '770'),
    (11, 'Charlotte',     'NC', 'United States', 'South',     '282'),
    (12, 'Nashville',     'TN', 'United States', 'South',     '372'),
    (13, 'Chicago',       'IL', 'United States', 'Midwest',   '606'),
    (14, 'Detroit',       'MI', 'United States', 'Midwest',   '482'),
    (15, 'Minneapolis',   'MN', 'United States', 'Midwest',   '554'),
    (16, 'Columbus',      'OH', 'United States', 'Midwest',   '432'),
    (17, 'St. Louis',     'MO', 'United States', 'Midwest',   '631'),
    (18, 'Seattle',       'WA', 'United States', 'West',      '981'),
    (19, 'Portland',      'OR', 'United States', 'West',      '972'),
    (20, 'San Francisco', 'CA', 'United States', 'West',      '941'),
    (21, 'Los Angeles',   'CA', 'United States', 'West',      '900'),
    (22, 'Denver',        'CO', 'United States', 'West',      '802'),
    (23, 'Phoenix',       'AZ', 'United States', 'West',      '850'),
    (24, 'Toronto',       'ON', 'Canada',        'Canada',    'M5H'),
    (25, 'Vancouver',     'BC', 'Canada',        'Canada',    'V6B');

-- ---- Product taxonomy: 15 subcategories under 5 categories ------------------
-- base_price drives the price ladder, so a laptop costs laptop money and a
-- notebook costs notebook money. Without a per-category base, every product
-- lands in the same price band and margin analysis becomes meaningless — every
-- category would look identically profitable.
DROP TEMPORARY TABLE IF EXISTS tmp_category;
CREATE TEMPORARY TABLE tmp_category (
    cat_id      INT           NOT NULL,
    category    VARCHAR(100)  NOT NULL,
    subcategory VARCHAR(100)  NOT NULL,
    base_price  DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (cat_id)
) ENGINE = InnoDB;

INSERT INTO tmp_category VALUES
    ( 1, 'Electronics',        'Laptops',           899.00),
    ( 2, 'Electronics',        'Smartphones',       649.00),
    ( 3, 'Electronics',        'Audio',             149.00),
    ( 4, 'Electronics',        'Cameras',           499.00),
    ( 5, 'Home & Kitchen',     'Cookware',           79.00),
    ( 6, 'Home & Kitchen',     'Small Appliances',  119.00),
    ( 7, 'Home & Kitchen',     'Furniture',         349.00),
    ( 8, 'Apparel',            'Mens Clothing',      44.00),
    ( 9, 'Apparel',            'Womens Clothing',    49.00),
    (10, 'Apparel',            'Footwear',           84.00),
    (11, 'Sports & Outdoors',  'Fitness Equipment', 199.00),
    (12, 'Sports & Outdoors',  'Camping Gear',      109.00),
    (13, 'Office',             'Stationery',         11.50),
    (14, 'Office',             'Printers',          179.00),
    (15, 'Office',             'Office Furniture',  249.00);

-- ---- Brands: 12 fictional names --------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_brand;
CREATE TEMPORARY TABLE tmp_brand (
    brand_id   INT          NOT NULL,
    brand_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (brand_id)
) ENGINE = InnoDB;

INSERT INTO tmp_brand VALUES
    ( 1, 'Aurora'),      ( 2, 'Northwind'),   ( 3, 'Contoso'),
    ( 4, 'Fabrikam'),    ( 5, 'Litware'),     ( 6, 'Proseware'),
    ( 7, 'Tailspin'),    ( 8, 'Wingtip'),     ( 9, 'Adventure Peak'),
    (10, 'Coho'),        (11, 'Lucerne'),     (12, 'Blue Harbour');

-- ---- 25 stores, one per geography -------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_store;
CREATE TEMPORARY TABLE tmp_store (
    store_num      INT           NOT NULL,
    store_id       VARCHAR(20)   NOT NULL,
    store_name     VARCHAR(200)  NOT NULL,
    store_type     VARCHAR(50)   NOT NULL,
    city           VARCHAR(100)  NOT NULL,
    state_province VARCHAR(100)  NOT NULL,
    country        VARCHAR(100)  NOT NULL,
    region         VARCHAR(100)  NOT NULL,
    square_feet    INT           NOT NULL,
    opened_date    DATE          NOT NULL,
    PRIMARY KEY (store_num)
) ENGINE = InnoDB;

INSERT INTO tmp_store
SELECT
    g.geo_id,
    CONCAT('S-', LPAD(g.geo_id, 3, '0')),
    CONCAT(g.city, ' ',
           ELT(1 + (g.geo_id % 4), 'Flagship', 'Central', 'Outlet', 'Express')),
    ELT(1 + (g.geo_id % 4), 'Flagship', 'Standard', 'Outlet', 'Express'),
    g.city, g.state_province, g.country, g.region,
    -- Flagships are large, Express formats small. Tied to store_type via the
    -- same modulo so the two attributes stay consistent with each other.
    ELT(1 + (g.geo_id % 4), 48000, 26000, 18000, 6500)
        + (CRC32(CONCAT('sqft', g.geo_id)) % 4000),
    -- Opened between 2005 and 2019, all comfortably before the sales window.
    DATE_ADD(DATE '2005-01-01',
             INTERVAL (CRC32(CONCAT('open', g.geo_id)) % 5475) DAY)
FROM tmp_geography g;

-- ---- 1,000 customers ---------------------------------------------------------
-- star_schema.md §6 sizes dim_customer at ~1,200 rows INCLUDING SCD2 versions.
-- 1,000 distinct customers here leaves headroom for the ~200 extra rows that
-- etl_simulation.sql will create when it processes tier and address changes.
-- Getting this right matters: if the seed already produced 1,200 customers,
-- there would be no room to demonstrate SCD2 growth.
DROP TEMPORARY TABLE IF EXISTS tmp_customer;
CREATE TEMPORARY TABLE tmp_customer (
    cust_num       INT          NOT NULL,
    customer_id    VARCHAR(20)  NOT NULL,
    customer_name  VARCHAR(200) NOT NULL,
    email          VARCHAR(255) NOT NULL,
    segment_code   VARCHAR(50)  NOT NULL,
    loyalty_tier   VARCHAR(50)  NOT NULL,
    signup_date    DATE         NOT NULL,
    city           VARCHAR(100) NOT NULL,
    state_province VARCHAR(100) NOT NULL,
    country        VARCHAR(100) NOT NULL,
    region         VARCHAR(100) NOT NULL,
    postal_code    VARCHAR(20)  NOT NULL,
    PRIMARY KEY (cust_num)
) ENGINE = InnoDB;

INSERT INTO tmp_customer
SELECT
    c.n,
    CONCAT('C-', LPAD(c.n, 5, '0')),
    CONCAT(
        ELT(1 + (CRC32(CONCAT('fn', c.n)) % 20),
            'James','Mary','Robert','Patricia','John','Jennifer','Michael',
            'Linda','David','Elizabeth','William','Barbara','Richard','Susan',
            'Joseph','Jessica','Thomas','Sarah','Charles','Karen'),
        ' ',
        ELT(1 + (CRC32(CONCAT('ln', c.n)) % 20),
            'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller',
            'Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez',
            'Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin')
    ),
    CONCAT('customer', c.n, '@example.com'),
    -- Verbose values, not codes (dimensional_modeling.md §4): 'Home Office',
    -- never 'HO'. Weighted so Consumer dominates, as it would in retail.
    CASE
        WHEN CRC32(CONCAT('seg', c.n)) % 100 < 55 THEN 'Consumer'
        WHEN CRC32(CONCAT('seg', c.n)) % 100 < 78 THEN 'Corporate'
        WHEN CRC32(CONCAT('seg', c.n)) % 100 < 92 THEN 'Home Office'
        ELSE 'Small Business'
    END,
    -- Loyalty pyramid: many Bronze, few Platinum. THIS IS THE SCD2 COLUMN —
    -- etl_simulation.sql will promote a subset of these customers mid-window,
    -- and the whole point of Type 2 is that their January orders must keep
    -- reporting under the tier they held in January.
    CASE
        WHEN CRC32(CONCAT('tier', c.n)) % 100 < 48 THEN 'Bronze'
        WHEN CRC32(CONCAT('tier', c.n)) % 100 < 78 THEN 'Silver'
        WHEN CRC32(CONCAT('tier', c.n)) % 100 < 94 THEN 'Gold'
        ELSE 'Platinum'
    END,
    -- Signed up between 2019-01-01 and the end of the sales window. Customers
    -- must not predate the calendar (2020) by much, and must not sign up after
    -- their own first order — handled by keeping signups inside 2019-2025 and
    -- orders inside 2023-2025.
    DATE_ADD(DATE '2019-01-01',
             INTERVAL (CRC32(CONCAT('signup', c.n)) % 2557) DAY),
    g.city, g.state_province, g.country, g.region,
    CONCAT(g.postal_prefix, LPAD(CRC32(CONCAT('zip', c.n)) % 100, 2, '0'))
-- tmp_number appears EXACTLY ONCE here. An earlier draft of this statement
-- joined it to itself to pair a sequence with a filter, which is the trap
-- documented in §3: MySQL answers that with ERROR 1137 "Can't reopen table",
-- not with a wrong result. Worth stating because the failure looks nothing like
-- its cause.
FROM       (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 1000) c
CROSS JOIN tmp_geography g
WHERE      g.geo_id = 1 + (CRC32(CONCAT('geo', c.n)) % 25);

-- ---- 500 products -----------------------------------------------------------
-- ~600 rows in dim_product after SCD2 versioning (star_schema.md §6), so 500
-- distinct products leaves room for ~100 cost/price change versions.
DROP TEMPORARY TABLE IF EXISTS tmp_product;
CREATE TEMPORARY TABLE tmp_product (
    prod_num     INT           NOT NULL,
    product_id   VARCHAR(20)   NOT NULL,
    product_name VARCHAR(200)  NOT NULL,
    category     VARCHAR(100)  NOT NULL,
    subcategory  VARCHAR(100)  NOT NULL,
    brand_name   VARCHAR(100)  NOT NULL,
    unit_cost    DECIMAL(12,2) NOT NULL,
    list_price   DECIMAL(12,2) NOT NULL,
    is_active    TINYINT(1)    NOT NULL,
    PRIMARY KEY (prod_num)
) ENGINE = InnoDB;

INSERT INTO tmp_product
SELECT
    p.n,
    CONCAT('P-', LPAD(p.n, 4, '0')),
    CONCAT(b.brand_name, ' ', c.subcategory, ' ',
           ELT(1 + (CRC32(CONCAT('mdl', p.n)) % 6),
               'Model A', 'Model X', 'Pro', 'Lite', 'Max', 'Classic'),
           ' ', 1 + (CRC32(CONCAT('gen', p.n)) % 9)),
    c.category,
    c.subcategory,
    b.brand_name,
    -- Cost is derived FROM list price at a 42-68% cost ratio, not generated
    -- independently. Independent generation would occasionally produce cost >
    -- price and hand Milestone 6 a set of permanently loss-making products,
    -- which reads as a data bug rather than as a business signal.
    ROUND(
        c.base_price
        * (0.55 + (CRC32(CONCAT('spread', p.n)) % 250) / 100.0)   -- price band
        * (0.42 + (CRC32(CONCAT('cost',   p.n)) % 27) / 100.0),   -- cost ratio
        2),
    ROUND(
        c.base_price
        * (0.55 + (CRC32(CONCAT('spread', p.n)) % 250) / 100.0),
        2),
    -- ~8% discontinued. Gives the ETL a reason to exercise is_active.
    IF(CRC32(CONCAT('active', p.n)) % 100 < 8, 0, 1)
FROM       (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 500) p
CROSS JOIN tmp_category c
CROSS JOIN tmp_brand    b
WHERE      c.cat_id   = 1 + (CRC32(CONCAT('cat',   p.n)) % 15)
  AND      b.brand_id = 1 + (CRC32(CONCAT('brand', p.n)) % 12);


-- =============================================================================
-- SECTION 9 — SERIALIZE TO STAGING (stores, customers, products)
-- =============================================================================
-- The typed generators become strings, and the deliberate defects go in.
--
-- Note _source_row_num: a monotonic ordinal per feed. For the CSV-sourced store
-- feed it is literally the line number in the delivered file, which is what
-- makes a rejection actionable — "row 17 failed" beats "a row failed".
-- =============================================================================

-- ---- stg_store: 25 rows + defects #7, #8 ------------------------------------
INSERT INTO staging.stg_store (
    store_id, store_name, store_type, city_name, state_province,
    country_name, region_name, square_feet, opened_date,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    s.store_id,
    s.store_name,
    s.store_type,
    s.city,
    s.state_province,
    s.country,
    -- DEFECT #8: store 13 arrives with no region. Must become 'Unknown' in the
    -- dimension, NOT NULL — dimension attributes are NOT NULL by rule
    -- (dimensional_modeling.md §4), and a NULL here would drop the store from
    -- every GROUP BY that touches region.
    IF(s.store_num = 13, '', s.region),
    -- DEFECT #7: store 7's square_feet is the literal string 'N/A'. The CSV feed
    -- is hand-edited, so this is the most realistic failure in the whole file.
    IF(s.store_num = 7, 'N/A', CAST(s.square_feet AS CHAR)),
    DATE_FORMAT(s.opened_date, '%Y-%m-%d'),
    @batch_id, 'STORE_CSV', s.store_num
FROM tmp_store s;

-- ---- stg_customer: 1,000 rows + defects #1, #2 ------------------------------
INSERT INTO staging.stg_customer (
    customer_id, customer_name, email, segment_code, loyalty_tier, signup_date,
    city_name, state_province, country_name, region_name, postal_code,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    -- DEFECT #1: exactly 4 customers (211, 422, 633, 844) arrive with no
    -- natural key -- multiples of 211 within 1..1000. Unfixable and
    -- unmatchable — there is nothing to join on and nothing to correct toward,
    -- so the only honest outcome is meta.rejected_record.
    IF(c.cust_num % 211 = 0, '', c.customer_id),
    c.customer_name,
    c.email,
    c.segment_code,
    c.loyalty_tier,
    -- DEFECT #2: two malformed dates, each a different failure.
    --   '31/02/2021' parses as a date format but is not a real day.
    --   'unknown'    is not a date at all.
    -- Both must be caught by the transform, not by MySQL's implicit conversion,
    -- which would quietly turn them into '0000-00-00' or NULL.
    CASE
        WHEN c.cust_num = 137 THEN '31/02/2021'
        WHEN c.cust_num = 642 THEN 'unknown'
        ELSE DATE_FORMAT(c.signup_date, '%Y-%m-%d')
    END,
    c.city, c.state_province, c.country, c.region, c.postal_code,
    @batch_id, 'SALES_OLTP', c.cust_num
FROM tmp_customer c;

-- DEFECT #3: four customers arrive TWICE in one feed, with a later signup_date
-- and a different tier on the duplicate. This is the everyday version of the
-- problem — not corruption, just an extract that overlapped a page boundary.
-- The transform must pick ONE row per customer_id deterministically ("latest
-- wins"), because loading both would violate uq_dim_customer_current and abort
-- the whole load.
INSERT INTO staging.stg_customer (
    customer_id, customer_name, email, segment_code, loyalty_tier, signup_date,
    city_name, state_province, country_name, region_name, postal_code,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    c.customer_id, c.customer_name, c.email, c.segment_code,
    'Platinum',
    DATE_FORMAT(DATE_ADD(c.signup_date, INTERVAL 30 DAY), '%Y-%m-%d'),
    c.city, c.state_province, c.country, c.region, c.postal_code,
    @batch_id, 'SALES_OLTP', 1000 + c.cust_num
FROM   tmp_customer c
WHERE  c.cust_num IN (11, 222, 333, 444);

-- ---- stg_product: 500 rows + defects #4, #5 ---------------------------------
INSERT INTO staging.stg_product (
    product_id, product_name, category_name, subcategory_name, brand_name,
    unit_cost, list_price, is_active,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand_name,
    -- DEFECT #4: three products report cost as 'N/A'. This is the exact case
    -- architecture.md §2.1 uses to justify TEXT staging columns — a typed
    -- staging column would have failed the whole load at the door, and nobody
    -- would ever have seen which three rows caused it.
    IF(p.prod_num IN (58, 199, 401), 'N/A', CAST(p.unit_cost AS CHAR)),
    -- DEFECT #5: two products carry a negative list price. Syntactically valid,
    -- semantically impossible — only a range check catches this class of error,
    -- which is why ck_dim_product_amounts exists.
    IF(p.prod_num IN (77, 288),
       CAST(-p.list_price AS CHAR),
       CAST(p.list_price AS CHAR)),
    IF(p.is_active = 1, 'Y', 'N'),
    @batch_id, 'PRODUCT_MDM', p.prod_num
FROM tmp_product p;

-- DEFECT #6: two products delivered twice, the duplicate carrying a price rise.
-- Same dedupe requirement as customers, and the same consequence if ignored:
-- uq_dim_product_current would reject the second row and fail the load.
INSERT INTO staging.stg_product (
    product_id, product_name, category_name, subcategory_name, brand_name,
    unit_cost, list_price, is_active,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    p.product_id, p.product_name, p.category, p.subcategory, p.brand_name,
    CAST(ROUND(p.unit_cost  * 1.08, 2) AS CHAR),
    CAST(ROUND(p.list_price * 1.08, 2) AS CHAR),
    'Y',
    @batch_id, 'PRODUCT_MDM', 500 + p.prod_num
FROM   tmp_product p
WHERE  p.prod_num IN (150, 350);


-- =============================================================================
-- SECTION 10 — ORDER HEADERS (temporary, ~20,500 rows)
-- =============================================================================
-- Orders are materialized before lines so that order-level attributes are
-- computed ONCE and shared by every line on the order. Deriving them per line
-- would let two lines of the same order disagree about the customer — which
-- would be a genuine grain violation, not a cosmetic flaw.
--
-- ---- SEASONALITY, SET-BASED --------------------------------------------------
-- Order volume is not uniform, and uniform test data hides real bugs: it makes
-- every month look identical, so a broken date join or a wrong GROUP BY still
-- produces a plausible flat line. The trick used here is worth remembering:
--
--     JOIN dim_date TO tmp_number ON n BETWEEN 1 AND <expression over the date>
--
-- The join condition is a function of the date, so each day produces exactly as
-- many rows as that day deserves. Seasonality falls out of a join condition —
-- no loop, no per-day iteration, one statement.
--
-- The applied factors:
--     Nov + Dec        x1.90   holiday peak
--     Jan + Feb        x0.75   post-holiday trough
--     Jun + Jul        x1.15   summer bump
--     Weekends         x1.35   retail reality
--     Per-day jitter   x0.80-1.20  so the curve is not visibly synthetic
--
-- Base 15/day x ~1.25 combined average x 1,096 days ~= 20,500 orders.
-- =============================================================================

DROP TEMPORARY TABLE IF EXISTS tmp_order;
CREATE TEMPORARY TABLE tmp_order (
    order_seq         INT          NOT NULL AUTO_INCREMENT,
    order_number      VARCHAR(50)  NOT NULL,
    order_date        DATE         NOT NULL,
    shipped_date      DATE         NULL,
    customer_id       VARCHAR(20)  NOT NULL,
    store_id          VARCHAR(20)  NOT NULL,
    channel_id        VARCHAR(20)  NOT NULL,
    order_status      VARCHAR(50)  NOT NULL,
    payment_method    VARCHAR(50)  NOT NULL,
    shipping_priority VARCHAR(50)  NOT NULL,
    is_gift           TINYINT(1)   NOT NULL,
    line_count        TINYINT      NOT NULL,
    promotion_id      VARCHAR(20)  NULL,
    promotion_pct     DECIMAL(6,4) NULL,
    PRIMARY KEY (order_seq)
) ENGINE = InnoDB;

-- -----------------------------------------------------------------------------
-- BATCHING (added after Error 2013 in MySQL Workbench 8.0)
-- -----------------------------------------------------------------------------
-- A previous version of this file generated all ~20,500 orders, all ~51,000
-- order lines, and all 40,000 inventory rows each in ONE INSERT ... SELECT.
-- The order-line statement in particular evaluates dozens of CRC32() calls per
-- output row; on a ~51,000-row result that ran long enough for MySQL
-- Workbench's client-side read timeout to give up mid-query and report
-- "Error 2013: Lost connection to MySQL server during query" — even though
-- the values themselves were never the problem.
--
-- THE FIX: split each of Sections 10-12 into many independent top-level
-- INSERT statements of roughly 500-1,000 rows apiece, instead of one giant
-- statement. Workbench (or the mysql CLI) executes a script one statement at
-- a time, so no individual statement runs long enough to trip any timeout,
-- even though the total amount of work is the same.
--
-- THIS IS NOT THE "WHILE loop" ANTI-PATTERN the file header warns against. A
-- server-side loop was considered and rejected: wrapping the batches in a
-- stored procedure and issuing one CALL would still be ONE statement from the
-- client's point of view, so it would not shorten any single round-trip and
-- would not have fixed the timeout. Each batch below is still a genuine
-- set-based INSERT ... SELECT of several hundred rows — nothing here is
-- row-by-row.
--
-- Sections 10 and 11 share the SAME 55 date-range boundaries (20 calendar
-- days per batch; 1,096 sales-window days / 20 = 55 batches, the last one 16
-- days) so that "batch 12 of orders" and "batch 12 of order lines" cover the
-- identical calendar window. These boundaries are literal, tied to
-- @sales_days = 1096 (Section 0) — if that constant ever changes, the
-- boundaries below need regenerating to match. Section 12 batches by product
-- number instead (50 batches of 10 products each; 500 / 10 divides evenly),
-- since inventory rows are not date-partitioned.
--
-- Every batch runs the EXACT SAME CRC32(...)-seeded expressions the original
-- single statement did, over a slice of the same rows — the WHERE-clause
-- boundary is the only thing that differs, so the generated values are
-- unaffected by batching. The one thing NOT guaranteed identical to a
-- hypothetical prior successful run is the AUTO_INCREMENT order in which
-- tmp_order.order_seq (and therefore stg_sales_order_line._source_row_num)
-- gets assigned: that was never a documented guarantee even in the original
-- single-statement form, since MySQL does not promise row-processing order
-- for an INSERT ... SELECT with no ORDER BY. Nothing downstream depends on
-- the specific value of order_seq — DEFECT #12's "first 25 rows" still picks
-- exactly 25 real rows to duplicate, just not provably the same 25 physical
-- rows as an unbatched run would have picked.
-- -----------------------------------------------------------------------------

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 0 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 20 DAY);  -- batch 1/55 (days 0-19)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 20 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 40 DAY);  -- batch 2/55 (days 20-39)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 40 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 60 DAY);  -- batch 3/55 (days 40-59)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 60 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 80 DAY);  -- batch 4/55 (days 60-79)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 80 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 100 DAY);  -- batch 5/55 (days 80-99)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 100 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 120 DAY);  -- batch 6/55 (days 100-119)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 120 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 140 DAY);  -- batch 7/55 (days 120-139)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 140 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 160 DAY);  -- batch 8/55 (days 140-159)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 160 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 180 DAY);  -- batch 9/55 (days 160-179)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 180 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 200 DAY);  -- batch 10/55 (days 180-199)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 200 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 220 DAY);  -- batch 11/55 (days 200-219)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 220 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 240 DAY);  -- batch 12/55 (days 220-239)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 240 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 260 DAY);  -- batch 13/55 (days 240-259)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 260 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 280 DAY);  -- batch 14/55 (days 260-279)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 280 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 300 DAY);  -- batch 15/55 (days 280-299)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 300 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 320 DAY);  -- batch 16/55 (days 300-319)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 320 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 340 DAY);  -- batch 17/55 (days 320-339)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 340 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 360 DAY);  -- batch 18/55 (days 340-359)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 360 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 380 DAY);  -- batch 19/55 (days 360-379)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 380 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 400 DAY);  -- batch 20/55 (days 380-399)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 400 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 420 DAY);  -- batch 21/55 (days 400-419)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 420 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 440 DAY);  -- batch 22/55 (days 420-439)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 440 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 460 DAY);  -- batch 23/55 (days 440-459)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 460 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 480 DAY);  -- batch 24/55 (days 460-479)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 480 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 500 DAY);  -- batch 25/55 (days 480-499)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 500 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 520 DAY);  -- batch 26/55 (days 500-519)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 520 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 540 DAY);  -- batch 27/55 (days 520-539)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 540 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 560 DAY);  -- batch 28/55 (days 540-559)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 560 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 580 DAY);  -- batch 29/55 (days 560-579)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 580 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 600 DAY);  -- batch 30/55 (days 580-599)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 600 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 620 DAY);  -- batch 31/55 (days 600-619)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 620 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 640 DAY);  -- batch 32/55 (days 620-639)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 640 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 660 DAY);  -- batch 33/55 (days 640-659)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 660 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 680 DAY);  -- batch 34/55 (days 660-679)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 680 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 700 DAY);  -- batch 35/55 (days 680-699)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 700 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 720 DAY);  -- batch 36/55 (days 700-719)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 720 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 740 DAY);  -- batch 37/55 (days 720-739)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 740 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 760 DAY);  -- batch 38/55 (days 740-759)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 760 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 780 DAY);  -- batch 39/55 (days 760-779)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 780 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 800 DAY);  -- batch 40/55 (days 780-799)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 800 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 820 DAY);  -- batch 41/55 (days 800-819)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 820 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 840 DAY);  -- batch 42/55 (days 820-839)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 840 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 860 DAY);  -- batch 43/55 (days 840-859)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 860 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 880 DAY);  -- batch 44/55 (days 860-879)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 880 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 900 DAY);  -- batch 45/55 (days 880-899)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 900 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 920 DAY);  -- batch 46/55 (days 900-919)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 920 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 940 DAY);  -- batch 47/55 (days 920-939)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 940 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 960 DAY);  -- batch 48/55 (days 940-959)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 960 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 980 DAY);  -- batch 49/55 (days 960-979)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 980 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 1000 DAY);  -- batch 50/55 (days 980-999)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 1000 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 1020 DAY);  -- batch 51/55 (days 1000-1019)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 1020 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 1040 DAY);  -- batch 52/55 (days 1020-1039)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 1040 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 1060 DAY);  -- batch 53/55 (days 1040-1059)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 1060 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 1080 DAY);  -- batch 54/55 (days 1060-1079)

INSERT INTO tmp_order (
    order_number, order_date, shipped_date, customer_id, store_id, channel_id,
    order_status, payment_method, shipping_priority, is_gift, line_count,
    promotion_id, promotion_pct
)
SELECT
    CONCAT('ORD-', d.date_key, '-', LPAD(o.n, 3, '0')) AS order_number,
    d.full_date,
    CASE
        WHEN st.order_status IN ('Cancelled', 'Awaiting Fulfilment') THEN NULL
        ELSE DATE_ADD(d.full_date,
                      INTERVAL 1 + (CRC32(CONCAT('ship', d.date_key, o.n)) % 7) DAY)
    END,
    CONCAT('C-', LPAD(1 + (CRC32(CONCAT('ocust', d.date_key, o.n)) % 1000), 5, '0')),
    CONCAT('S-', LPAD(1 + (CRC32(CONCAT('ostore', d.date_key, o.n)) % 25), 3, '0')),
    CASE
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 35 THEN 'CH-WEB'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 65 THEN 'CH-APP'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 85 THEN 'CH-STORE'
        WHEN CRC32(CONCAT('chan', d.date_key, o.n)) % 100 < 93 THEN 'CH-PHONE'
        ELSE 'CH-PARTNER'
    END,
    st.order_status,
    CASE
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 45 THEN 'Credit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 70 THEN 'Debit Card'
        WHEN CRC32(CONCAT('pay', d.date_key, o.n)) % 100 < 90 THEN 'PayPal'
        ELSE 'Gift Card'
    END,
    CASE
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 70 THEN 'Standard'
        WHEN CRC32(CONCAT('prio', d.date_key, o.n)) % 100 < 93 THEN 'Express'
        ELSE 'Overnight'
    END,
    IF(CRC32(CONCAT('gift', d.date_key, o.n)) % 100
           < IF(d.month_number = 12, 34, 9), 1, 0),
    1 + (CRC32(CONCAT('lines', d.date_key, o.n)) % 4),
    (SELECT pr.promotion_id FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1),
    (SELECT pr.discount_pct FROM warehouse.dim_promotion pr
      WHERE pr.promotion_key > 0
        AND d.full_date BETWEEN pr.start_date AND pr.end_date
      ORDER BY pr.promotion_key LIMIT 1)
FROM warehouse.dim_date d
JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 1 AND 60) o
  ON  o.n <= GREATEST(1, CAST(
          15
        * CASE WHEN d.month_number IN (11, 12) THEN 1.90
               WHEN d.month_number IN ( 1,  2) THEN 0.75
               WHEN d.month_number IN ( 6,  7) THEN 1.15
               ELSE 1.00 END
        * IF(d.is_weekend = 1, 1.35, 1.00)
        * (0.80 + (CRC32(CONCAT('vol', d.date_key)) % 41) / 100.0)
      AS SIGNED))
JOIN (
    SELECT 0 AS lo, 65 AS hi, 'Delivered'           AS order_status
    UNION ALL SELECT 65, 80, 'Shipped'
    UNION ALL SELECT 80, 90, 'Awaiting Fulfilment'
    UNION ALL SELECT 90, 96, 'Cancelled'
    UNION ALL SELECT 96, 100, 'Returned'
) st
  ON  CRC32(CONCAT('stat', d.date_key, o.n)) % 100 >= st.lo
  AND CRC32(CONCAT('stat', d.date_key, o.n)) % 100 <  st.hi
WHERE d.date_key > 0
  AND d.full_date >= DATE_ADD(@sales_start, INTERVAL 1080 DAY)
  AND d.full_date <  DATE_ADD(@sales_start, INTERVAL 1096 DAY);  -- batch 55/55 (days 1080-1095)


-- =============================================================================
-- SECTION 11 — staging.stg_sales_order_line   (~51,000 rows)
-- =============================================================================
-- The main event. Each order explodes into its line_count lines by joining to
-- the numbers table — the same "join to produce N rows" pattern as §10, applied
-- one level down.
--
-- PRODUCT POPULARITY IS DELIBERATELY SKEWED. A uniform product distribution
-- would make every "top 10 products" query return a meaningless near-tie and
-- every ABC/Pareto analysis in Milestone 6 pointless. Squaring a uniform draw
-- produces a Pareto-ish curve where a minority of SKUs carry most of the volume
-- — which is what real sales data looks like:
--
--     prod_num = 1 + FLOOR(POW(u, 2) * 500)     where u is uniform on [0,1)
--
-- MEASURES follow the product's own price ladder, because unit_price is joined
-- from tmp_product rather than invented. That keeps margin coherent: a laptop
-- line carries laptop money and laptop cost, so gross_profit_amount means
-- something when Milestone 6 aggregates it by category.
-- =============================================================================

-- BATCHING: see Section 10's header for the full rationale (Error 2013 in
-- MySQL Workbench). This section reuses the SAME 55 date-range boundaries,
-- applied here to o.order_date so each batch of lines lines up with the
-- matching batch of orders.

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 0 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 20 DAY);  -- batch 1/55 (days 0-19)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 20 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 40 DAY);  -- batch 2/55 (days 20-39)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 40 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 60 DAY);  -- batch 3/55 (days 40-59)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 60 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 80 DAY);  -- batch 4/55 (days 60-79)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 80 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 100 DAY);  -- batch 5/55 (days 80-99)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 100 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 120 DAY);  -- batch 6/55 (days 100-119)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 120 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 140 DAY);  -- batch 7/55 (days 120-139)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 140 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 160 DAY);  -- batch 8/55 (days 140-159)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 160 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 180 DAY);  -- batch 9/55 (days 160-179)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 180 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 200 DAY);  -- batch 10/55 (days 180-199)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 200 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 220 DAY);  -- batch 11/55 (days 200-219)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 220 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 240 DAY);  -- batch 12/55 (days 220-239)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 240 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 260 DAY);  -- batch 13/55 (days 240-259)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 260 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 280 DAY);  -- batch 14/55 (days 260-279)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 280 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 300 DAY);  -- batch 15/55 (days 280-299)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 300 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 320 DAY);  -- batch 16/55 (days 300-319)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 320 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 340 DAY);  -- batch 17/55 (days 320-339)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 340 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 360 DAY);  -- batch 18/55 (days 340-359)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 360 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 380 DAY);  -- batch 19/55 (days 360-379)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 380 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 400 DAY);  -- batch 20/55 (days 380-399)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 400 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 420 DAY);  -- batch 21/55 (days 400-419)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 420 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 440 DAY);  -- batch 22/55 (days 420-439)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 440 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 460 DAY);  -- batch 23/55 (days 440-459)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 460 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 480 DAY);  -- batch 24/55 (days 460-479)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 480 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 500 DAY);  -- batch 25/55 (days 480-499)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 500 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 520 DAY);  -- batch 26/55 (days 500-519)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 520 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 540 DAY);  -- batch 27/55 (days 520-539)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 540 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 560 DAY);  -- batch 28/55 (days 540-559)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 560 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 580 DAY);  -- batch 29/55 (days 560-579)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 580 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 600 DAY);  -- batch 30/55 (days 580-599)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 600 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 620 DAY);  -- batch 31/55 (days 600-619)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 620 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 640 DAY);  -- batch 32/55 (days 620-639)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 640 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 660 DAY);  -- batch 33/55 (days 640-659)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 660 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 680 DAY);  -- batch 34/55 (days 660-679)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 680 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 700 DAY);  -- batch 35/55 (days 680-699)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 700 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 720 DAY);  -- batch 36/55 (days 700-719)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 720 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 740 DAY);  -- batch 37/55 (days 720-739)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 740 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 760 DAY);  -- batch 38/55 (days 740-759)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 760 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 780 DAY);  -- batch 39/55 (days 760-779)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 780 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 800 DAY);  -- batch 40/55 (days 780-799)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 800 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 820 DAY);  -- batch 41/55 (days 800-819)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 820 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 840 DAY);  -- batch 42/55 (days 820-839)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 840 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 860 DAY);  -- batch 43/55 (days 840-859)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 860 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 880 DAY);  -- batch 44/55 (days 860-879)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 880 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 900 DAY);  -- batch 45/55 (days 880-899)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 900 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 920 DAY);  -- batch 46/55 (days 900-919)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 920 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 940 DAY);  -- batch 47/55 (days 920-939)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 940 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 960 DAY);  -- batch 48/55 (days 940-959)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 960 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 980 DAY);  -- batch 49/55 (days 960-979)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 980 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 1000 DAY);  -- batch 50/55 (days 980-999)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 1000 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 1020 DAY);  -- batch 51/55 (days 1000-1019)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 1020 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 1040 DAY);  -- batch 52/55 (days 1020-1039)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 1040 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 1060 DAY);  -- batch 53/55 (days 1040-1059)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 1060 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 1080 DAY);  -- batch 54/55 (days 1060-1079)

INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    o.order_number,
    CAST(ln.n AS CHAR),
    CONCAT(DATE_FORMAT(o.order_date, '%Y-%m-%d'), ' ',
           LPAD(8 + (CRC32(CONCAT('hr', o.order_number, ln.n)) % 13), 2, '0'), ':',
           LPAD(CRC32(CONCAT('mi', o.order_number, ln.n)) % 60, 2, '0'), ':00'),
    IFNULL(DATE_FORMAT(o.shipped_date, '%Y-%m-%d'), ''),
    IF(CRC32(CONCAT('badcust', o.order_number, ln.n)) % 1000 = 13,
       'C-99999', o.customer_id),
    o.store_id,
    o.channel_id,
    IF(CRC32(CONCAT('badprod', o.order_number, ln.n)) % 250 = 7,
       CONCAT('P-9', LPAD(CRC32(CONCAT('bp', o.order_number, ln.n)) % 900, 3, '0')),
       CONCAT('P-', LPAD(pr.prod_num, 4, '0'))),
    IF(o.promotion_id IS NOT NULL
       AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
       o.promotion_id, ''),
    o.order_status,
    o.payment_method,
    o.shipping_priority,
    CAST(o.is_gift AS CHAR),
    CASE
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 21 THEN 'N/A'
        WHEN CRC32(CONCAT('badqty', o.order_number, ln.n)) % 1000 = 22 THEN '0'
        ELSE CAST(
            CASE
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                ELSE 6
            END AS CHAR)
    END,
    CAST(ROUND(pr.list_price *
        CASE
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 78 THEN 1.00
            WHEN CRC32(CONCAT('mkdn', o.order_number, ln.n)) % 100 < 93 THEN 0.95
            ELSE 0.90
        END, 2) AS CHAR),
    CAST(ROUND(
        IF(o.promotion_id IS NOT NULL
           AND CRC32(CONCAT('promo', o.order_number, ln.n)) % 100 < 45,
           pr.list_price
             * CASE
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
                   WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
                   ELSE 6
               END
             * IFNULL(o.promotion_pct, 0),
           0), 2) AS CHAR),
    CAST(ROUND(pr.list_price
        * CASE
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 45 THEN 1
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 72 THEN 2
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 86 THEN 3
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 94 THEN 4
              WHEN CRC32(CONCAT('qty', o.order_number, ln.n)) % 100 < 98 THEN 5
              ELSE 6
          END
        * 0.0825, 2) AS CHAR),
    @batch_id, 'SALES_OLTP',
    o.order_seq * 10 + ln.n
FROM tmp_order  o
JOIN tmp_number ln
  ON ln.n BETWEEN 1 AND o.line_count
JOIN tmp_product pr
  ON pr.prod_num = 1 + CAST(FLOOR(
         POW((CRC32(CONCAT('prod', o.order_number, ln.n)) % 1000) / 1000.0, 2)
         * 500) AS SIGNED)
WHERE o.order_date >= DATE_ADD(@sales_start, INTERVAL 1080 DAY)
  AND o.order_date <  DATE_ADD(@sales_start, INTERVAL 1096 DAY);  -- batch 55/55 (days 1080-1095)


-- DEFECT #12: 25 order lines delivered a second time, byte-identical.
-- THE IDEMPOTENCY TEST. uq_fact_sales_natural on
-- (order_number, order_line_number, order_date_key) is what stops these from
-- becoming duplicate revenue; etl_simulation.sql must use INSERT ... ON
-- DUPLICATE KEY UPDATE so the second copy converges instead of doubling.
-- This is a rehearsal for the real scenario: a batch that failed halfway and
-- got re-run.
INSERT INTO staging.stg_sales_order_line (
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    order_number, order_line_number, ordered_at, shipped_date,
    customer_id, store_id, channel_id, product_id, promotion_id,
    order_status, payment_method, shipping_priority, is_gift,
    quantity, unit_price, discount_amount, tax_amount,
    @batch_id, 'SALES_OLTP', 900000 + _source_row_num
FROM   staging.stg_sales_order_line
WHERE  _source_row_num < 900000
ORDER  BY _source_row_num
LIMIT  25;


-- =============================================================================
-- SECTION 12 — staging.stg_inventory        (40,000 rows)
-- =============================================================================
-- The periodic snapshot source: 25 stores x ~80 stocked products x 20 days.
--
-- WHY NOT EVERY PRODUCT IN EVERY STORE: 500 x 25 x 20 = 250,000 rows, and it
-- would be a lie. No retailer stocks the full catalogue in every location. Each
-- store carries a deterministic ~16% subset, which also gives Milestone 6 a real
-- question to ask — "which stores don't carry our best sellers?"
--
-- THIS TABLE IS DENSE, AND THAT IS THE POINT. Unlike sales, a row exists for
-- every stocked product on every day whether or not anything moved, because
-- quantity_on_hand is a STATE rather than an EVENT. "No row" would be
-- indistinguishable from "zero stock", and the difference matters enormously:
-- one means we don't know, the other means we sold out.
--
-- The stock series is built to make the SEMI-ADDITIVE TRAP visible:
--   * ~25% of product/store pairs hold a FLAT quantity across all 20 days.
--     These are the star_schema.md §5 example made real — SUM over the window
--     returns 20x the true figure, while AVG and the closing balance are right.
--   * The rest drift down and get replenished, so AVG and closing balance
--     legitimately differ and the choice between them actually matters.
--   * Some pairs reach zero -> is_stockout, which IS summable across dates
--     because stockout-DAYS is an event count, not a repeated state.
-- =============================================================================

-- BATCHING: see Section 10's header for the full rationale (Error 2013 in
-- MySQL Workbench). This section batches by p.prod_num instead of by date
-- (inventory rows are not date-windowed the way orders are): 50 batches of
-- 10 products each, 1-500 divides evenly with no remainder.

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 1 AND 10;  -- batch 1/50 (products 1-10)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 11 AND 20;  -- batch 2/50 (products 11-20)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 21 AND 30;  -- batch 3/50 (products 21-30)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 31 AND 40;  -- batch 4/50 (products 31-40)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 41 AND 50;  -- batch 5/50 (products 41-50)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 51 AND 60;  -- batch 6/50 (products 51-60)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 61 AND 70;  -- batch 7/50 (products 61-70)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 71 AND 80;  -- batch 8/50 (products 71-80)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 81 AND 90;  -- batch 9/50 (products 81-90)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 91 AND 100;  -- batch 10/50 (products 91-100)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 101 AND 110;  -- batch 11/50 (products 101-110)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 111 AND 120;  -- batch 12/50 (products 111-120)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 121 AND 130;  -- batch 13/50 (products 121-130)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 131 AND 140;  -- batch 14/50 (products 131-140)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 141 AND 150;  -- batch 15/50 (products 141-150)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 151 AND 160;  -- batch 16/50 (products 151-160)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 161 AND 170;  -- batch 17/50 (products 161-170)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 171 AND 180;  -- batch 18/50 (products 171-180)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 181 AND 190;  -- batch 19/50 (products 181-190)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 191 AND 200;  -- batch 20/50 (products 191-200)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 201 AND 210;  -- batch 21/50 (products 201-210)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 211 AND 220;  -- batch 22/50 (products 211-220)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 221 AND 230;  -- batch 23/50 (products 221-230)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 231 AND 240;  -- batch 24/50 (products 231-240)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 241 AND 250;  -- batch 25/50 (products 241-250)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 251 AND 260;  -- batch 26/50 (products 251-260)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 261 AND 270;  -- batch 27/50 (products 261-270)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 271 AND 280;  -- batch 28/50 (products 271-280)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 281 AND 290;  -- batch 29/50 (products 281-290)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 291 AND 300;  -- batch 30/50 (products 291-300)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 301 AND 310;  -- batch 31/50 (products 301-310)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 311 AND 320;  -- batch 32/50 (products 311-320)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 321 AND 330;  -- batch 33/50 (products 321-330)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 331 AND 340;  -- batch 34/50 (products 331-340)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 341 AND 350;  -- batch 35/50 (products 341-350)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 351 AND 360;  -- batch 36/50 (products 351-360)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 361 AND 370;  -- batch 37/50 (products 361-370)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 371 AND 380;  -- batch 38/50 (products 371-380)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 381 AND 390;  -- batch 39/50 (products 381-390)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 391 AND 400;  -- batch 40/50 (products 391-400)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 401 AND 410;  -- batch 41/50 (products 401-410)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 411 AND 420;  -- batch 42/50 (products 411-420)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 421 AND 430;  -- batch 43/50 (products 421-430)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 431 AND 440;  -- batch 44/50 (products 431-440)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 441 AND 450;  -- batch 45/50 (products 441-450)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 451 AND 460;  -- batch 46/50 (products 451-460)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 461 AND 470;  -- batch 47/50 (products 461-470)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 471 AND 480;  -- batch 48/50 (products 471-480)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 481 AND 490;  -- batch 49/50 (products 481-490)

INSERT INTO staging.stg_inventory (
    snapshot_date, product_id, store_id,
    quantity_on_hand, quantity_on_order, reorder_point, unit_cost,
    _load_batch_id, _source_system, _source_row_num
)
SELECT
    DATE_FORMAT(
        DATE_ADD(DATE_ADD(@sales_start, INTERVAL (@sales_days - 20) DAY),
                 INTERVAL dy.n DAY), '%Y-%m-%d'),
    p.product_id,
    s.store_id,
    CASE
        WHEN CRC32(CONCAT('negstock', p.prod_num, s.store_num, dy.n)) % 2000 = 5
            THEN '-5'
        ELSE CAST(
            GREATEST(0,
                (20 + CRC32(CONCAT('stock', p.prod_num, s.store_num)) % 180)
                - dy.n * IF(CRC32(CONCAT('flat', p.prod_num, s.store_num)) % 100 < 25,
                            0,
                            CRC32(CONCAT('drift', p.prod_num, s.store_num)) % 10)
                + IF(dy.n >= 10
                     AND CRC32(CONCAT('repl', p.prod_num, s.store_num)) % 100 < 50,
                     60 + CRC32(CONCAT('replqty', p.prod_num, s.store_num)) % 90,
                     0)
            ) AS CHAR)
    END,
    CAST(IF(CRC32(CONCAT('onord', p.prod_num, s.store_num, dy.n)) % 100 < 30,
            10 + CRC32(CONCAT('oqty', p.prod_num, s.store_num, dy.n)) % 90,
            0) AS CHAR),
    CAST(10 + CRC32(CONCAT('reord', p.prod_num, s.store_num)) % 40 AS CHAR),
    CAST(p.unit_cost AS CHAR),
    @batch_id, 'SALES_OLTP',
    (dy.n * 100000) + (s.store_num * 1000) + p.prod_num
FROM       tmp_product p
CROSS JOIN tmp_store   s
CROSS JOIN (SELECT n FROM tmp_number WHERE n BETWEEN 0 AND 19) dy
WHERE CRC32(CONCAT('stocks', s.store_num, '-', p.prod_num)) % 100 < 16
  AND p.prod_num BETWEEN 491 AND 500;  -- batch 50/50 (products 491-500)


-- =============================================================================
-- SECTION 13 — CLOSE THE BATCH
-- =============================================================================
-- The seed reports its own row counts into the control plane, exactly as
-- etl_simulation.sql will. rows_read is 0 because nothing was extracted — this
-- batch generated its input rather than reading it, and overstating rows_read
-- would corrupt the reconciliation arithmetic (read = inserted + updated +
-- rejected) that makes meta.etl_batch worth having.
-- =============================================================================

UPDATE meta.etl_batch
SET    batch_status  = 'SUCCESS',
       ended_at      = CURRENT_TIMESTAMP(6),
       rows_inserted = (SELECT COUNT(*) FROM staging.stg_customer)
                     + (SELECT COUNT(*) FROM staging.stg_product)
                     + (SELECT COUNT(*) FROM staging.stg_store)
                     + (SELECT COUNT(*) FROM staging.stg_sales_order_line)
                     + (SELECT COUNT(*) FROM staging.stg_inventory)
                     + (SELECT COUNT(*) FROM warehouse.dim_date       WHERE date_key       > 0)
                     + (SELECT COUNT(*) FROM warehouse.dim_order_junk WHERE order_junk_key > 0)
                     + (SELECT COUNT(*) FROM warehouse.dim_channel    WHERE channel_key    > 0)
                     + (SELECT COUNT(*) FROM warehouse.dim_promotion  WHERE promotion_key  > 0)
WHERE  batch_id = @batch_id;


-- =============================================================================
-- SECTION 14 — VERIFICATION
-- =============================================================================
-- Run these after the script. Exact counts are exact because generation is
-- deterministic (§0); the CRC32-driven defect counts are stated as expected
-- ranges, since they depend on hash distribution rather than on arithmetic.
-- =============================================================================

-- 1. Volumes. EXACT expected values:
--      dim_date 4018 | dim_order_junk 120 | dim_channel 5 | dim_promotion 30
--      stg_store 25 | stg_customer 1004 | stg_product 502 | stg_inventory 40000
--      stg_sales_order_line ~51,000 (seasonality-driven; stable across runs)
SELECT 'warehouse.dim_date'           AS object_name, COUNT(*) AS row_count FROM warehouse.dim_date
UNION ALL SELECT 'warehouse.dim_order_junk',       COUNT(*) FROM warehouse.dim_order_junk
UNION ALL SELECT 'warehouse.dim_channel',          COUNT(*) FROM warehouse.dim_channel
UNION ALL SELECT 'warehouse.dim_promotion',        COUNT(*) FROM warehouse.dim_promotion
UNION ALL SELECT 'staging.stg_store',              COUNT(*) FROM staging.stg_store
UNION ALL SELECT 'staging.stg_customer',           COUNT(*) FROM staging.stg_customer
UNION ALL SELECT 'staging.stg_product',            COUNT(*) FROM staging.stg_product
UNION ALL SELECT 'staging.stg_sales_order_line',   COUNT(*) FROM staging.stg_sales_order_line
UNION ALL SELECT 'staging.stg_inventory',          COUNT(*) FROM staging.stg_inventory;

-- 2. dim_date sanity. Expect 4,018 rows / 4,018 distinct dates (no duplicate
--    day), 2020-01-01 to 2030-12-31, 1,148 weekend days, and 121 holidays
--    (11 per year x 11 years, less any that collide with a fixed date).
SELECT COUNT(*)                    AS days,
       COUNT(DISTINCT full_date)   AS distinct_days,
       MIN(full_date)              AS first_day,
       MAX(full_date)              AS last_day,
       SUM(is_weekend)             AS weekend_days,
       SUM(is_holiday)             AS holiday_days,
       SUM(prior_year_date_key = -1) AS no_prior_year
FROM   warehouse.dim_date
WHERE  date_key > 0;

-- 3. The junk dimension really is the full cross-product: 5 x 4 x 3 x 2 = 120,
--    with no combination missing and none duplicated.
SELECT COUNT(*)                                AS combinations,
       COUNT(DISTINCT order_status)            AS statuses,
       COUNT(DISTINCT payment_method)          AS payment_methods,
       COUNT(DISTINCT shipping_priority)       AS priorities,
       COUNT(DISTINCT is_gift)                 AS gift_flags
FROM   warehouse.dim_order_junk
WHERE  order_junk_key > 0;

-- 4. INJECTED DEFECTS. These are the targets etl_simulation.sql must handle.
--    Expected magnitudes are shown; treat a zero anywhere as a broken generator.
SELECT 'customer: empty customer_id'        AS defect, COUNT(*) AS rows_found, '4'    AS expected
  FROM staging.stg_customer WHERE customer_id = '' OR customer_id IS NULL
UNION ALL SELECT 'customer: bad signup_date', COUNT(*), '2'
  FROM staging.stg_customer WHERE signup_date IN ('31/02/2021', 'unknown')
UNION ALL SELECT 'customer: duplicate id', COUNT(*), '4'
  FROM (SELECT customer_id FROM staging.stg_customer
        WHERE customer_id <> '' GROUP BY customer_id HAVING COUNT(*) > 1) x
UNION ALL SELECT 'product: non-numeric unit_cost', COUNT(*), '3'
  FROM staging.stg_product WHERE unit_cost = 'N/A'
UNION ALL SELECT 'product: negative list_price', COUNT(*), '2'
  FROM staging.stg_product WHERE CAST(list_price AS DECIMAL(12,2)) < 0
UNION ALL SELECT 'product: duplicate id', COUNT(*), '2'
  FROM (SELECT product_id FROM staging.stg_product
        GROUP BY product_id HAVING COUNT(*) > 1) y
UNION ALL SELECT 'store: non-numeric square_feet', COUNT(*), '1'
  FROM staging.stg_store WHERE square_feet = 'N/A'
UNION ALL SELECT 'store: empty region', COUNT(*), '1'
  FROM staging.stg_store WHERE region_name = ''
UNION ALL SELECT 'line: unknown product_id', COUNT(*), '~200'
  FROM staging.stg_sales_order_line WHERE product_id LIKE 'P-9%'
UNION ALL SELECT 'line: unknown customer_id', COUNT(*), '~50'
  FROM staging.stg_sales_order_line WHERE customer_id = 'C-99999'
UNION ALL SELECT 'line: non-numeric quantity', COUNT(*), '~50'
  FROM staging.stg_sales_order_line WHERE quantity = 'N/A'
UNION ALL SELECT 'line: zero quantity', COUNT(*), '~50'
  FROM staging.stg_sales_order_line WHERE quantity = '0'
UNION ALL SELECT 'line: exact duplicates', COUNT(*), '25'
  FROM staging.stg_sales_order_line WHERE _source_row_num >= 900000
UNION ALL SELECT 'inventory: negative on_hand', COUNT(*), '~20'
  FROM staging.stg_inventory WHERE CAST(quantity_on_hand AS SIGNED) < 0;

-- 5. NOT defects — the legitimate Not-Applicable cases. These must resolve to
--    key 0, never to key -1. Getting this backwards is the single most likely
--    ETL mistake in Milestone 4, because both look like "missing data".
SELECT 'line: no promotion (-> key 0)'  AS legitimate_blank,
       COUNT(*)                          AS rows_found,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging.stg_sales_order_line), 1) AS pct
  FROM staging.stg_sales_order_line WHERE promotion_id = ''
UNION ALL
SELECT 'line: not yet shipped (-> key 0)', COUNT(*),
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging.stg_sales_order_line), 1)
  FROM staging.stg_sales_order_line WHERE shipped_date = '';

-- 6. Seasonality is real, not decorative. Nov/Dec should stand well above
--    Jan/Feb in every year — if this comes out flat, the §10 join is broken.
SELECT LEFT(ordered_at, 7) AS order_month, COUNT(*) AS lines
FROM   staging.stg_sales_order_line
WHERE  _source_row_num < 900000
GROUP  BY LEFT(ordered_at, 7)
ORDER  BY order_month;

-- 7. Product popularity is skewed, not uniform. The top 10 SKUs should carry
--    materially more volume than the bottom 10; a flat result means the Pareto
--    expression in §11 collapsed and every top-N query downstream is worthless.
SELECT product_id, COUNT(*) AS lines
FROM   staging.stg_sales_order_line
WHERE  product_id NOT LIKE 'P-9%'
GROUP  BY product_id
ORDER  BY lines DESC
LIMIT  10;

-- 8. The seed's own batch record closed cleanly.
SELECT batch_id, batch_name, batch_status, rows_inserted,
       TIMESTAMPDIFF(SECOND, started_at, ended_at) AS elapsed_seconds
FROM   meta.etl_batch
ORDER  BY batch_id DESC
LIMIT  1;

-- =============================================================================
-- END OF sample_data.sql
--
-- State after this file: staging is full, the four reference dimensions are
-- loaded, and dim_customer / dim_product / dim_store / fact_sales /
-- fact_inventory_snapshot are still EMPTY apart from their reserved members.
-- Filling them is Milestone 4's job.
--
-- Next: etl_simulation.sql — batch control, DQ rules writing to
-- meta.data_quality_result, quarantine to meta.rejected_record, SCD1 and SCD2
-- dimension loaders, and an idempotent fact load keyed on
-- uq_fact_sales_natural.
-- =============================================================================
