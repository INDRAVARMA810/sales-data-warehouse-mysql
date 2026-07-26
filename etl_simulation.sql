-- =============================================================================
-- etl_simulation.sql — Mini Sales Data Warehouse
-- =============================================================================
--
--   Target platform : MySQL 8.0.16+
--   Prerequisites   : schema.sql, then sample_data.sql
--   Run with:
--       mysql --user=root --password --show-warnings < etl_simulation.sql
--
--   NOTE ON `DELIMITER`: this file defines stored procedures, so it uses the
--   DELIMITER command to redefine the statement terminator. DELIMITER is a
--   command understood by the `mysql` CLIENT, not by the server. Running this
--   file through a driver that submits statements directly (JDBC, some GUI
--   tools) will fail on those lines. Use the CLI.
--
-- -----------------------------------------------------------------------------
-- WHY STORED PROCEDURES
-- -----------------------------------------------------------------------------
-- Every previous file is a flat script. This one is not, for one reason: the
-- pipeline runs THREE times below, and a flat script would mean three
-- copy-pasted versions of the SCD2 loader. Copy-pasted ETL is how two loaders
-- that were supposed to be identical quietly stop being identical.
--
-- The procedures are deliberately plain — parameters and set-based DML, no
-- cursors, no handlers, no dynamic SQL. Each is one step of the pipeline that
-- can be called, re-called, and reasoned about in isolation.
--
-- -----------------------------------------------------------------------------
-- WHAT THIS FILE PROVES, AND HOW
-- -----------------------------------------------------------------------------
-- A single-pass load cannot demonstrate a slowly changing dimension. If every
-- dimension row is inserted once and never revisited, the SCD2 code path never
-- executes and `version_number` is 1 forever. So the pipeline runs three times,
-- each with a purpose:
--
--   BATCH 1  "Historical load"
--            Staging as generated -> dimensions at version 1, facts for every
--            order BEFORE the change date.
--            Dimension rows get valid_from = 1900-01-01. See §5 for why that
--            date and not the load date — it is the single most common way to
--            get an initial SCD2 load wrong.
--
--   BATCH 2  "Next extract, with changes"
--            §9 mutates staging to simulate the following day's extract: ~200
--            customers change loyalty tier, ~50 change city, ~100 products get
--            a 12% cost rise, one store is renamed. Then:
--              * dim_store    overwrites in place        (TYPE 1 — history lost)
--              * dim_customer closes v1, opens v2        (TYPE 2 — history kept)
--              * dim_product  closes v1, opens v2        (TYPE 2 — history kept)
--            Facts for orders ON OR AFTER the change date are then loaded, and
--            they resolve to the v2 keys.
--
--            THIS IS THE PAYOFF. After batch 2, one customer_id owns two
--            customer_keys, and fact rows point at whichever version was
--            current on their own order date. §18 query 4 proves it.
--
--   BATCH 3  "Re-run, nothing changed"
--            Replays batch 2's fact load verbatim. Row counts must not move.
--            That is idempotency (architecture.md §5.8), and it is what makes
--            the recovery procedure for a half-failed pipeline "run it again".
--
-- -----------------------------------------------------------------------------
-- THE SEVERITY POLICY — the one rule that governs every DQ decision here
-- -----------------------------------------------------------------------------
--   ERROR  The row cannot be loaded. It goes to meta.rejected_record with its
--          raw payload, and it does NOT reach the warehouse.
--          Used when the defect makes the row meaningless: no natural key, an
--          unparseable measure, a negative price.
--
--   WARN   The row IS loaded, with a substituted value, and the substitution is
--          counted in meta.data_quality_result.
--          Used when the defect is survivable: a missing region becomes
--          'Unknown', an unresolvable product becomes key -1.
--
-- The distinction matters because ERROR silently deletes revenue if you apply
-- it too widely. An order line for an unknown product is still MONEY THAT WAS
-- TAKEN — rejecting it makes the warehouse disagree with the general ledger.
-- Hence: structural defects are ERROR, referential misses are WARN.
-- =============================================================================


-- =============================================================================
-- SECTION 0 — SESSION SETUP
-- =============================================================================

SET SESSION lc_time_names = 'en_US';
SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_AUTO_VALUE_ON_ZERO');

-- The SCD2 cutover. Orders before this date belong to dimension version 1;
-- orders on or after it belong to version 2. Chosen to sit inside the sales
-- window (2023-01-01 .. 2025-12-31) with real order volume on both sides —
-- a cutover outside the data would "work" and prove nothing.
SET @scd_change_date = DATE '2025-07-01';

-- The initial-load watermark. See §5.
SET @beginning_of_time = DATE '1900-01-01';


-- =============================================================================
-- SECTION 1 — CONTROL PLANE PROCEDURES
-- =============================================================================

DROP PROCEDURE IF EXISTS meta.sp_batch_start;
DROP PROCEDURE IF EXISTS meta.sp_batch_end;
DROP PROCEDURE IF EXISTS meta.sp_dq_log;

DELIMITER $$

-- Opens a batch and returns its id. Every load below runs inside one, so that
-- every warehouse row can be traced to the run that produced it.
CREATE PROCEDURE meta.sp_batch_start(
    IN  p_batch_name   VARCHAR(100),
    IN  p_source       VARCHAR(50),
    OUT p_batch_id     BIGINT
)
BEGIN
    INSERT INTO meta.etl_batch (batch_name, source_system, batch_status)
    VALUES (p_batch_name, p_source, 'RUNNING');

    SET p_batch_id = LAST_INSERT_ID();
END$$

-- Closes a batch with its outcome and reconciliation counts.
--
-- rows_read should equal inserted + updated + rejected. When it does not, rows
-- went missing between extract and load — which is exactly the failure that is
-- invisible without a control plane, because the warehouse still looks fine.
CREATE PROCEDURE meta.sp_batch_end(
    IN p_batch_id  BIGINT,
    IN p_status    VARCHAR(20),
    IN p_read      BIGINT,
    IN p_inserted  BIGINT,
    IN p_updated   BIGINT,
    IN p_rejected  BIGINT
)
BEGIN
    UPDATE meta.etl_batch
    SET    batch_status  = p_status,
           ended_at      = CURRENT_TIMESTAMP(6),
           rows_read     = p_read,
           rows_inserted = p_inserted,
           rows_updated  = p_updated,
           rows_rejected = p_rejected
    WHERE  batch_id = p_batch_id;
END$$

-- Records one data quality rule outcome.
--
-- is_passed is NOT a parameter — it is a generated column on the table,
-- computed as (records_failed = 0). A caller cannot report a rule as passing
-- while also reporting failures, because the schema will not let it.
CREATE PROCEDURE meta.sp_dq_log(
    IN p_batch_id  BIGINT,
    IN p_rule      VARCHAR(100),
    IN p_type      VARCHAR(30),
    IN p_target    VARCHAR(150),
    IN p_severity  VARCHAR(10),
    IN p_checked   BIGINT,
    IN p_failed    BIGINT
)
BEGIN
    INSERT INTO meta.data_quality_result (
        batch_id, rule_name, rule_type, target_object,
        severity, records_checked, records_failed
    )
    VALUES (p_batch_id, p_rule, p_type, p_target,
            p_severity, p_checked, p_failed);
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 2 — VALIDATE AND CLEANSE STAGING
-- =============================================================================
-- The TRANSFORM step. It reads the all-string staging tables and produces
-- properly TYPED temporary tables holding only rows fit to load, quarantining
-- the rest.
--
-- This is where staging's "no constraints" decision pays off. Because staging
-- accepted 'N/A' in a price column without complaint, we can now COUNT how
-- often that happens, quarantine those exact rows with their payloads, and load
-- everything else. A typed staging column would have thrown the whole file out
-- at the door with a conversion error naming no rows at all.
--
-- TWO VALIDATION IDIOMS ARE USED THROUGHOUT:
--   * `col REGEXP '^-?[0-9]+([.][0-9]+)?$'` for numerics. The character class
--     [.] avoids backslash escaping, which MySQL string literals would consume
--     before the regex engine ever saw it.
--   * `STR_TO_DATE(col, '%Y-%m-%d') IS NOT NULL` for dates. STR_TO_DATE returns
--     NULL (plus a warning) for anything it cannot parse, INCLUDING dates that
--     are well-formed but impossible like 2021-02-31. Never use CAST here: CAST
--     is happy to invent 0000-00-00.
-- =============================================================================

DROP PROCEDURE IF EXISTS meta.sp_stage_validate;

DELIMITER $$

CREATE PROCEDURE meta.sp_stage_validate(IN p_batch_id BIGINT)
BEGIN
    DECLARE v_checked BIGINT DEFAULT 0;
    DECLARE v_failed  BIGINT DEFAULT 0;

    -- =========================================================================
    -- 2.1  STORES  (source: store CSV feed — the least trustworthy input)
    -- =========================================================================
    -- An empty store_id is ERROR, not WARN: unlike square_feet or region, there
    -- is no substitute value that lets the row become a usable dimension row —
    -- store_id is the natural key everything else in the row hangs off. Row
    -- quarantined here (not just filtered) so the "never silently drop data"
    -- rule holds even for a defect this file's generator never actually plants.
    INSERT INTO meta.rejected_record (
        batch_id, source_object, source_row_num,
        reject_reason, reject_rule, raw_payload
    )
    SELECT
        p_batch_id, 'staging.stg_store', s._source_row_num,
        'store_id is empty - row cannot be keyed',
        'store_id_not_null',
        JSON_OBJECT('store_id', s.store_id, 'store_name', s.store_name,
                    'city_name', s.city_name, 'region_name', s.region_name)
    FROM staging.stg_store s
    WHERE TRIM(COALESCE(s.store_id,'')) = '';

    DROP TEMPORARY TABLE IF EXISTS tmp_clean_store;
    CREATE TEMPORARY TABLE tmp_clean_store (
        store_id       VARCHAR(50)  NOT NULL,
        store_name     VARCHAR(200) NOT NULL,
        store_type     VARCHAR(50)  NOT NULL,
        city           VARCHAR(100) NOT NULL,
        state_province VARCHAR(100) NOT NULL,
        country        VARCHAR(100) NOT NULL,
        region         VARCHAR(100) NOT NULL,
        square_feet    INT          NOT NULL,
        opened_date    DATE         NOT NULL,
        PRIMARY KEY (store_id)
    ) ENGINE = InnoDB;

    INSERT INTO tmp_clean_store
    SELECT store_id, store_name, store_type, city, state_province,
           country, region, square_feet, opened_date
    FROM (
        SELECT
            TRIM(s.store_id)                                          AS store_id,
            COALESCE(NULLIF(TRIM(s.store_name),     ''), 'Unknown')   AS store_name,
            COALESCE(NULLIF(TRIM(s.store_type),     ''), 'Unknown')   AS store_type,
            COALESCE(NULLIF(TRIM(s.city_name),      ''), 'Unknown')   AS city,
            COALESCE(NULLIF(TRIM(s.state_province), ''), 'Unknown')   AS state_province,
            COALESCE(NULLIF(TRIM(s.country_name),   ''), 'Unknown')   AS country,
            -- DEFECT #8 (empty region) -> 'Unknown', never NULL. A NULL here
            -- would drop the store out of every GROUP BY on region, which is
            -- worse than being visibly unknown.
            COALESCE(NULLIF(TRIM(s.region_name),    ''), 'Unknown')   AS region,
            -- DEFECT #7 (square_feet = 'N/A') -> 0. WARN, not ERROR: floor area
            -- is a descriptive attribute, and losing a whole store from the
            -- dimension over it would orphan every fact row that store owns.
            CASE WHEN TRIM(COALESCE(s.square_feet,'')) REGEXP '^[0-9]+$'
                 THEN CAST(TRIM(s.square_feet) AS SIGNED)
                 ELSE 0 END                                           AS square_feet,
            COALESCE(STR_TO_DATE(TRIM(s.opened_date), '%Y-%m-%d'),
                     DATE '1900-01-01')                               AS opened_date,
            ROW_NUMBER() OVER (PARTITION BY TRIM(s.store_id)
                               ORDER BY s._source_row_num DESC)       AS rn
        FROM staging.stg_store s
        WHERE TRIM(COALESCE(s.store_id, '')) <> ''
    ) x
    WHERE x.rn = 1;

    SELECT COUNT(*) INTO v_checked FROM staging.stg_store;

    SELECT COUNT(*) INTO v_failed FROM staging.stg_store
        WHERE TRIM(COALESCE(store_id,'')) = '';
    CALL meta.sp_dq_log(p_batch_id, 'store_id_not_null', 'NOT_NULL',
                        'staging.stg_store', 'ERROR', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed  FROM staging.stg_store
        WHERE TRIM(COALESCE(square_feet,'')) NOT REGEXP '^[0-9]+$';
    CALL meta.sp_dq_log(p_batch_id, 'store_square_feet_numeric', 'RANGE',
                        'staging.stg_store', 'WARN', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed FROM staging.stg_store
        WHERE TRIM(COALESCE(region_name,'')) = '';
    CALL meta.sp_dq_log(p_batch_id, 'store_region_not_null', 'NOT_NULL',
                        'staging.stg_store', 'WARN', v_checked, v_failed);

    -- =========================================================================
    -- 2.2  CUSTOMERS
    -- =========================================================================
    -- ERROR-severity rules, so failures are quarantined rather than loaded:
    --   * customer_id empty      -> nothing to key on, nothing to correct toward
    --   * signup_date unparseable-> a date column that is not a date
    --
    -- Quarantine BEFORE cleansing, so the raw payload recorded is what actually
    -- arrived rather than something already half-repaired.
    INSERT INTO meta.rejected_record (
        batch_id, source_object, source_row_num,
        reject_reason, reject_rule, raw_payload
    )
    SELECT
        p_batch_id,
        'staging.stg_customer',
        s._source_row_num,
        CASE
            WHEN TRIM(COALESCE(s.customer_id,'')) = ''
                THEN 'customer_id is empty - row cannot be keyed'
            ELSE CONCAT('signup_date not parseable as %Y-%m-%d: ',
                        COALESCE(s.signup_date, '<null>'))
        END,
        CASE
            WHEN TRIM(COALESCE(s.customer_id,'')) = '' THEN 'customer_id_not_null'
            ELSE 'customer_signup_date_parseable'
        END,
        -- JSON_OBJECT rather than a concatenated string: the payload stays
        -- machine-readable, so a correction script can pull fields back out
        -- instead of parsing prose.
        JSON_OBJECT(
            'customer_id',  s.customer_id,
            'customer_name',s.customer_name,
            'email',        s.email,
            'loyalty_tier', s.loyalty_tier,
            'signup_date',  s.signup_date,
            'city_name',    s.city_name,
            'region_name',  s.region_name
        )
    FROM staging.stg_customer s
    WHERE TRIM(COALESCE(s.customer_id,'')) = ''
       OR STR_TO_DATE(TRIM(s.signup_date), '%Y-%m-%d') IS NULL;

    DROP TEMPORARY TABLE IF EXISTS tmp_clean_customer;
    CREATE TEMPORARY TABLE tmp_clean_customer (
        customer_id      VARCHAR(50)  NOT NULL,
        customer_name    VARCHAR(200) NOT NULL,
        email            VARCHAR(255) NOT NULL,
        customer_segment VARCHAR(50)  NOT NULL,
        loyalty_tier     VARCHAR(50)  NOT NULL,
        city             VARCHAR(100) NOT NULL,
        state_province   VARCHAR(100) NOT NULL,
        country          VARCHAR(100) NOT NULL,
        region           VARCHAR(100) NOT NULL,
        postal_code      VARCHAR(20)  NOT NULL,
        signup_date      DATE         NOT NULL,
        PRIMARY KEY (customer_id)
    ) ENGINE = InnoDB;

    -- DEFECT #3 (duplicate customer_id in one feed) is resolved here by
    -- ROW_NUMBER() ... ORDER BY _source_row_num DESC — "latest wins".
    --
    -- Deduplication is NOT optional. dim_customer carries uq_dim_customer_current
    -- (at most one current row per customer), so loading both copies would abort
    -- the entire batch on a unique violation. The PRIMARY KEY on this temp table
    -- is a second line of defence: if the dedupe logic is ever broken, it fails
    -- here, against a 1,000-row temp table, instead of halfway through the
    -- dimension load.
    INSERT INTO tmp_clean_customer
    SELECT customer_id, customer_name, email, customer_segment, loyalty_tier,
           city, state_province, country, region, postal_code, signup_date
    FROM (
        SELECT
            TRIM(s.customer_id)                                        AS customer_id,
            COALESCE(NULLIF(TRIM(s.customer_name),  ''), 'Unknown')    AS customer_name,
            COALESCE(NULLIF(TRIM(s.email),          ''), 'Unknown')    AS email,
            COALESCE(NULLIF(TRIM(s.segment_code),   ''), 'Unknown')    AS customer_segment,
            COALESCE(NULLIF(TRIM(s.loyalty_tier),   ''), 'Unknown')    AS loyalty_tier,
            COALESCE(NULLIF(TRIM(s.city_name),      ''), 'Unknown')    AS city,
            COALESCE(NULLIF(TRIM(s.state_province), ''), 'Unknown')    AS state_province,
            COALESCE(NULLIF(TRIM(s.country_name),   ''), 'Unknown')    AS country,
            COALESCE(NULLIF(TRIM(s.region_name),    ''), 'Unknown')    AS region,
            COALESCE(NULLIF(TRIM(s.postal_code),    ''), 'Unknown')    AS postal_code,
            STR_TO_DATE(TRIM(s.signup_date), '%Y-%m-%d')               AS signup_date,
            ROW_NUMBER() OVER (PARTITION BY TRIM(s.customer_id)
                               ORDER BY s._source_row_num DESC)        AS rn
        FROM staging.stg_customer s
        WHERE TRIM(COALESCE(s.customer_id,'')) <> ''
          AND STR_TO_DATE(TRIM(s.signup_date), '%Y-%m-%d') IS NOT NULL
    ) x
    WHERE x.rn = 1;

    SELECT COUNT(*) INTO v_checked FROM staging.stg_customer;

    SELECT COUNT(*) INTO v_failed FROM staging.stg_customer
        WHERE TRIM(COALESCE(customer_id,'')) = '';
    CALL meta.sp_dq_log(p_batch_id, 'customer_id_not_null', 'NOT_NULL',
                        'staging.stg_customer', 'ERROR', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed FROM staging.stg_customer
        WHERE STR_TO_DATE(TRIM(signup_date), '%Y-%m-%d') IS NULL;
    CALL meta.sp_dq_log(p_batch_id, 'customer_signup_date_parseable', 'RANGE',
                        'staging.stg_customer', 'ERROR', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed FROM (
        SELECT 1 FROM staging.stg_customer
        WHERE TRIM(COALESCE(customer_id,'')) <> ''
        GROUP BY TRIM(customer_id) HAVING COUNT(*) > 1
    ) d;
    CALL meta.sp_dq_log(p_batch_id, 'customer_id_unique', 'UNIQUE',
                        'staging.stg_customer', 'WARN', v_checked, v_failed);

    -- =========================================================================
    -- 2.3  PRODUCTS
    -- =========================================================================
    INSERT INTO meta.rejected_record (
        batch_id, source_object, source_row_num,
        reject_reason, reject_rule, raw_payload
    )
    SELECT
        p_batch_id,
        'staging.stg_product',
        s._source_row_num,
        CASE
            WHEN TRIM(COALESCE(s.unit_cost,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$'
                THEN CONCAT('unit_cost is not numeric: ', COALESCE(s.unit_cost,'<null>'))
            WHEN TRIM(COALESCE(s.list_price,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$'
                THEN CONCAT('list_price is not numeric: ', COALESCE(s.list_price,'<null>'))
            ELSE CONCAT('list_price is negative: ', s.list_price)
        END,
        CASE
            WHEN TRIM(COALESCE(s.unit_cost,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$'
                THEN 'product_unit_cost_numeric'
            WHEN TRIM(COALESCE(s.list_price,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$'
                THEN 'product_list_price_numeric'
            ELSE 'product_list_price_non_negative'
        END,
        JSON_OBJECT(
            'product_id',   s.product_id,
            'product_name', s.product_name,
            'category',     s.category_name,
            'unit_cost',    s.unit_cost,
            'list_price',   s.list_price
        )
    FROM staging.stg_product s
    WHERE TRIM(COALESCE(s.product_id,'')) = ''
       OR TRIM(COALESCE(s.unit_cost,''))  NOT REGEXP '^-?[0-9]+([.][0-9]+)?$'
       OR TRIM(COALESCE(s.list_price,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$'
       -- DEFECT #5: negative list price. Note this is checked only AFTER the
       -- numeric test passes; CAST on 'N/A' would otherwise coerce to 0 and
       -- quietly satisfy the >= 0 test, letting a garbage row through.
       OR (TRIM(COALESCE(s.list_price,'')) REGEXP '^-?[0-9]+([.][0-9]+)?$'
           AND CAST(TRIM(s.list_price) AS DECIMAL(12,2)) < 0);

    DROP TEMPORARY TABLE IF EXISTS tmp_clean_product;
    CREATE TEMPORARY TABLE tmp_clean_product (
        product_id  VARCHAR(50)   NOT NULL,
        product_name VARCHAR(200) NOT NULL,
        category    VARCHAR(100)  NOT NULL,
        subcategory VARCHAR(100)  NOT NULL,
        brand       VARCHAR(100)  NOT NULL,
        unit_cost   DECIMAL(12,2) NOT NULL,
        list_price  DECIMAL(12,2) NOT NULL,
        is_active   TINYINT(1)    NOT NULL,
        PRIMARY KEY (product_id)
    ) ENGINE = InnoDB;

    -- DEFECT #6 (duplicate product_id) resolved by the same "latest wins" rule.
    INSERT INTO tmp_clean_product
    SELECT product_id, product_name, category, subcategory, brand,
           unit_cost, list_price, is_active
    FROM (
        SELECT
            TRIM(s.product_id)                                            AS product_id,
            COALESCE(NULLIF(TRIM(s.product_name),     ''), 'Unknown')     AS product_name,
            COALESCE(NULLIF(TRIM(s.category_name),    ''), 'Unknown')     AS category,
            COALESCE(NULLIF(TRIM(s.subcategory_name), ''), 'Unknown')     AS subcategory,
            COALESCE(NULLIF(TRIM(s.brand_name),       ''), 'Unknown')     AS brand,
            CAST(TRIM(s.unit_cost)  AS DECIMAL(12,2))                     AS unit_cost,
            CAST(TRIM(s.list_price) AS DECIMAL(12,2))                     AS list_price,
            -- The source sends Y/N, 1/0 and true/false interchangeably. All
            -- three are normalized here; anything unrecognized is treated as
            -- inactive, the conservative reading.
            CASE WHEN UPPER(TRIM(COALESCE(s.is_active,''))) IN ('Y','1','TRUE','YES')
                 THEN 1 ELSE 0 END                                        AS is_active,
            ROW_NUMBER() OVER (PARTITION BY TRIM(s.product_id)
                               ORDER BY s._source_row_num DESC)           AS rn
        FROM staging.stg_product s
        WHERE TRIM(COALESCE(s.product_id,'')) <> ''
          AND TRIM(COALESCE(s.unit_cost,''))  REGEXP '^-?[0-9]+([.][0-9]+)?$'
          AND TRIM(COALESCE(s.list_price,'')) REGEXP '^-?[0-9]+([.][0-9]+)?$'
          AND CAST(TRIM(s.list_price) AS DECIMAL(12,2)) >= 0
          AND CAST(TRIM(s.unit_cost)  AS DECIMAL(12,2)) >= 0
    ) x
    WHERE x.rn = 1;

    SELECT COUNT(*) INTO v_checked FROM staging.stg_product;

    SELECT COUNT(*) INTO v_failed FROM staging.stg_product
        WHERE TRIM(COALESCE(unit_cost,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$';
    CALL meta.sp_dq_log(p_batch_id, 'product_unit_cost_numeric', 'RANGE',
                        'staging.stg_product', 'ERROR', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed FROM staging.stg_product
        WHERE TRIM(COALESCE(list_price,'')) REGEXP '^-?[0-9]+([.][0-9]+)?$'
          AND CAST(TRIM(list_price) AS DECIMAL(12,2)) < 0;
    CALL meta.sp_dq_log(p_batch_id, 'product_list_price_non_negative', 'RANGE',
                        'staging.stg_product', 'ERROR', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed FROM (
        SELECT 1 FROM staging.stg_product
        WHERE TRIM(COALESCE(product_id,'')) <> ''
        GROUP BY TRIM(product_id) HAVING COUNT(*) > 1
    ) d;
    CALL meta.sp_dq_log(p_batch_id, 'product_id_unique', 'UNIQUE',
                        'staging.stg_product', 'WARN', v_checked, v_failed);

    -- =========================================================================
    -- 2.4  ORDER LINES
    -- =========================================================================
    -- DEFECT #11: quantity is either non-numeric ('N/A') or zero.
    --
    -- The zero case is the more instructive of the two. 'N/A' announces itself —
    -- any parse attempt fails loudly. '0' parses perfectly, satisfies every type
    -- check, and is still not a business event: nobody sold zero of something.
    -- Without an explicit rule it would sail into the fact table and be caught
    -- only by ck_fact_sales_quantity (quantity <> 0) — which would abort the
    -- whole batch rather than quarantine one row. Catching it here is the
    -- difference between a failed load and a logged exception.
    INSERT INTO meta.rejected_record (
        batch_id, source_object, source_row_num,
        reject_reason, reject_rule, raw_payload
    )
    SELECT
        p_batch_id,
        'staging.stg_sales_order_line',
        s._source_row_num,
        CASE
            WHEN TRIM(COALESCE(s.quantity,'')) NOT REGEXP '^-?[0-9]+$'
                THEN CONCAT('quantity is not an integer: ', COALESCE(s.quantity,'<null>'))
            WHEN CAST(TRIM(s.quantity) AS SIGNED) = 0
                THEN 'quantity is zero - not a business event'
            WHEN STR_TO_DATE(LEFT(TRIM(s.ordered_at),10), '%Y-%m-%d') IS NULL
                THEN CONCAT('ordered_at not parseable: ', COALESCE(s.ordered_at,'<null>'))
            ELSE CONCAT('unit_price is not numeric: ', COALESCE(s.unit_price,'<null>'))
        END,
        CASE
            WHEN TRIM(COALESCE(s.quantity,'')) NOT REGEXP '^-?[0-9]+$'
                THEN 'orderline_quantity_numeric'
            WHEN CAST(TRIM(s.quantity) AS SIGNED) = 0
                THEN 'orderline_quantity_non_zero'
            WHEN STR_TO_DATE(LEFT(TRIM(s.ordered_at),10), '%Y-%m-%d') IS NULL
                THEN 'orderline_ordered_at_parseable'
            ELSE 'orderline_unit_price_numeric'
        END,
        JSON_OBJECT(
            'order_number',      s.order_number,
            'order_line_number', s.order_line_number,
            'ordered_at',        s.ordered_at,
            'product_id',        s.product_id,
            'quantity',          s.quantity,
            'unit_price',        s.unit_price
        )
    FROM staging.stg_sales_order_line s
    WHERE TRIM(COALESCE(s.quantity,'')) NOT REGEXP '^-?[0-9]+$'
       OR CAST(TRIM(s.quantity) AS SIGNED) = 0
       OR STR_TO_DATE(LEFT(TRIM(s.ordered_at),10), '%Y-%m-%d') IS NULL
       OR TRIM(COALESCE(s.unit_price,'')) NOT REGEXP '^-?[0-9]+([.][0-9]+)?$';

    DROP TEMPORARY TABLE IF EXISTS tmp_clean_order_line;
    CREATE TEMPORARY TABLE tmp_clean_order_line (
        order_number      VARCHAR(50)   NOT NULL,
        order_line_number SMALLINT      NOT NULL,
        order_date        DATE          NOT NULL,
        shipped_date      DATE          NULL,
        -- Tracks whether the RAW field was blank, separately from whether it
        -- parsed. STR_TO_DATE() collapses both "blank" and "malformed" to SQL
        -- NULL, and those two cases must resolve to DIFFERENT reserved
        -- members (0 vs -1) in §6. Without this flag they would be
        -- indistinguishable once they reach this table.
        shipped_date_blank TINYINT(1)   NOT NULL,
        customer_id       VARCHAR(50)   NOT NULL,
        store_id          VARCHAR(50)   NOT NULL,
        channel_id        VARCHAR(50)   NOT NULL,
        product_id        VARCHAR(50)   NOT NULL,
        promotion_id      VARCHAR(50)   NOT NULL,
        order_status      VARCHAR(50)   NOT NULL,
        payment_method    VARCHAR(50)   NOT NULL,
        shipping_priority VARCHAR(50)   NOT NULL,
        is_gift           TINYINT(1)    NOT NULL,
        quantity          INT           NOT NULL,
        unit_price        DECIMAL(12,2) NOT NULL,
        discount_amount   DECIMAL(12,2) NOT NULL,
        tax_amount        DECIMAL(12,2) NOT NULL,
        source_row_num    BIGINT        NULL,
        INDEX ix_tmp_order_line_date (order_date)
    ) ENGINE = InnoDB;

    -- DELIBERATELY NOT DEDUPLICATED.
    --
    -- Defect #12 planted 25 byte-identical order lines. They are allowed through
    -- to the fact loader on purpose, so that uq_fact_sales_natural and
    -- INSERT ... ON DUPLICATE KEY UPDATE are the mechanism that absorbs them.
    -- Deduplicating here would be easy and would prove nothing — the idempotency
    -- guarantee has to hold at the fact table, because that is where a re-run of
    -- a half-finished batch actually collides.
    -- Hence this table has NO primary key, unlike every other clean table above.
    INSERT INTO tmp_clean_order_line
    SELECT
        TRIM(s.order_number),
        CAST(TRIM(s.order_line_number) AS SIGNED),
        STR_TO_DATE(LEFT(TRIM(s.ordered_at),10), '%Y-%m-%d'),
        STR_TO_DATE(NULLIF(TRIM(s.shipped_date),''), '%Y-%m-%d'),
        IF(TRIM(COALESCE(s.shipped_date,'')) = '', 1, 0),
        COALESCE(NULLIF(TRIM(s.customer_id),''), 'UNKNOWN'),
        COALESCE(NULLIF(TRIM(s.store_id),''),    'UNKNOWN'),
        COALESCE(NULLIF(TRIM(s.channel_id),''),  'UNKNOWN'),
        COALESCE(NULLIF(TRIM(s.product_id),''),  'UNKNOWN'),
        -- Empty promotion_id stays empty. §6 maps '' to key 0.
        COALESCE(TRIM(s.promotion_id), ''),
        COALESCE(NULLIF(TRIM(s.order_status),''),      'Unknown'),
        COALESCE(NULLIF(TRIM(s.payment_method),''),    'Unknown'),
        COALESCE(NULLIF(TRIM(s.shipping_priority),''), 'Unknown'),
        CASE WHEN TRIM(COALESCE(s.is_gift,'')) IN ('1','Y','TRUE','YES') THEN 1 ELSE 0 END,
        CAST(TRIM(s.quantity) AS SIGNED),
        CAST(TRIM(s.unit_price) AS DECIMAL(12,2)),
        -- Missing money defaults to 0, never NULL: these are ADDITIVE measures,
        -- and SUM() silently skips NULLs, so a NULL discount would understate
        -- total discounts without any error anywhere.
        CASE WHEN TRIM(COALESCE(s.discount_amount,'')) REGEXP '^[0-9]+([.][0-9]+)?$'
             THEN CAST(TRIM(s.discount_amount) AS DECIMAL(12,2)) ELSE 0.00 END,
        CASE WHEN TRIM(COALESCE(s.tax_amount,'')) REGEXP '^[0-9]+([.][0-9]+)?$'
             THEN CAST(TRIM(s.tax_amount) AS DECIMAL(12,2)) ELSE 0.00 END,
        s._source_row_num
    FROM staging.stg_sales_order_line s
    WHERE TRIM(COALESCE(s.quantity,'')) REGEXP '^-?[0-9]+$'
      AND CAST(TRIM(s.quantity) AS SIGNED) <> 0
      AND STR_TO_DATE(LEFT(TRIM(s.ordered_at),10), '%Y-%m-%d') IS NOT NULL
      AND TRIM(COALESCE(s.unit_price,'')) REGEXP '^-?[0-9]+([.][0-9]+)?$'
      AND CAST(TRIM(s.unit_price) AS DECIMAL(12,2)) >= 0;

    SELECT COUNT(*) INTO v_checked FROM staging.stg_sales_order_line;

    SELECT COUNT(*) INTO v_failed FROM staging.stg_sales_order_line
        WHERE TRIM(COALESCE(quantity,'')) NOT REGEXP '^-?[0-9]+$';
    CALL meta.sp_dq_log(p_batch_id, 'orderline_quantity_numeric', 'RANGE',
                        'staging.stg_sales_order_line', 'ERROR', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed FROM staging.stg_sales_order_line
        WHERE TRIM(COALESCE(quantity,'')) REGEXP '^-?[0-9]+$'
          AND CAST(TRIM(quantity) AS SIGNED) = 0;
    CALL meta.sp_dq_log(p_batch_id, 'orderline_quantity_non_zero', 'RANGE',
                        'staging.stg_sales_order_line', 'ERROR', v_checked, v_failed);

    -- REFERENTIAL rules are WARN, not ERROR — the defining decision of this
    -- pipeline. An order line naming a product we have never heard of is still
    -- money that changed hands. Rejecting it would make warehouse revenue
    -- disagree with the general ledger, and the discrepancy would be invisible
    -- because the rows simply would not be there to miss.
    SELECT COUNT(*) INTO v_failed
    FROM   staging.stg_sales_order_line s
    LEFT   JOIN staging.stg_product p ON TRIM(p.product_id) = TRIM(s.product_id)
    WHERE  p.product_id IS NULL;
    CALL meta.sp_dq_log(p_batch_id, 'orderline_product_resolvable', 'REFERENTIAL',
                        'staging.stg_sales_order_line', 'WARN', v_checked, v_failed);

    SELECT COUNT(*) INTO v_failed
    FROM   staging.stg_sales_order_line s
    LEFT   JOIN staging.stg_customer c ON TRIM(c.customer_id) = TRIM(s.customer_id)
    WHERE  c.customer_id IS NULL;
    CALL meta.sp_dq_log(p_batch_id, 'orderline_customer_resolvable', 'REFERENTIAL',
                        'staging.stg_sales_order_line', 'WARN', v_checked, v_failed);

    -- =========================================================================
    -- 2.5  INVENTORY
    -- =========================================================================
    INSERT INTO meta.rejected_record (
        batch_id, source_object, source_row_num,
        reject_reason, reject_rule, raw_payload
    )
    SELECT
        p_batch_id,
        'staging.stg_inventory',
        s._source_row_num,
        CASE
            WHEN TRIM(COALESCE(s.quantity_on_hand,'')) NOT REGEXP '^-?[0-9]+$'
                THEN CONCAT('quantity_on_hand is not an integer: ',
                            COALESCE(s.quantity_on_hand,'<null>'))
            ELSE CONCAT('quantity_on_hand is negative: ', s.quantity_on_hand)
        END,
        'inventory_quantity_on_hand_non_negative',
        JSON_OBJECT(
            'snapshot_date',    s.snapshot_date,
            'product_id',       s.product_id,
            'store_id',         s.store_id,
            'quantity_on_hand', s.quantity_on_hand
        )
    FROM staging.stg_inventory s
    WHERE TRIM(COALESCE(s.quantity_on_hand,'')) NOT REGEXP '^-?[0-9]+$'
       -- DEFECT #13: negative stock. Physically impossible, and blocked at the
       -- warehouse by ck_fact_inventory_quantities. Caught here so the row is
       -- quarantined instead of aborting the load.
       OR CAST(TRIM(s.quantity_on_hand) AS SIGNED) < 0
       OR STR_TO_DATE(TRIM(s.snapshot_date), '%Y-%m-%d') IS NULL;

    DROP TEMPORARY TABLE IF EXISTS tmp_clean_inventory;
    CREATE TEMPORARY TABLE tmp_clean_inventory (
        snapshot_date     DATE          NOT NULL,
        product_id        VARCHAR(50)   NOT NULL,
        store_id          VARCHAR(50)   NOT NULL,
        quantity_on_hand  INT           NOT NULL,
        quantity_on_order INT           NOT NULL,
        reorder_point     INT           NOT NULL,
        unit_cost         DECIMAL(12,2) NOT NULL,
        PRIMARY KEY (snapshot_date, product_id, store_id)
    ) ENGINE = InnoDB;

    -- The PK here mirrors the declared grain of fact_inventory_snapshot exactly:
    -- one row per product, per store, per day. Enforcing it on the clean table
    -- means a grain violation surfaces before it can reach the fact table.
    INSERT INTO tmp_clean_inventory
    SELECT snapshot_date, product_id, store_id, quantity_on_hand,
           quantity_on_order, reorder_point, unit_cost
    FROM (
        SELECT
            STR_TO_DATE(TRIM(s.snapshot_date), '%Y-%m-%d')              AS snapshot_date,
            TRIM(s.product_id)                                          AS product_id,
            TRIM(s.store_id)                                            AS store_id,
            CAST(TRIM(s.quantity_on_hand) AS SIGNED)                    AS quantity_on_hand,
            CASE WHEN TRIM(COALESCE(s.quantity_on_order,'')) REGEXP '^[0-9]+$'
                 THEN CAST(TRIM(s.quantity_on_order) AS SIGNED) ELSE 0 END AS quantity_on_order,
            CASE WHEN TRIM(COALESCE(s.reorder_point,'')) REGEXP '^[0-9]+$'
                 THEN CAST(TRIM(s.reorder_point) AS SIGNED) ELSE 0 END  AS reorder_point,
            CASE WHEN TRIM(COALESCE(s.unit_cost,'')) REGEXP '^[0-9]+([.][0-9]+)?$'
                 THEN CAST(TRIM(s.unit_cost) AS DECIMAL(12,2)) ELSE 0.00 END AS unit_cost,
            ROW_NUMBER() OVER (
                PARTITION BY STR_TO_DATE(TRIM(s.snapshot_date), '%Y-%m-%d'),
                             TRIM(s.product_id), TRIM(s.store_id)
                ORDER BY s._source_row_num DESC)                        AS rn
        FROM staging.stg_inventory s
        WHERE TRIM(COALESCE(s.quantity_on_hand,'')) REGEXP '^-?[0-9]+$'
          AND CAST(TRIM(s.quantity_on_hand) AS SIGNED) >= 0
          AND STR_TO_DATE(TRIM(s.snapshot_date), '%Y-%m-%d') IS NOT NULL
    ) x
    WHERE x.rn = 1;

    SELECT COUNT(*) INTO v_checked FROM staging.stg_inventory;
    SELECT COUNT(*) INTO v_failed  FROM staging.stg_inventory
        WHERE TRIM(COALESCE(quantity_on_hand,'')) NOT REGEXP '^-?[0-9]+$'
           OR CAST(TRIM(quantity_on_hand) AS SIGNED) < 0;
    CALL meta.sp_dq_log(p_batch_id, 'inventory_quantity_on_hand_non_negative', 'RANGE',
                        'staging.stg_inventory', 'ERROR', v_checked, v_failed);
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 3 — SCD TYPE 1 LOADER: dim_store
-- =============================================================================
-- The simple case, and it is here mainly for contrast with §4 and §5.
--
-- Two statements: insert what is new, overwrite what changed. No versions, no
-- validity dates, no is_current flag — because Type 1 keeps no history.
--
-- WHAT THE OVERWRITE COSTS, stated plainly: after a store is renamed, EVERY
-- historical report shows the new name, including reports about periods before
-- the rename. For a corrected typo that is exactly right. For a genuine change
-- of business meaning it is exactly wrong, and it is unrecoverable — the old
-- value is gone from the database entirely.
--
-- dim_store is Type 1 on purpose (architecture.md §4.2): 25 rows whose changes
-- are corrections, chosen this way to prove that Type 2 elsewhere was a decision
-- rather than a default. dw_updated_at is the only surviving evidence that an
-- overwrite ever happened.
-- =============================================================================

DROP PROCEDURE IF EXISTS warehouse.sp_load_dim_store;

DELIMITER $$

CREATE PROCEDURE warehouse.sp_load_dim_store(IN p_batch_id BIGINT)
BEGIN
    -- New stores.
    INSERT INTO warehouse.dim_store (
        store_id, store_name, store_type, city, state_province, country, region,
        square_feet, opened_date, dw_batch_id, dw_source_system
    )
    SELECT s.store_id, s.store_name, s.store_type, s.city, s.state_province,
           s.country, s.region, s.square_feet, s.opened_date,
           p_batch_id, 'STORE_CSV'
    FROM      tmp_clean_store s
    LEFT JOIN warehouse.dim_store d ON d.store_id = s.store_id
    WHERE     d.store_key IS NULL;

    -- THE TYPE 1 OVERWRITE. The WHERE clause matters: without the change
    -- comparison this would rewrite dw_updated_at on all 25 rows every run,
    -- destroying the only signal that says which stores actually changed.
    UPDATE warehouse.dim_store d
    JOIN   tmp_clean_store s ON s.store_id = d.store_id
    SET    d.store_name     = s.store_name,
           d.store_type     = s.store_type,
           d.city           = s.city,
           d.state_province = s.state_province,
           d.country        = s.country,
           d.region         = s.region,
           d.square_feet    = s.square_feet,
           d.opened_date    = s.opened_date,
           d.dw_batch_id    = p_batch_id,
           d.dw_updated_at  = CURRENT_TIMESTAMP(6)
    WHERE  d.store_key > 0
      AND (d.store_name     <> s.store_name
        OR d.store_type     <> s.store_type
        OR d.city           <> s.city
        OR d.state_province <> s.state_province
        OR d.country        <> s.country
        OR d.region         <> s.region
        OR d.square_feet    <> s.square_feet
        OR d.opened_date    <> s.opened_date);
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 4 — SCD TYPE 2 LOADER: dim_customer
-- =============================================================================
-- The centrepiece of the whole project.
--
-- ---- THE FIVE STEPS, AND WHY THE ORDER IS NOT NEGOTIABLE -------------------
--
--   1. DETECT   Materialize the change set into a temp table.
--   2. CLOSE    Expire the current row of every changed customer.
--   3. OPEN     Insert the new version.
--   4. INSERT   Add customers never seen before.
--   5. TYPE 1   Overwrite non-historized attributes in place.
--
-- Step 2 MUST precede step 3. uq_dim_customer_current permits exactly one row
-- per customer_id with is_current = 1. Inserting the new version while the old
-- one is still open violates it and aborts the batch. This is the constraint
-- doing its job — it makes the incorrect order impossible rather than merely
-- discouraged.
--
-- Step 3 MUST precede step 4. Step 4 finds "customers with no current row" —
-- which, in the window between steps 2 and 3, is every changed customer. Run it
-- there and each changed customer gets a spurious duplicate at version 1.
--
-- Step 1 exists because of steps 2 and 3 together. Detecting changes inline in
-- both would mean detecting them against a dimension that step 2 has already
-- modified, so step 3 would find nothing. The change set has to be frozen first.
--
-- ---- WHICH ATTRIBUTES TRIGGER A VERSION -------------------------------------
--   TRACKED (Type 2): loyalty_tier, city, state_province, country, region
--       Slice-by-history attributes. A tier change must not retroactively
--       reclassify last January's orders.
--   NOT TRACKED (Type 1): customer_name, email, customer_segment, postal_code
--       Corrections. Nobody asks "what was revenue by the email address the
--       customer had at the time", so versioning on them would inflate the
--       dimension for no analytical gain.
--   SCD0: signup_date — a fact about the customer, not about the version.
--       Copied forward unchanged onto every new row.
--
-- Mixing Type 1 and Type 2 columns in one dimension is a TYPE 6 HYBRID. It is
-- the common real-world shape, and it is why step 5 exists.
--
-- ---- valid_from ON THE FIRST-EVER LOAD --------------------------------------
-- p_initial_from is a separate parameter from p_effective_date, and conflating
-- them is the most common way to break an initial SCD2 load.
--
-- The fact loader resolves customers with
--     order_date BETWEEN valid_from AND valid_to
-- so if the historical load stamped valid_from = the load date, EVERY order
-- older than the load date would fall outside every validity window and resolve
-- to customer_key = -1. The warehouse would come up with all history attributed
-- to Unknown, and the FKs would all be satisfied, so nothing would error.
--
-- Batch 1 therefore passes 1900-01-01 — "as far back as this warehouse knows".
-- Batch 2 passes the real change date, because by then a genuinely new customer
-- really did appear on that day.
-- =============================================================================

DROP PROCEDURE IF EXISTS warehouse.sp_load_dim_customer;

DELIMITER $$

CREATE PROCEDURE warehouse.sp_load_dim_customer(
    IN p_batch_id       BIGINT,
    IN p_effective_date DATE,
    IN p_initial_from   DATE
)
BEGIN
    -- ---- STEP 1: DETECT --------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_scd2_customer;
    CREATE TEMPORARY TABLE tmp_scd2_customer (
        customer_id      VARCHAR(50)  NOT NULL,
        customer_name    VARCHAR(200) NOT NULL,
        email            VARCHAR(255) NOT NULL,
        customer_segment VARCHAR(50)  NOT NULL,
        loyalty_tier     VARCHAR(50)  NOT NULL,
        city             VARCHAR(100) NOT NULL,
        state_province   VARCHAR(100) NOT NULL,
        country          VARCHAR(100) NOT NULL,
        region           VARCHAR(100) NOT NULL,
        postal_code      VARCHAR(20)  NOT NULL,
        signup_date      DATE         NOT NULL,
        prior_version    INT          NOT NULL,
        PRIMARY KEY (customer_id)
    ) ENGINE = InnoDB;

    INSERT INTO tmp_scd2_customer
    SELECT s.customer_id, s.customer_name, s.email, s.customer_segment,
           s.loyalty_tier, s.city, s.state_province, s.country, s.region,
           s.postal_code, s.signup_date, d.version_number
    FROM tmp_clean_customer s
    JOIN warehouse.dim_customer d
      ON  d.customer_id = s.customer_id
      AND d.is_current  = 1
    WHERE d.customer_key > 0            -- never version the reserved members
      AND (d.loyalty_tier   <> s.loyalty_tier
        OR d.city           <> s.city
        OR d.state_province <> s.state_province
        OR d.country        <> s.country
        OR d.region         <> s.region);

    -- ---- STEP 2: CLOSE ---------------------------------------------------
    -- valid_to is the day BEFORE the new version opens, so the two windows abut
    -- without overlapping. An overlap would make the as-of join in §6 return two
    -- rows for one customer and multiply fact rows — silently doubling revenue
    -- for exactly the customers who changed.
    UPDATE warehouse.dim_customer d
    JOIN   tmp_scd2_customer c ON c.customer_id = d.customer_id
    SET    d.valid_to      = DATE_SUB(p_effective_date, INTERVAL 1 DAY),
           d.is_current    = 0,
           d.dw_batch_id   = p_batch_id,
           d.dw_updated_at = CURRENT_TIMESTAMP(6)
    WHERE  d.is_current = 1
      AND  d.customer_key > 0;

    -- ---- STEP 3: OPEN ----------------------------------------------------
    INSERT INTO warehouse.dim_customer (
        customer_id, customer_name, email, customer_segment, loyalty_tier,
        city, state_province, country, region, postal_code, signup_date,
        valid_from, valid_to, is_current, version_number,
        dw_batch_id, dw_source_system
    )
    SELECT c.customer_id, c.customer_name, c.email, c.customer_segment,
           c.loyalty_tier, c.city, c.state_province, c.country, c.region,
           c.postal_code,
           c.signup_date,                       -- SCD0: carried forward
           p_effective_date,
           DATE '9999-12-31',                   -- high date, never NULL
           1,
           c.prior_version + 1,
           p_batch_id, 'SALES_OLTP'
    FROM tmp_scd2_customer c;

    -- ---- STEP 4: BRAND-NEW CUSTOMERS -------------------------------------
    INSERT INTO warehouse.dim_customer (
        customer_id, customer_name, email, customer_segment, loyalty_tier,
        city, state_province, country, region, postal_code, signup_date,
        valid_from, valid_to, is_current, version_number,
        dw_batch_id, dw_source_system
    )
    SELECT s.customer_id, s.customer_name, s.email, s.customer_segment,
           s.loyalty_tier, s.city, s.state_province, s.country, s.region,
           s.postal_code, s.signup_date,
           p_initial_from,                      -- see the header block
           DATE '9999-12-31', 1, 1,
           p_batch_id, 'SALES_OLTP'
    FROM      tmp_clean_customer s
    LEFT JOIN warehouse.dim_customer d
           ON d.customer_id = s.customer_id
          AND d.is_current  = 1
    WHERE     d.customer_key IS NULL;

    -- ---- STEP 5: TYPE 1 OVERWRITE ----------------------------------------
    -- Non-historized attributes are corrected in place on the CURRENT row only.
    -- Closed rows are left untouched: they record what was true then, and a
    -- corrected email does not change what was true then.
    UPDATE warehouse.dim_customer d
    JOIN   tmp_clean_customer s
        ON s.customer_id = d.customer_id
       AND d.is_current  = 1
    SET    d.customer_name    = s.customer_name,
           d.email            = s.email,
           d.customer_segment = s.customer_segment,
           d.postal_code      = s.postal_code,
           d.dw_batch_id      = p_batch_id,
           d.dw_updated_at    = CURRENT_TIMESTAMP(6)
    WHERE  d.customer_key > 0
      AND (d.customer_name    <> s.customer_name
        OR d.email            <> s.email
        OR d.customer_segment <> s.customer_segment
        OR d.postal_code      <> s.postal_code);
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 5 — SCD TYPE 2 LOADER: dim_product
-- =============================================================================
-- Structurally identical to §4; only the tracked attribute list differs.
--
--   TRACKED (Type 2): unit_cost, list_price
--   NOT TRACKED (Type 1): product_name, category, subcategory, brand, is_active
--
-- WHY COST AND PRICE ARE THE HISTORIZED ONES: margin is a historical fact. When
-- a supplier raises a widget's cost in July, June's reported margin must not
-- move. Type 1 here would silently restate every historical margin figure every
-- time a price list was updated — and nothing would look broken, which is what
-- makes it dangerous.
--
-- A renamed product or a re-parented category, by contrast, is housekeeping.
-- Nobody asks for revenue by the category a product used to sit in.
-- =============================================================================

DROP PROCEDURE IF EXISTS warehouse.sp_load_dim_product;

DELIMITER $$

CREATE PROCEDURE warehouse.sp_load_dim_product(
    IN p_batch_id       BIGINT,
    IN p_effective_date DATE,
    IN p_initial_from   DATE
)
BEGIN
    -- ---- STEP 1: DETECT --------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_scd2_product;
    CREATE TEMPORARY TABLE tmp_scd2_product (
        product_id    VARCHAR(50)   NOT NULL,
        product_name  VARCHAR(200)  NOT NULL,
        category      VARCHAR(100)  NOT NULL,
        subcategory   VARCHAR(100)  NOT NULL,
        brand         VARCHAR(100)  NOT NULL,
        unit_cost     DECIMAL(12,2) NOT NULL,
        list_price    DECIMAL(12,2) NOT NULL,
        is_active     TINYINT(1)    NOT NULL,
        prior_version INT           NOT NULL,
        PRIMARY KEY (product_id)
    ) ENGINE = InnoDB;

    INSERT INTO tmp_scd2_product
    SELECT s.product_id, s.product_name, s.category, s.subcategory, s.brand,
           s.unit_cost, s.list_price, s.is_active, d.version_number
    FROM tmp_clean_product s
    JOIN warehouse.dim_product d
      ON  d.product_id = s.product_id
      AND d.is_current = 1
    WHERE d.product_key > 0
      AND (d.unit_cost  <> s.unit_cost
        OR d.list_price <> s.list_price);

    -- ---- STEP 2: CLOSE ---------------------------------------------------
    UPDATE warehouse.dim_product d
    JOIN   tmp_scd2_product p ON p.product_id = d.product_id
    SET    d.valid_to      = DATE_SUB(p_effective_date, INTERVAL 1 DAY),
           d.is_current    = 0,
           d.dw_batch_id   = p_batch_id,
           d.dw_updated_at = CURRENT_TIMESTAMP(6)
    WHERE  d.is_current = 1
      AND  d.product_key > 0;

    -- ---- STEP 3: OPEN ----------------------------------------------------
    INSERT INTO warehouse.dim_product (
        product_id, product_name, category, subcategory, brand,
        unit_cost, list_price, is_active,
        valid_from, valid_to, is_current, version_number,
        dw_batch_id, dw_source_system
    )
    SELECT p.product_id, p.product_name, p.category, p.subcategory, p.brand,
           p.unit_cost, p.list_price, p.is_active,
           p_effective_date, DATE '9999-12-31', 1, p.prior_version + 1,
           p_batch_id, 'PRODUCT_MDM'
    FROM tmp_scd2_product p;

    -- ---- STEP 4: BRAND-NEW PRODUCTS --------------------------------------
    INSERT INTO warehouse.dim_product (
        product_id, product_name, category, subcategory, brand,
        unit_cost, list_price, is_active,
        valid_from, valid_to, is_current, version_number,
        dw_batch_id, dw_source_system
    )
    SELECT s.product_id, s.product_name, s.category, s.subcategory, s.brand,
           s.unit_cost, s.list_price, s.is_active,
           p_initial_from, DATE '9999-12-31', 1, 1,
           p_batch_id, 'PRODUCT_MDM'
    FROM      tmp_clean_product s
    LEFT JOIN warehouse.dim_product d
           ON d.product_id = s.product_id
          AND d.is_current = 1
    WHERE     d.product_key IS NULL;

    -- ---- STEP 5: TYPE 1 OVERWRITE ----------------------------------------
    UPDATE warehouse.dim_product d
    JOIN   tmp_clean_product s
        ON s.product_id = d.product_id
       AND d.is_current = 1
    SET    d.product_name  = s.product_name,
           d.category      = s.category,
           d.subcategory   = s.subcategory,
           d.brand         = s.brand,
           d.is_active     = s.is_active,
           d.dw_batch_id   = p_batch_id,
           d.dw_updated_at = CURRENT_TIMESTAMP(6)
    WHERE  d.product_key > 0
      AND (d.product_name <> s.product_name
        OR d.category     <> s.category
        OR d.subcategory  <> s.subcategory
        OR d.brand        <> s.brand
        OR d.is_active    <> s.is_active);
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 6 — FACT LOADER: fact_sales
-- =============================================================================
-- Where the star gets assembled. Every natural key becomes a surrogate key, and
-- every lookup that fails resolves to a reserved member instead of a NULL.
--
-- ---- THE AS-OF JOIN — the single most important join in this file -----------
--
--     JOIN dim_customer c
--       ON  c.customer_id = s.customer_id
--       AND s.order_date BETWEEN c.valid_from AND c.valid_to
--
-- NOT `c.is_current = 1`. Using is_current would attach every fact to the
-- customer's CURRENT attributes, which is precisely the history destruction the
-- warehouse exists to prevent — and it would look completely correct, because
-- every join would still find exactly one row.
--
-- The BETWEEN is what makes the fact carry the version that was true ON ITS OWN
-- ORDER DATE. It is also what makes late-arriving facts correct for free: a sale
-- from three months ago that turns up today still lands on the version that was
-- open three months ago (dimensional_modeling.md §9).
--
-- ---- UNKNOWN (-1) VS NOT APPLICABLE (0) ------------------------------------
-- The distinction is enforced per column, not globally:
--
--     ship_date_key   NULL shipped_date -> 0   (order genuinely not shipped)
--                     unparseable date  -> -1  (we do not know)
--     promotion_key   empty promotion_id -> 0  (there was no promotion)
--                     unmatched id       -> -1 (we do not know)
--     product_key     unmatched          -> -1 always; there is no such thing
--                                          as a sale of no product
--
-- Getting these backwards produces a warehouse where "no promotion" and "broken
-- promotion feed" are the same number, and the data quality metric becomes
-- meaningless.
--
-- ---- unit_cost IS SNAPSHOTTED, NOT LOOKED UP -------------------------------
-- unit_cost comes from the AS-OF dim_product row and is written onto the fact.
-- architecture.md §5.6: this is belt-and-braces with SCD2. The SCD2 row gives
-- the attribute history; this column gives the transaction truth even if someone
-- later joins to the current dimension row by mistake.
--
-- ---- IDEMPOTENCY ------------------------------------------------------------
-- INSERT ... ON DUPLICATE KEY UPDATE against uq_fact_sales_natural
-- (order_number, order_line_number, order_date_key). Re-running a batch
-- converges instead of duplicating revenue, so recovery from a half-finished run
-- is "run it again" rather than a forensic exercise.
--
-- VALUES() is deprecated from MySQL 8.0.20 in favour of a row alias, but the
-- alias form requires 8.0.19+. VALUES() is used here because this project's
-- floor is 8.0.16. On a modern-only deployment, prefer:
--     INSERT ... AS new ON DUPLICATE KEY UPDATE col = new.col
-- =============================================================================

DROP PROCEDURE IF EXISTS warehouse.sp_load_fact_sales;

DELIMITER $$

CREATE PROCEDURE warehouse.sp_load_fact_sales(
    IN p_batch_id  BIGINT,
    IN p_from_date DATE,
    IN p_to_date   DATE
)
BEGIN
    INSERT INTO warehouse.fact_sales (
        order_date_key, ship_date_key, customer_key, product_key, store_key,
        promotion_key, channel_key, order_junk_key,
        order_number, order_line_number,
        quantity, unit_price, unit_cost, discount_amount, tax_amount,
        dw_batch_id, dw_source_system
    )
    SELECT
        IFNULL(od.date_key, -1),
        -- Three-way, using the blank flag rather than `shipped_date IS NULL`
        -- alone, because that column is NULL for BOTH a blank field and an
        -- unparseable one (STR_TO_DATE collapses both to NULL):
        --   raw field was blank        -> 0  (NOT APPLICABLE: genuinely unshipped)
        --   raw field present, garbled -> -1 (UNKNOWN: a real data problem)
        --   parsed, but no dim_date row -> -1 (UNKNOWN: should not happen in-range)
        CASE
            WHEN s.shipped_date_blank = 1 THEN 0
            WHEN s.shipped_date IS NULL   THEN -1
            ELSE IFNULL(sd.date_key, -1)
        END,
        IFNULL(c.customer_key,  -1),
        IFNULL(p.product_key,   -1),
        IFNULL(st.store_key,    -1),
        CASE WHEN s.promotion_id = '' THEN 0 ELSE IFNULL(pr.promotion_key, -1) END,
        IFNULL(ch.channel_key,  -1),
        IFNULL(j.order_junk_key, -1),

        s.order_number,                 -- degenerate dimension
        s.order_line_number,

        s.quantity,
        s.unit_price,
        -- Snapshotted from the as-of product version. The Unknown member carries
        -- cost 0.00, so an unresolvable product reports 100% margin — absurd on
        -- its face, and deliberately so: it gets noticed and fixed, where a
        -- plausible guess would not.
        IFNULL(p.unit_cost, 0.00),
        s.discount_amount,
        s.tax_amount,

        p_batch_id, 'SALES_OLTP'
    FROM tmp_clean_order_line s

    -- dim_date joined TWICE — the role-playing dimension, made physical. This is
    -- legal here because dim_date is a permanent table; the same trick with a
    -- TEMPORARY table would fail with ERROR 1137.
    LEFT JOIN warehouse.dim_date     od ON od.full_date = s.order_date
    LEFT JOIN warehouse.dim_date     sd ON sd.full_date = s.shipped_date

    -- THE AS-OF JOINS. See the header block.
    LEFT JOIN warehouse.dim_customer c
           ON  c.customer_id = s.customer_id
           AND s.order_date BETWEEN c.valid_from AND c.valid_to
    LEFT JOIN warehouse.dim_product  p
           ON  p.product_id = s.product_id
           AND s.order_date BETWEEN p.valid_from AND p.valid_to

    -- Type 1 dimensions need no as-of predicate: there is only ever one row.
    LEFT JOIN warehouse.dim_store     st ON st.store_id     = s.store_id
    LEFT JOIN warehouse.dim_promotion pr ON pr.promotion_id = s.promotion_id
    LEFT JOIN warehouse.dim_channel   ch ON ch.channel_id   = s.channel_id

    -- The junk dimension resolves on the full four-column combination, which
    -- uq_dim_order_junk_combo indexes.
    LEFT JOIN warehouse.dim_order_junk j
           ON  j.order_status      = s.order_status
           AND j.payment_method    = s.payment_method
           AND j.shipping_priority = s.shipping_priority
           AND j.is_gift           = s.is_gift

    WHERE s.order_date >= p_from_date
      AND s.order_date <= p_to_date

    ON DUPLICATE KEY UPDATE
        ship_date_key   = VALUES(ship_date_key),
        customer_key    = VALUES(customer_key),
        product_key     = VALUES(product_key),
        store_key       = VALUES(store_key),
        promotion_key   = VALUES(promotion_key),
        channel_key     = VALUES(channel_key),
        order_junk_key  = VALUES(order_junk_key),
        quantity        = VALUES(quantity),
        unit_price      = VALUES(unit_price),
        unit_cost       = VALUES(unit_cost),
        discount_amount = VALUES(discount_amount),
        tax_amount      = VALUES(tax_amount),
        dw_batch_id     = VALUES(dw_batch_id),
        dw_updated_at   = CURRENT_TIMESTAMP(6);
        -- The four generated columns are absent on purpose: MySQL forbids
        -- assigning to a generated column, and they recompute themselves.
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 7 — FACT LOADER: fact_inventory_snapshot
-- =============================================================================
-- The periodic snapshot. Simpler than fact_sales — three dimensions, no
-- degenerate dimension, no role-playing.
--
-- IDEMPOTENCY COMES FREE HERE. The primary key IS the declared grain
-- (snapshot_date_key, product_key, store_key), so ON DUPLICATE KEY UPDATE needs
-- no separate unique constraint. Re-loading a day's snapshot overwrites that
-- day's rows in place, which is exactly the desired behaviour for a restatement.
--
-- inventory_value_amount is computed at LOAD time from the unit_cost that
-- travelled with the snapshot — not looked up later. Valuing last month's stock
-- at today's cost would silently restate inventory history, the same class of
-- error that SCD2 exists to prevent on the sales side.
--
-- REMINDER, carried into the column comments in schema.sql: quantity_on_hand and
-- inventory_value_amount are SEMI-ADDITIVE. Sum them across products and stores
-- freely; never sum them across dates.
-- =============================================================================

DROP PROCEDURE IF EXISTS warehouse.sp_load_fact_inventory;

DELIMITER $$

CREATE PROCEDURE warehouse.sp_load_fact_inventory(IN p_batch_id BIGINT)
BEGIN
    INSERT INTO warehouse.fact_inventory_snapshot (
        snapshot_date_key, product_key, store_key,
        quantity_on_hand, quantity_on_order, inventory_value_amount,
        reorder_point, dw_batch_id, dw_source_system
    )
    SELECT
        IFNULL(d.date_key,     -1),
        IFNULL(p.product_key,  -1),
        IFNULL(st.store_key,   -1),
        s.quantity_on_hand,
        s.quantity_on_order,
        ROUND(s.quantity_on_hand * s.unit_cost, 2),
        s.reorder_point,
        p_batch_id, 'SALES_OLTP'
    FROM tmp_clean_inventory s
    LEFT JOIN warehouse.dim_date    d  ON d.full_date  = s.snapshot_date
    -- As-of again: stock is valued against the product version that was current
    -- on the snapshot date.
    LEFT JOIN warehouse.dim_product p
           ON  p.product_id = s.product_id
           AND s.snapshot_date BETWEEN p.valid_from AND p.valid_to
    LEFT JOIN warehouse.dim_store   st ON st.store_id = s.store_id

    ON DUPLICATE KEY UPDATE
        quantity_on_hand       = VALUES(quantity_on_hand),
        quantity_on_order      = VALUES(quantity_on_order),
        inventory_value_amount = VALUES(inventory_value_amount),
        reorder_point          = VALUES(reorder_point),
        dw_batch_id            = VALUES(dw_batch_id),
        dw_updated_at          = CURRENT_TIMESTAMP(6);
        -- is_stockout omitted: generated column.
END$$

DELIMITER ;


-- =============================================================================
-- SECTION 8 — BATCH 1: HISTORICAL LOAD
-- =============================================================================
-- Staging exactly as sample_data.sql produced it. Dimensions at version 1 with
-- valid_from = 1900-01-01; facts for every order strictly BEFORE the change
-- date.
-- =============================================================================

CALL meta.sp_batch_start('batch_1_historical_load', 'SALES_OLTP', @batch1);
SELECT CONCAT('BATCH 1 started, id = ', @batch1) AS progress;

CALL meta.sp_stage_validate(@batch1);

CALL warehouse.sp_load_dim_store(@batch1);
CALL warehouse.sp_load_dim_customer(@batch1, @beginning_of_time, @beginning_of_time);
CALL warehouse.sp_load_dim_product (@batch1, @beginning_of_time, @beginning_of_time);

CALL warehouse.sp_load_fact_sales(
        @batch1,
        DATE '1900-01-01',
        DATE_SUB(@scd_change_date, INTERVAL 1 DAY));

-- Reconciliation counters. rows_read is every staging row the batch considered;
-- rejected comes from the quarantine table; inserted is what actually landed.
--
-- KNOWN SIMPLIFICATION: @b1_ins counts every warehouse row STAMPED with this
-- batch id, which is true whether that row was INSERTed or UPDATEd (the Type 1
-- loaders stamp dw_batch_id on both paths). rows_updated is therefore reported
-- as a flat 0 below rather than a real split. Getting a precise split would mean
-- capturing ROW_COUNT() immediately after each DML statement inside every
-- procedure, before any other statement can overwrite it — a real change, left
-- for the polish pass rather than risked here untested. This does not weaken the
-- IDEMPOTENCY proof: that proof is the direct before/after table-count
-- comparison in §11 query 5, which does not depend on this bookkeeping at all.
SET @b1_read = (SELECT COUNT(*) FROM staging.stg_customer)
             + (SELECT COUNT(*) FROM staging.stg_product)
             + (SELECT COUNT(*) FROM staging.stg_store)
             + (SELECT COUNT(*) FROM staging.stg_sales_order_line);
SET @b1_rej  = (SELECT COUNT(*) FROM meta.rejected_record WHERE batch_id = @batch1);
SET @b1_ins  = (SELECT COUNT(*) FROM warehouse.dim_customer WHERE dw_batch_id = @batch1)
             + (SELECT COUNT(*) FROM warehouse.dim_product  WHERE dw_batch_id = @batch1)
             + (SELECT COUNT(*) FROM warehouse.dim_store    WHERE dw_batch_id = @batch1)
             + (SELECT COUNT(*) FROM warehouse.fact_sales   WHERE dw_batch_id = @batch1);

CALL meta.sp_batch_end(@batch1, 'SUCCESS', @b1_read, @b1_ins, 0, @b1_rej);


-- =============================================================================
-- SECTION 9 — BATCH 2: THE NEXT EXTRACT, WITH CHANGES
-- =============================================================================
-- SIMULATION NOTE: a real pipeline would TRUNCATE staging and reload it from
-- the source (architecture.md §2.1). Here the next day's extract is simulated by
-- UPDATE-ing staging in place, which produces the same input state with far less
-- machinery. The rows below are what the source system would have sent had these
-- business events occurred overnight.
--
-- Four changes, each chosen to exercise a different code path:
--   ~200 customers change loyalty tier   -> SCD2 versioning
--    ~50 customers change city           -> SCD2 versioning on a second attribute
--   ~100 products get a 12% cost rise    -> SCD2, and the margin-history argument
--      1 store is renamed                -> SCD1 overwrite, history destroyed
-- =============================================================================

CALL meta.sp_batch_start('batch_2_incremental_load', 'SALES_OLTP', @batch2);
SELECT CONCAT('BATCH 2 started, id = ', @batch2) AS progress;

-- ~200 customers (every 5th) move up a tier; Platinum steps back down so that
-- the selected set ALWAYS produces a detectable change. A no-op update would
-- leave the change set empty and the SCD2 path untested.
UPDATE staging.stg_customer
SET    loyalty_tier = CASE loyalty_tier
                          WHEN 'Bronze'   THEN 'Silver'
                          WHEN 'Silver'   THEN 'Gold'
                          WHEN 'Gold'     THEN 'Platinum'
                          WHEN 'Platinum' THEN 'Gold'
                          ELSE loyalty_tier
                      END,
       _load_batch_id = @batch2
WHERE  customer_id REGEXP '^C-[0-9]+$'
  AND  CAST(SUBSTRING(customer_id, 3) AS UNSIGNED) % 5 = 0;

-- ~50 customers (every 20th) relocate. Deliberately overlapping the tier cohort:
-- a customer who changes two tracked attributes at once must still produce
-- exactly ONE new version, not two.
UPDATE staging.stg_customer
SET    city_name      = 'Austin',
       state_province = 'TX',
       country_name   = 'United States',
       region_name    = 'South',
       _load_batch_id = @batch2
WHERE  customer_id REGEXP '^C-[0-9]+$'
  AND  CAST(SUBSTRING(customer_id, 3) AS UNSIGNED) % 20 = 0;

-- ~100 products (every 5th) take a 12% cost and price rise. The numeric guard
-- excludes the 'N/A' rows from defect #4 — arithmetic on those would produce 0
-- and quietly turn a rejected row into a loadable one.
UPDATE staging.stg_product
SET    unit_cost  = CAST(ROUND(CAST(unit_cost  AS DECIMAL(12,2)) * 1.12, 2) AS CHAR),
       list_price = CAST(ROUND(CAST(list_price AS DECIMAL(12,2)) * 1.12, 2) AS CHAR),
       _load_batch_id = @batch2
WHERE  product_id REGEXP '^P-[0-9]+$'
  AND  CAST(SUBSTRING(product_id, 3) AS UNSIGNED) % 5 = 0
  AND  unit_cost  REGEXP '^[0-9]+([.][0-9]+)?$'
  AND  list_price REGEXP '^[0-9]+([.][0-9]+)?$';

-- One store renamed. S-009 is Austin, echoing the worked example in
-- dimensional_modeling.md §5. After this runs, every historical report shows the
-- new name — including reports about periods before the rename. That is Type 1,
-- and it is why the choice between Type 1 and Type 2 is a design decision rather
-- than a preference.
UPDATE staging.stg_store
SET    store_name     = 'Austin Downtown',
       _load_batch_id = @batch2
WHERE  store_id = 'S-009';

-- Re-validate the mutated staging, then re-run every loader.
CALL meta.sp_stage_validate(@batch2);

CALL warehouse.sp_load_dim_store(@batch2);
CALL warehouse.sp_load_dim_customer(@batch2, @scd_change_date, @scd_change_date);
CALL warehouse.sp_load_dim_product (@batch2, @scd_change_date, @scd_change_date);

-- Facts from the change date forward. These resolve to the NEW dimension
-- versions, because the as-of join finds the row whose validity window contains
-- their order date.
CALL warehouse.sp_load_fact_sales(@batch2, @scd_change_date, DATE '9999-12-31');

-- The inventory snapshot covers 2025-12-12..2025-12-31, which sits after the
-- change date, so it values stock against the v2 product costs.
CALL warehouse.sp_load_fact_inventory(@batch2);

SET @b2_read = (SELECT COUNT(*) FROM staging.stg_customer)
             + (SELECT COUNT(*) FROM staging.stg_product)
             + (SELECT COUNT(*) FROM staging.stg_store)
             + (SELECT COUNT(*) FROM staging.stg_sales_order_line)
             + (SELECT COUNT(*) FROM staging.stg_inventory);
SET @b2_rej  = (SELECT COUNT(*) FROM meta.rejected_record WHERE batch_id = @batch2);
SET @b2_ins  = (SELECT COUNT(*) FROM warehouse.fact_sales              WHERE dw_batch_id = @batch2)
             + (SELECT COUNT(*) FROM warehouse.fact_inventory_snapshot WHERE dw_batch_id = @batch2)
             + (SELECT COUNT(*) FROM warehouse.dim_customer            WHERE dw_batch_id = @batch2)
             + (SELECT COUNT(*) FROM warehouse.dim_product             WHERE dw_batch_id = @batch2);

CALL meta.sp_batch_end(@batch2, 'SUCCESS', @b2_read, @b2_ins, 0, @b2_rej);


-- =============================================================================
-- SECTION 10 — BATCH 3: THE IDEMPOTENCY PROOF
-- =============================================================================
-- Replays batch 2's fact load with staging untouched — the shape of a real
-- recovery, where a batch failed after loading some rows and is simply re-run.
--
-- The row counts captured either side must be IDENTICAL. If they are not,
-- uq_fact_sales_natural or the ON DUPLICATE KEY UPDATE is not doing its job, and
-- every re-run would be inflating revenue.
-- =============================================================================

SET @fact_rows_before = (SELECT COUNT(*) FROM warehouse.fact_sales);
SET @inv_rows_before  = (SELECT COUNT(*) FROM warehouse.fact_inventory_snapshot);

CALL meta.sp_batch_start('batch_3_idempotency_replay', 'SALES_OLTP', @batch3);
SELECT CONCAT('BATCH 3 started, id = ', @batch3) AS progress;

CALL warehouse.sp_load_fact_sales(@batch3, @scd_change_date, DATE '9999-12-31');
CALL warehouse.sp_load_fact_inventory(@batch3);

SET @fact_rows_after = (SELECT COUNT(*) FROM warehouse.fact_sales);
SET @inv_rows_after  = (SELECT COUNT(*) FROM warehouse.fact_inventory_snapshot);

-- The delta passed as rows_updated is a NET row-count change, not a count of
-- rows the UPDATE clause actually touched — every row this batch re-affirms
-- shows up here as 0 whether or not ON DUPLICATE KEY UPDATE fired on it. For
-- this batch specifically that is the right number to see (net 0 IS the
-- idempotency claim), but it should not be read as "no row was touched"; it
-- means "the touching, if any, changed nothing observable". §11 query 5
-- repeats this same before/after comparison as the authoritative proof.
CALL meta.sp_batch_end(@batch3, 'SUCCESS', 0, 0,
                       @fact_rows_after - @fact_rows_before, 0);


-- =============================================================================
-- SECTION 11 — VERIFICATION
-- =============================================================================

-- 1. Warehouse volumes.
--    Expected: dim_customer ~1,200 (1,000 base + ~200 v2)
--              dim_product  ~600   (500 base + ~100 v2)
--              dim_store    27     (25 + 2 reserved)
--              fact_sales   ~51,000 minus rejects minus the 25 absorbed dupes
--              fact_inventory_snapshot ~40,000 minus ~20 negative-stock rejects
SELECT 'dim_customer'            AS table_name, COUNT(*) AS rows_total FROM warehouse.dim_customer
UNION ALL SELECT 'dim_product',              COUNT(*) FROM warehouse.dim_product
UNION ALL SELECT 'dim_store',                COUNT(*) FROM warehouse.dim_store
UNION ALL SELECT 'dim_date',                 COUNT(*) FROM warehouse.dim_date
UNION ALL SELECT 'dim_promotion',            COUNT(*) FROM warehouse.dim_promotion
UNION ALL SELECT 'dim_channel',              COUNT(*) FROM warehouse.dim_channel
UNION ALL SELECT 'dim_order_junk',           COUNT(*) FROM warehouse.dim_order_junk
UNION ALL SELECT 'fact_sales',               COUNT(*) FROM warehouse.fact_sales
UNION ALL SELECT 'fact_inventory_snapshot',  COUNT(*) FROM warehouse.fact_inventory_snapshot;

-- 2. SCD TYPE 2 WORKED. Expect ~200 customers at version 2 and ~100 products.
--    A result of zero means the change set was empty and the SCD2 path never ran.
SELECT 'dim_customer' AS dimension, version_number, COUNT(*) AS rows_at_version
FROM   warehouse.dim_customer WHERE customer_key > 0 GROUP BY version_number
UNION ALL
SELECT 'dim_product', version_number, COUNT(*)
FROM   warehouse.dim_product WHERE product_key > 0 GROUP BY version_number
ORDER  BY dimension, version_number;

-- 3. NO OVERLAPPING VALIDITY WINDOWS, and exactly one current row per natural
--    key. Both must return 0. A non-zero first column means the as-of join can
--    match two rows and silently double the facts for those customers.
SELECT
    (SELECT COUNT(*) FROM warehouse.dim_customer a
      JOIN warehouse.dim_customer b
        ON  b.customer_id = a.customer_id
       AND b.customer_key <> a.customer_key
       AND b.valid_from  <= a.valid_to
       AND b.valid_to    >= a.valid_from
     WHERE a.customer_key > 0)                                AS customer_overlaps,
    (SELECT COUNT(*) FROM (
        SELECT customer_id FROM warehouse.dim_customer
        WHERE is_current = 1 AND customer_key > 0
        GROUP BY customer_id HAVING COUNT(*) > 1) x)          AS customers_multi_current,
    (SELECT COUNT(*) FROM (
        SELECT product_id FROM warehouse.dim_product
        WHERE is_current = 1 AND product_key > 0
        GROUP BY product_id HAVING COUNT(*) > 1) y)           AS products_multi_current;

-- 4. THE POINT-IN-TIME PROOF.
--    One customer, two versions, and fact rows attached to whichever version was
--    current on their own order date. The tier shown against pre-July orders MUST
--    be the old tier — that is the entire justification for SCD Type 2.
SELECT c.customer_id, c.customer_key, c.loyalty_tier, c.city,
       c.valid_from, c.valid_to, c.is_current, c.version_number,
       COUNT(f.sales_key)              AS fact_rows,
       MIN(d.full_date)                AS first_order,
       MAX(d.full_date)                AS last_order
FROM   warehouse.dim_customer c
LEFT   JOIN warehouse.fact_sales f ON f.customer_key = c.customer_key
LEFT   JOIN warehouse.dim_date   d ON d.date_key     = f.order_date_key
WHERE  c.customer_id = (
           SELECT customer_id FROM warehouse.dim_customer
           WHERE customer_key > 0 GROUP BY customer_id
           HAVING COUNT(*) > 1 ORDER BY customer_id LIMIT 1)
GROUP  BY c.customer_id, c.customer_key, c.loyalty_tier, c.city,
          c.valid_from, c.valid_to, c.is_current, c.version_number
ORDER  BY c.version_number;

-- 5. IDEMPOTENCY. Both deltas must be exactly 0.
SELECT @fact_rows_before AS fact_before, @fact_rows_after AS fact_after,
       @fact_rows_after - @fact_rows_before AS fact_delta,
       @inv_rows_before  AS inv_before,  @inv_rows_after  AS inv_after,
       @inv_rows_after  - @inv_rows_before  AS inv_delta;

-- 6. RESERVED MEMBER USAGE — the data quality metric that reserved members buy.
--    unknown_product should be ~200 and unknown_customer ~50 (defects #9, #10).
--    na_promotion should be large: most sales are not promoted, and that is
--    NOT-APPLICABLE, not unknown. na_ship_date is the unshipped orders.
SELECT COUNT(*)                                        AS fact_rows,
       SUM(product_key   = -1)                         AS unknown_product,
       SUM(customer_key  = -1)                         AS unknown_customer,
       SUM(store_key     = -1)                         AS unknown_store,
       SUM(promotion_key = -1)                         AS unknown_promotion,
       SUM(promotion_key =  0)                         AS na_promotion,
       SUM(ship_date_key =  0)                         AS na_ship_date,
       SUM(order_date_key = -1)                        AS unknown_order_date
FROM   warehouse.fact_sales;

-- 7. DATA QUALITY RESULTS, per batch. is_passed is generated, so a rule cannot
--    claim success while reporting failures.
SELECT batch_id, rule_name, rule_type, severity,
       records_checked, records_failed, is_passed
FROM   meta.data_quality_result
ORDER  BY batch_id, rule_name;

-- 8. QUARANTINE. Every row here is one the warehouse refused, with the payload
--    needed to correct and replay it.
SELECT batch_id, source_object, reject_rule, COUNT(*) AS rows_rejected
FROM   meta.rejected_record
GROUP  BY batch_id, source_object, reject_rule
ORDER  BY batch_id, source_object, reject_rule;

-- 9. BATCH RECONCILIATION. rows_read should account for inserted + rejected;
--    a gap means rows vanished between extract and load.
SELECT batch_id, batch_name, batch_status,
       rows_read, rows_inserted, rows_updated, rows_rejected,
       TIMESTAMPDIFF(SECOND, started_at, ended_at) AS elapsed_seconds
FROM   meta.etl_batch
ORDER  BY batch_id;

-- 10. THE SEMI-ADDITIVE TRAP, demonstrated on real loaded data.
--     wrong_sum triple-counts the same physical units across 20 days.
--     correct_avg and correct_closing are the two defensible answers.
--     If wrong_sum is not far larger than the others, the snapshot did not load.
SELECT p.product_id, s.store_id,
       SUM(i.quantity_on_hand)                       AS wrong_sum_across_dates,
       ROUND(AVG(i.quantity_on_hand), 1)             AS correct_avg,
       SUBSTRING_INDEX(
           GROUP_CONCAT(i.quantity_on_hand
                        ORDER BY i.snapshot_date_key DESC), ',', 1)
                                                     AS correct_closing_balance,
       SUM(i.is_stockout)                            AS stockout_days
FROM   warehouse.fact_inventory_snapshot i
JOIN   warehouse.dim_product p ON p.product_key = i.product_key
JOIN   warehouse.dim_store   s ON s.store_key   = i.store_key
GROUP  BY p.product_id, s.store_id
ORDER  BY stockout_days DESC, wrong_sum_across_dates DESC
LIMIT  10;

-- =============================================================================
-- END OF etl_simulation.sql
--
-- Next: indexes.sql — the post-load analytical indexing strategy. Built AFTER
-- the bulk load, per architecture.md §8, because maintaining a B-tree during a
-- 50,000-row insert costs more than building it once at the end.
-- =============================================================================
