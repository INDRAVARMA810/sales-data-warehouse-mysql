-- =============================================================================
-- indexes.sql — Mini Sales Data Warehouse
-- =============================================================================
--
--   Target platform : MySQL 8.0.16+
--   Prerequisites   : schema.sql, sample_data.sql, etl_simulation.sql
--   Run with:
--       mysql --user=root --password --show-warnings < indexes.sql
--
--   Contains one stored procedure (§4), so — like etl_simulation.sql — this
--   file uses DELIMITER, a `mysql` CLIENT command, not server syntax. Run it
--   through the CLI, not through a driver that submits statements directly.
--
-- -----------------------------------------------------------------------------
-- WHY THIS RUNS AFTER THE LOAD, NOT BEFORE
-- -----------------------------------------------------------------------------
-- architecture.md §8: "Indexes are built after the bulk load, not before,
-- because maintaining a B-tree during a 50,000-row insert is slower than
-- building it once at the end." Every INSERT into an indexed table pays to keep
-- every one of its indexes correct; building the index once, after the data is
-- final, does the same work exactly once instead of ~50,000 times.
--
-- -----------------------------------------------------------------------------
-- WHAT ALREADY EXISTS — the point of §1 below
-- -----------------------------------------------------------------------------
-- schema.sql already created a substantial amount of indexing as a side effect
-- of ordinary constraint declarations:
--   * Every PRIMARY KEY  (the InnoDB clustered index)
--   * Every UNIQUE constraint (dim_customer/dim_product current+version,
--     dim_store/dim_promotion/dim_channel/dim_order_junk natural keys,
--     fact_sales's idempotency key)
--   * Every FOREIGN KEY column — InnoDB REQUIRES a supporting index on the
--     referencing column of every FK and creates one automatically if none
--     exists. This alone covers a single-column index on every FK in both fact
--     tables: order_date_key, ship_date_key, customer_key, product_key,
--     store_key, promotion_key, channel_key, order_junk_key on fact_sales;
--     product_key and store_key on fact_inventory_snapshot (its PK already
--     leads with snapshot_date_key, so that FK needed no separate index).
--   * Two deliberate as-of lookup indexes: ix_dim_customer_asof,
--     ix_dim_product_asof — declared in schema.sql itself because
--     etl_simulation.sql's as-of join runs once per incoming row; without them
--     the fact load is a nested full scan of the dimension.
--   * Three meta control-plane indexes for operational queries.
--
-- The job of THIS file is narrower than it might look: find the handful of
-- ANALYTICAL access patterns those existing indexes do not yet serve well, add
-- exactly those, and then clean up anything the new additions make redundant.
-- Adding an index nobody asked for is not free — it is write cost and storage
-- paid against a benefit nobody collects.
-- =============================================================================


-- =============================================================================
-- SECTION 1 — BASELINE: WHAT INDEXING ALREADY EXISTS
-- =============================================================================
-- Run this first and actually read the output. Every index created below is
-- justified against what THIS query shows is missing — not against a generic
-- checklist. If a query pattern is already served by something in this list,
-- adding another index for it would be pure waste.
-- =============================================================================

SELECT table_name, index_name,
       GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ', ') AS columns,
       MAX(non_unique)  AS non_unique,      -- 0 = enforces uniqueness
       MAX(index_type)  AS index_type
FROM   information_schema.statistics
WHERE  table_schema = 'warehouse'
GROUP  BY table_name, index_name
ORDER  BY table_name, index_name;

-- Snapshot the count so §8's verification can report exactly how many indexes
-- this file added, not just what the final total happens to be.
SET @baseline_index_count = (
    SELECT COUNT(DISTINCT CONCAT(table_name, '.', index_name))
    FROM   information_schema.statistics
    WHERE  table_schema = 'warehouse'
);


-- =============================================================================
-- SECTION 2 — THE CASE FOR *NOT* INDEXING THE SMALL TABLES
-- =============================================================================
-- This resolves a note left open in schema.sql. dim_date's column comment
-- there reads: "No index on year/month columns here; those belong in
-- indexes.sql. At ~4,000 rows MySQL will scan this table faster than it can
-- descend a secondary index." That sentence names two conclusions in one
-- breath — "add it later" and "you won't need it" — and this section is where
-- that gets settled, explicitly, rather than left as a loose thread.
--
-- THE VERDICT: no additional index on dim_date. A full table scan of ~4,018
-- rows is a handful of sequential page reads; a secondary B-tree lookup is
-- several random page reads PLUS a return trip to the clustered row for any
-- column not in the index. Below a few tens of thousands of rows, the scan
-- usually wins outright, and it always wins on simplicity.
--
-- THE SAME VERDICT APPLIES, for the same reason, to every other small table in
-- this warehouse:
--
--     Table               Rows         Verdict
--     dim_date            ~4,018       scan
--     dim_customer        ~1,200       scan (as-of index already covers the
--                                       one lookup that actually runs per-row
--                                       during ETL — see schema.sql)
--     dim_product          ~600        scan (same reasoning)
--     dim_order_junk        120        scan
--     dim_promotion          30        scan
--     dim_store              27        scan
--     dim_channel              5       scan
--     meta.etl_batch        low tens   already has ix_etl_batch_status_started
--                                       for the one operational query that
--                                       matters; nothing analytical runs here
--
-- The general principle, stated once so it does not need repeating per table:
-- INDEX THE FACT TABLES FOR THEIR ANALYTICAL ACCESS PATTERNS. Dimensions are
-- small enough that a scan is already fast, and dimensions are exactly where
-- star_schema.md §6 says to spend width freely, not where to spend index
-- maintenance cost. Confirm this any time the data volume assumptions change —
-- it is a size-dependent conclusion, not a permanent one.
-- =============================================================================


-- =============================================================================
-- SECTION 3 — FACT_SALES: ANALYTICAL COMPOSITE INDEXES
-- =============================================================================
-- Five indexes. Each is tied to a specific, named analytical need already
-- documented elsewhere in this repository — none of these are speculative
-- additions, except where explicitly flagged as such.
--
-- Every statement below states ALGORITHM=INPLACE and LOCK=NONE explicitly,
-- rather than relying on MySQL's defaults. Adding a plain secondary index is an
-- online InnoDB operation by default — it does not rewrite the table and does
-- not block concurrent reads or writes — but stating the contract explicitly
-- means the statement FAILS LOUDLY if that ever stops being true (a MySQL
-- version, storage engine, or column type change that forces a table rebuild),
-- rather than silently taking a table lock nobody was expecting. The same
-- discipline as declaring ON DELETE/ON UPDATE explicitly on every FK in
-- schema.sql rather than leaning on defaults.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ix_fact_sales_date_product (order_date_key, product_key)
-- -----------------------------------------------------------------------------
-- SERVES: product performance trended over time — "revenue by product by
-- month" — and doubles as the fact_sales side of the drill-across query in
-- star_schema.md §2, which joins fact_sales to fact_inventory_snapshot on
-- product_key AND date. Leading with order_date_key matches the observation in
-- architecture.md §5.4 that analytical queries are "nearly always
-- date-bounded" — a date-range predicate can use this index directly, and
-- product_key narrows the same scan without a second lookup.
-- -----------------------------------------------------------------------------
-- NOTE ON CLAUSE ORDER: index_option (COMMENT here) must precede the
-- algorithm_option/lock_option pair in MySQL's CREATE INDEX grammar. Every
-- statement below follows that order deliberately, not just this one.
CREATE INDEX ix_fact_sales_date_product
    ON warehouse.fact_sales (order_date_key, product_key)
    COMMENT 'Product trend over time; anchors the drill-across with fact_inventory_snapshot'
    ALGORITHM=INPLACE LOCK=NONE;

-- -----------------------------------------------------------------------------
-- ix_fact_sales_date_store (order_date_key, store_key)
-- -----------------------------------------------------------------------------
-- SERVES: store performance trended over time — "revenue by store by month",
-- the store-level counterpart to the product index above. Kept as a SEPARATE
-- composite rather than folded into one 3-column (date, product, store) index:
-- a single 3-column index only serves store_key efficiently when product_key
-- is ALSO in the predicate (leftmost-prefix rule), which most store-trend
-- queries do not need.
-- -----------------------------------------------------------------------------
CREATE INDEX ix_fact_sales_date_store
    ON warehouse.fact_sales (order_date_key, store_key)
    COMMENT 'Store performance trend over time'
    ALGORITHM=INPLACE LOCK=NONE;

-- -----------------------------------------------------------------------------
-- ix_fact_sales_promotion_date (promotion_key, order_date_key)
-- -----------------------------------------------------------------------------
-- SERVES: promotional lift analysis — dim_promotion is explicitly the CAUSAL
-- dimension (dimensional_modeling.md §7 pattern table), and "was this
-- promotion's lift sustained over its whole window, or front-loaded" is exactly
-- the kind of question that needs promotion_key leading so every sale under one
-- campaign clusters together, with order_date_key giving a cheap in-index sort
-- for the trend. promotion_key is low-cardinality (32 values including the two
-- reserved members), which is exactly when a leading equality column pays off
-- most: it isolates a small slice of a 50,000-row table immediately.
-- -----------------------------------------------------------------------------
CREATE INDEX ix_fact_sales_promotion_date
    ON warehouse.fact_sales (promotion_key, order_date_key)
    COMMENT 'Promotional lift trended over time'
    ALGORITHM=INPLACE LOCK=NONE;

-- -----------------------------------------------------------------------------
-- ix_fact_sales_channel_date_covering (channel_key, order_date_key, net_sales_amount)
-- -----------------------------------------------------------------------------
-- SERVES: "revenue by channel by month" — schema.sql's dim_channel header calls
-- this out by name: "'revenue by channel' is a headline report, not an
-- incidental filter." A headline report run often enough is worth optimizing
-- past "index the join columns" and into "answer entirely from the index".
--
-- THIS IS A COVERING INDEX, the only one in this file, and it is deliberately
-- the only one: including net_sales_amount means
--     SELECT channel_key, order_date_key, SUM(net_sales_amount)
--     FROM fact_sales WHERE order_date_key BETWEEN ... GROUP BY channel_key, ...
-- never touches the clustered row at all — MySQL calls this an INDEX-ONLY scan,
-- visible in EXPLAIN as "Using index" (§8 has a runnable EXPLAIN to check).
--
-- THIS ALSO HONORS A CLAIM MADE BACK IN schema.sql: the derived measures were
-- declared STORED rather than VIRTUAL specifically because "only STORED columns
-- can be indexed — which indexes.sql will need." Indexing net_sales_amount here
-- is that need actually arriving. A VIRTUAL column could not have been placed
-- in this index at all.
--
-- THE COST, stated as plainly as the benefit: this index carries a copy of
-- net_sales_amount for every one of ~50,000 rows, and every fact insert now
-- maintains a THREE-column B-tree instead of a plain lookup index. That cost is
-- paid ONLY here, for ONE report explicitly named as a headline in the approved
-- design — not applied by default to every index in this file.
-- -----------------------------------------------------------------------------
CREATE INDEX ix_fact_sales_channel_date_covering
    ON warehouse.fact_sales (channel_key, order_date_key, net_sales_amount)
    COMMENT 'COVERING index for the channel revenue headline report - index-only scan'
    ALGORITHM=INPLACE LOCK=NONE;

-- -----------------------------------------------------------------------------
-- ix_fact_sales_customer_date (customer_key, order_date_key)
-- -----------------------------------------------------------------------------
-- SERVES: customer purchase history / recency analysis — "when did this
-- customer last buy, and how often" (an RFM-style query).
--
-- FLAGGED AS THE MOST SPECULATIVE OF THE FIVE. Nothing in the approved design
-- documents names customer-level trend analysis as a headline report the way
-- they do for channel (schema.sql) or promotion (dimensional_modeling.md §7).
-- It is a plausible, common retail question — but "plausible" is a weaker
-- justification than the other four indexes have, and that gap is exactly what
-- §6 exists to test before committing to it long-term.
-- -----------------------------------------------------------------------------
CREATE INDEX ix_fact_sales_customer_date
    ON warehouse.fact_sales (customer_key, order_date_key)
    COMMENT 'Customer purchase history / RFM-style analysis - see Section 6 for validation'
    ALGORITHM=INPLACE LOCK=NONE;


-- =============================================================================
-- SECTION 4 — FACT_INVENTORY_SNAPSHOT: THE COMPLEMENTARY COMPOSITE
-- =============================================================================
-- ix_fact_inventory_product_store_date (product_key, store_key, snapshot_date_key)
--
-- schema.sql's fact_inventory_snapshot header explains, at length, WHY the
-- primary key leads with snapshot_date_key: InnoDB clusters the table on its
-- PK, so a date-leading PK means one day's snapshot sits in contiguous pages,
-- and "last 30 days" — the dominant access pattern — reads sequentially.
--
-- That same header names the access pattern the date-leading PK does NOT serve
-- well: "per-product stock across stores" and the drill-across query in
-- star_schema.md §2, both of which look up by product_key (and often store_key)
-- FIRST and only secondarily care about the date. Against a date-leading PK,
-- that lookup has no useful prefix to start from and degrades toward a scan.
--
-- This index is the other half of that PK's own stated tradeoff, not a
-- duplicate of it: same three columns, reordered so PRODUCT-FIRST lookups get
-- an equally direct path. Two orderings of the same three columns, each serving
-- the access pattern the other cannot.
-- =============================================================================

CREATE INDEX ix_fact_inventory_product_store_date
    ON warehouse.fact_inventory_snapshot (product_key, store_key, snapshot_date_key)
    COMMENT 'Per-product stock across stores/time; complements the date-leading PK'
    ALGORITHM=INPLACE LOCK=NONE;


-- =============================================================================
-- SECTION 5 — REDUNDANCY CLEANUP
-- =============================================================================
-- Four of the five fact_sales indexes above LEAD with a column that already
-- had its own single-column index — the one InnoDB auto-created in schema.sql
-- to support that column's foreign key (see the file header). Once a composite
-- index puts that same column in the LEFTMOST position, the single-column
-- index becomes redundant: MySQL's leftmost-prefix rule means any query the
-- short index could serve, the long one can serve identically, via the same
-- prefix. Concretely, after §3 and §4:
--
--     Column          Made redundant by                    Table
--     order_date_key  ix_fact_sales_date_product (or _store) fact_sales
--     promotion_key   ix_fact_sales_promotion_date            fact_sales
--     channel_key     ix_fact_sales_channel_date_covering     fact_sales
--     customer_key    ix_fact_sales_customer_date             fact_sales
--     product_key     ix_fact_inventory_product_store_date    fact_inventory_snapshot
--
-- (product_key and store_key on fact_sales are UNAFFECTED: both new fact_sales
-- composites put them in the SECOND position, not the first, so their own
-- single-column indexes remain the only thing that serves a product-alone or
-- store-alone lookup.)
--
-- Leaving the redundant ones in place would be the exact anti-pattern
-- schema.sql warned against when explaining why this file declares no
-- additional single-key indexes in the first place: "exact duplicates,
-- consuming space and slowing writes for no benefit." Having created that
-- duplication as a side effect of §3/§4, cleaning it up is this section's job.
--
-- THE SAFETY NET: dropping any of these is only correct because a superseding
-- composite already exists to keep supporting that column's foreign key: if it
-- did not, InnoDB itself would refuse the DROP with
-- "ERROR 1553: Cannot drop index needed in a foreign key constraint" rather
-- than silently leaving a FK unsupported. The procedure below checks for a
-- superseding composite before attempting anything — but even if that check
-- were wrong, this backstop means the failure mode is a loud error, never a
-- silent loss of referential integrity.
--
-- IDENTIFIED DYNAMICALLY, NOT BY HARDCODED NAME. InnoDB's auto-generated index
-- name is an implementation detail (typically the constraint's own name, but
-- not contractually guaranteed), so this looks the actual name up in
-- information_schema rather than guessing it.
-- =============================================================================

DROP PROCEDURE IF EXISTS meta.sp_drop_redundant_index;

DELIMITER $$

CREATE PROCEDURE meta.sp_drop_redundant_index(
    IN p_schema VARCHAR(64),
    IN p_table  VARCHAR(64),
    IN p_column VARCHAR(64)
)
BEGIN
    DECLARE v_redundant_index    VARCHAR(64) DEFAULT NULL;
    DECLARE v_superseding_exists INT         DEFAULT 0;

    -- Find a SINGLE-COLUMN index whose one and only column is p_column.
    SELECT s.index_name INTO v_redundant_index
    FROM   information_schema.statistics s
    WHERE  s.table_schema = p_schema
      AND  s.table_name   = p_table
      AND  s.seq_in_index  = 1
      AND  s.column_name   = p_column
      AND  s.index_name IN (
              SELECT index_name FROM information_schema.statistics
              WHERE  table_schema = p_schema AND table_name = p_table
              GROUP  BY index_name
              HAVING COUNT(*) = 1
          )
    LIMIT 1;

    -- Confirm a DIFFERENT, longer index exists with p_column as ITS first
    -- column too — the composite that makes the one above redundant.
    IF v_redundant_index IS NOT NULL THEN
        SELECT COUNT(*) INTO v_superseding_exists
        FROM   information_schema.statistics s
        WHERE  s.table_schema = p_schema
          AND  s.table_name   = p_table
          AND  s.seq_in_index  = 1
          AND  s.column_name   = p_column
          AND  s.index_name   <> v_redundant_index
          AND  s.index_name IN (
                  SELECT index_name FROM information_schema.statistics
                  WHERE  table_schema = p_schema AND table_name = p_table
                  GROUP  BY index_name
                  HAVING COUNT(*) > 1
              );
    END IF;

    IF v_redundant_index IS NOT NULL AND v_superseding_exists > 0 THEN
        -- NOTE: unlike CREATE INDEX (see §3), ALTER TABLE's grammar treats
        -- DROP INDEX, ALGORITHM=, and LOCK= as comma-separated peers in the
        -- same alter_option list, not trailing clauses. Commas are required
        -- here even though they were wrong for CREATE INDEX above.
        SET @sql = CONCAT('ALTER TABLE ', p_schema, '.', p_table,
                           ' DROP INDEX `', v_redundant_index, '`',
                           ', ALGORITHM=INPLACE, LOCK=NONE');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        SELECT CONCAT('DROPPED redundant index `', v_redundant_index, '` on ',
                       p_schema, '.', p_table,
                       ' (superseded by a composite leading with `', p_column, '`)')
               AS action_taken;
    ELSE
        SELECT CONCAT('No redundant single-column index found for ',
                       p_schema, '.', p_table, '.', p_column, ' - nothing dropped')
               AS action_taken;
    END IF;
END$$

DELIMITER ;

CALL meta.sp_drop_redundant_index('warehouse', 'fact_sales', 'order_date_key');
CALL meta.sp_drop_redundant_index('warehouse', 'fact_sales', 'promotion_key');
CALL meta.sp_drop_redundant_index('warehouse', 'fact_sales', 'channel_key');
CALL meta.sp_drop_redundant_index('warehouse', 'fact_sales', 'customer_key');
CALL meta.sp_drop_redundant_index('warehouse', 'fact_inventory_snapshot', 'product_key');


-- =============================================================================
-- SECTION 6 — THE INVISIBLE INDEX TECHNIQUE (MySQL 8 only)
-- =============================================================================
-- ix_fact_sales_date_store — flagged in §3 as the more speculative of the two
-- date-leading composites, since ix_fact_sales_date_product already covers
-- order_date_key alone and doubles as the drill-across anchor. This is where a
-- real DBA would pause before committing to a second one.
--
-- MySQL 8 lets an index be marked INVISIBLE: still fully maintained on every
-- write, still enforcing any constraint it happens to back, but INVISIBLE TO
-- THE QUERY OPTIMIZER, which will never choose it for a plan. That makes it a
-- reversible experiment instead of a bet — run the real workload, watch
-- EXPLAIN / the slow query log / performance_schema for the queries this index
-- was meant to help, and only DROP it for good once it's proven to not matter.
-- Since invisibility never affects constraint enforcement, this is safe to
-- rehearse even on an index that happens to be load-bearing for a foreign key
-- (this one is not: ix_fact_sales_date_product already backs order_date_key's
-- FK on its own).
--
-- The toggle below is a REHEARSAL within this script — flip invisible, prove
-- the mechanism, flip back — so the warehouse is left with the index active,
-- exactly as §3 concluded it should be. In a real production run, the pause
-- between the two ALTER statements would be however long it takes to watch a
-- real day's query traffic, not the instant it takes here.
-- =============================================================================

ALTER TABLE warehouse.fact_sales
    ALTER INDEX ix_fact_sales_date_store INVISIBLE;

-- Confirm the flip. IS_VISIBLE is a MySQL-8-specific column on this view.
SELECT table_name, index_name, is_visible
FROM   information_schema.statistics
WHERE  table_schema = 'warehouse'
  AND  table_name    = 'fact_sales'
  AND  index_name    = 'ix_fact_sales_date_store';

-- In production this is where you would run the representative workload and
-- watch it, not immediately re-enable. Flipping back here so this script
-- leaves the warehouse in the state §3 justified.
ALTER TABLE warehouse.fact_sales
    ALTER INDEX ix_fact_sales_date_store VISIBLE;


-- =============================================================================
-- SECTION 7 — REFRESH OPTIMIZER STATISTICS
-- =============================================================================
-- InnoDB's cost-based optimizer decides which index to use — or whether to
-- ignore all of them and scan — based on PERSISTENT STATISTICS: a sampled
-- estimate of table cardinality and index selectivity, stored in
-- mysql.innodb_table_stats / innodb_index_stats (innodb_stats_persistent is
-- MySQL 8's default). Those statistics were sampled, if at all, against
-- whatever the tables looked like when they were smaller or differently
-- shaped. Two events in this pipeline have invalidated them since:
--   1. etl_simulation.sql's bulk load changed every table's row count and
--      value distribution.
--   2. Sections 3-6 above changed the SET of indexes available to choose from.
-- Skipping this step does not produce an error — it produces a warehouse that
-- runs, but where EXPLAIN and real query plans are being decided against
-- stale information, which is a much harder problem to notice than a failed
-- statement.
-- =============================================================================

ANALYZE TABLE
    warehouse.dim_date,
    warehouse.dim_customer,
    warehouse.dim_product,
    warehouse.dim_store,
    warehouse.dim_promotion,
    warehouse.dim_channel,
    warehouse.dim_order_junk,
    warehouse.fact_sales,
    warehouse.fact_inventory_snapshot;


-- =============================================================================
-- SECTION 8 — VERIFICATION
-- =============================================================================

-- 1. Final index inventory, same shape as the §1 baseline — read this and
--    compare by eye against what §1 printed.
SELECT table_name, index_name,
       GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ', ') AS columns,
       MAX(non_unique) AS non_unique
FROM   information_schema.statistics
WHERE  table_schema = 'warehouse'
GROUP  BY table_name, index_name
ORDER  BY table_name, index_name;

-- 2. The net change. Expect exactly +1: six indexes ADDED in §3/§4, five
--    redundant single-column indexes REMOVED in §5 (six minus five is one).
--    Anything else means either an index failed to create, or the redundancy
--    cleanup found something unexpected — read query 1's output to see which.
SELECT
    @baseline_index_count                                          AS before_count,
    (SELECT COUNT(DISTINCT CONCAT(table_name, '.', index_name))
     FROM   information_schema.statistics
     WHERE  table_schema = 'warehouse')                            AS after_count,
    (SELECT COUNT(DISTINCT CONCAT(table_name, '.', index_name))
     FROM   information_schema.statistics
     WHERE  table_schema = 'warehouse') - @baseline_index_count     AS net_change;

-- 3. REDUNDANCY CHECK. For every pair of indexes on the same table where one's
--    full column list is a strict PREFIX of the other's, this returns a row.
--    Expect ZERO ROWS — if this returns anything, an index added above (or one
--    already in schema.sql) is wasted space and wasted write cost.
--    CAVEAT this query cannot itself judge: a UNIQUE index that happens to be a
--    prefix of a longer non-unique index is NOT redundant — the UNIQUE index
--    enforces a constraint the longer one does not. None of the pairs in this
--    warehouse fall into that shape today, but a future reader adding indexes
--    should re-check that caveat by hand, not just by this query's row count.
WITH idx_cols AS (
    SELECT table_name, index_name,
           GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS col_list
    FROM   information_schema.statistics
    WHERE  table_schema = 'warehouse'
    GROUP  BY table_name, index_name
)
SELECT a.table_name,
       a.index_name AS shorter_index, a.col_list AS shorter_cols,
       b.index_name AS longer_index,  b.col_list AS longer_cols
FROM   idx_cols a
JOIN   idx_cols b
       ON  a.table_name  = b.table_name
       AND a.index_name <> b.index_name
       AND b.col_list LIKE CONCAT(a.col_list, ',%')
ORDER  BY a.table_name, a.index_name;

-- 4. Storage cost of the indexing layer. DATA_LENGTH is the clustered table;
--    INDEX_LENGTH is every secondary index combined. A healthy analytical
--    fact table typically carries a non-trivial index footprint relative to
--    its data — that is the price being paid for §3/§4, made visible in bytes
--    rather than left as an abstract claim.
SELECT table_name,
       table_rows,
       ROUND(data_length  / 1024 / 1024, 2) AS data_mb,
       ROUND(index_length / 1024 / 1024, 2) AS index_mb
FROM   information_schema.tables
WHERE  table_schema = 'warehouse'
  AND  table_name IN ('fact_sales', 'fact_inventory_snapshot')
ORDER  BY table_name;

-- 5. RECOMMENDED EXPLAIN CHECKS. These execute and return a real plan; read
--    the `key` column (which index MySQL chose, if any) and `Extra` (look for
--    "Using index" - an index-only scan - on the covering-index query).

-- 5a. Channel headline report - should use ix_fact_sales_channel_date_covering
--     with "Using index" in Extra.
EXPLAIN
SELECT channel_key, order_date_key, SUM(net_sales_amount) AS revenue
FROM   warehouse.fact_sales
WHERE  order_date_key BETWEEN 20250101 AND 20250630
GROUP  BY channel_key, order_date_key;

-- 5b. Store trend - should use ix_fact_sales_date_store.
EXPLAIN
SELECT store_key, SUM(net_sales_amount) AS revenue
FROM   warehouse.fact_sales
WHERE  order_date_key BETWEEN 20250101 AND 20251231
GROUP  BY store_key;

-- 5c. Drill-across anchor - should use ix_fact_inventory_product_store_date on
--     the inventory side.
EXPLAIN
SELECT i.product_key, i.store_key, AVG(i.quantity_on_hand) AS avg_stock
FROM   warehouse.fact_inventory_snapshot i
WHERE  i.product_key = (SELECT MIN(product_key) FROM warehouse.dim_product WHERE product_key > 0)
GROUP  BY i.product_key, i.store_key;

-- =============================================================================
-- END OF indexes.sql
--
-- Next: views.sql — the mart semantic layer. architecture.md §2.3: views join
-- the star back together and apply business-friendly names; materialized views
-- pre-aggregate the expensive rollups. No new business logic that is not
-- already in the warehouse - a metric defined here is defined once, the same
-- discipline schema.sql applied to gross/net/profit via generated columns.
-- =============================================================================
