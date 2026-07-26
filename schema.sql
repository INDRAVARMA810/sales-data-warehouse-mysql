-- =============================================================================
-- schema.sql — Mini Sales Data Warehouse
-- =============================================================================
--
--   Target platform : MySQL 8.0.16 or later  (8.0.16 is the floor: CHECK
--                     constraints are parsed-but-ignored before that version,
--                     which would silently disable half the guarantees below)
--   Storage engine  : InnoDB  (the only engine with foreign keys)
--   Methodology     : Kimball dimensional modeling — see architecture.md
--
--   Run with:
--       mysql --user=root --password --show-warnings < schema.sql
--
--   `--show-warnings` matters for the same reason `ON_ERROR_STOP=1` matters in
--   psql: MySQL demotes a surprising number of problems to warnings, and a
--   warehouse that was built with warnings you never read is a warehouse you
--   cannot trust.
--
-- -----------------------------------------------------------------------------
-- PLATFORM NOTE — this file targets MySQL 8; architecture.md §1 says PostgreSQL
-- -----------------------------------------------------------------------------
-- architecture.md is the contract, and it currently names PostgreSQL 14+. Until
-- that line is updated, this file and that document disagree. Every place where
-- the port forced a change from the approved design is marked with a
--
--     -- PORT NOTE:
--
-- comment, so the deviations are reviewable rather than buried. There are five.
-- They are summarized here and explained in full at each site:
--
--   1. Schemas -> databases.        MySQL has no schema-inside-database concept;
--                                   `warehouse.dim_customer` still resolves.
--   2. Partitioning vs foreign keys. InnoDB forbids FKs on partitioned tables.
--                                   Only one of architecture.md §5.4 and §5.7
--                                   can be honored. §5.7 wins by default;
--                                   the §5.4 variant ships commented out.
--   3. Partial unique indexes.      No `WHERE is_current` in MySQL. Replaced
--                                   with a STORED generated column + UNIQUE.
--   4. `year_month` renamed.        YEAR_MONTH is a MySQL reserved word.
--   5. Negative surrogate keys.     Needs NO_AUTO_VALUE_ON_ZERO to seed key 0.
--
-- =============================================================================


-- =============================================================================
-- SECTION 0 — SESSION SETUP
-- =============================================================================
--
-- NO_AUTO_VALUE_ON_ZERO is not a stylistic preference here — the schema is
-- incorrect without it.
--
-- architecture.md §5.2 reserves surrogate key 0 for the "Not Applicable"
-- member of every dimension. But MySQL's default behavior is that inserting 0
-- into an AUTO_INCREMENT column means "please generate the next value for me",
-- exactly as if NULL had been passed. Seeding `VALUES (0, 'Not Applicable')`
-- would therefore silently produce key 1, every fact row pointing at 0 would
-- fail its foreign key, and the error message would blame the fact load rather
-- than the seed that actually caused it.
--
-- NO_AUTO_VALUE_ON_ZERO makes 0 mean literal zero.
--
-- Negative keys (-1 = Unknown) need no special mode: MySQL stores an explicit
-- negative value as given, and because it is below the current counter it does
-- not advance the AUTO_INCREMENT sequence. The corollary is a hard rule for the
-- rest of this file:
--
--     NO surrogate key column may ever be declared UNSIGNED.
--
-- An UNSIGNED key column cannot hold -1, and MySQL will clamp it to 0 rather
-- than raise an error — collapsing "Unknown" and "Not Applicable" into the same
-- member and destroying exactly the distinction architecture.md §5.2 exists to
-- preserve.
--
-- This mode is session-scoped. etl_simulation.sql and sample_data.sql must set
-- it too wherever they touch reserved members.
-- -----------------------------------------------------------------------------

SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_AUTO_VALUE_ON_ZERO');


-- =============================================================================
-- TEARDOWN — deliberately commented out
-- =============================================================================
-- Uncomment only when you intend to destroy the warehouse and rebuild it.
-- Left inert because a stray full-file re-run should fail loudly on an existing
-- object, not silently delete a loaded warehouse.
--
-- Order matters: `mart` and `staging` first (no inbound FKs), then `warehouse`,
-- then `meta`.
--
--   DROP DATABASE IF EXISTS mart;
--   DROP DATABASE IF EXISTS staging;
--   DROP DATABASE IF EXISTS warehouse;
--   DROP DATABASE IF EXISTS meta;
-- =============================================================================


-- =============================================================================
-- SECTION 1 — THE FOUR LAYERS
-- =============================================================================
--
-- PORT NOTE (1 of 5): schemas -> databases.
--
-- architecture.md §2 defines four PostgreSQL *schemas* inside one database.
-- MySQL has no such concept — CREATE SCHEMA is a literal synonym for
-- CREATE DATABASE. So each layer becomes its own database.
--
-- This is a closer match than it first appears. MySQL resolves `db.table`
-- across databases in a single query, and InnoDB supports foreign keys that
-- cross database boundaries. Every qualified name in the approved documents
-- (`warehouse.dim_customer`, `meta.etl_batch`, `staging.stg_product`) resolves
-- unchanged, and every query written against the PostgreSQL design still parses.
--
-- What is genuinely lost:
--   * No `search_path`. Queries must qualify names or issue `USE <db>`.
--   * Permissions are granted per database rather than per schema — in practice
--     an improvement here, since `GRANT SELECT ON mart.*` to analysts while
--     withholding `staging` is precisely the boundary you want.
--
-- Creation order is dependency order, and it is the same order the layers
-- appear in the pipeline: meta -> staging -> warehouse -> mart.
-- -----------------------------------------------------------------------------

-- utf8mb4 is the only correct choice: it is real 4-byte Unicode. MySQL's legacy
-- `utf8` alias is 3-byte and cannot store emoji or many CJK characters — a
-- customer name it cannot represent becomes a truncation, not an error.
CREATE DATABASE IF NOT EXISTS meta
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS staging
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS warehouse
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

-- `mart` is created empty. It is populated by views.sql (Milestone 5) and holds
-- no base tables by design — a mart table would be a second copy of the truth.
CREATE DATABASE IF NOT EXISTS mart
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;


-- =============================================================================
-- SECTION 2 — meta: THE CONTROL PLANE
-- =============================================================================
-- architecture.md §2.4. Built first because every other layer carries a batch
-- id, and because a warehouse without a control plane cannot distinguish
-- "sales were down" from "the loader dropped a file".
-- =============================================================================


-- -----------------------------------------------------------------------------
-- meta.etl_batch
-- -----------------------------------------------------------------------------
-- WHY THIS TABLE EXISTS
-- One row per pipeline execution. It is the anchor for all lineage: every
-- warehouse row carries `dw_batch_id`, so "where did this number come from?"
-- becomes a join rather than an afternoon of reading logs.
--
-- It also makes the pipeline *restartable*. architecture.md §5.8 requires
-- idempotency; idempotency requires knowing which batch is in flight, which
-- requires this row to exist before any data moves.
--
-- KEYS
--   PK  batch_id — surrogate AUTO_INCREMENT. Monotonic by construction, so the
--       clustered index appends rather than splitting pages, and `ORDER BY
--       batch_id` is a valid proxy for chronological order.
--   IX  ix_etl_batch_status_started — supports the single most-run operational
--       query, "show me failed or still-running batches, newest first".
--       Deliberately placed here and not in indexes.sql: indexes.sql is about
--       *analytical* access to loaded facts, and is built after bulk load. This
--       index serves operators during the load, so it must exist beforehand.
-- -----------------------------------------------------------------------------
CREATE TABLE meta.etl_batch (
    batch_id          BIGINT       NOT NULL AUTO_INCREMENT,
    batch_name        VARCHAR(100) NOT NULL
        COMMENT 'Logical job name, e.g. daily_sales_load',
    source_system     VARCHAR(50)  NOT NULL
        COMMENT 'Upstream system this batch extracted from',

    -- Nullable on purpose: a batch that is still running genuinely has no end
    -- time. A sentinel value here would make "is it still running?" a guess.
    started_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ended_at          DATETIME(6)  NULL
        COMMENT 'NULL while the batch is in flight',

    batch_status      VARCHAR(20)  NOT NULL DEFAULT 'RUNNING',

    -- Row counters. rows_read minus the three outcomes should reconcile to
    -- zero; when it does not, the loader lost rows and this is where you see it.
    rows_read         BIGINT       NOT NULL DEFAULT 0,
    rows_inserted     BIGINT       NOT NULL DEFAULT 0,
    rows_updated      BIGINT       NOT NULL DEFAULT 0,
    rows_rejected     BIGINT       NOT NULL DEFAULT 0,

    error_message     TEXT         NULL,

    CONSTRAINT pk_etl_batch PRIMARY KEY (batch_id),

    -- An enumerated CHECK rather than an ENUM column: adding a value to an ENUM
    -- is an ALTER TABLE that rewrites the table, whereas this is a metadata-only
    -- constraint swap. ENUM also compares by ordinal position, which produces
    -- genuinely surprising ORDER BY results.
    CONSTRAINT ck_etl_batch_status CHECK (
        batch_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'ABORTED')
    ),
    CONSTRAINT ck_etl_batch_rowcounts CHECK (
        rows_read     >= 0 AND rows_inserted >= 0
    AND rows_updated  >= 0 AND rows_rejected >= 0
    ),
    -- Guards against a clock skew or a botched restart writing a batch that
    -- finished before it started.
    CONSTRAINT ck_etl_batch_timespan CHECK (
        ended_at IS NULL OR ended_at >= started_at
    ),

    INDEX ix_etl_batch_status_started (batch_status, started_at)
) ENGINE = InnoDB
  COMMENT = 'One row per ETL run. Anchor for all warehouse lineage.';


-- -----------------------------------------------------------------------------
-- meta.data_quality_result
-- -----------------------------------------------------------------------------
-- WHY THIS TABLE EXISTS
-- Data quality that is only checked is not data quality — it has to be recorded,
-- so that "the null rate on customer_id" becomes a trend you can put an SLA on
-- rather than a thing someone noticed once.
--
-- Storing results as rows (not log lines) means the DQ history is queryable with
-- the same SQL as everything else: rule pass rates over time, which rule fails
-- most, whether a spike in rejections preceded a bad report.
--
-- KEYS
--   PK  dq_result_id — surrogate. The natural key would be
--       (batch_id, rule_name), but rules get re-run within a batch during
--       development, and a PK that forbids that is a PK fighting its users.
--   FK  batch_id -> meta.etl_batch. Enforced: a DQ result with no batch is
--       unattributable, and therefore worthless.
--       ON DELETE CASCADE is correct *here specifically* — deleting a batch
--       record means erasing that run from history, and its DQ results have no
--       independent meaning. Note this is the ONLY cascade in the entire
--       schema; see §4 for why dimensions use RESTRICT instead.
--   IX  ix_dq_result_rule — "how has this rule behaved over time?"
-- -----------------------------------------------------------------------------
CREATE TABLE meta.data_quality_result (
    dq_result_id      BIGINT       NOT NULL AUTO_INCREMENT,
    batch_id          BIGINT       NOT NULL,

    rule_name         VARCHAR(100) NOT NULL,
    rule_type         VARCHAR(30)  NOT NULL
        COMMENT 'NOT_NULL | UNIQUE | RANGE | REFERENTIAL | RECONCILIATION',
    target_object     VARCHAR(150) NOT NULL
        COMMENT 'Fully qualified table the rule was evaluated against',

    -- WARN vs ERROR is the difference between "log it and keep going" and "stop
    -- the batch". Encoding severity as data rather than as loader code means the
    -- policy can change without a deployment.
    severity          VARCHAR(10)  NOT NULL DEFAULT 'ERROR',

    records_checked   BIGINT       NOT NULL DEFAULT 0,
    records_failed    BIGINT       NOT NULL DEFAULT 0,

    -- GENERATED, not written by the loader. The pass/fail verdict is a pure
    -- function of the counts, so letting a caller supply it independently just
    -- creates the opportunity for a row that claims to pass while reporting
    -- failures. Same principle as architecture.md §5.5, applied to metadata.
    is_passed         TINYINT(1)   AS (records_failed = 0) STORED,

    checked_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_data_quality_result PRIMARY KEY (dq_result_id),
    CONSTRAINT fk_dq_result_batch FOREIGN KEY (batch_id)
        REFERENCES meta.etl_batch (batch_id)
        ON DELETE CASCADE ON UPDATE RESTRICT,

    CONSTRAINT ck_dq_result_severity CHECK (severity IN ('WARN', 'ERROR')),
    -- More failures than records checked is arithmetically impossible and
    -- indicates a broken rule implementation. Catch it at write time.
    CONSTRAINT ck_dq_result_counts CHECK (
        records_checked >= 0
    AND records_failed  >= 0
    AND records_failed  <= records_checked
    ),

    INDEX ix_dq_result_rule (rule_name, checked_at)
) ENGINE = InnoDB
  COMMENT = 'Per-rule data quality outcomes, queryable as history.';


-- -----------------------------------------------------------------------------
-- meta.rejected_record
-- -----------------------------------------------------------------------------
-- WHY THIS TABLE EXISTS
-- The quarantine. architecture.md §2.1 sets out the principle: a row that fails
-- validation must be *set aside*, never dropped and never allowed to fail the
-- whole load. Both alternatives are worse — dropping loses data silently, and
-- failing the batch lets one malformed row block the other 49,999.
--
-- Keeping the raw payload means a rejected row can be corrected and replayed
-- without re-extracting from the source, which may no longer hold that version.
--
-- KEYS
--   PK  rejected_record_id — surrogate; rejects have no meaningful natural key
--       (that is frequently *why* they were rejected).
--   FK  batch_id -> meta.etl_batch, CASCADE for the same reason as above.
--   IX  ix_rejected_record_batch_object — drives the triage query: "what did
--       last night's run reject, grouped by source table?"
-- -----------------------------------------------------------------------------
CREATE TABLE meta.rejected_record (
    rejected_record_id BIGINT       NOT NULL AUTO_INCREMENT,
    batch_id           BIGINT       NOT NULL,

    source_object      VARCHAR(150) NOT NULL
        COMMENT 'Staging table the row was rejected from',
    source_row_num     BIGINT       NULL
        COMMENT 'Ordinal within the source file, for pinpointing the bad line',

    reject_reason      VARCHAR(255) NOT NULL,
    reject_rule        VARCHAR(100) NULL
        COMMENT 'Joins back to data_quality_result.rule_name when applicable',

    -- JSON rather than TEXT: MySQL 8 validates it on write, so a malformed
    -- payload cannot enter the quarantine table itself, and JSON_EXTRACT can
    -- query into rejects without parsing strings in application code.
    raw_payload        JSON         NULL
        COMMENT 'The offending row, verbatim, for correction and replay',

    rejected_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_rejected_record PRIMARY KEY (rejected_record_id),
    CONSTRAINT fk_rejected_record_batch FOREIGN KEY (batch_id)
        REFERENCES meta.etl_batch (batch_id)
        ON DELETE CASCADE ON UPDATE RESTRICT,

    INDEX ix_rejected_record_batch_object (batch_id, source_object)
) ENGINE = InnoDB
  COMMENT = 'Quarantine for rows that failed validation. Never drop data.';


-- =============================================================================
-- SECTION 3 — staging: LAND IT, DO NOT JUDGE IT
-- =============================================================================
-- architecture.md §2.1.
--
-- Three rules govern every table in this section, and each looks like bad
-- practice until you have debugged a failed load without them:
--
--   1. EVERY BUSINESS COLUMN IS A STRING. Including numbers. Including dates.
--      A source that starts sending "N/A" in a price column must produce a
--      quarantined row that names the problem — not a load that dies at the
--      door with a type error, and not a silent coercion to 0.
--
--   2. NO CONSTRAINTS. No primary keys, no foreign keys, no NOT NULL on
--      business columns. Constraints here would reject bad rows at the boundary,
--      which sounds desirable but means the bad rows are gone before anyone can
--      look at them. Validation belongs in the TRANSFORM step, where a failure
--      produces a meta.rejected_record you can read.
--
--   3. EVERY TABLE CARRIES THE SAME FOUR AUDIT COLUMNS. Underscore-prefixed per
--      architecture.md §6, so staging metadata is visually distinct from both
--      source columns and the warehouse's `dw_` columns.
--
-- PORT NOTE: architecture.md §2.1 specifies TEXT. This file uses generously
-- sized VARCHAR instead. The intent — "accept anything, judge nothing" — is
-- identical, but in InnoDB a TEXT column is stored off-page with only a pointer
-- inline, so every scan of a staging table becomes a scatter of extra reads.
-- Staging tables are scanned in full on every single run. VARCHAR keeps the
-- data inline. The lengths are set far wider than any legitimate value so they
-- do not act as a de facto validation rule, which would violate rule 2.
--
-- Truncated and reloaded every run: the tables persist so `SHOW CREATE TABLE`
-- still documents the source contract, but the rows do not.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- staging.stg_customer  <- source CUSTOMER + ADDRESS + CITY + COUNTRY
-- -----------------------------------------------------------------------------
-- Lands the source's four-table normalized customer geography (er_diagram.md §1)
-- pre-flattened by the extract query. Flattening in the extract rather than the
-- transform is deliberate: the join is cheap on the source side where those
-- tables are indexed for it, and it means staging mirrors one row per customer,
-- which makes row-count reconciliation trivial.
--
-- `loyalty_tier` is one of the five MUTATED IN PLACE columns. Every extract sees
-- only the current value — capturing the change is what SCD2 in dim_customer is
-- for, and it is the reason this pipeline exists at all.
--
-- No PK: a duplicated customer in the feed is a fact to be *detected and
-- reported*, not an insert to be blocked.
-- -----------------------------------------------------------------------------
CREATE TABLE staging.stg_customer (
    customer_id      VARCHAR(100) NULL,
    customer_name    VARCHAR(255) NULL,
    email            VARCHAR(255) NULL,
    segment_code     VARCHAR(100) NULL,
    loyalty_tier     VARCHAR(100) NULL COMMENT 'Mutated in place at source',
    signup_date      VARCHAR(50)  NULL COMMENT 'String: may arrive malformed',
    city_name        VARCHAR(255) NULL,
    state_province   VARCHAR(255) NULL,
    country_name     VARCHAR(255) NULL,
    region_name      VARCHAR(255) NULL,
    postal_code      VARCHAR(50)  NULL,

    _load_batch_id   BIGINT       NULL,
    _loaded_at       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    _source_system   VARCHAR(50)  NULL,
    _source_row_num  BIGINT       NULL
) ENGINE = InnoDB
  COMMENT = 'Raw customer extract. No constraints by design.';


-- -----------------------------------------------------------------------------
-- staging.stg_product  <- source PRODUCT + SUBCATEGORY + CATEGORY + BRAND
-- -----------------------------------------------------------------------------
-- The product hierarchy arrives pre-joined and stays denormalized all the way
-- through to dim_product — this is the star-not-snowflake decision from
-- star_schema.md §3, applied at the earliest possible point.
--
-- `unit_cost` and `list_price` are the other two MUTATED IN PLACE columns, and
-- the reason dim_product is SCD Type 2: without version history, historical
-- gross margin becomes permanently unrecoverable the moment a supplier
-- re-prices.
-- -----------------------------------------------------------------------------
CREATE TABLE staging.stg_product (
    product_id        VARCHAR(100) NULL,
    product_name      VARCHAR(255) NULL,
    category_name     VARCHAR(255) NULL,
    subcategory_name  VARCHAR(255) NULL,
    brand_name        VARCHAR(255) NULL,
    unit_cost         VARCHAR(50)  NULL COMMENT 'String: may arrive non-numeric',
    list_price        VARCHAR(50)  NULL COMMENT 'String: may arrive non-numeric',
    is_active         VARCHAR(20)  NULL COMMENT 'Y/N, 1/0, true/false all seen',

    _load_batch_id    BIGINT       NULL,
    _loaded_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    _source_system    VARCHAR(50)  NULL,
    _source_row_num   BIGINT       NULL
) ENGINE = InnoDB
  COMMENT = 'Raw product master extract. No constraints by design.';


-- -----------------------------------------------------------------------------
-- staging.stg_store  <- CSV feed
-- -----------------------------------------------------------------------------
-- The store feed is a file, not a database extract (architecture.md §2). That
-- makes it the least trustworthy of the three sources — no schema enforcement
-- upstream, no referential integrity, and a human editing it in a spreadsheet
-- is a realistic failure mode. `_source_row_num` earns its keep here: it maps a
-- rejection straight back to a line number in the delivered file.
-- -----------------------------------------------------------------------------
CREATE TABLE staging.stg_store (
    store_id         VARCHAR(100) NULL,
    store_name       VARCHAR(255) NULL,
    store_type       VARCHAR(100) NULL,
    city_name        VARCHAR(255) NULL,
    state_province   VARCHAR(255) NULL,
    country_name     VARCHAR(255) NULL,
    region_name      VARCHAR(255) NULL,
    square_feet      VARCHAR(50)  NULL,
    opened_date      VARCHAR(50)  NULL,

    _load_batch_id   BIGINT       NULL,
    _loaded_at       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    _source_system   VARCHAR(50)  NULL,
    _source_row_num  BIGINT       NULL
) ENGINE = InnoDB
  COMMENT = 'Raw store CSV feed. No constraints by design.';


-- -----------------------------------------------------------------------------
-- staging.stg_sales_order_line  <- SALES_ORDER + ORDER_LINE
-- -----------------------------------------------------------------------------
-- The highest-volume staging table, and the one that becomes fact_sales.
--
-- It lands at ORDER LINE grain, matching the declared grain of fact_sales
-- (architecture.md §3). Staging at order grain instead would force the transform
-- to invent line detail it was never given — and per star_schema.md §4, detail
-- you chose not to store is not recoverable.
--
-- Order-level attributes (order_number, customer, channel, status) are repeated
-- on every line. That redundancy is correct here: it keeps the extract a single
-- flat query, and it is discarded on the way into the warehouse where the
-- order-level flags collapse into dim_order_junk and order_number survives as a
-- degenerate dimension.
--
-- The four junk-dimension source flags — order_status, payment_method,
-- shipping_priority, is_gift — arrive as separate strings and are resolved to
-- one order_junk_key during transform.
-- -----------------------------------------------------------------------------
CREATE TABLE staging.stg_sales_order_line (
    order_number      VARCHAR(100) NULL,
    order_line_number VARCHAR(20)  NULL,
    ordered_at        VARCHAR(50)  NULL,
    shipped_date      VARCHAR(50)  NULL COMMENT 'Empty when not yet shipped',

    customer_id       VARCHAR(100) NULL,
    store_id          VARCHAR(100) NULL,
    channel_id        VARCHAR(100) NULL,
    product_id        VARCHAR(100) NULL,
    promotion_id      VARCHAR(100) NULL COMMENT 'Empty = no promotion applied',

    -- The four junk-dimension inputs.
    order_status      VARCHAR(100) NULL,
    payment_method    VARCHAR(100) NULL,
    shipping_priority VARCHAR(100) NULL,
    is_gift           VARCHAR(20)  NULL,

    -- The measures. Strings, like everything else.
    quantity          VARCHAR(50)  NULL,
    unit_price        VARCHAR(50)  NULL,
    discount_amount   VARCHAR(50)  NULL,
    tax_amount        VARCHAR(50)  NULL,

    _load_batch_id    BIGINT       NULL,
    _loaded_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    _source_system    VARCHAR(50)  NULL,
    _source_row_num   BIGINT       NULL
) ENGINE = InnoDB
  COMMENT = 'Raw order line extract at line grain. No constraints by design.';


-- -----------------------------------------------------------------------------
-- staging.stg_inventory  <- INVENTORY_LEVEL
-- -----------------------------------------------------------------------------
-- The source (er_diagram.md §1) holds ONE row per product/store with
-- quantity_on_hand mutated in place. There is no history table upstream at all.
--
-- That is the entire justification for fact_inventory_snapshot: yesterday's
-- stock position is unrecoverable from the source, so the warehouse must capture
-- it daily or lose it permanently. `snapshot_date` is supplied by the extract,
-- not by the source system, because the source has no concept of "as at".
-- -----------------------------------------------------------------------------
CREATE TABLE staging.stg_inventory (
    snapshot_date     VARCHAR(50)  NULL COMMENT 'Stamped by the extract, not the source',
    product_id        VARCHAR(100) NULL,
    store_id          VARCHAR(100) NULL,
    quantity_on_hand  VARCHAR(50)  NULL,
    quantity_on_order VARCHAR(50)  NULL,
    reorder_point     VARCHAR(50)  NULL,
    unit_cost         VARCHAR(50)  NULL COMMENT 'Used to value the position',

    _load_batch_id    BIGINT       NULL,
    _loaded_at        DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    _source_system    VARCHAR(50)  NULL,
    _source_row_num   BIGINT       NULL
) ENGINE = InnoDB
  COMMENT = 'Raw daily inventory position. No constraints by design.';


-- =============================================================================
-- SECTION 4 — warehouse: THE CONFORMED STAR (DIMENSIONS)
-- =============================================================================
-- architecture.md §2.2 and §4.
--
-- Dimensions are created before facts because every fact declares foreign keys
-- into them, and InnoDB requires the referenced table to exist.
--
-- Two conventions apply to every table in this section.
--
-- ON DELETE RESTRICT ON UPDATE RESTRICT, EVERYWHERE.
--   Every fact-to-dimension FK below is RESTRICT on both sides, and this is a
--   substantive choice rather than a default:
--     * CASCADE DELETE would let one careless `DELETE FROM dim_product` silently
--       erase revenue. A dimension row that facts reference must be
--       undeletable — that is the guarantee, and RESTRICT is how you state it.
--     * CASCADE UPDATE is meaningless here by construction. Surrogate keys are
--       immutable; that immutability is precisely what architecture.md §5.1 buys
--       by refusing to use natural keys. If a surrogate key ever needs to
--       change, the design has already failed.
--   The single exception in this whole file is meta's CASCADE, §2, where
--   deleting a batch genuinely should erase its subordinate records.
--
-- SIGNED KEY COLUMNS, EVERYWHERE.
--   No key column is UNSIGNED, so that reserved member -1 can be stored. See §0.
--
-- MYSQL AUTOMATICALLY INDEXES FOREIGN KEY COLUMNS.
--   Worth stating plainly, because it differs from PostgreSQL and it changes
--   what indexes.sql needs to do. InnoDB requires an index on the referencing
--   column of every FK and creates one silently if you do not. So every
--   `<dim>_key` column on both facts gets a single-column B-tree for free.
--   That is exactly the index a star join needs, which is why Section 5 declares
--   no additional single-key indexes — they would be exact duplicates,
--   consuming space and slowing writes for no benefit. indexes.sql (Milestone 4)
--   is therefore free to focus on composite and covering indexes, which is where
--   the real thought goes.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- warehouse.dim_date          SCD TYPE 0 — conformed
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- Almost every analytical question is time-bounded, and nearly all of them are
-- phrased in calendar concepts the database does not know: fiscal year, holiday,
-- weekend, "same month last year". Computing those with date functions at query
-- time means every query re-derives them, none of them can use an index, and two
-- analysts will eventually disagree about when the fiscal year starts.
--
-- A date dimension makes the business calendar *data*: defined once, joined to,
-- indexed, and identical for every consumer.
--
-- SCD TYPE 0 — retain original, never update. Calendar facts do not change.
-- There will never be a restatement of what 15 March 2024 means. This dimension
-- is pre-generated by sample_data.sql and never touched by the ETL again.
--
-- KEYS
--   PK  date_key — the one intelligent key in the schema (architecture.md §5.3).
--       A YYYYMMDD integer, e.g. 20240315. It earns the exception because:
--         * it makes range partitioning readable and prunable (see §5);
--         * date is the only dimension whose source key can never be restated;
--         * it makes fact rows debuggable by eye.
--       THE RULE THAT KEEPS THIS SAFE: date_key is a key, not a number.
--       `order_date_key - ship_date_key` is nonsense — it is not 5, it is not
--       anything. All date arithmetic goes through `full_date`, which is why
--       that column is NOT NULL and unique rather than merely convenient.
--   UK  uq_dim_date_full_date — one row per real calendar day. Without it a
--       generator bug could emit 20240229 twice in a non-leap year and every
--       aggregate downstream would silently double.
--
-- No index on year/month columns here; those belong in indexes.sql. At ~4,000
-- rows MySQL will scan this table faster than it can descend a secondary index.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_date (
    date_key             INT         NOT NULL
        COMMENT 'Smart key YYYYMMDD. A KEY, NOT A NUMBER - never do arithmetic',
    full_date            DATE        NOT NULL
        COMMENT 'The real date. ALL date arithmetic goes through this column',

    year_number          SMALLINT    NOT NULL,
    quarter_number       TINYINT     NOT NULL,
    month_number         TINYINT     NOT NULL,
    month_name           VARCHAR(20) NOT NULL,
    day_of_month         TINYINT     NOT NULL,
    week_of_year         TINYINT     NOT NULL,
    day_name             VARCHAR(20) NOT NULL,

    -- PORT NOTE (4 of 5): renamed from `year_month`.
    -- YEAR_MONTH is a MySQL reserved word (it is an INTERVAL unit, as in
    -- `INTERVAL 1 YEAR_MONTH`). Using it would force backticks on every
    -- reference forever, which architecture.md §6 explicitly forbids.
    -- Stored as an integer 202403 rather than a string so that ORDER BY sorts
    -- chronologically and the column is 4 bytes instead of 7.
    -- ACTION REQUIRED: star_schema.md §2 shows `d.year_month` in its worked
    -- drill-across query. That reference needs updating to year_month_number.
    year_month_number    INT         NOT NULL
        COMMENT 'YYYYMM, e.g. 202403. Renamed: YEAR_MONTH is reserved in MySQL',

    is_weekend           TINYINT(1)  NOT NULL DEFAULT 0,
    is_holiday           TINYINT(1)  NOT NULL DEFAULT 0,

    fiscal_year          SMALLINT    NOT NULL,
    fiscal_quarter       TINYINT     NOT NULL,

    -- Precomputed so year-over-year comparison is a self-join on an integer key
    -- rather than `full_date - INTERVAL 1 YEAR`, which cannot use an index and
    -- has to be written correctly by every analyst independently.
    -- NOT a foreign key to dim_date: the earliest year in the calendar has no
    -- prior year loaded, so a self-FK would make the first year unloadable.
    prior_year_date_key  INT         NOT NULL,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key),
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date),

    -- Ranges start below 1 to admit the reserved members, which carry 0.
    CONSTRAINT ck_dim_date_month   CHECK (month_number    BETWEEN 0 AND 12),
    CONSTRAINT ck_dim_date_quarter CHECK (quarter_number  BETWEEN 0 AND 4),
    CONSTRAINT ck_dim_date_week    CHECK (week_of_year    BETWEEN 0 AND 53),
    CONSTRAINT ck_dim_date_day     CHECK (day_of_month    BETWEEN 0 AND 31)
) ENGINE = InnoDB
  COMMENT = 'SCD0 conformed calendar. Pre-generated, never loaded by ETL.';


-- -----------------------------------------------------------------------------
-- warehouse.dim_customer      SCD TYPE 2
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- To answer "who bought it", and to answer it *as of the purchase*.
--
-- WHY SCD TYPE 2 (architecture.md §4.2)
-- The source mutates loyalty_tier and address in place. If the warehouse did the
-- same, a customer promoted to Gold in June would retroactively make January's
-- orders look like Gold revenue — every historical tier report would silently
-- change every time anyone's tier changed. That is not a reporting bug that
-- shows up as an error; it shows up as numbers that quietly stopped matching
-- last quarter's deck.
--
-- HOW SCD TYPE 2 IS IMPLEMENTED HERE
-- On a tracked change the ETL does two things (etl_simulation.sql, Milestone 3):
--   1. Closes the existing row:  valid_to = change_date - 1 day, is_current = 0
--   2. Inserts a new row:        new surrogate key, valid_from = change_date,
--                                valid_to = '9999-12-31', is_current = 1,
--                                version_number = previous + 1
--
--   customer_key | customer_id | tier   | valid_from | valid_to   | is_current
--   -------------+-------------+--------+------------+------------+-----------
--            501 | C-1042      | Silver | 2023-01-15 | 2024-05-31 | 0
--            892 | C-1042      | Gold   | 2024-06-01 | 9999-12-31 | 1
--
-- Facts before June carry key 501; facts after carry 892. NO FACT ROW IS EVER
-- UPDATED — that is the whole trick. History stays correct with no maintenance,
-- forever, because the fact's pointer already encodes which version applied.
--
-- Three details that look optional and are not:
--   * valid_to = '9999-12-31', never NULL. A real high date keeps BETWEEN range
--     logic simple and index-friendly. NULL forces
--     `(valid_to IS NULL OR valid_to >= x)` into every single query.
--   * is_current is redundant with valid_to, and still worth its byte:
--     `WHERE is_current = 1` is cheaper and clearer than a date predicate, and
--     it is what the unique-current index below is built on.
--   * version_number carries no logic; it makes SCD2 chains readable during
--     debugging, which is when you need them readable.
--
-- KEYS
--   PK  customer_key — surrogate, AUTO_INCREMENT. MANDATORY for SCD2, not
--       stylistic: after the change above, TWO rows carry customer_id 'C-1042',
--       so the natural key is not unique and therefore cannot be the PK. This is
--       the concrete reason behind architecture.md §5.1.
--   UK  uq_dim_customer_current — at most one live row per customer.
--   UK  uq_dim_customer_version — no two versions of one customer may open on
--       the same date. Catches the classic double-run bug where a re-executed
--       ETL appends a duplicate version instead of recognizing no change.
--   IX  ix_dim_customer_asof — the late-arriving-fact lookup from
--       dimensional_modeling.md §9: resolve a customer as of a historical date,
--       `customer_id = ? AND ? BETWEEN valid_from AND valid_to`. Declared here
--       rather than in indexes.sql because etl_simulation.sql runs this lookup
--       once per incoming row; without it, the fact load is a nested full scan.
--
-- NOTE customer_id is deliberately NOT unique. That is SCD2 working, not a
-- missing constraint.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_customer (
    customer_key        BIGINT       NOT NULL AUTO_INCREMENT,

    customer_id         VARCHAR(50)  NOT NULL
        COMMENT 'Natural key. NOT unique - SCD2 gives one customer many rows',

    customer_name       VARCHAR(200) NOT NULL,
    email               VARCHAR(255) NOT NULL,

    -- Verbose values, not codes (dimensional_modeling.md §4): 'Home Office',
    -- never 'HO'. A report should be readable without a lookup sheet.
    customer_segment    VARCHAR(50)  NOT NULL,
    loyalty_tier        VARCHAR(50)  NOT NULL COMMENT 'SCD2 TRACKED',

    -- Geography denormalized flat, collapsing the source's
    -- ADDRESS -> CITY -> COUNTRY chain. This is the six-joins-becomes-one payoff
    -- from er_diagram.md §3, and the address is SCD2 tracked because where a
    -- customer lived when they bought is a real analytical question.
    city                VARCHAR(100) NOT NULL COMMENT 'SCD2 TRACKED',
    state_province      VARCHAR(100) NOT NULL COMMENT 'SCD2 TRACKED',
    country             VARCHAR(100) NOT NULL COMMENT 'SCD2 TRACKED',
    region              VARCHAR(100) NOT NULL COMMENT 'SCD2 TRACKED',
    postal_code         VARCHAR(20)  NOT NULL,

    -- SCD0 within an SCD2 dimension: signup_date is a property of the customer,
    -- not of the version, so it is copied forward unchanged on every new row.
    signup_date         DATE         NOT NULL COMMENT 'SCD0 - copied forward',

    -- ---- SCD Type 2 control columns -------------------------------------
    valid_from          DATE         NOT NULL,
    valid_to            DATE         NOT NULL DEFAULT '9999-12-31'
        COMMENT 'High date, never NULL - keeps BETWEEN logic index-friendly',
    is_current          TINYINT(1)   NOT NULL DEFAULT 1,
    version_number      INT          NOT NULL DEFAULT 1,

    -- PORT NOTE (3 of 5): partial unique index workaround.
    --
    -- architecture.md §6 and er_diagram.md §2 specify a PostgreSQL partial
    -- index: UNIQUE (customer_id) WHERE is_current. MySQL has no filtered
    -- indexes, so the filter is pushed into a STORED generated column instead.
    --
    -- The mechanism: this column equals customer_id on the live row and NULL on
    -- every closed row. MySQL's UNIQUE indexes permit unlimited NULLs but only
    -- one of any given non-NULL value, so a UNIQUE index on this column enforces
    -- exactly "at most one current row per customer" and ignores history
    -- entirely. Identical semantics to the PostgreSQL partial index; the index
    -- is even comparably small, since closed rows store only a NULL marker.
    --
    -- STORED, not VIRTUAL: MySQL cannot build a UNIQUE index on a VIRTUAL
    -- generated column.
    current_customer_id VARCHAR(50)
        AS (IF(is_current = 1, customer_id, NULL)) STORED
        COMMENT 'MySQL substitute for a PostgreSQL partial unique index',

    -- ---- Audit / lineage (architecture.md §7) ---------------------------
    dw_batch_id         BIGINT       NOT NULL,
    dw_source_system    VARCHAR(50)  NOT NULL,
    dw_inserted_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at       DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                     ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key),
    CONSTRAINT uq_dim_customer_current UNIQUE (current_customer_id),
    CONSTRAINT uq_dim_customer_version UNIQUE (customer_id, valid_from),

    CONSTRAINT ck_dim_customer_validity CHECK (valid_to >= valid_from),
    CONSTRAINT ck_dim_customer_version  CHECK (version_number >= 1),

    INDEX ix_dim_customer_asof (customer_id, valid_from, valid_to)
) ENGINE = InnoDB
  COMMENT = 'SCD2. Tier and address are point-in-time correct.';


-- -----------------------------------------------------------------------------
-- warehouse.dim_product       SCD TYPE 2 — conformed
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- To answer "what was sold", and it is CONFORMED — the same rows serve both
-- fact_sales and fact_inventory_snapshot. That sharing is what makes
-- "did stockouts precede the sales dip?" a single query instead of two reports
-- and an argument (star_schema.md §2).
--
-- WHY SCD TYPE 2
-- unit_cost and list_price mutate in place at the source. Margin is a
-- HISTORICAL fact: if a supplier raises a widget's cost next month, last
-- month's reported margin must not move. Without version history that
-- correction is not merely unavailable — it is unrecoverable, because the old
-- cost no longer exists anywhere.
--
-- WHY THE HIERARCHY IS FLAT
-- category / subcategory / brand are plain columns repeated on every row rather
-- than separate tables. That is the star-over-snowflake decision
-- (star_schema.md §3): normalization prevents *write*-time update anomalies, and
-- dimensions are only ever written by a controlled ETL — never by an
-- application, never by a user. The anomaly cannot occur, so paying
-- normalization's price (three joins on every product query, forever) buys
-- insurance against a risk that does not exist here.
--
-- KEYS — same structure and same reasoning as dim_customer.
--   PK  product_key           surrogate; required by SCD2
--   UK  uq_dim_product_current   one live row per product_id
--   UK  uq_dim_product_version   no duplicate version start dates
--   IX  ix_dim_product_asof      as-of resolution for late-arriving facts
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_product (
    product_key        BIGINT        NOT NULL AUTO_INCREMENT,

    product_id         VARCHAR(50)   NOT NULL
        COMMENT 'Natural key. NOT unique - SCD2 gives one product many rows',

    product_name       VARCHAR(200)  NOT NULL,

    -- Denormalized hierarchy — the star, not the snowflake.
    category           VARCHAR(100)  NOT NULL,
    subcategory        VARCHAR(100)  NOT NULL,
    brand              VARCHAR(100)  NOT NULL,

    unit_cost          DECIMAL(12,2) NOT NULL COMMENT 'SCD2 TRACKED',
    list_price         DECIMAL(12,2) NOT NULL COMMENT 'SCD2 TRACKED',

    is_active          TINYINT(1)    NOT NULL DEFAULT 1,

    -- ---- SCD Type 2 control columns -------------------------------------
    valid_from         DATE          NOT NULL,
    valid_to           DATE          NOT NULL DEFAULT '9999-12-31',
    is_current         TINYINT(1)    NOT NULL DEFAULT 1,
    version_number     INT           NOT NULL DEFAULT 1,

    -- Same partial-unique-index workaround as dim_customer. See PORT NOTE (3).
    current_product_id VARCHAR(50)
        AS (IF(is_current = 1, product_id, NULL)) STORED
        COMMENT 'MySQL substitute for a PostgreSQL partial unique index',

    -- ---- Audit / lineage -------------------------------------------------
    dw_batch_id        BIGINT        NOT NULL,
    dw_source_system   VARCHAR(50)   NOT NULL,
    dw_inserted_at     DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at      DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                     ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
    CONSTRAINT uq_dim_product_current UNIQUE (current_product_id),
    CONSTRAINT uq_dim_product_version UNIQUE (product_id, valid_from),

    CONSTRAINT ck_dim_product_validity CHECK (valid_to >= valid_from),
    CONSTRAINT ck_dim_product_version  CHECK (version_number >= 1),
    -- Negative cost or price is always a parse failure upstream, never a real
    -- value. Zero is allowed: the reserved members carry it.
    CONSTRAINT ck_dim_product_amounts  CHECK (unit_cost >= 0 AND list_price >= 0),

    INDEX ix_dim_product_asof (product_id, valid_from, valid_to)
) ENGINE = InnoDB
  COMMENT = 'SCD2 conformed. Cost and price are point-in-time correct.';


-- -----------------------------------------------------------------------------
-- warehouse.dim_store         SCD TYPE 1 — conformed
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- To answer "where was it sold" and "where was it stocked" — conformed across
-- both facts, so store is a valid drill-across axis.
--
-- WHY SCD TYPE 1, AND WHY THAT IS THE POINT
-- Type 1 overwrites. A renamed store simply becomes the new name, everywhere,
-- including in reports about last year.
--
-- That is a genuine loss, accepted deliberately. Store attributes here are
-- corrections (a fixed typo, a re-measured floor area), not business changes
-- worth slicing history by, and the table is 25 rows.
--
-- More importantly, dim_store is Type 1 *specifically to contrast with*
-- dim_customer and dim_product (architecture.md §4.2). A schema where every
-- dimension is Type 2 has not made a choice — it has applied a default. Type 2
-- costs unbounded dimension growth, and paying that for 25 rows of store names
-- buys nothing.
--
-- CONSEQUENCE WORTH KNOWING: a Type 1 dimension can only ever answer "as-is",
-- never "as-was" (dimensional_modeling.md §5). If the business later needs
-- "revenue by the region a store was in at the time", this table must be
-- converted to Type 2 — and history before the conversion cannot be recovered.
--
-- KEYS
--   PK  store_key — surrogate. Kept even though Type 1 does not strictly require
--       it (store_id IS unique here), for three reasons: consistency of the fact
--       join pattern, insulation from a source renumbering, and the fact that
--       an integer key is narrower on 50,000 fact rows than a string.
--   UK  uq_dim_store_store_id — UNLIKE the Type 2 dimensions, the natural key IS
--       unique here, because Type 1 means exactly one row per store, ever. This
--       contrast is the clearest possible illustration of why SCD2 forces the
--       surrogate key: same modeling pattern, different SCD type, and the
--       constraint that is possible changes.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_store (
    store_key        BIGINT       NOT NULL AUTO_INCREMENT,

    store_id         VARCHAR(50)  NOT NULL
        COMMENT 'Natural key. UNIQUE - Type 1 means exactly one row per store',

    store_name       VARCHAR(200) NOT NULL,
    store_type       VARCHAR(50)  NOT NULL,

    city             VARCHAR(100) NOT NULL,
    state_province   VARCHAR(100) NOT NULL,
    country          VARCHAR(100) NOT NULL,
    region           VARCHAR(100) NOT NULL,

    square_feet      INT          NOT NULL DEFAULT 0,
    opened_date      DATE         NOT NULL,

    -- No valid_from / valid_to / is_current: there is only ever one version.
    -- dw_updated_at is the sole record that a Type 1 overwrite occurred, which
    -- is exactly the history Type 1 trades away.

    dw_batch_id      BIGINT       NOT NULL,
    dw_source_system VARCHAR(50)  NOT NULL,
    dw_inserted_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                  ON UPDATE CURRENT_TIMESTAMP(6)
        COMMENT 'Only surviving evidence of a Type 1 overwrite',

    CONSTRAINT pk_dim_store PRIMARY KEY (store_key),
    CONSTRAINT uq_dim_store_store_id UNIQUE (store_id),
    CONSTRAINT ck_dim_store_square_feet CHECK (square_feet >= 0)
) ENGINE = InnoDB
  COMMENT = 'SCD1 conformed. Overwrite in place - contrast with SCD2 dims.';


-- -----------------------------------------------------------------------------
-- warehouse.dim_promotion     SCD TYPE 1
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- To answer "why did they buy" — the causal dimension. It is also what makes
-- promotional lift measurable: comparing sales under a promotion against the
-- same product outside its window.
--
-- WHY SCD TYPE 1
-- A campaign is defined once, before it launches. Later edits are corrections to
-- the definition, not changes to the campaign. And the promotion's *identity*
-- already carries its own time bounds in start_date / end_date, so the
-- historical question "was this order inside the promo window?" is answerable
-- from the attributes without needing version history.
--
-- NOTE ON discount_pct — NON-ADDITIVE (star_schema.md §5). SUM is meaningless,
-- and AVG is worse, because it looks reasonable while weighting a $5 order the
-- same as a $5,000 one. Effective discount must always be recomputed from
-- additive components: SUM(discount_amount) / NULLIF(SUM(gross_sales_amount),0).
-- Stored as a fraction (0.1500 = 15%) rather than 15.00, so it multiplies
-- directly against an amount with no hidden /100.
--
-- KEYS
--   PK  promotion_key — surrogate, consistent with every other dimension.
--   UK  uq_dim_promotion_promotion_id — Type 1, so one row per promotion.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_promotion (
    promotion_key    BIGINT        NOT NULL AUTO_INCREMENT,

    promotion_id     VARCHAR(50)   NOT NULL COMMENT 'Natural key. UNIQUE (SCD1)',
    promotion_name   VARCHAR(200)  NOT NULL,
    promotion_type   VARCHAR(50)   NOT NULL
        COMMENT 'e.g. Percent Off, BOGO, Seasonal Clearance',

    discount_pct     DECIMAL(6,4)  NOT NULL DEFAULT 0
        COMMENT 'NON-ADDITIVE. Fraction: 0.1500 = 15%. Never SUM or AVG',

    start_date       DATE          NOT NULL,
    end_date         DATE          NOT NULL,

    dw_batch_id      BIGINT        NOT NULL,
    dw_source_system VARCHAR(50)   NOT NULL,
    dw_inserted_at   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at    DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                   ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_dim_promotion PRIMARY KEY (promotion_key),
    CONSTRAINT uq_dim_promotion_promotion_id UNIQUE (promotion_id),

    CONSTRAINT ck_dim_promotion_window CHECK (end_date >= start_date),
    -- A discount outside 0..1 means someone stored 15 meaning 15%, which would
    -- silently multiply revenue by fifteen. Cheap constraint, expensive bug.
    CONSTRAINT ck_dim_promotion_pct CHECK (discount_pct BETWEEN 0 AND 1)
) ENGINE = InnoDB
  COMMENT = 'SCD1 causal dimension. discount_pct is NON-ADDITIVE.';


-- -----------------------------------------------------------------------------
-- warehouse.dim_channel       SCD TYPE 1
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- To answer "how did they buy" — five rows: Web, Mobile App, Store, Phone,
-- Partner.
--
-- A reasonable objection: five low-cardinality values could have gone into
-- dim_order_junk with the other flags. They did not, because channel is a
-- PRIMARY ANALYTICAL AXIS — "revenue by channel" is a headline report, not an
-- incidental filter. Junk dimensions are for attributes nobody builds a report
-- around. Keeping channel separate also gives it room for its own attributes;
-- channel_group (Digital vs Physical) is already one, and a junk dimension
-- cannot carry a hierarchy without multiplying its own row count.
--
-- The line worth being able to defend: a junk dimension is for flags you filter
-- on, not for axes you group by.
--
-- KEYS
--   PK  channel_key — surrogate, for consistency.
--   UK  uq_dim_channel_channel_id — Type 1, one row per channel.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_channel (
    channel_key      BIGINT       NOT NULL AUTO_INCREMENT,

    channel_id       VARCHAR(50)  NOT NULL COMMENT 'Natural key. UNIQUE (SCD1)',
    channel_name     VARCHAR(100) NOT NULL COMMENT 'Web, Mobile App, Store, ...',
    channel_group    VARCHAR(50)  NOT NULL COMMENT 'Digital | Physical',

    dw_batch_id      BIGINT       NOT NULL,
    dw_source_system VARCHAR(50)  NOT NULL,
    dw_inserted_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                  ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_dim_channel PRIMARY KEY (channel_key),
    CONSTRAINT uq_dim_channel_channel_id UNIQUE (channel_id)
) ENGINE = InnoDB
  COMMENT = 'SCD1 reference list. Separate from junk dim: it is a report axis.';


-- -----------------------------------------------------------------------------
-- warehouse.dim_order_junk    SCD TYPE 0 — junk dimension
-- -----------------------------------------------------------------------------
-- WHY THIS DIMENSION EXISTS
-- Four low-cardinality order flags with nowhere sensible to live:
--   order_status      (5)  x  payment_method    (4)
--   shipping_priority (3)  x  is_gift           (2)   =  120 combinations
--
-- Three options, and the arithmetic decides it:
--   a) Four columns directly on fact_sales — four extra columns on the largest
--      table in the warehouse, repeated 50,000 times and growing.
--   b) Four separate dimension tables — four extra joins on every query, to
--      resolve tables of 5, 4, 3 and 2 rows.
--   c) ONE junk dimension holding the cross-product — 120 rows, one FK on the
--      fact, one join.
--
-- (c) wins on both axes at once, which is unusual and is why the pattern exists.
-- It also gives the flags a home where they can be labeled verbosely
-- ('Awaiting Fulfilment' rather than 'AWF') without that text landing on every
-- fact row.
--
-- WHY SCD TYPE 0
-- The cross-product is generated once and never changes. A new payment method
-- means generating the new combinations and appending them — existing rows are
-- never touched, because a combination's meaning cannot change.
--
-- KEYS
--   PK  order_junk_key — surrogate, INT not BIGINT. At ~120 rows BIGINT would
--       waste 4 bytes on every fact row for range that will never be used.
--       Fact-row width is the thing worth guarding (star_schema.md §6).
--   UK  uq_dim_order_junk_combo — the natural key is the full combination, and
--       it must be unique or the generator could emit a duplicate and split one
--       logical group's facts across two keys, quietly halving every total.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.dim_order_junk (
    order_junk_key    INT          NOT NULL AUTO_INCREMENT,

    order_status      VARCHAR(50)  NOT NULL,
    payment_method    VARCHAR(50)  NOT NULL,
    shipping_priority VARCHAR(50)  NOT NULL,
    is_gift           TINYINT(1)   NOT NULL DEFAULT 0,

    -- Pre-rendered label, so BI tools and `GROUP BY` output are readable without
    -- concatenating four columns in every query.
    junk_description  VARCHAR(255) NOT NULL,

    dw_batch_id       BIGINT       NOT NULL,
    dw_source_system  VARCHAR(50)  NOT NULL,
    dw_inserted_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                   ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_dim_order_junk PRIMARY KEY (order_junk_key),
    CONSTRAINT uq_dim_order_junk_combo UNIQUE (
        order_status, payment_method, shipping_priority, is_gift
    )
) ENGINE = InnoDB
  COMMENT = 'SCD0 junk dimension. 4 flags collapsed into 1 FK.';


-- =============================================================================
-- SECTION 4b — RESERVED DIMENSION MEMBERS
-- =============================================================================
-- architecture.md §5.2, star_schema.md §7, dimensional_modeling.md §8.
--
-- Seeded here, in schema.sql, rather than in sample_data.sql, because the
-- facts in Section 5 declare NOT NULL foreign keys into these dimensions. Every
-- reserved member must exist before a single fact row can land.
--
--   -1  UNKNOWN        A value was expected and could not be resolved.
--                      A sale references a product missing from the master feed.
--    0  NOT APPLICABLE No value is expected; legitimately absent.
--                      An order placed with no promotion.
--
-- WHY BOTH, RATHER THAN JUST ONE
-- "We don't know which promotion" and "there was no promotion" are different
-- business facts, and an analyst will eventually ask which a row represents.
-- Collapsing them makes that question unanswerable, and makes the Unknown count
-- useless as a data quality metric because it is polluted with legitimate blanks.
--
-- WHY THIS PATTERN INSTEAD OF NULL FOREIGN KEYS
-- Three concrete properties, each of which is otherwise lost:
--   1. No revenue is ever silently dropped. A row with an unresolvable product
--      still contributes its money to every total.
--   2. Every FK stays NOT NULL and every join stays an INNER JOIN. No defensive
--      LEFT JOINs, no COALESCE scattered through the codebase, no COUNT that is
--      subtly wrong because it skipped NULLs.
--   3. Data quality becomes measurable. `WHERE product_key = -1` is a direct
--      count of the feed's failure rate — a number you can put an SLA on.
--
-- REMINDER (§0): the NO_AUTO_VALUE_ON_ZERO session mode set at the top of this
-- file is what makes key 0 land as literal zero rather than as AUTO_INCREMENT 1.
-- These inserts are silently wrong without it.
--
-- Attributes read 'Unknown' / 'Not Applicable', never NULL and never '' —
-- dimension attributes are NOT NULL by rule (dimensional_modeling.md §4), and a
-- NULL here would reintroduce every problem the reserved member exists to solve.
--
-- Batch id 0 marks these as schema-provided rather than ETL-loaded; no
-- meta.etl_batch row will ever claim id 0, since that sequence starts at 1.
-- =============================================================================

-- dim_date. The sentinel dates are arbitrary but must be real, distinct, and far
-- outside any business range: full_date is NOT NULL and UNIQUE, so the two
-- reserved rows cannot share one. 1900 is chosen so that a sentinel accidentally
-- reaching a report is obviously wrong at a glance rather than plausible.
INSERT INTO warehouse.dim_date (
    date_key, full_date, year_number, quarter_number, month_number, month_name,
    day_of_month, week_of_year, day_name, year_month_number,
    is_weekend, is_holiday, fiscal_year, fiscal_quarter, prior_year_date_key
) VALUES
    (-1, '1900-01-01', 1900, 0, 0, 'Unknown',        0, 0, 'Unknown',        190001, 0, 0, 1900, 0, -1),
    ( 0, '1900-01-02', 1900, 0, 0, 'Not Applicable', 0, 0, 'Not Applicable', 190001, 0, 0, 1900, 0,  0);

INSERT INTO warehouse.dim_customer (
    customer_key, customer_id, customer_name, email, customer_segment,
    loyalty_tier, city, state_province, country, region, postal_code,
    signup_date, valid_from, valid_to, is_current, version_number,
    dw_batch_id, dw_source_system
) VALUES
    (-1, 'UNKNOWN', 'Unknown', 'unknown@unknown.invalid', 'Unknown',
     'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
     '1900-01-01', '1900-01-01', '9999-12-31', 1, 1, 0, 'SYSTEM'),
    ( 0, 'N/A', 'Not Applicable', 'na@unknown.invalid', 'Not Applicable',
     'Not Applicable', 'Not Applicable', 'Not Applicable', 'Not Applicable',
     'Not Applicable', 'N/A',
     '1900-01-01', '1900-01-01', '9999-12-31', 1, 1, 0, 'SYSTEM');

-- unit_cost / list_price of 0 on the Unknown member is a deliberate choice with
-- a visible consequence: a sale against an unresolved product reports 100% gross
-- margin, which is absurd on its face and therefore gets noticed and fixed.
-- Any non-zero guess would produce a plausible-looking wrong margin instead —
-- and a wrong number that looks right is worse than one that looks wrong.
INSERT INTO warehouse.dim_product (
    product_key, product_id, product_name, category, subcategory, brand,
    unit_cost, list_price, is_active, valid_from, valid_to, is_current,
    version_number, dw_batch_id, dw_source_system
) VALUES
    (-1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
     0.00, 0.00, 0, '1900-01-01', '9999-12-31', 1, 1, 0, 'SYSTEM'),
    ( 0, 'N/A', 'Not Applicable', 'Not Applicable', 'Not Applicable',
     'Not Applicable', 0.00, 0.00, 0, '1900-01-01', '9999-12-31', 1, 1, 0, 'SYSTEM');

INSERT INTO warehouse.dim_store (
    store_key, store_id, store_name, store_type, city, state_province,
    country, region, square_feet, opened_date, dw_batch_id, dw_source_system
) VALUES
    (-1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
     'Unknown', 'Unknown', 0, '1900-01-01', 0, 'SYSTEM'),
    ( 0, 'N/A', 'Not Applicable', 'Not Applicable', 'Not Applicable',
     'Not Applicable', 'Not Applicable', 'Not Applicable', 0, '1900-01-01', 0, 'SYSTEM');

-- The Not Applicable promotion is the one reserved member that carries real
-- analytical traffic: MOST order lines have no promotion, so promotion_key = 0
-- will be the single most common value in the column. That is the pattern
-- working exactly as designed — no NULLs, no LEFT JOIN, and "non-promoted sales"
-- expressible as an ordinary equality predicate.
INSERT INTO warehouse.dim_promotion (
    promotion_key, promotion_id, promotion_name, promotion_type,
    discount_pct, start_date, end_date, dw_batch_id, dw_source_system
) VALUES
    (-1, 'UNKNOWN', 'Unknown', 'Unknown', 0.0000,
     '1900-01-01', '9999-12-31', 0, 'SYSTEM'),
    ( 0, 'N/A', 'No Promotion', 'Not Applicable', 0.0000,
     '1900-01-01', '9999-12-31', 0, 'SYSTEM');

INSERT INTO warehouse.dim_channel (
    channel_key, channel_id, channel_name, channel_group,
    dw_batch_id, dw_source_system
) VALUES
    (-1, 'UNKNOWN', 'Unknown', 'Unknown', 0, 'SYSTEM'),
    ( 0, 'N/A', 'Not Applicable', 'Not Applicable', 0, 'SYSTEM');

INSERT INTO warehouse.dim_order_junk (
    order_junk_key, order_status, payment_method, shipping_priority,
    is_gift, junk_description, dw_batch_id, dw_source_system
) VALUES
    (-1, 'Unknown', 'Unknown', 'Unknown', 0,
     'Unknown order attributes', 0, 'SYSTEM'),
    ( 0, 'Not Applicable', 'Not Applicable', 'Not Applicable', 0,
     'Not applicable order attributes', 0, 'SYSTEM');


-- =============================================================================
-- SECTION 5 — warehouse: THE FACTS
-- =============================================================================
-- Created last: every foreign key below requires its dimension to exist.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- warehouse.fact_sales        TRANSACTION FACT
-- -----------------------------------------------------------------------------
-- GRAIN — ONE ROW PER LINE ITEM ON ONE CUSTOMER ORDER.
--
-- This is the most important sentence in the file. The grain is declared in
-- business language before a single column is chosen, and it then acts as a test
-- every proposed column must pass: if a column is not single-valued at line
-- grain, it does not belong here. Order-level shipping cost fails that test — it
-- must be allocated down to lines or left out entirely. A three-item order
-- produces exactly three rows.
--
-- WHY LINE GRAIN AND NOT ORDER GRAIN
-- Line item is the LOWEST grain the source supports, and the rule is to always
-- model at the lowest available grain. Order grain would make this table ~5x
-- smaller and permanently destroy every product-level question — no category
-- analysis, no product margin, no basket affinity. Rolling UP from fine grain is
-- a GROUP BY; rolling DOWN from coarse grain is impossible.
--
-- WHY THIS FACT TABLE EXISTS
-- It models the business process "a customer buys something" — a process, not a
-- department and not a report. Processes are stable for decades; org charts and
-- report requests change quarterly.
--
-- ---- COLUMN GROUPS ----------------------------------------------------------
--
-- 1. SURROGATE KEY. sales_key, AUTO_INCREMENT.
--
-- 2. FOREIGN KEYS — eight of them, one per dimension, all NOT NULL.
--    NOT NULL is only possible because of the reserved members seeded in §4b.
--    That is the payoff: every join in every downstream query is an INNER JOIN.
--
--    Note dim_date appears TWICE — order_date_key and ship_date_key. One
--    physical table serving two business roles is a ROLE-PLAYING DIMENSION,
--    disambiguated by aliasing at query time:
--
--        JOIN warehouse.dim_date od ON od.date_key = f.order_date_key
--        JOIN warehouse.dim_date sd ON sd.date_key = f.ship_date_key
--
--    Maintaining dim_date_order and dim_date_ship as separate physical copies
--    would be duplication with guaranteed drift.
--
--    An unshipped order gets ship_date_key = 0 (Not Applicable) — NOT -1. It is
--    not that we don't know the ship date; it is that there isn't one yet.
--
-- 3. DEGENERATE DIMENSION. order_number sits on the fact with no dimension
--    table, because it has no attributes of its own — everything else about the
--    order (customer, date, channel, status) is already a dimension, so a
--    dim_order would be a key column and nothing else. It still does real work:
--    COUNT(DISTINCT order_number) is the order count, and it forms the natural
--    key that makes the load idempotent.
--
-- 4. MEASURES. Additivity is documented per column below and is the single
--    easiest thing in this schema to get silently wrong.
--
-- ---- KEYS AND CONSTRAINTS ---------------------------------------------------
--
--   PK  pk_fact_sales (sales_key)
--       In InnoDB the PK is the CLUSTERED index — the table is physically
--       ordered by it. A monotonic AUTO_INCREMENT means inserts always append to
--       the rightmost page instead of splitting pages throughout the table,
--       which matters a great deal during a 50,000-row bulk load.
--       It also keeps every secondary index narrow: InnoDB appends the PK to
--       every secondary index, so a wide PK inflates every other index on the
--       table.
--
--   UK  uq_fact_sales_natural (order_number, order_line_number, order_date_key)
--       THE IDEMPOTENCY KEY, and the mechanism behind architecture.md §5.8.
--       Pipelines fail halfway. If recovery requires a human to reason about
--       what was partially applied, the pipeline is not production-ready. This
--       constraint lets the fact load use INSERT ... ON DUPLICATE KEY UPDATE, so
--       the recovery procedure is "run it again" — re-running a batch converges
--       to the same warehouse state instead of duplicating revenue.
--       order_date_key is included both because it is part of the business
--       natural key and because it is what the partitioned variant below
--       requires; keeping it here means the two variants have identical keys.
--
--   FK  Eight constraints, all RESTRICT/RESTRICT. See the Section 4 preamble.
--       Enforced deliberately (architecture.md §5.7): the usual advice to drop
--       FKs in a warehouse is a BULK-LOAD THROUGHPUT argument, and at 50,000
--       rows it does not apply. Enforced FKs make an orphaned fact row
--       structurally impossible. If load speed ever did become the bottleneck,
--       the correct move is DROP the constraints, bulk load, re-add them — not
--       never having had them.
--
--   IX  No additional indexes here. MySQL auto-creates one on each of the eight
--       FK columns (see the Section 4 preamble), which covers the single-key
--       star joins. Composite and covering indexes belong in indexes.sql, built
--       AFTER the bulk load — maintaining a B-tree during a 50,000-row insert is
--       slower than building it once at the end.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.fact_sales (
    sales_key           BIGINT        NOT NULL AUTO_INCREMENT,

    -- ---- Dimension foreign keys (all NOT NULL - see §4b) ------------------
    order_date_key      INT           NOT NULL
        COMMENT 'Role-playing dim_date. A KEY, NOT A NUMBER',
    ship_date_key       INT           NOT NULL
        COMMENT 'Role-playing dim_date. 0 = not yet shipped',
    customer_key        BIGINT        NOT NULL COMMENT 'SCD2 - version at sale',
    product_key         BIGINT        NOT NULL COMMENT 'SCD2 - version at sale',
    store_key           BIGINT        NOT NULL,
    promotion_key       BIGINT        NOT NULL COMMENT '0 = no promotion',
    channel_key         BIGINT        NOT NULL,
    order_junk_key      INT           NOT NULL COMMENT '4 order flags in 1 FK',

    -- ---- Degenerate dimension --------------------------------------------
    order_number        VARCHAR(50)   NOT NULL
        COMMENT 'DEGENERATE DIMENSION - no dim_order table exists',
    order_line_number   SMALLINT      NOT NULL,

    -- ---- Base measures ----------------------------------------------------
    quantity            INT           NOT NULL
        COMMENT 'ADDITIVE across every dimension',

    unit_price          DECIMAL(12,2) NOT NULL
        COMMENT 'NON-ADDITIVE rate. SUM is meaningless; AVG is misleading',

    -- Snapshotted from dim_product AS OF THE SALE DATE, not looked up at query
    -- time (architecture.md §5.6). Margin is a historical fact: if the supplier
    -- re-prices next month, last month's reported margin must not move.
    -- This is belt-and-braces with SCD2 on dim_product — the SCD2 row preserves
    -- the ATTRIBUTE history, this column preserves the TRANSACTION truth even if
    -- someone later joins to the current dimension row by mistake.
    unit_cost           DECIMAL(12,2) NOT NULL
        COMMENT 'Snapshotted at sale time. NON-ADDITIVE rate',

    discount_amount     DECIMAL(12,2) NOT NULL DEFAULT 0.00
        COMMENT 'ADDITIVE. Already allocated to line grain',
    tax_amount          DECIMAL(12,2) NOT NULL DEFAULT 0.00
        COMMENT 'ADDITIVE. Excluded from net_sales_amount by definition',

    -- ---- Derived measures — GENERATED ALWAYS AS ... STORED ----------------
    -- architecture.md §5.5. Computed by the database from base columns, never
    -- by a query and never by a view.
    --
    -- WHY: "net sales" defined independently in eleven dashboards becomes eleven
    -- different numbers, and the finance meeting stops being about the business
    -- and starts being about whose number is right. Defining it once in DDL
    -- makes that divergence STRUCTURALLY IMPOSSIBLE — a consumer cannot compute
    -- it differently, because they cannot compute it at all.
    --
    -- STORED, not VIRTUAL: reads vastly outnumber writes here, and only STORED
    -- columns can be indexed — which indexes.sql will need.
    --
    -- MySQL permits a generated column to reference an earlier one, so the
    -- definitions below read as the actual metric lineage rather than as four
    -- copies of the same arithmetic. ORDER IS THEREFORE LOAD-BEARING: each
    -- column must appear after the ones it depends on.
    --
    -- COST, accepted deliberately: changing a definition requires a DDL
    -- migration and a table rebuild. Chaining adds a second cost — MySQL will
    -- refuse to ALTER a generated column while another one depends on it, so
    -- redefining gross_sales_amount means dropping net_sales_amount and
    -- gross_profit_amount first and rebuilding them after.
    -- Both frictions are features. The definition of revenue SHOULD be hard to
    -- change, and a migration that forces you to look at every downstream metric
    -- is a migration doing its job.

    -- The two roots. Everything below is derived from these plus discount.
    gross_sales_amount  DECIMAL(14,2)
        AS (quantity * unit_price) STORED
        COMMENT 'ADDITIVE. Revenue before discount',

    total_cost_amount   DECIMAL(14,2)
        AS (quantity * unit_cost) STORED
        COMMENT 'ADDITIVE. COGS at the cost that applied on the sale date',

    -- Note these reference the generated columns above rather than restating
    -- their arithmetic. That is the whole point: "gross sales" has exactly ONE
    -- definition in this schema, so net sales cannot drift from it even by a
    -- typo in a later migration.
    net_sales_amount    DECIMAL(14,2)
        AS (gross_sales_amount - discount_amount) STORED
        COMMENT 'ADDITIVE. Gross less discount, EXCLUDING tax. THE revenue number',

    gross_profit_amount DECIMAL(14,2)
        AS (net_sales_amount - total_cost_amount) STORED
        COMMENT 'ADDITIVE. net_sales_amount - total_cost_amount',

    -- ---- Audit / lineage --------------------------------------------------
    dw_batch_id         BIGINT        NOT NULL,
    dw_source_system    VARCHAR(50)   NOT NULL,
    dw_inserted_at      DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at       DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                      ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_fact_sales PRIMARY KEY (sales_key),

    -- The idempotency key. See the header block above.
    CONSTRAINT uq_fact_sales_natural UNIQUE (
        order_number, order_line_number, order_date_key
    ),

    -- ---- Foreign keys -----------------------------------------------------
    -- Two constraints to the SAME table: the role-playing dimension, made
    -- physical. Constraint names are database-scoped in MySQL, not table-scoped,
    -- so every name in this file is globally distinct.
    CONSTRAINT fk_fact_sales_order_date FOREIGN KEY (order_date_key)
        REFERENCES warehouse.dim_date (date_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_ship_date FOREIGN KEY (ship_date_key)
        REFERENCES warehouse.dim_date (date_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key)
        REFERENCES warehouse.dim_customer (customer_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key)
        REFERENCES warehouse.dim_product (product_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_store FOREIGN KEY (store_key)
        REFERENCES warehouse.dim_store (store_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_promotion FOREIGN KEY (promotion_key)
        REFERENCES warehouse.dim_promotion (promotion_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_channel FOREIGN KEY (channel_key)
        REFERENCES warehouse.dim_channel (channel_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_sales_order_junk FOREIGN KEY (order_junk_key)
        REFERENCES warehouse.dim_order_junk (order_junk_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,

    -- A zero-quantity sale is a data error, not a business event. Returns are
    -- modeled as negative quantity, so the range is deliberately open below zero.
    CONSTRAINT ck_fact_sales_quantity CHECK (quantity <> 0),
    CONSTRAINT ck_fact_sales_amounts CHECK (
        unit_price      >= 0
    AND unit_cost       >= 0
    AND discount_amount >= 0
    AND tax_amount      >= 0
    ),
    CONSTRAINT ck_fact_sales_line_number CHECK (order_line_number > 0)
) ENGINE = InnoDB
  COMMENT = 'Transaction fact. GRAIN: one row per order line.';


-- -----------------------------------------------------------------------------
-- OPTIONAL VARIANT — fact_sales PARTITIONED BY YEAR  (architecture.md §5.4)
-- -----------------------------------------------------------------------------
-- PORT NOTE (2 of 5): partitioning and foreign keys are mutually exclusive here.
--
-- THE CONFLICT
-- architecture.md asks for both:
--   §5.4  range-partition fact_sales by year on order_date_key
--   §5.7  declare and enforce real foreign key constraints
--
-- On PostgreSQL you can have both. On MySQL you cannot, and this is not a
-- workaround-able limitation: InnoDB rejects foreign keys on a partitioned table
-- in BOTH directions — a partitioned table can neither declare an FK nor be
-- referenced by one. Attempting it fails outright:
--
--     ERROR 1506 (HY000): Foreign keys are not yet supported
--                         in conjunction with partitioning
--
-- WHICH ONE THIS FILE CHOSE, AND WHY
-- The version created above keeps the FOREIGN KEYS and drops the partitioning.
-- The reasoning:
--
--   * architecture.md §5.4 already concedes that below ~10M rows partitioning
--     "adds planning overhead for little gain", and states it is present because
--     the pattern is the point. This project targets ~50,000 rows — four orders
--     of magnitude below where partitioning starts paying.
--   * The foreign keys, by contrast, are load-bearing for reasoning that runs
--     through every approved document: NOT NULL FKs, reserved members, and
--     INNER-JOIN-everywhere all depend on referential integrity actually being
--     enforced. Dropping FKs would quietly turn several documented guarantees
--     into conventions.
--   * The failure modes are asymmetric. No partitioning costs some milliseconds
--     on a table small enough not to notice. No FKs costs silent orphan rows,
--     discovered later, in a report.
--
-- To switch: comment out the CREATE TABLE above, uncomment this, and remove the
-- eight FK constraints from it. The columns, PK and unique key are otherwise
-- identical, so nothing downstream changes.
--
-- NOTE THE PK. It is the composite (sales_key, order_date_key), not sales_key
-- alone. MySQL — like PostgreSQL — requires every UNIQUE and PRIMARY key to
-- contain all partitioning columns, because it cannot enforce uniqueness across
-- partitions it may never open. Same tradeoff architecture.md §5.4 already
-- accepts, arrived at for the same reason.
--
-- WHAT PARTITIONING WOULD BUY AT REAL SCALE
--   * Partition pruning — analytical queries are nearly always date-bounded, so
--     the optimizer can skip whole years unread.
--   * Instant archival: ALTER TABLE ... DROP PARTITION replaces a DELETE that
--     would bloat the table and its indexes.
--   * Per-partition maintenance — OPTIMIZE and index rebuilds run one year at a
--     time instead of locking the whole table.
--
--   CREATE TABLE warehouse.fact_sales (
--       sales_key           BIGINT        NOT NULL AUTO_INCREMENT,
--       order_date_key      INT           NOT NULL,
--       ship_date_key       INT           NOT NULL,
--       customer_key        BIGINT        NOT NULL,
--       product_key         BIGINT        NOT NULL,
--       store_key           BIGINT        NOT NULL,
--       promotion_key       BIGINT        NOT NULL,
--       channel_key         BIGINT        NOT NULL,
--       order_junk_key      INT           NOT NULL,
--       order_number        VARCHAR(50)   NOT NULL,
--       order_line_number   SMALLINT      NOT NULL,
--       quantity            INT           NOT NULL,
--       unit_price          DECIMAL(12,2) NOT NULL,
--       unit_cost           DECIMAL(12,2) NOT NULL,
--       discount_amount     DECIMAL(12,2) NOT NULL DEFAULT 0.00,
--       tax_amount          DECIMAL(12,2) NOT NULL DEFAULT 0.00,
--       gross_sales_amount  DECIMAL(14,2) AS (quantity * unit_price) STORED,
--       total_cost_amount   DECIMAL(14,2) AS (quantity * unit_cost) STORED,
--       net_sales_amount    DECIMAL(14,2) AS (gross_sales_amount - discount_amount) STORED,
--       gross_profit_amount DECIMAL(14,2) AS (net_sales_amount - total_cost_amount) STORED,
--       dw_batch_id         BIGINT        NOT NULL,
--       dw_source_system    VARCHAR(50)   NOT NULL,
--       dw_inserted_at      DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
--       dw_updated_at       DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
--                                         ON UPDATE CURRENT_TIMESTAMP(6),
--
--       -- Partition column MUST be part of every key.
--       CONSTRAINT pk_fact_sales PRIMARY KEY (sales_key, order_date_key),
--       CONSTRAINT uq_fact_sales_natural UNIQUE (order_number, order_line_number, order_date_key)
--
--       -- NO FOREIGN KEYS ARE POSSIBLE HERE. Referential integrity becomes the
--       -- ETL's responsibility, verified after each load by a
--       -- meta.data_quality_result rule of type REFERENTIAL rather than by the
--       -- database. That is a real downgrade: a convention enforced by code you
--       -- must remember to run, instead of a guarantee enforced by the engine.
--   ) ENGINE = InnoDB
--     COMMENT = 'Transaction fact, partitioned by year. NO FKs possible.'
--     PARTITION BY RANGE (order_date_key) (
--         PARTITION p_2022    VALUES LESS THAN (20230101),
--         PARTITION p_2023    VALUES LESS THAN (20240101),
--         PARTITION p_2024    VALUES LESS THAN (20250101),
--         PARTITION p_2025    VALUES LESS THAN (20260101),
--         PARTITION p_2026    VALUES LESS THAN (20270101),
--         -- MAXVALUE is the equivalent of PostgreSQL's DEFAULT partition. Without
--         -- it, a row past the last boundary is REJECTED, not stored — a silent
--         -- pipeline failure every 1 January until someone adds next year's
--         -- partition. Note the reserved date keys -1 and 0 land in p_2022, which
--         -- is harmless: they are below every boundary and are never scanned by a
--         -- real date-range query.
--         PARTITION p_max     VALUES LESS THAN MAXVALUE
--     );
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- warehouse.fact_inventory_snapshot     PERIODIC SNAPSHOT FACT
-- -----------------------------------------------------------------------------
-- GRAIN — ONE ROW PER PRODUCT, PER STORE, AT THE CLOSE OF ONE DAY.
--
-- WHY THIS FACT TABLE EXISTS
-- Two independent reasons, and both matter.
--
-- 1. THE HISTORY DOES NOT EXIST ANYWHERE ELSE. The source INVENTORY_LEVEL table
--    (er_diagram.md §1) holds one row per product/store with quantity_on_hand
--    mutated in place, and there is NO history table upstream at all. Yesterday's
--    stock position is destroyed by today's update. If this warehouse does not
--    capture it daily, it is gone permanently.
--
-- 2. IT IS A SECOND FACT TYPE, DELIBERATELY. A project with one transaction fact
--    can avoid the additivity discussion entirely. This one cannot.
--
-- HOW A SNAPSHOT FACT DIFFERS FROM A TRANSACTION FACT
--   * DENSE, not sparse. fact_sales stores only what happened. This table stores
--     a row for every product/store/day combination REGARDLESS OF ACTIVITY,
--     because quantity_on_hand is a STATE, not an EVENT. A product that did not
--     move still has a stock position, and "no row" would be indistinguishable
--     from "zero stock". This is the one documented exception to the usual
--     "facts are sparse" rule.
--   * Rows appear on a schedule, not in response to a business event.
--   * The measures are SEMI-ADDITIVE, which is the trap below.
--
-- ---- THE SEMI-ADDITIVE TRAP -------------------------------------------------
--
-- Store S1 holds 100 units of product P on Monday. Nothing moves Tue or Wed.
--
--     SELECT SUM(quantity_on_hand) ... WHERE snapshot_date_key
--                                      BETWEEN 20240101 AND 20240103;
--     -->  300   *** WRONG ***  There were never 300 units.
--
-- The same 100 physical units were counted three times. Correct answers depend
-- on the question being asked:
--
--     AVG(quantity_on_hand)                        --> 100  average position
--     ORDER BY snapshot_date_key DESC LIMIT 1      --> 100  closing balance
--     SUM(...) WHERE snapshot_date_key = 20240103  -->  OK  one day only
--
-- SUM is not wrong in general — it is wrong ACROSS THE DATE DIMENSION
-- SPECIFICALLY. Summing across products or across stores on a single day is
-- entirely valid. That is precisely what "semi-additive" means, and it is the
-- single most common source of silently incorrect warehouse reporting: the
-- number looks plausible, no error is raised, and nobody notices.
--
-- The column COMMENTs below carry this warning into the live catalog, where
-- SHOW FULL COLUMNS and most BI tools will surface it to whoever is about to
-- drag the field onto a report.
--
-- ---- KEYS AND CONSTRAINTS ---------------------------------------------------
--
--   PK  pk_fact_inventory_snapshot (snapshot_date_key, product_key, store_key)
--       A COMPOSITE NATURAL KEY, not a surrogate — a deliberate contrast with
--       fact_sales, and correct for three reasons:
--         * The grain IS the key. There cannot be two rows for one product, in
--           one store, on one day; the PK enforces the declared grain directly,
--           which is the cleanest form that enforcement can take.
--         * It makes the load idempotent for free. Re-running a day's snapshot
--           updates in place via ON DUPLICATE KEY UPDATE — no separate unique
--           constraint needed, unlike fact_sales.
--         * No surrogate key is needed because nothing references this table.
--           Facts are never referenced by anything; adding a surrogate key here
--           would cost 8 bytes per row and buy nothing.
--
--       COLUMN ORDER IS A PERFORMANCE DECISION, NOT ALPHABETICAL.
--       InnoDB clusters the table on the PK, so rows are physically stored in
--       PK order. Leading with snapshot_date_key means one day's snapshot lives
--       in contiguous pages, and a date-range scan — the dominant access
--       pattern by a wide margin — reads sequentially instead of seeking.
--       Leading with product_key instead would scatter each day across the whole
--       table and turn every "last 30 days" query into random I/O.
--
--   FK  Three constraints to the CONFORMED dimensions — dim_date, dim_product,
--       dim_store. That these are the SAME dimension rows fact_sales points at
--       is the entire payoff of conformance: it is what makes the drill-across
--       query in star_schema.md §2 possible, aligning sales and inventory on the
--       same product and the same store in one statement. If sales keyed on
--       product_id and inventory on sku_code with different category spellings,
--       that query could not be written at all — you would have two reports that
--       disagree and no way to reconcile them.
--
--       Only ONE date role here. There is no ship date for a stock position, so
--       dim_date does not role-play in this fact. Compare the bus matrix in
--       architecture.md §4.1, which marks dim_date "x2" for fact_sales only.
--
--   IX  None beyond the PK and the auto-created FK indexes. The composite PK
--       already covers date-leading access; product- and store-leading lookups
--       ride the FK indexes MySQL creates automatically.
-- -----------------------------------------------------------------------------
CREATE TABLE warehouse.fact_inventory_snapshot (
    -- Date first: clustering order is the access pattern. See above.
    snapshot_date_key      INT           NOT NULL
        COMMENT 'Close of this day. A KEY, NOT A NUMBER',
    product_key            BIGINT        NOT NULL COMMENT 'CONFORMED with fact_sales',
    store_key              BIGINT        NOT NULL COMMENT 'CONFORMED with fact_sales',

    -- ---- Semi-additive measures -------------------------------------------
    quantity_on_hand       INT           NOT NULL
        COMMENT 'SEMI-ADDITIVE: sum across product/store, NEVER across dates',
    quantity_on_order      INT           NOT NULL DEFAULT 0
        COMMENT 'SEMI-ADDITIVE: sum across product/store, NEVER across dates',
    inventory_value_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00
        COMMENT 'SEMI-ADDITIVE: qty_on_hand x unit_cost AT SNAPSHOT TIME',

    -- ---- Additive-at-a-point support --------------------------------------
    reorder_point          INT           NOT NULL DEFAULT 0
        COMMENT 'NON-ADDITIVE threshold attribute, not a measure',

    -- GENERATED rather than loaded, for the same reason as the fact_sales
    -- derived measures: "stockout" gets defined once, here, instead of being
    -- re-expressed as `quantity_on_hand = 0` in every query that needs it —
    -- where someone will eventually write `< 1` or `<= 0` and produce a number
    -- that disagrees with the dashboard next to it.
    --
    -- Unlike the semi-additive measures above, COUNTing or SUMing this flag
    -- across dates is legitimate and meaningful: it yields stockout-DAYS, which
    -- is a genuine event count rather than a repeated state.
    is_stockout            TINYINT(1)
        AS (quantity_on_hand <= 0) STORED
        COMMENT 'Derived. SUM across dates IS valid here: gives stockout-days',

    -- ---- Audit / lineage ---------------------------------------------------
    dw_batch_id            BIGINT        NOT NULL,
    dw_source_system       VARCHAR(50)   NOT NULL,
    dw_inserted_at         DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    dw_updated_at          DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                         ON UPDATE CURRENT_TIMESTAMP(6),

    -- The PK IS the declared grain, stated as a constraint.
    CONSTRAINT pk_fact_inventory_snapshot PRIMARY KEY (
        snapshot_date_key, product_key, store_key
    ),

    CONSTRAINT fk_fact_inventory_date FOREIGN KEY (snapshot_date_key)
        REFERENCES warehouse.dim_date (date_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_inventory_product FOREIGN KEY (product_key)
        REFERENCES warehouse.dim_product (product_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_fact_inventory_store FOREIGN KEY (store_key)
        REFERENCES warehouse.dim_store (store_key)
        ON DELETE RESTRICT ON UPDATE RESTRICT,

    -- Negative stock is physically impossible and always indicates a source or
    -- parse error. Catching it here converts a wrong number into a failed load.
    CONSTRAINT ck_fact_inventory_quantities CHECK (
        quantity_on_hand  >= 0
    AND quantity_on_order >= 0
    AND reorder_point     >= 0
    ),
    CONSTRAINT ck_fact_inventory_value CHECK (inventory_value_amount >= 0)
) ENGINE = InnoDB
  COMMENT = 'Periodic snapshot fact. GRAIN: product x store x day. SEMI-ADDITIVE.';


-- =============================================================================
-- SECTION 6 — VERIFICATION
-- =============================================================================
-- Run after the script completes. These check STRUCTURE, not data — the
-- warehouse is empty at this point apart from the reserved members.
-- =============================================================================

-- 1. Object inventory. Expect exactly three rows: meta 3, staging 5,
--    warehouse 9. `mart` correctly returns NO ROW at all — it holds only views,
--    which views.sql adds in Milestone 5.
SELECT table_schema, COUNT(*) AS table_count
FROM   information_schema.tables
WHERE  table_schema IN ('meta', 'staging', 'warehouse', 'mart')
  AND  table_type = 'BASE TABLE'
GROUP  BY table_schema
ORDER  BY FIELD(table_schema, 'meta', 'staging', 'warehouse', 'mart');

-- 2. Reserved members. Expect exactly 2 rows per dimension, keys -1 and 0.
--    If any dimension reports key 1 instead of 0, NO_AUTO_VALUE_ON_ZERO was not
--    in effect (see §0) and the schema must be rebuilt before loading.
SELECT 'dim_date'       AS dimension, date_key       AS reserved_key FROM warehouse.dim_date       WHERE date_key       <= 0
UNION ALL SELECT 'dim_customer',   customer_key   FROM warehouse.dim_customer   WHERE customer_key   <= 0
UNION ALL SELECT 'dim_product',    product_key    FROM warehouse.dim_product    WHERE product_key    <= 0
UNION ALL SELECT 'dim_store',      store_key      FROM warehouse.dim_store      WHERE store_key      <= 0
UNION ALL SELECT 'dim_promotion',  promotion_key  FROM warehouse.dim_promotion  WHERE promotion_key  <= 0
UNION ALL SELECT 'dim_channel',    channel_key    FROM warehouse.dim_channel    WHERE channel_key    <= 0
UNION ALL SELECT 'dim_order_junk', order_junk_key FROM warehouse.dim_order_junk WHERE order_junk_key <= 0
ORDER BY dimension, reserved_key;

-- 3. Referential integrity. Expect 11 rows:
--    8 for fact_sales (dim_date twice - the role-playing dimension made visible)
--    3 for fact_inventory_snapshot.
SELECT table_name, constraint_name, column_name, referenced_table_name
FROM   information_schema.key_column_usage
WHERE  table_schema = 'warehouse'
  AND  referenced_table_name IS NOT NULL
ORDER  BY table_name, constraint_name;

-- 4. Generated columns. Expect 8:
--      4  fact_sales               derived measures (architecture.md §5.5)
--      1  fact_inventory_snapshot  is_stockout
--      1  meta.data_quality_result is_passed
--      2  dim_customer/dim_product partial-unique-index substitutes, PORT NOTE 3
--    Filtered on EXTRA, not on an IS_GENERATED column: MySQL's
--    information_schema.COLUMNS has no such column (that spelling is the SQL
--    standard's, and PostgreSQL's). MySQL records it in EXTRA as
--    'STORED GENERATED' or 'VIRTUAL GENERATED'. All 8 below must read STORED —
--    a VIRTUAL column here could not be indexed.
SELECT table_schema, table_name, column_name, extra, generation_expression
FROM   information_schema.columns
WHERE  table_schema IN ('warehouse', 'meta')
  AND  extra LIKE '%GENERATED%'
ORDER  BY table_schema, table_name, ordinal_position;

-- 5. CHECK constraints — the single most likely silent failure on this file.
--    Expect 22:  meta 5  (etl_batch 3, data_quality_result 2)
--                warehouse 17 (dim_date 4, dim_customer 2, dim_product 3,
--                              dim_store 1, dim_promotion 2, fact_sales 3,
--                              fact_inventory_snapshot 2)
--    A count of 0 means the server predates 8.0.16, where CHECK is parsed and
--    then silently ignored. Every range guarantee in this schema would be
--    absent, the build would report complete success, and nothing would tell you
--    until a bad number reached a report.
SELECT constraint_schema, COUNT(*) AS check_constraint_count
FROM   information_schema.table_constraints
WHERE  constraint_schema IN ('meta', 'warehouse')
  AND  constraint_type = 'CHECK'
GROUP  BY constraint_schema WITH ROLLUP;

-- =============================================================================
-- END OF schema.sql
--
-- Next: sample_data.sql — generate dim_date (~4,000 rows), seed the SCD1
-- dimensions and the dim_order_junk cross-product, and produce ~50,000 staging
-- order lines.
--
-- Reminder from architecture.md §8: analytical indexes are built AFTER the bulk
-- load, in indexes.sql. Maintaining a B-tree during a 50,000-row insert is
-- slower than building it once at the end.
-- =============================================================================
