# Dimensional Modeling

The design method behind this warehouse, as formalized by Ralph Kimball. Every
concept here is implemented somewhere in the repository, and each section says
where.

---

## 1. The core idea

Every business process produces **measurements** taken in a **context**.

> *"We sold **3 units** at **$19.99** — of a **Blue Widget**, to **Jane Doe**, at
> the **Austin store**, on **15 March 2024**, via the **mobile app**, under the
> **Spring Sale** promotion."*

Split that sentence in two:

| Part | Dimensional name | Answers | Becomes |
|---|---|---|---|
| 3 units, $19.99 | **Facts / measures** | *how much?* | columns on a **fact table** |
| Widget, Jane, Austin, Mar 15, app, Spring Sale | **Dimensions** | *who / what / where / when / how / why?* | **dimension tables** |

That's the entire model. Numbers you aggregate go in the middle; words you
filter and group by go around the edges.

The rule of thumb that resolves most arguments:

> **If you'd put it after `SUM(`, it's a fact. If you'd put it after `GROUP BY`,
> it's a dimension.**

---

## 2. Kimball's four-step design process

Applied in strict order. Skipping or reordering steps is the usual root cause of
a warehouse that has to be rebuilt.

### Step 1 — Select the business process

Model a *process*, not a department or a report. `fact_sales` models "a customer
buys something", not "the sales team's dashboard". Processes are stable for
decades; org charts and report requests change quarterly.

*Here:* order placement (`fact_sales`) and daily stock position
(`fact_inventory_snapshot`).

### Step 2 — Declare the grain

State in **plain business language** what one fact row represents. Do this
before choosing any column.

*Here:*
- `fact_sales` — *one row per line item on a customer order.*
- `fact_inventory_snapshot` — *one row per product, per store, at close of each day.*

**Always model at the lowest available grain.** Rolling *up* is a `GROUP BY`;
rolling *down* is impossible. Choosing order-grain instead of line-grain would
shrink the table ~5× and permanently forfeit every product-level question.

A declared grain is also a working test: any proposed column that isn't true at
the grain doesn't belong. Order-level shipping cost is not true at line grain —
so it must be *allocated* down to lines or left out.

### Step 3 — Identify the dimensions

Ask *"how will the business want to slice these numbers?"* Every answer is a
dimension, and each must be **single-valued at the declared grain**.

*Here:* date (×2 roles), customer, product, store, promotion, channel, order flags.

### Step 4 — Identify the facts

Numeric measurements true at the grain. Every one must be additive across as
many dimensions as possible — and where it isn't, that must be documented (§6).

*Here:* quantity, unit price, unit cost, discount, tax, plus generated
gross/net/profit amounts.

---

## 3. Fact tables

### Three types

| Type | Grain | Rows appear | Measures | *Here* |
|---|---|---|---|---|
| **Transaction** | One row per event | When something happens | Additive | `fact_sales` |
| **Periodic snapshot** | One row per entity per period | On a schedule, regardless of activity | **Semi-additive** | `fact_inventory_snapshot` |
| **Accumulating snapshot** | One row per workflow instance | Row is **`UPDATE`d** as it progresses | Lags/durations | *not modeled* |

Both included types are deliberate: they force the additivity discussion that a
single-fact project can avoid.

An accumulating snapshot would suit order fulfilment (`ordered_at`, `picked_at`,
`packed_at`, `shipped_at`, `delivered_at` on one row, each populated as the
milestone occurs). It's the one fact type that violates the usual
"facts are insert-only" rule.

### Fact table rules

1. **Mostly foreign keys and numbers.** If a fact has descriptive text columns,
   they probably belong in a dimension.
2. **Narrow matters.** Facts hold ~99 % of the rows, so every column costs real
   storage and I/O. Dimensions are tiny — denormalize those freely.
3. **Sparse, not dense.** Only store rows for things that happened. 500 products
   × 25 stores × 365 days = 4.5 M rows if you stored a row for every
   possibility; only ~50 k sales actually occurred.
   *(Snapshot facts are the exception — they are intentionally dense, because a
   product with no movement still has a stock position worth recording.)*
4. **Insert-only**, except accumulating snapshots.

### Factless fact tables

A fact table with no numeric measures — the *event itself* is the fact.

```sql
-- Which promotions were a product eligible for? (coverage, not sales)
CREATE TABLE fact_promotion_coverage (
    date_key      INT    NOT NULL,
    product_key   BIGINT NOT NULL,
    store_key     BIGINT NOT NULL,
    promotion_key BIGINT NOT NULL
);
```

Its value is answering **"what didn't happen?"** — products that were *eligible*
for a promotion but recorded no sales. That question is unanswerable from
`fact_sales` alone, because non-events leave no rows there.

---

## 4. Dimension tables

### Rules

1. **Wide and descriptive.** Many columns is good; each is a potential
   `GROUP BY` or filter.
2. **Denormalized.** Store `category` and `subcategory` inline (star), not in
   separate tables (snowflake). See [`../diagrams/star_schema.md`](../diagrams/star_schema.md) §3.
3. **Surrogate primary key.** Always.
4. **Verbose values, not codes.** Store `'Home Office'`, not `'HO'`. Reports
   should be readable without a lookup sheet, and `WHERE segment = 'HO'` is
   guesswork for a new analyst.
5. **No `NULL`s in attributes.** Use `'Unknown'` / `'Not Applicable'`. `NULL`
   silently drops rows from `GROUP BY` results and breaks `=` comparisons.

### Surrogate keys — why they're non-negotiable

A meaningless integer PK generated by the warehouse, with the source key kept as
an ordinary attribute.

| Reason | Detail |
|---|---|
| **SCD2 requires it** | After Jane moves, two rows share `customer_id = 'C-1042'`. The source key is no longer unique — it *cannot* be the PK |
| **Source independence** | A CRM migration that renumbers every customer becomes a dimension update, not a rewrite of 50 M fact rows |
| **Integration** | Two source systems both using `id = 1` for different customers coexist without collision |
| **Narrow facts** | 4–8 bytes beats a 40-char composite string, 50 M times over |
| **Unknown members** | You can't invent a natural key meaning "unknown"; you can reserve integer `-1` |

**The one exception is `dim_date`,** which uses a smart `YYYYMMDD` integer. Date
is uniquely safe: its meaning can never be restated, and the readable key makes
partition pruning and eyeball-debugging much easier. The discipline that keeps
this safe: **`date_key` is a key, not a number.** Never do arithmetic on it —
`order_date_key - ship_date_key` is meaningless. Date math goes through
`full_date`.

---

## 5. Slowly Changing Dimensions

The mechanism for the problem in §3 of [`oltp_vs_olap.md`](oltp_vs_olap.md): the
source overwrites attributes, destroying the history analytics needs.

### The types

| Type | Strategy | History | Use when |
|---|---|---|---|
| **0** | Retain original — never update | Original only | Immutable attributes (`signup_date`, `dim_date`) |
| **1** | **Overwrite** | ❌ none | Corrections; attribute isn't analytically interesting |
| **2** | **Add a new row** | ✅ full | **The default for anything you'd want to slice historically** |
| **3** | Add a *column* (`prior_value`) | Limited — one step back | A single known restructure ("old region" vs "new region") |
| **4** | Mini-dimension | Full, separated | Rapidly-changing attributes that would explode a Type 2 dim |
| **6** | Hybrid 1+2+3 | Both as-was and as-is | You need "sales under the rep who owned it *then*" **and** "…who owns it *now*" |

### Type 1 — overwrite

```sql
UPDATE warehouse.dim_store
SET store_name = 'Austin Downtown', dw_updated_at = now()
WHERE store_id = 'S-004';
```

Simple, but **it retroactively rewrites the past**: every historical report now
shows the new name. That is correct for a typo fix and wrong for a real change.

*Here:* `dim_store`, `dim_promotion`, `dim_channel`.

### Type 2 — add a new row *(the important one)*

Jane Doe was Silver tier in Austin. On 2024-06-01 she reaches Gold.

**Before**

| customer_key | customer_id | name | tier | city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|---|
| 501 | C-1042 | Jane Doe | Silver | Austin | 2023-01-15 | 9999-12-31 | `true` |

**After**

| customer_key | customer_id | name | tier | city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|---|
| 501 | C-1042 | Jane Doe | **Silver** | Austin | 2023-01-15 | **2024-05-31** | **`false`** |
| **892** | C-1042 | Jane Doe | **Gold** | Austin | **2024-06-01** | 9999-12-31 | `true` |

The mechanics that make this work:

1. **Facts before June point at key 501; facts after point at 892.** No fact row
   is ever updated — history is correct automatically, forever.
2. `valid_to = '9999-12-31'` on the open row, not `NULL`. A real high date keeps
   `BETWEEN` range logic simple and index-friendly; `NULL` forces
   `(valid_to IS NULL OR valid_to >= x)` everywhere.
3. `is_current` is redundant with `valid_to` but earns its place: `WHERE
   is_current` is far cheaper and clearer than a date predicate, and it's what
   the partial unique index is built on.

**The two query modes this unlocks:**

```sql
-- "As-was" — revenue by the tier customers held AT PURCHASE TIME.
-- No date predicate needed: the fact already points at the right version.
SELECT c.loyalty_tier, SUM(f.net_sales_amount)
FROM warehouse.fact_sales f
JOIN warehouse.dim_customer c ON c.customer_key = f.customer_key
GROUP BY c.loyalty_tier;

-- "As-is" — the same revenue attributed to customers' CURRENT tier.
SELECT cur.loyalty_tier, SUM(f.net_sales_amount)
FROM warehouse.fact_sales f
JOIN warehouse.dim_customer c   ON c.customer_key = f.customer_key
JOIN warehouse.dim_customer cur ON cur.customer_id = c.customer_id
                               AND cur.is_current
GROUP BY cur.loyalty_tier;
```

Both are legitimate; they answer different questions. *"How much did Gold
customers spend?"* is genuinely ambiguous, and a good analyst asks which one is
meant. **A Type 1 dimension can only ever answer the second.**

*Here:* `dim_customer` (tier, address), `dim_product` (cost, price) — implemented
in [`../etl_simulation.sql`](../etl_simulation.sql).

### Type 2's cost, stated plainly

Dimensions grow without bound. A customer whose attributes change monthly
generates 12 rows a year. That's why choosing Type 2 *selectively* — only for
attributes worth slicing historically — is a design decision, not a default to
apply everywhere. `dim_store` is Type 1 here specifically to demonstrate that
the choice was made rather than assumed.

### Type 4 — mini-dimensions

When some attributes change far faster than others, splitting prevents an
explosion. Customer *demographics* (age band, income band, credit score band)
might change quarterly while name and address are stable. Put the volatile
attributes in `dim_customer_demographics` and give the fact **two** FKs. The
combinatorial growth lands in a small dimension instead of multiplying the main
one.

---

## 6. Additivity

Which aggregate functions are *legal* on a measure, across which dimensions.

| Class | `SUM` valid across | Example | Correct handling |
|---|---|---|---|
| **Additive** | **all** dimensions | `net_sales_amount` | `SUM` freely |
| **Semi-additive** | some, **not date** | `quantity_on_hand` | `AVG` over time, or closing balance |
| **Non-additive** | **none** | `unit_price`, `discount_pct` | Recompute from additive components |

### The non-additive rule

```sql
-- ✗ WRONG — averages an average, weighting a 1-unit sale
--   the same as a 1,000-unit sale
SELECT AVG(unit_price) FROM warehouse.fact_sales;

-- ✓ RIGHT — weighted by volume, derived from additive components
SELECT SUM(net_sales_amount) / NULLIF(SUM(quantity), 0) AS avg_selling_price
FROM warehouse.fact_sales;
```

**Ratios are computed *after* aggregation, never averaged.** `AVG` of a
percentage column is one of the most common wrong numbers in business
intelligence, and it never looks wrong.

### The semi-additive rule

Summing `quantity_on_hand` across dates double-counts the same physical units —
100 units held for 3 days is not 300 units. Full worked example in
[`../diagrams/star_schema.md`](../diagrams/star_schema.md) §5.

---

## 7. Special dimension patterns

### Degenerate dimension

A source identifier with **no attributes of its own**. `order_number` is a good
example: knowing an order is `ORD-000123` tells you nothing further — everything
else about the order (customer, date, channel) is already a dimension.

A `dim_order` table would contain a key and nothing else. So the identifier stays
on the fact table with no dimension at all. It still does real work: grouping
lines into baskets (`COUNT(DISTINCT order_number)` = order count), and forming
the natural key that makes the fact load idempotent.

*Here:* `fact_sales.order_number`.

### Role-playing dimension

One physical dimension serving several logical roles. `dim_date` is joined twice
to `fact_sales` — once as order date, once as ship date — and disambiguated by
aliasing:

```sql
JOIN warehouse.dim_date od ON od.date_key = f.order_date_key
JOIN warehouse.dim_date sd ON sd.date_key = f.ship_date_key
```

Maintaining `dim_date_order` and `dim_date_ship` as separate copies would be
duplication with guaranteed drift. Optionally, create views (`vw_dim_order_date`)
with prefixed column names so BI tools present the roles distinctly.

*Here:* `order_date_key` and `ship_date_key`.

### Junk dimension

Several low-cardinality flags that don't justify dimensions of their own —
`order_status` (5) × `payment_method` (4) × `shipping_priority` (3) ×
`is_gift` (2).

The options are: four columns on the fact (wide, 50 M times over), four tiny
dimension tables (four extra joins), or **one junk dimension** holding the
cross-product — 5 × 4 × 3 × 2 = 120 rows, one FK on the fact, one join.

*Here:* `dim_order_junk`.

### Conformed dimensions

Dimensions **shared, identically, across multiple fact tables**. This is the
foundation of Kimball's bus architecture and the thing that turns a set of data
marts into a warehouse.

Because `fact_sales` and `fact_inventory_snapshot` join to the *same*
`dim_product` rows, you can drill across processes in one query. If sales keyed
on `product_id` and inventory on `sku_code`, with different category spellings,
that query is impossible — you'd have two reports that disagree and no way to
reconcile them.

*Here:* `dim_date`, `dim_product`, `dim_store` — see the bus matrix in
[`../architecture.md`](../architecture.md) §4.1.

### Bridge tables

For genuine **many-to-many** relationships between a fact and a dimension —
several sales reps on one order, several diagnoses on one hospital visit.

A bridge table sits between them with an allocation factor (weights summing to
1.0 per group) so you can report either *allocated* revenue (sums correctly,
each rep gets a share) or *full* revenue (double-counts on purpose — "revenue
this rep touched"). Both are valid; which one is meant must be stated.

*Not modeled here* — each order line has exactly one product.

---

## 8. Unknown and Not-Applicable members

Every dimension is seeded with two reserved rows before any real load.

| Key | Member | Means |
|---:|---|---|
| `-1` | **Unknown** | A value was expected but couldn't be resolved (late-arriving product, bad source data) |
| `0` | **Not Applicable** | No value is expected — legitimately absent (an order with no promotion) |

The alternative — `NULL` foreign keys — costs you: every join becomes a
defensive `LEFT JOIN`, `COUNT(promotion_key)` silently excludes rows,
`GROUP BY` drops the `NULL` group from some tools' output, and every predicate
needs `IS NULL` handling. With reserved members, every FK is `NOT NULL`, every
join is an `INNER JOIN`, and *"unresolvable rows"* becomes a measurable data
quality metric: `WHERE product_key = -1`.

The Unknown/N-A distinction is not pedantry. *"We don't know the promotion"* and
*"there was no promotion"* have different business meanings, and an analyst will
eventually ask which one a row represents.

---

## 9. Late-arriving data

Two real problems this model has answers for.

**Late-arriving dimension (early-arriving fact).** A sale references a product
not yet in the master feed. Options: reject the row (loses revenue — no),
hold it in a suspense area (adds a reprocessing pipeline), or **insert an
inferred placeholder row** with the natural key and `'Unknown'` attributes, then
let the next dimension load fill it in. The last preserves the money and
self-heals.

**Late-arriving fact.** A sale from three months ago arrives today. It must join
to the dimension version that was current **on the sale date**, not today's:

```sql
JOIN warehouse.dim_customer c
  ON c.customer_id = s.customer_id
 AND s.order_date BETWEEN c.valid_from AND c.valid_to   -- NOT c.is_current
```

This is precisely why `valid_from`/`valid_to` are stored rather than relying on
`is_current` alone.

---

## 10. Kimball vs Inmon

The classic architectural debate. Both are legitimate; the honest answer names
the tradeoff rather than picking a side reflexively.

| | **Kimball** (bottom-up) | **Inmon** (top-down) |
|---|---|---|
| Build order | Business process at a time | Enterprise 3NF warehouse first, marts after |
| Core structure | Star schemas + conformed dimension bus | Normalized CIF, then dimensional marts |
| Time to first value | **Weeks** | Months to years |
| Integration | Emerges via conformed dimensions | Designed centrally up front |
| Redundancy | Accepted | Minimized |
| Risk | Marts can drift apart without dimension governance | Long delivery; may never reach users |

**This project is Kimball**, because it fits the goal: deliver a usable star for
one process quickly, and demonstrate that integration comes from conformed
dimensions rather than from a big up-front normalized layer.

**Where Inmon genuinely wins:** heavily regulated environments needing a single
auditable enterprise record, or organizations with many source systems where
central integration must be designed rather than allowed to emerge.

**In practice most modern stacks are hybrid** — a raw/normalized landing layer
(Inmon-ish) feeding dimensional marts (Kimball-ish), which is essentially the
staging → warehouse → mart layering used here. And *Data Vault* is a third
option that shines when source systems change constantly and full auditability
is mandatory.

---

## 11. Concept → implementation map

| Concept | Where it lives |
|---|---|
| Grain declaration | [`../architecture.md`](../architecture.md) §3 |
| Star schema | [`../diagrams/star_schema.md`](../diagrams/star_schema.md) |
| Surrogate keys | `schema.sql` — every `dim_*` |
| SCD Type 2 | `dim_customer`, `dim_product` + `etl_simulation.sql` |
| SCD Type 1 | `dim_store`, `dim_promotion`, `dim_channel` |
| Role-playing dimension | `fact_sales.order_date_key` / `ship_date_key` |
| Degenerate dimension | `fact_sales.order_number` |
| Junk dimension | `dim_order_junk` |
| Conformed dimensions | Bus matrix — [`../architecture.md`](../architecture.md) §4.1 |
| Unknown / N-A members | `schema.sql` seed + `sample_data.sql` |
| Semi-additive measures | `fact_inventory_snapshot.quantity_on_hand` |
| Transaction fact | `fact_sales` |
| Periodic snapshot fact | `fact_inventory_snapshot` |
| Late-arriving handling | `etl_simulation.sql` |
