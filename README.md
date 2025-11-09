## Data Lake for E-Commerce Analytics (Olist + Clickstream, AWS)

### 1. Project Summary

This project builds a **production-style data lake** for an e-commerce company using:

* **Real Olist dataset (Kaggle)** for orders, customers, products, etc.
* **Synthetic clickstream events** generated to align with Olist customers, products, and orders.
* **AWS stack**: S3, Glue, Glue Data Catalog, Athena.
* Structured **Raw → Cleaned → Curated** zones.
* A **star schema** that supports analytics and ML use cases like funnel analysis, product performance, and customer behavior.

You can present this as:

> “I designed and implemented an AWS-based data lake that integrates transactional and behavioral data to provide a unified analytics layer for the business.”

---

## 2. Architecture Overview

**Services used:**

* **Amazon S3** – Data lake storage (raw, cleaned, curated).
* **AWS Glue Crawler** – Auto-detect schemas in raw data.
* **AWS Glue Jobs (PySpark)** – ETL from raw → cleaned.
* **AWS Glue Data Catalog** – Central metadata store for Athena.
* **Amazon Athena** – Serverless SQL engine to create curated tables & run analytics.

**High-level flow:**

1. Ingest Olist CSVs + generated clickstream CSV into **S3 Raw**.
2. Use **Glue Crawlers** to create tables in `ecom_raw_db`.
3. Use **Glue Jobs (PySpark)** to clean, standardize, and write **Parquet** into `ecom_cleaned_db`.
4. Use **Athena CTAS** to build **Curated Star Schema** in `ecom_curated_db`.
5. Run analytics queries for:

   * Funnel (page_view → purchase)
   * Product/category performance
   * Customer behavior
   * ML feature generation

This is effectively a small **data lakehouse**.

---

## 3. Data Sources

### 3.1 Olist (Real Data)

From Kaggle “Brazilian E-Commerce Public Dataset by Olist”:

Key files used:

* `olist_orders_dataset.csv`
* `olist_order_items_dataset.csv`
* `olist_customers_dataset.csv`
* `olist_products_dataset.csv`
* `olist_order_payments_dataset.csv`
* `olist_order_reviews_dataset.csv`
* `olist_sellers_dataset.csv`
* `olist_geolocation_dataset.csv`
* `product_category_name_translation.csv`

These provide real relationships between orders, customers, products, sellers, and locations.

### 3.2 Clickstream (Synthetic, Tailored to Olist)

File: `olist_clickstream_events.csv`

Generated using a Python script that:

* Uses real `customer_id`, `product_id`, and `order_id` from Olist.
* Creates realistic sessions:

  * `page_view`, `view_product`, `add_to_cart`, `checkout`, `purchase`
* Links some `purchase` events to real Olist `order_id` and timestamps.
* Includes:

  * `session_id`, `device_type`, `traffic_source`, `is_authenticated`,
  * `customer_city`, `customer_state`
* Includes non-converting sessions to simulate real funnels.

This gives you a **full journey**: browse → add to cart → purchase.

---

## 4. S3 Layout

Use a dedicated bucket:

```text
s3://ecom-olist-lake/

  raw/
    olist/
      olist_orders_dataset.csv
      olist_order_items_dataset.csv
      ...
    olist_clickstream/
      olist_clickstream_events.csv

  cleaned/
    customers/
    products/
    orders/
    order_items/
    olist_clickstream_events/

  curated/
    dim_customer/
    dim_product/
    fact_orders/
    fact_order_items/
    fact_events/

  athena-results/   (for Athena query output)
```

Raw = as received.
Cleaned = typed, deduped, Parquet.
Curated = modeled for analytics.

---

## 5. Step-by-Step Setup

### 5.1 Glue Data Catalog – Raw

1. Create database:

```sql
CREATE DATABASE IF NOT EXISTS ecom_raw_db;
```

2. Create **Glue Crawler** for Olist:

* Source: `s3://ecom-olist-lake/raw/olist/`
* Target DB: `ecom_raw_db`
* IAM role: read access to bucket.

3. Create **Glue Crawler** for clickstream:

* Source: `s3://ecom-olist-lake/raw/olist_clickstream/`
* Target DB: `ecom_raw_db`

Run both.
You now have raw tables like:

* `ecom_raw_db.olist_orders_dataset`
* `ecom_raw_db.olist_order_items_dataset`
* `ecom_raw_db.olist_clickstream_events`
* etc.

---

### 5.2 Cleaned Layer (Glue ETL)

Create:

```sql
CREATE DATABASE IF NOT EXISTS ecom_cleaned_db;
```

Then use **Glue Jobs (PySpark)** to:

#### Example: Clean Orders

Input: `ecom_raw_db.olist_orders_dataset`
Output: Parquet → `s3://ecom-olist-lake/cleaned/orders/`

Core logic:

* Cast timestamps.
* Derive `order_date`.
* Drop duplicates.

```python
from pyspark.sql.functions import to_timestamp, to_date, col

df = spark.table("ecom_raw_db.olist_orders_dataset")

clean_df = (
    df.select(
        "order_id",
        "customer_id",
        to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
        "order_status"
    )
    .withColumn("order_date", to_date(col("order_purchase_timestamp")))
    .dropDuplicates(["order_id"])
)

(clean_df
 .write.mode("overwrite")
 .format("parquet")
 .save("s3://ecom-olist-lake/cleaned/orders/"))
```

Create Athena table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ecom_cleaned_db.orders (
  order_id string,
  customer_id string,
  order_purchase_timestamp timestamp,
  order_status string,
  order_date date
)
STORED AS PARQUET
LOCATION 's3://ecom-olist-lake/cleaned/orders/';
```

Follow same pattern for:

* `customers` (id, city, state)
* `products` (id, category)
* `order_items` (order_id, product_id, price, freight_value)
* `olist_clickstream_events`:

  * cast `event_ts` → timestamp
  * create `event_date`
  * validate `event_type`

Clickstream cleaned table:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ecom_cleaned_db.olist_clickstream_events (
  event_id string,
  session_id string,
  customer_id string,
  event_type string,
  event_ts timestamp,
  product_id string,
  order_id string,
  device_type string,
  traffic_source string,
  is_authenticated int,
  customer_city string,
  customer_state string,
  event_date date
)
STORED AS PARQUET
LOCATION 's3://ecom-olist-lake/cleaned/olist_clickstream_events/';
```

---

### 5.3 Curated Layer (Star Schema in Athena)

Create curated DB:

```sql
CREATE DATABASE IF NOT EXISTS ecom_curated_db;
```

#### dim_customer

```sql
CREATE TABLE ecom_curated_db.dim_customer
WITH (
  format = 'PARQUET',
  external_location = 's3://ecom-olist-lake/curated/dim_customer/'
) AS
SELECT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state
FROM ecom_cleaned_db.customers;
```

#### dim_product

```sql
CREATE TABLE ecom_curated_db.dim_product
WITH (
  format = 'PARQUET',
  external_location = 's3://ecom-olist-lake/curated/dim_product/'
) AS
SELECT
  product_id,
  product_category_name
FROM ecom_cleaned_db.products;
```

#### fact_orders

(Unpartitioned for simplicity; monthly partitioning is optional.)

```sql
CREATE TABLE ecom_curated_db.fact_orders
WITH (
  format = 'PARQUET',
  external_location = 's3://ecom-olist-lake/curated/fact_orders/'
) AS
SELECT
  order_id,
  customer_id,
  order_purchase_timestamp,
  order_date,
  order_status
FROM ecom_cleaned_db.orders;
```

#### fact_order_items

```sql
CREATE TABLE ecom_curated_db.fact_order_items
WITH (
  format = 'PARQUET',
  external_location = 's3://ecom-olist-lake/curated/fact_order_items/'
) AS
SELECT
  order_id,
  product_id,
  price,
  freight_value
FROM ecom_cleaned_db.order_items;
```

#### fact_events (clickstream)

```sql
CREATE TABLE ecom_curated_db.fact_events
WITH (
  format = 'PARQUET',
  external_location = 's3://ecom-olist-lake/curated/fact_events/'
) AS
SELECT
  event_id,
  session_id,
  customer_id,
  event_type,
  event_ts,
  product_id,
  order_id,
  device_type,
  traffic_source,
  is_authenticated,
  customer_city,
  customer_state,
  event_date
FROM ecom_cleaned_db.olist_clickstream_events;
```

**Relationships:**

* `fact_orders.customer_id` → `dim_customer.customer_id`
* `fact_order_items.product_id` → `dim_product.product_id`
* `fact_events.order_id` → `fact_orders.order_id`
* `fact_events.customer_id` → `dim_customer.customer_id`

This gives a **full funnel + transaction model**.

---

## 6. Example Analytics (Showcase)

Use these in interview and screenshots.

### 6.1 Funnel Analysis

```sql
SELECT
  event_type,
  COUNT(DISTINCT session_id) AS sessions
FROM ecom_curated_db.fact_events
GROUP BY event_type
ORDER BY sessions DESC;
```

### 6.2 Conversion by Traffic Source

```sql
WITH purchases AS (
  SELECT DISTINCT session_id
  FROM ecom_curated_db.fact_events
  WHERE event_type = 'purchase'
)
SELECT
  e.traffic_source,
  COUNT(DISTINCT e.session_id) AS total_sessions,
  COUNT(DISTINCT p.session_id) AS converted_sessions,
  100.0 * COUNT(DISTINCT p.session_id) / COUNT(DISTINCT e.session_id) AS conversion_rate_pct
FROM ecom_curated_db.fact_events e
LEFT JOIN purchases p
  ON e.session_id = p.session_id
GROUP BY e.traffic_source;
```

### 6.3 Top Categories by Revenue

```sql
SELECT
  p.product_category_name,
  SUM(oi.price) AS revenue
FROM ecom_curated_db.fact_order_items oi
JOIN ecom_curated_db.dim_product p
  ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10;
```

These queries prove:

* Interconnected model works.
* Business questions are answered from curated zone.

