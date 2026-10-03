# Homework

## Exercise 1

For each scenario below, choose the most appropriate ingestion pattern and justify your choice in 1–2 sentences.

There may be more than one defensible answer, but your justification must be sound.

1. Your supplier sends a daily CSV file containing their entire product catalog, all 8000 product references. 
The file always reflects the current state, including price changes and discontinued products.

2. A payments service publishes one Kafka message per completed transaction. A transaction is 
immutable once written, it is never modified or cancelled.

3. Your company's CRM stores customer records (name, email, shipping address). Customers regularly 
update their details, and they can request full account deletion under GDPR.

4. You need to ingest yesterday's web clickstream: 300 million events stored as Parquet files partitioned by 
hour on S3. Files are written once and never modified.


---

## Exercise 2

You are building a daily analytics pipeline for a retail company. You receive two datasets 
each day:

- A **full export** of the product catalog: `product_id`, `name`, `category`, `price`
- **New orders** placed that day: `order_id`, `product_id`, `customer`, `amount`, `order_date`

Build a PySpark + Delta Lake pipeline that does the following:

1. Loads the product catalog using the ingestion pattern that fits it best
2. Loads the daily orders using the ingestion pattern that fits them best, stored 
partitioned by `order_date`
3. Enriches the orders by joining them with the product catalog to add `name` and `category` to each order
4. Applies the **Parallel Split** pattern on the enriched orders to produce two separate outputs:
   - A **summary table**: total revenue per `category` per `order_date` (aggregated — no individual order rows)
   - A **detail table**: every individual enriched order, partitioned by `order_date`

You are free to define the datasets inline with `spark.createDataFrame`.

Your submission must include:
- The working PySpark code
- A short written justification for each pattern choice you made (ingestion, partitioning, split)
