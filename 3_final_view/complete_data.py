# Databricks notebook source
from pyspark.sql import functions as F

customers = spark.table("retailsphere.gold.dim_customers")
products = spark.table("retailsphere.gold.dim_products")
sales = spark.table("retailsphere.gold.fact_sales")

# Check customer dimension uniqueness
duplicate_customers = (
    customers
    .groupBy("customer_id")
    .count()
    .filter(
        F.col("customer_id").isNull() | (F.col("count") > 1)
    )
)

# Check product dimension uniqueness
duplicate_products = (
    products
    .groupBy("product_id")
    .count()
    .filter(
        F.col("product_id").isNull() | (F.col("count") > 1)
    )
)

print("Invalid/duplicate customer keys:", duplicate_customers.count())
print("Invalid/duplicate product keys:", duplicate_products.count())

if duplicate_customers.limit(1).count() > 0:
    raise ValueError(
        "dim_customers has null or duplicate customer_id values. "
        "Resolve these before creating the dashboard view."
    )

if duplicate_products.limit(1).count() > 0:
    raise ValueError(
        "dim_products has null or duplicate product_id values. "
        "Resolve these before creating the dashboard view."
    )

# COMMAND ----------

dashboard_view = """
CREATE OR REPLACE VIEW retailsphere.gold.vw_sales_dashboard AS

SELECT
    -- Transaction identifiers
    f.order_id,
    f.order_date,

    -- Date attributes
    f.order_year,
    f.order_quarter,
    f.order_month,
    f.order_month_name,
    f.order_month_start,
    f.order_year_month,
    f.order_day_of_week,
    f.order_day_of_week_num,
    f.is_weekend,

    -- Customer attributes
    f.customer_id,
    c.customer_name,
    c.email AS customer_email,
    c.signup_date AS customer_signup_date,
    c.signup_year,
    c.signup_month,
    c.signup_quarter,
    c.signup_year_month,
    c.signup_month_start,
    c.customer_tenure_days,
    c.tenure_segment,
    c.loyalty_tier,
    c.loyalty_tier_rank,
    c.region AS customer_region,
    c.has_email,
    c.profile_completeness_pct,

    -- Product attributes
    f.product_id,
    f.product_name AS transaction_product_name,
    p.product_name AS master_product_name,
    f.category AS transaction_category,
    p.category AS master_category,
    p.unit_price AS master_unit_price,
    p.price_band,
    p.price_band_sort,
    p.category_avg_price,
    p.price_vs_category_avg_pct,
    p.category_price_rank,
    p.category_product_count,

    -- Transaction measures
    f.quantity,
    f.unit_price AS transaction_unit_price,
    f.discount_percent,
    f.gross_sales_amount,
    f.discount_amount,
    f.net_sales_amount,
    f.shipping_cost,
    f.sales_plus_shipping_amount,
    f.discount_band,

    -- Order and location attributes
    f.city,
    f.payment_method,
    f.order_status,

    -- Order status indicators
    f.is_completed_order,
    f.is_cancelled_order,
    f.is_returned_order,
    f.is_shipped_order,
    f.is_pending_order,

    -- Reporting indicators
    f.sales_record_count,
    f.is_reportable_sales_record

FROM retailsphere.gold.fact_sales AS f

LEFT JOIN retailsphere.gold.dim_customers AS c
    ON f.customer_id = c.customer_id

LEFT JOIN retailsphere.gold.dim_products AS p
    ON f.product_id = p.product_id
"""

spark.sql(dashboard_view)

print("Created view: retailsphere.gold.vw_sales_dashboard")

# COMMAND ----------

from pyspark.sql import functions as F

view_name = "retailsphere.gold.vw_sales_dashboard"

dashboard_df = spark.table(view_name)
fact_df = spark.table("retailsphere.gold.fact_sales")

# 1. Inspect the output
dashboard_df.printSchema()
display(dashboard_df.limit(10))

# 2. Confirm that the view preserves the fact-table row count
fact_count = fact_df.count()
view_count = dashboard_df.count()

print("Fact sales rows:", fact_count)
print("Dashboard view rows:", view_count)

assert fact_count == view_count, (
    "Row counts differ. Check dimension key uniqueness and join conditions."
)

# 3. Review overall dashboard KPIs
display(
    dashboard_df.agg(
        F.count("*").alias("total_sales_records"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("gross_sales_amount"), 2).alias("gross_sales"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount"),
        F.round(F.sum("net_sales_amount"), 2).alias("net_sales"),
        F.sum(
            F.when(F.col("customer_name").isNull(), 1).otherwise(0)
        ).alias("rows_without_customer_details"),
        F.sum(
            F.when(F.col("master_product_name").isNull(), 1).otherwise(0)
        ).alias("rows_without_product_details")
    )
)

# 4. Test whether the view is suitable for reporting
display(
    dashboard_df
    .groupBy("order_year_month")
    .agg(
        F.round(F.sum("net_sales_amount"), 2).alias("net_sales"),
        F.countDistinct("order_id").alias("distinct_orders")
    )
    .orderBy("order_year_month")
)