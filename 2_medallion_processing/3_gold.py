# Databricks notebook source
from pyspark.sql import functions as F

catalog_name = "retailsphere"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

slv_customers = spark.table(f"{catalog_name}.silver.slv_customers")

display(slv_customers.limit(10))

# COMMAND ----------

gold_customers = (
    slv_customers

    # Signup year
    .withColumn(
        "signup_year",
        F.year("signup_date")
    )

    # Signup month number: 1 to 12
    .withColumn(
        "signup_month",
        F.month("signup_date")
    )

    # Signup quarter: Q1, Q2, Q3, Q4
    .withColumn(
        "signup_quarter",
        F.when(
            F.col("signup_date").isNotNull(),
            F.concat(
                F.lit("Q"),
                F.quarter("signup_date").cast("string")
            )
        )
    )

    # Year-month label, e.g. 2024-06
    .withColumn(
        "signup_year_month",
        F.date_format("signup_date", "yyyy-MM")
    )

    # First day of signup month, retained as a date
    .withColumn(
        "signup_month_start",
        F.trunc("signup_date", "month")
    )
)

# COMMAND ----------

display(gold_customers.limit(10))

# COMMAND ----------

gold_customers = (
    gold_customers

    .withColumn(
        "customer_tenure_days",
        F.when(
            F.col("signup_date").isNotNull()
            & (F.col("signup_date") <= F.current_date()),
            F.datediff(
                F.current_date(),
                F.col("signup_date")
            )
        )
    )

    .withColumn(
        "tenure_segment",
        F.when(
            F.col("signup_date").isNull(),
            F.lit("Unknown")
        )
        .when(
            F.col("signup_date") > F.current_date(),
            F.lit("Invalid Signup Date")
        )
        .when(
            F.col("customer_tenure_days") < 90,
            F.lit("New Customer")
        )
        .when(
            F.col("customer_tenure_days") < 365,
            F.lit("Recent Customer")
        )
        .when(
            F.col("customer_tenure_days") < 1095,
            F.lit("Established Customer")
        )
        .otherwise(
            F.lit("Long-term Customer")
        )
    )
)

# COMMAND ----------

display(gold_customers.limit(10))

# COMMAND ----------

gold_customers = gold_customers.withColumn(
    "loyalty_tier_rank",
    F.when(F.col("loyalty_tier") == "Bronze", 1)
     .when(F.col("loyalty_tier") == "Silver", 2)
     .when(F.col("loyalty_tier") == "Gold", 3)
     .when(F.col("loyalty_tier") == "Platinum", 4)
     .otherwise(F.lit(None).cast("int"))
)

# COMMAND ----------

display(gold_customers.limit(10))

# COMMAND ----------

gold_customers = (
    gold_customers

    .withColumn(
        "has_email",
        F.when(
            F.col("email").isNotNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    .withColumn(
        "profile_completeness_pct",
        F.round(
            (
                F.when(F.col("customer_name").isNotNull(), 1)
                .otherwise(0)
                +
                F.when(F.col("email").isNotNull(), 1)
                .otherwise(0)
                +
                F.when(F.col("signup_date").isNotNull(), 1)
                .otherwise(0)
                +
                F.when(F.col("loyalty_tier").isNotNull(), 1)
                .otherwise(0)
                +
                F.when(F.col("region").isNotNull(), 1)
                .otherwise(0)
            ) / F.lit(5.0) * F.lit(100),
            2
        )
    )
)

# COMMAND ----------

display(gold_customers.limit(10))

# COMMAND ----------

display(
    gold_customers.select(
        "customer_id",
        "customer_name",
        "loyalty_tier",
        "loyalty_tier_rank",
        "region",
        "signup_date",
        "signup_year",
        "signup_quarter",
        "signup_year_month",
        "customer_tenure_days",
        "tenure_segment",
        "has_email",
        "profile_completeness_pct"
    ).orderBy("customer_id")
)

# COMMAND ----------

print(
    "Rows with missing customer_id:",
    gold_customers.filter(
        F.col("customer_id").isNull()
    ).count()
)

display(
    gold_customers
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

# COMMAND ----------

gold_customers_final = gold_customers.filter(
    F.col("customer_id").isNotNull()
)

(
    gold_customers_final.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.gold.dim_customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

slv_products = spark.table(f"{catalog_name}.silver.slv_products")

display(slv_products.limit(10))

# COMMAND ----------

gold_products = (
    slv_products

    .withColumn(
        "price_band",
        F.when(
            F.col("unit_price") <= 100,
            F.lit("Budget")
        )
        .when(
            F.col("unit_price") <= 250,
            F.lit("Mid-Range")
        )
        .when(
            F.col("unit_price") <= 350,
            F.lit("Premium")
        )
        .when(
            F.col("unit_price") > 350,
            F.lit("High-End")
        )
        .otherwise(F.lit(None).cast("string"))
    )

    .withColumn(
        "price_band_sort",
        F.when(F.col("price_band") == "Budget", 1)
         .when(F.col("price_band") == "Mid-Range", 2)
         .when(F.col("price_band") == "Premium", 3)
         .when(F.col("price_band") == "High-End", 4)
         .otherwise(F.lit(None).cast("int"))
    )
)

# COMMAND ----------

display(gold_products.limit(10))

# COMMAND ----------

gold_products = (
    gold_products

    .withColumn(
        "product_name_length",
        F.length(F.trim(F.col("product_name")))
    )

    .withColumn(
        "product_name_word_count",
        F.when(
            F.col("product_name").isNull()
            | (F.trim(F.col("product_name")) == ""),
            F.lit(None).cast("int")
        ).otherwise(
            F.size(
                F.split(F.trim(F.col("product_name")), r"\s+")
            )
        )
    )
)

# COMMAND ----------

display(gold_products.limit(10))

# COMMAND ----------

from pyspark.sql.window import Window

category_window = Window.partitionBy("category")

gold_products = (
    gold_products

    .withColumn(
        "category_avg_price",
        F.round(
            F.avg("unit_price").over(category_window),
            2
        ).cast("decimal(10,2)")
    )

    .withColumn(
        "price_vs_category_avg_pct",
        F.when(
            F.col("category_avg_price").isNotNull()
            & (F.col("category_avg_price") != 0)
            & F.col("unit_price").isNotNull(),
            F.round(
                (
                    F.col("unit_price")
                    - F.col("category_avg_price")
                )
                / F.col("category_avg_price")
                * 100,
                2
            )
        ).otherwise(F.lit(None).cast("double"))
    )
)

# COMMAND ----------

display(gold_products.orderBy(F.col("product_id").asc()).limit(10))

# COMMAND ----------

category_price_window = (
    Window
    .partitionBy("category")
    .orderBy(F.col("unit_price").desc())
)

category_count_window = Window.partitionBy("category")

gold_products = (
    gold_products

    .withColumn(
        "category_price_rank",
        F.rank().over(category_price_window)
    )

    .withColumn(
        "category_product_count",
        F.count("product_id").over(category_count_window)
    )
)

# COMMAND ----------

display(gold_products.limit(10))

# COMMAND ----------

display(
    gold_products
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "price_band",
        "price_band_sort",
        "category_avg_price",
        "price_vs_category_avg_pct",
        "category_price_rank",
        "category_product_count"
    )
    .orderBy("category", "category_price_rank")
)

# COMMAND ----------

gold_products_final = gold_products.filter(
    F.col("product_id").isNotNull()
)

(
    gold_products_final.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.gold.dim_products")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "retailsphere.silver.slv_sales"
raw_copy_table = "retailsphere.silver.slv_sales_gold_work_raw"
work_table = "retailsphere.silver.slv_sales_gold_work"

# Step 1: Read the original Silver sales table
silver_sales_df = spark.table(source_table)

print("Original Silver schema:")
silver_sales_df.printSchema()

print("Silver row count:", silver_sales_df.count())

# Step 2: Create a separate physical copy, including all columns
(
    silver_sales_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(raw_copy_table)
)

# Step 3: Read the copied table
sales_copy_df = spark.table(raw_copy_table)

# Identify every column beginning with "_"
underscore_columns = [
    column_name
    for column_name in sales_copy_df.columns
    if column_name.startswith("_")
]

print("Columns to remove:", underscore_columns)

# Step 4: Remove those columns from the working copy
sales_work_df = sales_copy_df.drop(*underscore_columns)

# Step 5: Save the cleaned working copy as a separate Delta table
(
    sales_work_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(work_table)
)

# Step 6: Verify the cleaned working table
sales_work_df = spark.table(work_table)

print("Working table schema:")
sales_work_df.printSchema()

print("Working table row count:", sales_work_df.count())

assert not any(
    column_name.startswith("_")
    for column_name in sales_work_df.columns
), "Some underscore-prefixed columns remain!"

# COMMAND ----------

sales_df = spark.table("retailsphere.silver.slv_sales_gold_work")

sales_df.printSchema()

# COMMAND ----------

sales_df = sales_df.select(
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount_percent",
    "total_amount",
    "shipping_cost",
    "city",
    "payment_method",
    "order_status"
)

# COMMAND ----------

sales_df = (
    sales_df
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    .withColumn(
        "discount_percent",
        F.col("discount_percent").cast("decimal(5,2)")
    )
    .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
    .withColumn(
        "shipping_cost",
        F.col("shipping_cost").cast("decimal(18,2)")
    )
)

# COMMAND ----------

sales_df = (
    sales_df
    .withColumn("order_year", F.year("order_date"))
    .withColumn(
        "order_quarter",
        F.when(F.col("order_date").isNotNull(),
               F.concat(F.lit("Q"), F.quarter("order_date")))
    )
    .withColumn("order_month", F.month("order_date"))
    .withColumn("order_month_name", F.date_format("order_date", "MMMM"))
    .withColumn("order_month_start", F.trunc("order_date", "month"))
    .withColumn(
        "order_year_month",
        F.date_format("order_date", "yyyy-MM")
    )
    # Spark dayofweek: Sunday = 1, Saturday = 7
    .withColumn(
        "order_day_of_week_num",
        F.dayofweek("order_date")
    )
    .withColumn(
        "order_day_of_week",
        F.date_format("order_date", "EEEE")
    )
    .withColumn(
        "is_weekend",
        F.when(
            F.dayofweek("order_date").isin([1, 7]), F.lit(True)
        ).when(
            F.col("order_date").isNotNull(), F.lit(False)
        ).otherwise(F.lit(None).cast("boolean"))
    )
)

# COMMAND ----------

display(sales_df.limit(10))

# COMMAND ----------

sales_df = (
    sales_df
    .withColumn(
        "gross_sales_amount",
        F.when(
            (F.col("quantity") > 0) & (F.col("unit_price") >= 0),
            F.round(
                F.col("quantity") * F.col("unit_price"), 2
            ).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "discount_amount",
        F.when(
            F.col("gross_sales_amount").isNotNull()
            & F.col("discount_percent").between(0, 100),
            F.round(
                F.col("gross_sales_amount")
                * F.col("discount_percent") / F.lit(100),
                2
            ).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "calculated_net_sales_amount",
        F.when(
            F.col("gross_sales_amount").isNotNull()
            & F.col("discount_amount").isNotNull(),
            (
                F.col("gross_sales_amount")
                - F.col("discount_amount")
            ).cast("decimal(18,2)")
        )
    )
)

# COMMAND ----------

sales_df = sales_df.withColumn(
    "net_sales_amount",
    F.when(
        F.col("total_amount").isNotNull()
        & (F.col("total_amount") >= 0),
        F.col("total_amount")
    ).otherwise(F.col("calculated_net_sales_amount"))
)

# COMMAND ----------

sales_df = sales_df.withColumn(
    "sales_plus_shipping_amount",
    F.when(
        F.col("net_sales_amount").isNotNull()
        & F.col("shipping_cost").isNotNull()
        & (F.col("shipping_cost") >= 0),
        (
            F.col("net_sales_amount")
            + F.col("shipping_cost")
        ).cast("decimal(18,2)")
    )
)

# COMMAND ----------

sales_df = sales_df.withColumn(
    "discount_band",
    F.when(
        F.col("discount_percent").isNull(),
        F.lit("Unknown")
    )
    .when(
        F.col("discount_percent") == 0,
        F.lit("No Discount")
    )
    .when(
        F.col("discount_percent") <= 10,
        F.lit("Low Discount")
    )
    .when(
        F.col("discount_percent") <= 20,
        F.lit("Medium Discount")
    )
    .otherwise(F.lit("High Discount"))
)

# COMMAND ----------

status = F.lower(F.trim(F.col("order_status")))

sales_df = (
    sales_df
    .withColumn(
        "is_completed_order",
        F.when(status == "completed", True).otherwise(False)
    )
    .withColumn(
        "is_cancelled_order",
        F.when(status == "cancelled", True).otherwise(False)
    )
    .withColumn(
        "is_returned_order",
        F.when(status == "returned", True).otherwise(False)
    )
    .withColumn(
        "is_shipped_order",
        F.when(status == "shipped", True).otherwise(False)
    )
    .withColumn(
        "is_pending_order",
        F.when(status == "pending", True).otherwise(False)
    )
)

# COMMAND ----------

sales_df = (
    sales_df
    .withColumn("sales_record_count", F.lit(1))
    .withColumn(
        "is_reportable_sales_record",
        F.when(
            F.col("order_date").isNotNull()
            & F.col("order_id").isNotNull()
            & F.col("product_id").isNotNull()
            & (F.col("quantity") > 0)
            & F.col("net_sales_amount").isNotNull(),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

gold_sales_df = sales_df.select(
    "order_id",
    "order_date",
    "order_year",
    "order_quarter",
    "order_month",
    "order_month_name",
    "order_month_start",
    "order_year_month",
    "order_day_of_week",
    "order_day_of_week_num",
    "is_weekend",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount_percent",
    "gross_sales_amount",
    "discount_amount",
    "net_sales_amount",
    "shipping_cost",
    "sales_plus_shipping_amount",
    "discount_band",
    "city",
    "payment_method",
    "order_status",
    "is_completed_order",
    "is_cancelled_order",
    "is_returned_order",
    "is_shipped_order",
    "is_pending_order",
    "sales_record_count",
    "is_reportable_sales_record"
)

# COMMAND ----------

display(gold_sales_df.limit(20))

# COMMAND ----------

(
    gold_sales_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.gold.fact_sales")
)