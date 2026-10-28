# Databricks notebook source
import pyspark.sql.functions as F

catalog_name = "retailsphere"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

brz_customers = spark.table(f"{catalog_name}.bronze.brz_customers")

display(brz_customers.limit(10))

# COMMAND ----------

customers_clean = (
    brz_customers
    .withColumn(
        "customer_id",
        F.upper(F.trim(F.col("customer_id")))
    )
    .withColumn(
        "customer_id",
        F.when(
            F.col("customer_id").isNull()
            | (F.col("customer_id") == "")
            | (F.lower(F.col("customer_id")) == "unknown"),
            F.lit(None).cast("string")
        ).otherwise(F.col("customer_id"))
    )
    .withColumn(
        "customer_id",
        F.when(
            F.col("customer_id").rlike(r"^CUST\d{5}$"),
            F.col("customer_id")
        ).otherwise(F.lit(None).cast("string"))
    )
)

# COMMAND ----------

display(customers_clean.limit(20))

# COMMAND ----------

customers_clean = (
    customers_clean
    .withColumn(
        "customer_name",
        F.initcap(F.trim(F.col("customer_name")))
    )
    .withColumn(
        "customer_name",
        F.when(
            F.col("customer_name").isNull()
            | (F.col("customer_name") == "")
            | (F.lower(F.col("customer_name")) == "unknown"),
            F.lit(None).cast("string")
        ).otherwise(F.col("customer_name"))
    )
)

# COMMAND ----------

display(customers_clean.limit(20))

# COMMAND ----------

email_pattern = (
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

customers_clean = (
    customers_clean
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "email",
        F.when(
            F.col("email").isNull()
            | (F.col("email") == "")
            | (F.lower(F.col("email")) == "unknown"),
            F.lit(None).cast("string")
        ).when(
            F.col("email").rlike(email_pattern),
            F.col("email")
        ).otherwise(
            F.lit(None).cast("string")
        )
    )
)

# COMMAND ----------

display(customers_clean.limit(20))

# COMMAND ----------

customers_clean = (
    customers_clean
    .withColumn(
        "signup_date",
        F.trim(F.col("signup_date"))
    )
    .withColumn(
        "signup_date",
        F.when(
            F.col("signup_date").isNull()
            | (F.col("signup_date") == "")
            | (F.lower(F.col("signup_date")) == "unknown"),
            F.lit(None).cast("string")
        ).otherwise(F.col("signup_date"))
    )
    .withColumn(
        "signup_date",
        F.expr("""
            to_date(
                coalesce(
                    try_to_timestamp(signup_date, 'yyyy-MM-dd'),
                    try_to_timestamp(signup_date, 'MM/dd/yyyy')
                )
            )
        """)
    )
)

# COMMAND ----------

display(customers_clean.limit(20))

# COMMAND ----------

valid_loyalty_tiers = [
    "Bronze",
    "Silver",
    "Gold",
    "Platinum"
]

customers_clean = (
    customers_clean
    .withColumn(
        "loyalty_tier",
        F.initcap(F.trim(F.col("loyalty_tier")))
    )
    .withColumn(
        "loyalty_tier",
        F.when(
            F.col("loyalty_tier").isin(valid_loyalty_tiers),
            F.col("loyalty_tier")
        ).otherwise(
            F.lit(None).cast("string")
        )
    )
)

# COMMAND ----------

display(customers_clean.limit(20))

# COMMAND ----------

valid_regions = [
    "East",
    "West",
    "North",
    "South"
]

customers_clean = (
    customers_clean
    .withColumn(
        "region",
        F.initcap(F.trim(F.col("region")))
    )
    .withColumn(
        "region",
        F.when(
            F.col("region").isin(valid_regions),
            F.col("region")
        ).otherwise(
            F.lit(None).cast("string")
        )
    )
)

# COMMAND ----------

display(customers_clean.limit(20))

# COMMAND ----------

(
    customers_clean.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.silver.slv_customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

brz_products = spark.table(f"{catalog_name}.bronze.brz_products")

display(brz_products.limit(10))

# COMMAND ----------

products_clean = (
    brz_products
    .withColumn(
        "product_id",
        F.upper(F.trim(F.col("product_id")))
    )
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").rlike(r"^PROD\d{4}$"),
            F.col("product_id")
        ).otherwise(F.lit(None).cast("string"))
    )
)

# COMMAND ----------

display(products_clean.limit(20))

# COMMAND ----------

products_clean = (
    products_clean
    .withColumn(
        "product_name",
        F.trim(F.col("product_name"))
    )
    .withColumn(
        "product_name",
        F.when(
            F.col("product_name").isNull()
            | (F.col("product_name") == "")
            | F.lower(F.col("product_name")).isin(
                "unknown", "n/a", "null"
            ),
            F.lit(None).cast("string")
        ).otherwise(F.col("product_name"))
    )
)

# COMMAND ----------

display(products_clean.limit(20))

# COMMAND ----------

valid_categories = [
    "Electronics",
    "Clothing",
    "Home & Kitchen",
    "Sports",
    "Books",
    "Beauty",
    "Toys"
]

products_clean = (
    products_clean
    .withColumn(
        "category",
        F.trim(F.col("category"))
    )
    .withColumn(
        "category",
        F.when(
            F.col("category").isNull()
            | (F.col("category") == ""),
            F.lit(None).cast("string")
        ).otherwise(F.initcap(F.col("category")))
    )
    .withColumn(
        "category",
        F.when(
            F.col("category").isin(valid_categories),
            F.col("category")
        ).otherwise(F.lit(None).cast("string"))
    )
)

# COMMAND ----------

display(products_clean.limit(20))

# COMMAND ----------

from pyspark.sql.types import DecimalType

products_clean = (
    products_clean
    .withColumn(
        "unit_price",
        F.trim(F.col("unit_price").cast("string"))
    )
    .withColumn(
        "unit_price",
        F.when(
            F.col("unit_price").rlike(r"^\d+(\.\d{1,2})?$"),
            F.col("unit_price").cast(DecimalType(10, 2))
        ).otherwise(F.lit(None).cast(DecimalType(10, 2)))
    )
)

# COMMAND ----------

display(products_clean.limit(20))

# COMMAND ----------

display(
    products_clean.filter(
        F.col("unit_price").isNull()
        | (F.col("unit_price") <= 0)
    )
)

# COMMAND ----------

(
    products_clean.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.silver.slv_products")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from decimal import Decimal

catalog_name = "retailsphere"

sales_columns = [
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
    "city",
    "payment_method",
    "order_status",
    "customer_email",
    "shipping_cost"
]

sales_raw_schema = StructType([
    StructField(column, StringType(), True)
    for column in sales_columns
])

sales_path = (
    f"/Volumes/{catalog_name}/source_data/raw_data/sales/*.csv"
)

sales_bronze = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .schema(sales_raw_schema)
    .csv(sales_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("ingested_at", F.current_timestamp())
)

(
    sales_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.bronze.brz_sales")
)

# COMMAND ----------

bronze_sales = spark.table(
    f"{catalog_name}.bronze.brz_sales"
)

silver_customers = spark.table(
    f"{catalog_name}.silver.slv_customers"
)

silver_products = spark.table(
    f"{catalog_name}.silver.slv_products"
)

customer_dim = (
    silver_customers
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        F.col("email").alias("master_customer_email")
    )
    .dropDuplicates(["customer_id"])
)

product_dim = (
    silver_products
    .filter(F.col("product_id").isNotNull())
    .select(
        "product_id",
        F.col("product_name").alias("master_product_name"),
        F.col("category").alias("master_category"),
        F.col("unit_price").alias("master_unit_price")
    )
    .dropDuplicates(["product_id"])
)

# COMMAND ----------

assert (
    customer_dim.count()
    == customer_dim.select("customer_id").distinct().count()
), "Customer dimension contains duplicate keys."

assert (
    product_dim.count()
    == product_dim.select("product_id").distinct().count()
), "Product dimension contains duplicate keys."

# COMMAND ----------

sales_clean = (
    bronze_sales
    .withColumn(
        "order_id",
        F.upper(F.trim(F.col("order_id")))
    )
    .withColumn(
        "product_id",
        F.upper(F.trim(F.col("product_id")))
    )
    .withColumn(
        "_customer_id_candidate",
        F.upper(F.trim(F.col("customer_id")))
    )
)

sales_clean = sales_clean.withColumn(
    "order_id",
    F.when(
        F.col("order_id").rlike(r"^ORD\d{6}$"),
        F.col("order_id")
    ).otherwise(F.lit(None).cast("string"))
)

sales_clean = sales_clean.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(r"^PROD\d{4}$"),
        F.col("product_id")
    ).otherwise(F.lit(None).cast("string"))
)

# COMMAND ----------

display(sales_clean.limit(10))

# COMMAND ----------

sales_clean = (
    sales_clean
    .withColumn(
        "_customer_id_format_valid",
        F.col("_customer_id_candidate").rlike(r"^CUST\d{5}$")
    )
    .join(
        customer_dim.select(
            F.col("customer_id").alias("_matched_customer_id")
        ),
        F.col("_customer_id_candidate")
        == F.col("_matched_customer_id"),
        "left"
    )
    .withColumn(
        "_customer_key_status",
        F.when(
            F.col("_customer_id_candidate").isNull()
            | (F.col("_customer_id_candidate") == "")
            | (F.lower(F.col("_customer_id_candidate")).isin(
                "unknown", "n/a", "null"
            )),
            F.lit("MISSING")
        )
        .when(
            ~F.col("_customer_id_format_valid"),
            F.lit("INVALID_FORMAT")
        )
        .when(
            F.col("_matched_customer_id").isNull(),
            F.lit("NOT_FOUND_IN_CUSTOMERS")
        )
        .otherwise(F.lit("MATCHED"))
    )
    .withColumn(
        "customer_id",
        F.when(
            F.col("_customer_key_status") == "MATCHED",
            F.col("_matched_customer_id")
        ).otherwise(F.lit(None).cast("string"))
    )
    .drop(
        "_matched_customer_id",
        "_customer_id_format_valid",
        "_customer_id_candidate"
    )
)

# COMMAND ----------

display(sales_clean.limit(10))

# COMMAND ----------

sales_clean = (
    sales_clean
    .withColumn(
        "order_date",
        F.trim(F.col("order_date"))
    )
    .withColumn(
        "order_date",
        F.when(
            F.lower(F.col("order_date")).isin(
                "", "n/a", "na", "unknown", "null"
            ),
            F.lit(None).cast("string")
        ).otherwise(F.col("order_date"))
    )
    .withColumn(
        "order_date",
        F.expr("""
            to_date(
                coalesce(
                    try_to_timestamp(order_date, 'yyyy-MM-dd'),
                    try_to_timestamp(order_date, 'yyyy/MM/dd'),
                    try_to_timestamp(order_date, 'yyyy/M/d'),
                    try_to_timestamp(order_date, 'MM/dd/yyyy'),
                    try_to_timestamp(order_date, 'M/d/yyyy'),
                    try_to_timestamp(order_date, 'dd/MM/yyyy'),
                    try_to_timestamp(order_date, 'd/M/yyyy'),
                    try_to_timestamp(order_date, 'MM-dd-yyyy'),
                    try_to_timestamp(order_date, 'M-d-yyyy'),
                    try_to_timestamp(order_date, 'dd-MM-yyyy'),
                    try_to_timestamp(order_date, 'd-M-yyyy'),
                    try_to_timestamp(order_date, 'dd-MMM-yyyy'),
                    try_to_timestamp(order_date, 'd-MMM-yyyy'),
                    try_to_timestamp(order_date, 'MMM-dd-yyyy'),
                    try_to_timestamp(order_date, 'MMM-d-yyyy')
                )
            )
        """)
    )
)

# COMMAND ----------

display(sales_clean.limit(20))

# COMMAND ----------

sales_clean = (
    sales_clean
    .withColumn(
        "quantity",
        F.lower(F.trim(F.col("quantity")))
    )
    .withColumn(
        "quantity",
        F.when(
            F.col("quantity") == "two",
            F.lit("2")
        ).otherwise(F.col("quantity"))
    )
    .withColumn(
        "quantity",
        F.expr("try_cast(quantity AS INT)")
    )
    .withColumn(
        "_quantity_issue",
        F.when(F.col("quantity").isNull(), "MISSING_OR_INVALID")
         .when(F.col("quantity") <= 0, "NON_POSITIVE")
         .when(F.col("quantity") == 999, "SUSPICIOUS_OUTLIER")
         .otherwise("OK")
    )
    .withColumn(
        "quantity",
        F.when(
            F.col("_quantity_issue") == "OK",
            F.col("quantity")
        ).otherwise(F.lit(None).cast("int"))
    )
)

# COMMAND ----------

display(sales_clean.orderBy(F.col("order_id").asc()).limit(25))

# COMMAND ----------

sales_clean = (
    sales_clean
    .withColumn(
        "discount_percent",
        F.regexp_replace(
            F.trim(F.col("discount_percent")),
            "%",
            ""
        )
    )
    .withColumn(
        "discount_percent",
        F.expr(
            "try_cast(discount_percent AS DECIMAL(5,2))"
        )
    )
    .withColumn(
        "_discount_issue",
        F.when(
            F.col("discount_percent").isNull(),
            F.lit("MISSING_OR_INVALID")
        )
        .when(
            (F.col("discount_percent") < 0)
            | (F.col("discount_percent") > 100),
            F.lit("OUT_OF_RANGE")
        )
        .otherwise(F.lit("OK"))
    )
    .withColumn(
        "discount_percent",
        F.when(
            F.col("_discount_issue") == "OK",
            F.col("discount_percent")
        ).otherwise(
            F.lit(None).cast("decimal(5,2)")
        )
    )
)

# COMMAND ----------

display(sales_clean.orderBy(F.col("order_id").asc()).limit(25))

# COMMAND ----------

# Preserve the source email as an audit field.
# Gold will use the customer dimension's email instead.
sales_clean = sales_clean.withColumnRenamed(
    "customer_email",
    "source_customer_email"
)

# Clean the source product attributes and price.
sales_clean = (
    sales_clean
    .withColumn(
        "_source_product_name",
        F.trim(F.col("product_name"))
    )
    .withColumn(
        "_source_category",
        F.trim(F.col("category"))
    )
    .withColumn(
        "_source_unit_price",
        F.trim(F.col("unit_price"))
    )
    .withColumn(
        "unit_price",
        F.expr("""
            try_cast(
                regexp_replace(trim(unit_price), '[$,]', '')
                AS DECIMAL(12,2)
            )
        """)
    )
)

# Join the product master using product_id only.
sales_clean = (
    sales_clean
    .join(product_dim, on="product_id", how="left")
    .withColumn(
        "_product_key_status",
        F.when(
            F.col("product_id").isNull(),
            F.lit("MISSING_OR_INVALID")
        )
        .when(
            F.col("master_product_name").isNull(),
            F.lit("NOT_FOUND_IN_PRODUCTS")
        )
        .otherwise(F.lit("MATCHED"))
    )
    .withColumn(
        "_unit_price_issue",
        F.when(
            F.col("unit_price").isNull(),
            F.lit("MISSING_OR_INVALID")
        )
        .when(
            F.col("unit_price") <= 0,
            F.lit("NON_POSITIVE")
        )
        .when(
            F.col("master_unit_price").isNotNull()
            & (
                F.col("unit_price")
                > F.col("master_unit_price") * F.lit(2)
            ),
            F.lit("EXTREME_PRICE")
        )
        .otherwise(F.lit("OK"))
    )
    .withColumn(
        "unit_price",
        F.when(
            F.col("_unit_price_issue") != "OK",
            F.col("master_unit_price").cast("decimal(12,2)")
        ).otherwise(
            F.col("unit_price").cast("decimal(12,2)")
        )
    )
    .withColumn(
        "_unit_price_differs_from_master",
        F.col("unit_price").isNotNull()
        & F.col("master_unit_price").isNotNull()
        & (
            F.col("unit_price")
            != F.col("master_unit_price").cast("decimal(12,2)")
        )
    )
    # Canonical product attributes from the product dimension
    .withColumn(
        "product_name",
        F.col("master_product_name")
    )
    .withColumn(
        "category",
        F.col("master_category")
    )
    .drop(
        "master_product_name",
        "master_category",
        "master_unit_price"
    )
)

# COMMAND ----------

sales_clean = (
    sales_clean
    .withColumn(
        "_source_total_amount",
        F.trim(F.col("total_amount"))
    )
    .withColumn(
        "total_amount",
        F.expr("""
            try_cast(
                regexp_replace(trim(total_amount), '[$,]', '')
                AS DECIMAL(14,2)
            )
        """)
    )
    .withColumn(
        "_calculated_total_amount",
        F.when(
            (F.col("quantity") > 0)
            & (F.col("unit_price") > 0)
            & F.col("discount_percent").isNotNull(),
            F.round(
                F.col("quantity")
                * F.col("unit_price")
                * (
                    F.lit(1)
                    - F.col("discount_percent") / F.lit(100)
                ),
                2
            ).cast("decimal(14,2)")
        ).otherwise(F.lit(None).cast("decimal(14,2)"))
    )
    .withColumn(
        "_amount_issue",
        F.when(
            F.col("total_amount").isNull(),
            F.lit("MISSING_OR_INVALID")
        )
        .when(
            F.col("total_amount") < 0,
            F.lit("NEGATIVE_AMOUNT")
        )
        .otherwise(F.lit("SOURCE_AMOUNT_AVAILABLE"))
    )
    .withColumn(
        "total_amount",
        F.when(
            F.col("total_amount").isNull()
            | (F.col("total_amount") < 0),
            F.col("_calculated_total_amount")
        ).otherwise(F.col("total_amount"))
    )
    .withColumn(
        "_amount_reconciliation_issue",
        F.when(
            F.col("_calculated_total_amount").isNull(),
            F.lit("CANNOT_RECONCILE")
        )
        .when(
            F.abs(
                F.col("_calculated_total_amount")
                - F.col("total_amount")
            ) > F.lit(0.01),
            F.lit("AMOUNT_MISMATCH")
        )
        .otherwise(F.lit("OK"))
    )
)

# COMMAND ----------

display(sales_clean.orderBy(F.col("order_id").asc()).limit(25))

# COMMAND ----------

sales_clean = (
    sales_clean
    .withColumn(
        "shipping_cost",
        F.lower(F.trim(F.col("shipping_cost")))
    )
    .withColumn(
        "shipping_cost",
        F.when(
            F.col("shipping_cost") == "free",
            F.lit("0")
        ).otherwise(F.col("shipping_cost"))
    )
    .withColumn(
        "shipping_cost",
        F.expr("""
            try_cast(
                regexp_replace(trim(shipping_cost), '[$,]', '')
                AS DECIMAL(10,2)
            )
        """)
    )
    .withColumn(
        "_shipping_cost_issue",
        F.when(
            F.col("shipping_cost").isNull(),
            F.lit("MISSING_OR_INVALID")
        )
        .when(
            F.col("shipping_cost") < 0,
            F.lit("NEGATIVE_COST")
        )
        .otherwise(F.lit("OK"))
    )
    .withColumn(
        "shipping_cost",
        F.when(
            F.col("_shipping_cost_issue") == "OK",
            F.col("shipping_cost")
        ).otherwise(F.lit(None).cast("decimal(10,2)"))
    )
)

# COMMAND ----------

display(sales_clean.orderBy(F.col("order_id").asc()).limit(25))

# COMMAND ----------

# City
sales_clean = (
    sales_clean
    .withColumn("city", F.trim(F.col("city")))
    .withColumn(
        "city",
        F.when(
            F.lower(F.col("city")).isin(
                "", "n/a", "na", "unknown", "null"
            ),
            F.lit(None).cast("string")
        )
        .otherwise(F.initcap(F.col("city")))
    )
    .withColumn(
        "city",
        F.when(F.col("city") == "Houston,", "Houston")
         .when(F.col("city") == "New York", "New York")
         .otherwise(F.col("city"))
    )
)

# Payment method
payment_method_clean = (
    F.lower(F.trim(F.col("payment_method")))
)

sales_clean = (
    sales_clean
    .withColumn(
        "payment_method",
        F.when(
            payment_method_clean.isin(
                "", "n/a", "na", "unknown", "null"
            ),
            F.lit(None).cast("string")
        )
        .when(
            payment_method_clean.isin("cod", "cash on delivery"),
            F.lit("Cash on Delivery")
        )
        .when(
            payment_method_clean == "cash",
            F.lit("Cash")
        )
        .when(
            payment_method_clean == "credit card",
            F.lit("Credit Card")
        )
        .when(
            payment_method_clean == "debit card",
            F.lit("Debit Card")
        )
        .when(
            payment_method_clean == "net banking",
            F.lit("Net Banking")
        )
        .when(
            payment_method_clean == "upi",
            F.lit("UPI")
        )
        .when(
            payment_method_clean == "paypal",
            F.lit("PayPal")
        )
        .otherwise(F.lit(None).cast("string"))
    )
)

# Order status
status_clean = F.lower(F.trim(F.col("order_status")))

sales_clean = (
    sales_clean
    .withColumn(
        "order_status",
        F.when(
            status_clean.isin(
                "", "n/a", "na", "unknown", "null"
            ),
            F.lit(None).cast("string")
        )
        .when(status_clean == "pending", F.lit("Pending"))
        .when(status_clean == "shipped", F.lit("Shipped"))
        .when(status_clean == "completed", F.lit("Completed"))
        .when(status_clean == "cancelled", F.lit("Cancelled"))
        .when(status_clean == "returned", F.lit("Returned"))
        .otherwise(F.lit(None).cast("string"))
    )
)

# Validate the source email for auditing only.
email_pattern = (
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

sales_clean = (
    sales_clean
    .withColumn(
        "source_customer_email",
        F.lower(F.trim(F.col("source_customer_email")))
    )
    .withColumn(
        "source_customer_email",
        F.when(
            F.col("source_customer_email").rlike(email_pattern),
            F.col("source_customer_email")
        ).otherwise(F.lit(None).cast("string"))
    )
)

# COMMAND ----------

display(sales_clean.orderBy(F.col("order_id").asc()).limit(25))

# COMMAND ----------

(
    sales_clean.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.silver.slv_sales")
)