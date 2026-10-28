# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

catalog_name = "retailsphere"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("region", StringType(), True)
])

# COMMAND ----------

cust_data_path = f"/Volumes/{catalog_name}/source_data/raw_data/customers/*.csv"

cust_df = spark.read.option("header", True).option("delimiter", ",").schema(customer_schema).csv(cust_data_path)

cust_df = cust_df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(cust_df.limit(10))

# COMMAND ----------

(
    cust_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True)
])

# COMMAND ----------

prod_data_path = f"/Volumes/{catalog_name}/source_data/raw_data/products/*.csv"

prod_df = spark.read.option("header", True).option("delimiter", ",").schema(product_schema).csv(prod_data_path)

prod_df = prod_df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(prod_df.limit(10))

# COMMAND ----------

(
    prod_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.bronze.brz_products")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_percent", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("shipping_cost", DoubleType(), True)
])

# COMMAND ----------

sales_data_path = f"/Volumes/{catalog_name}/source_data/raw_data/sales/*.csv"

sales_df = spark.read.option("header", True).option("delimiter", ",").schema(sales_schema).csv(sales_data_path)

sales_df = sales_df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("ingested_at", F.current_timestamp())

display(sales_df.limit(10))

# COMMAND ----------

(
    sales_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog_name}.bronze.brz_sales")
)