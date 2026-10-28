# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create catalog if not exists retailsphere;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog retailsphere;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists retailsphere.bronze;
# MAGIC create schema if not exists retailsphere.silver;
# MAGIC create schema if not exists retailsphere.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists source_data;