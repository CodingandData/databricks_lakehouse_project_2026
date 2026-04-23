# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import col, trim, lead, length
from pyspark.sql.window import Window

# COMMAND ----------

RENAME_MAP = {
    "prd_id": "product_id",
    "prd_key": "product_key",
    "cat_id": "category_id",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "product_start_date",
    "prd_end_dt": "product_end_date"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.crm_prd_info")

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trimming

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

df = df.withColumn(
    "cat_id",
    F.regexp_replace(F.substring(F.col("prd_key"), 1, 5), "-", "_")
).withColumn(
    "prd_key",
    F.substring(F.col("prd_key"), 7, F.length(F.col("prd_key")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Normalization

# COMMAND ----------

df = (
    df
    .withColumn(
        "prd_cost",
        F.when(length(col("prd_cost")) > 0, col("prd_cost").cast("int"))
        .otherwise(0)
    )
    .withColumn(
        "prd_line",
        F.when(F.upper(F.col("prd_line")) == "R", "Road")
         .when(F.upper(F.col("prd_line")) == "S", "Other Sales")
         .when(F.upper(F.col("prd_line")) == "M", "Mountain")
         .when(F.upper(F.col("prd_line")) == "T", "Touring")
         .otherwise("N/A")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning Dates

# COMMAND ----------

windowSpec = Window.partitionBy("prd_key").orderBy("prd_start_dt")

df = df.withColumn(
    "prd_end_dt",
    F.date_sub(lead(col("prd_start_dt")).over(windowSpec), 1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Renaming the Columns

# COMMAND ----------

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

df = df.select("product_id", "product_key", "category_id", "product_name", "product_cost", "product_line", "product_start_date", "product_end_date" )

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Into Silver Table

# COMMAND ----------

(
    df.write
      .mode("overwrite")
      .format("delta")
      .saveAsTable("silver.crm_products")
)