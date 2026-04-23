# Databricks notebook source
# MAGIC %md
# MAGIC #Intialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import trim, col

# COMMAND ----------

RENAME_MAP = {
    "CID": "customer_id",
    "BDATE": "birth_date",
    "GEN": "gender"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.erp_cust_az12")

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
    "CID",
    F.when(col("CID").startswith("NAS"), F.substring(col("CID"), 4, F.length(col("CID"))))
    .otherwise(col("CID"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning Dates

# COMMAND ----------

df = df.withColumn(
    "bdate",
    F.when(col("bdate") > F.
    current_date(), F.lit(None))
    .otherwise(col("bdate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Normalization

# COMMAND ----------

df = df.withColumn(
    "gen",
    F.when(F.upper(col("gen")).isin('F', 'FEMALE'), F.lit('Female'))
    .when(F.upper(col("gen")).isin('M', 'MALE'), F.lit('Male'))
    .otherwise(F.lit('N/A'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming the Columns

# COMMAND ----------

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write Into Silver Table

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_customers")
)