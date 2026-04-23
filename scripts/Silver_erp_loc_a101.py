# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

RENAME_MAP = {
    "CID": "customer_id",
    "CNTRY": "country"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.erp_loc_a101")

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Tranformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trimming

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

df = df.withColumn("CID", F.regexp_replace(col("CID"), "-", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Standardization & Consistency

# COMMAND ----------

df = df.withColumn(
    'CNTRY',
    F.when(col('CNTRY') == 'DE', 'Germany')
     .when(col('CNTRY').isin('US', 'USA'), 'United States')
     .when((col('CNTRY') == '') | (col('CNTRY').isNull()), 'N/A')
     .otherwise(col('CNTRY'))
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
    .saveAsTable("silver.erp_customer_location")
)