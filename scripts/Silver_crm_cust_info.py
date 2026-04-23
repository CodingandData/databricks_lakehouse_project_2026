# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col
from pyspark.sql.window import Window

# COMMAND ----------

RENAME_MAP = {
    "cst_id": "customer_id",
    "cst_key": "customer_number",
    "cst_firstname": "first_name",
    "cst_lastname": "last_name",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "created_date"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.crm_cust_info")

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

# MAGIC %md
# MAGIC ##Normalization

# COMMAND ----------

df = (
    df
    .withColumn(
        "cst_marital_status",
        F.when(F.upper(F.col("cst_marital_status")) == "S", "Single")
         .when(F.upper(F.col("cst_marital_status")) == "M", "Married")
         .otherwise("N/A")
    )
    .withColumn(
        "cst_gndr",
        F.when(F.upper(F.col("cst_gndr")) == "F", "Female")
         .when(F.upper(F.col("cst_gndr")) == "M", "Male")
         .otherwise("N/A")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 11
window_spec = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
df = (df.filter(F.col("cst_id").isNotNull())

.withColumn("flag_last", F.row_number().over(window_spec)).filter(F.col("flag_last") == 1).drop("flag_last")
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
    .saveAsTable("silver.crm_customers")
)