# Databricks notebook source
# MAGIC %md
# MAGIC #Initialization

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Business Transformation and Modeling

# COMMAND ----------

query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.product_start_date, pn.product_key) AS product_key,
    pn.product_id,
    pn.product_key AS product_number, 
    pn.product_name,
    pn.category_id,
    pc.category,
    pc.sub_category,
    pc.maintenance,
    pn.product_cost,
    pn.product_line,
    pn.product_start_date
FROM workspace.silver.crm_products pn
LEFT JOIN workspace.silver.erp_product_details pc
    ON pn.category_id = pc.category_id
WHERE pn.product_end_date IS NULL -- Filter out all historical data
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write it to Gold Table

# COMMAND ----------

(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.dim_products")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dim_products