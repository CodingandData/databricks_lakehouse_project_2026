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
    sd.order_number,
    pr.product_key,
    cu.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.sales_amount,
    sd.quantity,
    sd.price
FROM workspace.silver.crm_sales sd
LEFT JOIN workspace.gold.dim_products pr
ON sd.product_key = pr.product_number
LEFT JOIN workspace.gold.dim_customers cu
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
    .saveAsTable("gold.fact_sales")
)