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

# DBTITLE 1,Cell 6
query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,
    ci.customer_id,
    ci.customer_number,
    ci.first_name,
    ci.last_name,
    la.country,
    ci.marital_status,
    CASE
        WHEN ci.gender <> 'N/A' THEN ci.gender
        ELSE COALESCE(ca.gender, 'N/A')
    END AS gender,
    ca.birth_date AS birthdate,
    ci.created_date AS create_date
FROM silver.crm_customers ci
LEFT JOIN silver.erp_customers ca
    ON ci.customer_number = ca.customer_id
LEFT JOIN silver.erp_customer_location la
    ON ci.customer_number = la.customer_id
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
    .saveAsTable("gold.dim_customers")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dim_customers