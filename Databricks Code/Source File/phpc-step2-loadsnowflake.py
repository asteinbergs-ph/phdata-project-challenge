# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 2. Load from Snowflake
# MAGIC 
# MAGIC 1. Create a DB in the Databricks cluster for the data to stay hosted while pulling KPIs
# MAGIC 2. Connect to Snowflake using the Spark connector & read data directly into tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS user_andrew_db;

# COMMAND ----------

# Hardcode username and password for Snowflake - this would be done using secrets in production
user = "asteinbergs"
password = "EmbsxMf3cC#VE7"

# snowflake connection options
options = {
  "sfUrl": "od23954.us-east-2.aws.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "USER_ANDREW",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "INTERVIEW_WH"
}

# COMMAND ----------

# Read data from Snowflake and write into Databricks table (Airlines)
spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRLINES") \
  .load() \
  .write \
  .mode("overwrite") \
  .saveAsTable("user_andrew_db.AIRLINES")
#.format("parquet") \

# COMMAND ----------

# Read data from Snowflake and write into Databricks table (Airports)
spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRPORTS") \
  .load() \
  .write \
  .mode("overwrite") \
  .saveAsTable("user_andrew_db.AIRPORTS")

# COMMAND ----------

# Read data from Snowflake and write into Databricks table (Flights)
spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "FLIGHTS") \
  .load() \
  .write \
  .mode("overwrite") \
  .saveAsTable("user_andrew_db.FLIGHTS")
