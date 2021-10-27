# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 6
# MAGIC #### Airline with the most unique routes

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
    a.AIRLINE AS `Airline Name` \
    ,a.IATA_CODE AS `Airline Code` \
    ,COUNT(1) AS `Unique Routes` \
FROM user_andrew_db.AIRLINES a \
LEFT JOIN ( \
    SELECT \
        AIRLINE \
        ,ORIGIN_AIRPORT \
        ,DESTINATION_AIRPORT \
        ,COUNT(1) \
    FROM user_andrew_db.FLIGHTS \
    GROUP BY \
        AIRLINE \
        ,ORIGIN_AIRPORT \
        ,DESTINATION_AIRPORT \
 ) b \
  ON a.IATA_CODE = b.AIRLINE \
GROUP BY a.AIRLINE, a.IATA_CODE \
ORDER BY COUNT(1) DESC \
LIMIT 1"

df = sqlContext.sql(sql_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 6.1: Airline with the most unique routes**
# MAGIC > ###### *Summary record (Databricks Visualizer)*

# COMMAND ----------

display(df)
