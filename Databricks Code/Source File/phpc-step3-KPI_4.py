# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 4
# MAGIC #### Cancellation reasons by airport

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
    a.AIRPORT AS `Airport Name` \
    ,a.IATA_CODE AS `Airport Code` \
    ,SUM(CASE WHEN CANCELLATION_REASON = 'A' THEN 1 ELSE 0 END) AS `Airline/Carrier` \
    ,SUM(CASE WHEN CANCELLATION_REASON = 'B' THEN 1 ELSE 0 END) AS `Weather` \
    ,SUM(CASE WHEN CANCELLATION_REASON = 'C' THEN 1 ELSE 0 END) AS `National Air System` \
    ,SUM(CASE WHEN CANCELLATION_REASON = 'D' THEN 1 ELSE 0 END) AS `Security` \
FROM user_andrew_db.AIRPORTS a \
LEFT JOIN ( \
    SELECT \
        ORIGIN_AIRPORT AS AIRPORT \
        ,CANCELLATION_REASON \
    FROM user_andrew_db.FLIGHTS \
    WHERE CANCELLED = 1 \
    UNION ALL \
    SELECT \
        DESTINATION_AIRPORT AS AIRPORT \
        ,CANCELLATION_REASON \
    FROM user_andrew_db.FLIGHTS \
    WHERE CANCELLED = 1 \
) b \
  ON a.IATA_CODE = b.AIRPORT \
GROUP BY a.AIRPORT, a.IATA_CODE \
ORDER BY a.AIRPORT, a.IATA_CODE"

df = sqlContext.sql(sql_str)

# COMMAND ----------

airport_name = dbutils.widgets.get("Airports")

filtered_df = df.where(f"`Airport Name` = \"{airport_name}\"")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 4.1: Cancellation reasons by airport**
# MAGIC > ###### *Summary record (Databricks Visualizer)*

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 4.2: Cancellation reasons by airport**
# MAGIC > ###### *Bar chart (Databricks Visualizer)*

# COMMAND ----------

display(filtered_df)
