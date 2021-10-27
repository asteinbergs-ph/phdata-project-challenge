# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 1B
# MAGIC #### Total number of flights by airport on a monthly basis

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
  a.AIRPORT AS AIRPORT_NAME \
  ,a.IATA_CODE AS AIRPORT_CODE \
  ,c.MONTH \
  ,c.YEAR \
  ,c.FLIGHT_COUNT \
FROM user_andrew_db.AIRPORTS a \
LEFT JOIN ( \
  SELECT \
    AIRPORT \
    ,MONTH \
    ,YEAR \
    ,COUNT(1) AS FLIGHT_COUNT \
  FROM ( \
    SELECT \
      ORIGIN_AIRPORT AS AIRPORT \
      ,MONTH \
      ,YEAR \
    FROM user_andrew_db.FLIGHTS \
    UNION ALL \
    SELECT \
      DESTINATION_AIRPORT AS AIRPORT \
      ,MONTH \
      ,YEAR \
    FROM user_andrew_db.FLIGHTS \
  ) a \
  GROUP BY AIRPORT, MONTH, YEAR \
) c \
  ON a.IATA_CODE = c.AIRPORT \
ORDER BY a.AIRPORT, a.IATA_CODE, c.YEAR, c.MONTH"
 
df = sqlContext.sql(sql_str)

# COMMAND ----------

airport_name = dbutils.widgets.get("Airports")

filtered_df = df.where(f"AIRPORT_NAME = \"{airport_name}\"")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 1B.1: Total number of flights by airport on a monthly basis**
# MAGIC > ###### *Summary table (Databricks Visualizer)*

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 1B.2: Total number of flights by airport on a monthly basis**
# MAGIC > ###### *Bar chart (Databricks Visualizer)*

# COMMAND ----------

display(filtered_df)
