# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 2
# MAGIC #### On time percentage of each airline for the year 2015

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
    a.AIRLINE AS AIRLINE_NAME \
    ,a.IATA_CODE AS AIRLINE_CODE \
    ,(1 - (b.DELAYED_FLIGHTS / b.FLIGHT_COUNT)) * 100 AS ON_TIME_PCT \
    ,(b.SEVERE_DELAY_FLIGHTS / b.FLIGHT_COUNT) * 100  AS SEVERE_DELAY_FLIGHT_PCT \
FROM user_andrew_db.AIRLINES a \
LEFT JOIN ( \
  SELECT \
    AIRLINE \
    ,YEAR \
    ,COUNT(1) AS FLIGHT_COUNT \
    ,SUM( \
        CASE WHEN DEPARTURE_DELAY > 0 OR ARRIVAL_DELAY > 0 THEN 1 ELSE 0 END \
    ) AS DELAYED_FLIGHTS \
    ,SUM( \
        CASE WHEN DEPARTURE_DELAY > 60 OR ARRIVAL_DELAY > 60 THEN 1 ELSE 0 END \
    ) AS SEVERE_DELAY_FLIGHTS \
  FROM user_andrew_db.FLIGHTS \
  WHERE YEAR = 2015 \
  GROUP BY AIRLINE, YEAR \
) b \
  ON a.IATA_CODE = b.AIRLINE \
ORDER BY a.AIRLINE, a.IATA_CODE, b.YEAR" \

df = sqlContext.sql(sql_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 2.1: On time percentage of each airline for the year 2015**
# MAGIC > ###### *Summary table (Databricks Visualizer)*

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 2.1: On time percentage of each airline for the year 2015**
# MAGIC > ###### *Bar chart (Databricks Visualizer)*

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 2.3 (recommended): Severe delay (>60 minutes) percentage of each airline for the year 2015**
# MAGIC > ###### *Bar chart (Databricks Visualizer)*

# COMMAND ----------

display(df)
