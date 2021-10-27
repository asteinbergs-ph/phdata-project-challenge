# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 3
# MAGIC #### Airlines with the largest number of delays

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
    a.AIRLINE AS `Airline Name` \
    ,a.IATA_CODE AS `Airline Code` \
    ,b.DELAYED_FLIGHTS AS `Delayed Flights` \
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
  GROUP BY AIRLINE, YEAR \
) b \
  ON a.IATA_CODE = b.AIRLINE \
ORDER BY a.AIRLINE, a.IATA_CODE, b.YEAR" \

df = sqlContext.sql(sql_str)

# COMMAND ----------

# Select top 5 airlines, ordered by total number of delayed flights

df.createOrReplaceTempView("kpi3")

df = spark.sql("SELECT * FROM kpi3 ORDER BY `Delayed Flights` DESC LIMIT 5")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 3.1: Top 5 airlines, by total number of delays**
# MAGIC > ###### *Summary table (Databricks Visualizer)*

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 3.2: Top 5 airlines, by total number of delays**
# MAGIC > ###### *Bar chart (Databricks Visualizer)*

# COMMAND ----------

display(df)
