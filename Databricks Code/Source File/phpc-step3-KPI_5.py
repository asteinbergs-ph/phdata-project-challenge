# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 5
# MAGIC #### Delay reasons by airport

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
    a.AIRPORT AS `Airport Name` \
    ,a.IATA_CODE AS `Airport Code` \
    ,SUM(AIR_SYSTEM_DELAY) AS `Air System Delay` \
    ,SUM(SECURITY_DELAY) AS `Security Delay` \
    ,SUM(AIRLINE_DELAY) AS `Airline Delay` \
    ,SUM(LATE_AIRCRAFT_DELAY) AS `Late Aircraft Delay` \
    ,SUM(WEATHER_DELAY) AS `Weather Delay` \
FROM user_andrew_db.AIRPORTS a \
LEFT JOIN ( \
    SELECT \
        ORIGIN_AIRPORT AS AIRPORT \
        ,CASE WHEN AIR_SYSTEM_DELAY > 0 THEN 1 ELSE 0 END AS AIR_SYSTEM_DELAY \
        ,CASE WHEN SECURITY_DELAY > 0 THEN 1 ELSE 0 END AS SECURITY_DELAY \
        ,CASE WHEN AIRLINE_DELAY > 0 THEN 1 ELSE 0 END AS AIRLINE_DELAY \
        ,CASE WHEN LATE_AIRCRAFT_DELAY > 0 THEN 1 ELSE 0 END AS LATE_AIRCRAFT_DELAY \
        ,CASE WHEN WEATHER_DELAY > 0 THEN 1 ELSE 0 END AS WEATHER_DELAY \
    FROM user_andrew_db.FLIGHTS \
    UNION ALL \
    SELECT \
        DESTINATION_AIRPORT AS AIRPORT \
        ,CASE WHEN AIR_SYSTEM_DELAY > 0 THEN 1 ELSE 0 END AS AIR_SYSTEM_DELAY \
        ,CASE WHEN SECURITY_DELAY > 0 THEN 1 ELSE 0 END AS SECURITY_DELAY \
        ,CASE WHEN AIRLINE_DELAY > 0 THEN 1 ELSE 0 END AS AIRLINE_DELAY \
        ,CASE WHEN LATE_AIRCRAFT_DELAY > 0 THEN 1 ELSE 0 END AS LATE_AIRCRAFT_DELAY \
        ,CASE WHEN WEATHER_DELAY > 0 THEN 1 ELSE 0 END AS WEATHER_DELAY \
    FROM user_andrew_db.FLIGHTS \
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
# MAGIC ##### **KPI 5.1: Delay reasons by airport**
# MAGIC > ###### *Summary record (Databricks Visualizer)*

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 5.2: Delay reasons by airport**
# MAGIC > ###### *Bar chart (Databricks Visualizer)*

# COMMAND ----------

display(filtered_df)
