# Databricks notebook source
# MAGIC %md
# MAGIC ## KPI 1A
# MAGIC #### Total number of flights by airline on a monthly basis

# COMMAND ----------

# Query the tables loaded from Snowflake for the data needed for this KPI, read results into a dataframe

sql_str = "SELECT \
    a.AIRLINE AS AIRLINE_NAME \
    ,a.IATA_CODE AS AIRLINE_CODE \
    ,b.MONTH \
    ,b.YEAR \
    ,b.FLIGHT_COUNT \
FROM user_andrew_db.AIRLINES a \
LEFT JOIN ( \
  SELECT \
    AIRLINE \
    ,MONTH \
    ,YEAR \
    ,COUNT(1) AS FLIGHT_COUNT \
  FROM user_andrew_db.FLIGHTS \
  GROUP BY AIRLINE, MONTH, YEAR \
) b \
  ON a.IATA_CODE = b.AIRLINE \
ORDER BY a.AIRLINE, a.IATA_CODE, b.YEAR, b.MONTH"

df = sqlContext.sql(sql_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 1A.1: Total number of flights by airline on a monthly basis**
# MAGIC > ###### *Summary table (Databricks Visualizer)*

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 1A.2: Total number of flights by airline on a monthly basis**
# MAGIC > ###### *Line chart (Databricks Visualizer)*

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **KPI 1A.3: Total number of flights by airline on a monthly basis**
# MAGIC > ###### *Per-airline bar chart (matplotlib)*

# COMMAND ----------

import random
from matplotlib import pyplot as plt
import calendar
import pyspark

# Convert to a Pandas dataframe to use additional functionality (apply, groupby)
if type(df) == pyspark.sql.dataframe.DataFrame:
  df = df.toPandas()
# Create a new appended column of month + year for more obvious display values
df['YEAR & MONTH'] = df.apply(lambda row: calendar.month_abbr[int(row.MONTH)] + ' ' + str(row.YEAR), axis = 1)

# Create a grouped dataframe, one dataframe for each airline, to show separate plots
grouped_df = df.groupby('AIRLINE_NAME')
# Dataframe length will be used to set figure size & number of subplots on figure
len_gdf = len(grouped_df)

# Plot each airline's data
# fig, axes = plt.subplots(int(len_gdf / 2), 2, figsize=(30,10*int(len_gdf / 2)))
fig, axes = plt.subplots(len_gdf, figsize=(15,10*len_gdf))
i_ax = 0
for key, grp in grouped_df:
  # Create a random color for each graph, scaled to 80% to make colors less intense
  rand_color = (random.random()*.8, random.random()*.8, random.random()*.8)
  axes[i_ax].bar(grp['YEAR & MONTH'], grp['FLIGHT_COUNT'], color=[rand_color], label=key)
  axes[i_ax].set_title(key, fontsize=20)
  axes[i_ax].set_xlabel("Year & Month", fontsize=14, labelpad=20.0)
  axes[i_ax].set_ylabel("Flights", fontsize=16, labelpad=20.0)
  i_ax += 1

# Set figure title, centered over plot
# Find mid point of left and right x-positions on subplot
mid = (fig.subplotpars.right + fig.subplotpars.left)/2
fig.suptitle("Total number of flights by airline on a monthly basis", x=mid, y=1, horizontalalignment='center', fontsize=26)
fig.tight_layout(pad=6.5)
plt.show()
